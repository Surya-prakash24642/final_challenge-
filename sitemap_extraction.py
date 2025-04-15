import time
import re
import pandas as pd
import google.generativeai as genai
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from urllib.parse import urljoin, urlparse
import xml.etree.ElementTree as ET
import requests
from supabase import create_client, Client
import json
from playwright.sync_api import sync_playwright

with sync_playwright() as p:
    browser = p.chromium.launch(headless=True, args=["--enable-unsafe-swiftshader","--disable-logging", "--disable-webgl"])
    page = browser.new_page()
    page.goto("https://www.standardlife.de/")
    #print(page.content())
    browser.close()


# === Supabase Setup ===
SUPABASE_URL = "https://edekginkagyjntqetfej.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImVkZWtnaW5rYWd5am50cWV0ZmVqIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NDQ3MDk2NTMsImV4cCI6MjA2MDI4NTY1M30.4mQBtWI4QnPSiclCjCWQBlI69Mb98AbxxYrdNo01cSc"
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

def get_company_data_from_supabase(company_name):
    """Fetch company data from Supabase if it exists."""
    try:
        response = supabase.table("companies").select("*").eq("company_name", company_name).execute()
        if len(response.data) > 0:
            return response.data[0]
    except Exception as e:
        print(f"Supabase fetch error: {e}")
    return None

def save_company_data_to_supabase(company_data):
    """Save company data to Supabase."""
    try:
        response = supabase.table("companies").upsert(company_data).execute()
        if response.status_code == 200:
            print(f"Successfully saved data for {company_data['company_name']}")
        else:
            print(f"Failed to save data for {company_data['company_name']}: {response}")
    except Exception as e:
        print(f"Supabase save error: {e}")
# === Configuration ===

GEMINI_API_KEY = "AIzaSyDk5wK3w7PoWLKgXkKmFZDVqdWVYgOh05w"
SERPER_API_KEY = "ae5b05ac0c4c9f9494db99a743593d3408d386b1"
SERPER_API_URL = "https://google.serper.dev/search"

genai.configure(api_key=GEMINI_API_KEY)

question_to_title = {
    "What is the website of the company?": "Website",
    "Provide a short description of the company": "Description",
    "What software category does this company fall under?": "Software Classification",
    "Is the software enterprise-grade or SMB-focused?": "Enterprise Grade Classification",
    "What industry does the company belong to?": "Industry",
    "List a few of the company's notable customers": "Customers",
    "What is the employee headcount?": "Employee Count",
    "Who are the known investors of the company?": "Investors",
    "What geographic regions does the company serve?": "Geography",
    "Does the company have a parent company? If yes, name it": "Parent Company",
    "What is the street address of the company?": "Street Address",
    "What is the postal code?": "Postal Code",
    "What city is the company located in?": "City",
    "What country is the company based in?": "Country",
    "Provide financial information (revenue, funding) if available": "Financial Info",
    "What is the contact email of the company?": "Email",
    "What is the contact phone number of the company?": "Phone"
}

QUESTIONS = list(question_to_title.keys())

# === Selenium Setup ===
def init_driver():
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")
    
    driver = webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=chrome_options
    )
    return driver

# === Core Functions ===

def get_sitemap_or_links(driver, website_url):
    sitemap_variants = ["sitemap.xml", "sitemap_index.xml", "wp-sitemap.xml"]
    parsed = urlparse(website_url)
    base_url = f"{parsed.scheme}://{parsed.netloc}"

    for variant in sitemap_variants:
        sitemap_url = urljoin(base_url, variant)
        driver.get(sitemap_url)
        time.sleep(1)
        if "xml" in driver.page_source[:100]:
            return parse_sitemap(driver.page_source)

    return extract_links_from_homepage(driver, base_url)

def parse_sitemap(xml_content):
    urls = []
    try:
        root = ET.fromstring(xml_content)
        for url in root.findall("{http://www.sitemaps.org/schemas/sitemap/0.9}url"):
            loc = url.find("{http://www.sitemaps.org/schemas/sitemap/0.9}loc")
            if loc is not None:
                urls.append(loc.text)
    except ET.ParseError:
        pass
    return urls[:5]

def extract_links_from_homepage(driver, base_url):
    try:
        print(f"Scanning {base_url} for links...")
        driver.get(base_url)
        
        try:
            WebDriverWait(driver, 10).until(
                lambda d: d.execute_script("return document.readyState") == "complete"
            )
        except TimeoutException:
            print("Page load timed out, proceeding with current state")

        driver.execute_script("window.scrollTo(0, document.body.scrollHeight)")
        time.sleep(2)

        links = set()

        # Standard link extraction
        for a in driver.find_elements(By.TAG_NAME, "a"):
            href = a.get_attribute("href")
            if href and urlparse(href).netloc == urlparse(base_url).netloc:
                links.add(href)

        # Additional methods for SPAs
        for element in driver.find_elements(By.XPATH, "//*[@onclick]"):
            onclick = element.get_attribute("onclick")
            if any(js in onclick for js in ["location.href", "window.open", "navigate"]):
                try:
                    url = onclick.split("'")[1] if "'" in onclick else onclick.split('"')[1]
                    if url.startswith(('http', '/')):
                        links.add(urljoin(base_url, url))
                except IndexError:
                    pass

        # Common navigation patterns
        for selector in ['.nav a', '.menu a', 'header a', '[role="navigation"] a']:
            for el in driver.find_elements(By.CSS_SELECTOR, selector):
                href = el.get_attribute("href")
                if href:
                    links.add(urljoin(base_url, href))

        # SPA routes
        spa_routes = driver.execute_script("""
            const routes = new Set();
            document.querySelectorAll('[routerlink], [data-route], [data-path]').forEach(el => {
                routes.add(el.getAttribute('routerlink') || 
                         el.getAttribute('data-route') || 
                         el.getAttribute('data-path'));
            });
            return Array.from(routes);
        """)
        
        for route in spa_routes:
            if route and not route.startswith('#'):
                links.add(urljoin(base_url, route))

        if not links:
            common_routes = ['/about', '/team', '/contact', '/services']
            links.update(urljoin(base_url, route) for route in common_routes)

        print(f"Found {len(links)} potential links")
        return list(links)[:15]

    except Exception as e:
        print(f"Link extraction failed: {e}")
        return [base_url]

def get_final_url(driver, url):
    try:
        driver.get(url)
        time.sleep(2)
        return driver.current_url
    except Exception as e:
        print(f"URL resolution error: {e}")
        return url

def fetch_content(driver, url):
    try:
        print(f"Fetching content from: {url}")
        driver.get(url)

        try:
            WebDriverWait(driver, 10).until(
                lambda d: d.execute_script("return document.readyState") == "complete"
            )
        except TimeoutException:
            print("Page load timed out, proceeding with current state")

        driver.execute_script("""
            const removals = ['header', 'footer', 'nav', 'iframe', 'script', 'style'];
            removals.forEach(selector => {
                document.querySelectorAll(selector).forEach(el => el.remove());
            });
        """)

        content = driver.execute_script("""
            const spaRoots = ['app-root', '#root', '.app-container', 'main', 'article'];
            for (const selector of spaRoots) {
                const el = document.querySelector(selector);
                if (el?.innerText?.trim()) return el.innerText;
            }
            return document.body.innerText;
        """)
        return content.strip()

    except Exception as e:
        print(f"Failed to fetch content from {url}: {e}")
        return None

def save_to_supabase(company_name, final_url, page_url, raw_content):
    try:
        # Check if the record already exists
        existing_record = supabase.table("scraped_data").select("id").eq("company_name", company_name).execute()

        if existing_record.data:  # If data exists, skip insertion
            print(f"Data for {company_name} already exists. Skipping insertion.")
            return

        # Insert data into the Supabase table
        response = supabase.table("scraped_data").insert({
            "company_name": company_name,
            "final_url": final_url,
            "page_url": page_url,
            "raw_content": raw_content,
        }).execute()

        # Check for errors in the response
        if response.error:
            print(f"Error inserting data for {company_name}: {response.error}")
        else:
            print(f"Data for {company_name} successfully inserted.")

    except Exception as e:
        print(f"Supabase insert error: {e}")


def ask_gemini(content):
    model = genai.GenerativeModel("gemini-1.5-pro")
    
    prompt = f"""Extract ONLY factual information about the company from this content.
Answer in the EXACT order of these questions. Use "None" if unknown:

{', '.join(QUESTIONS)}

RULES:
1. IGNORE irrelevant content
2. For addresses/contacts, return ONLY official company data
3. Be concise and factual

CONTENT:
{content}"""

    try:
        response = model.generate_content(prompt)
        return response.text
    except Exception as e:
        return f"Error: {e}"

def parse_answers(raw_text):
    result = {}
    lines = [line.strip() for line in raw_text.strip().splitlines() if line.strip()]
    
    for i, q in enumerate(QUESTIONS):
        title = question_to_title[q]
        if i < len(lines):
            # Directly store the answer without including the question
            answer = lines[i].split(": ", 1)[-1].strip() if ": " in lines[i] else lines[i].strip()
            
            if not answer or answer.lower() in ["n/a", "unknown", "not available"]:
                answer = "None"
            
            # Refined parsing for specific fields
            if title == 'Email':
                answer = extract_first_email(answer)
            elif title == 'Phone':
                answer = extract_phone_numbers(answer)
            elif title == 'Postal Code':
                answer = extract_postal_code(answer)
            elif title == 'City':
                answer = extract_city(answer)
                
            result[title] = answer
        else:
            result[title] = "None"
    return result

def extract_city(text):
    """
    Extracts the city name from the given text.
    """
    # Regex to match the word 'headquartered in' followed by the city and country.
    city_pattern = r'\b(?:headquartered|based)\s+in\s+([A-Za-z\s]+),\s+[A-Za-z\s]+(?:[,.]|$)'
    
    match = re.search(city_pattern, text, re.IGNORECASE)
    return match.group(1).strip() if match else "None"


def extract_first_email(text):
    """
    Extracts the first valid email address from the given text.
    """
    email_pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b'
    match = re.search(email_pattern, text)
    return match.group(0) if match else "None"

def extract_phone_numbers(text):
    """
    Extracts all valid phone numbers from the given text.
    """
    # Regex to match phone numbers
    phone_pattern = r'\+?\d{1,3}[\s.-]?\(?\d{1,4}\)?[\s.-]?\d{1,4}[\s.-]?\d{1,4}(?:[\s.-]\d{1,4})?'
    # Find all matches
    matches = re.findall(phone_pattern, text)
    # Return the list of phone numbers
    return matches

def extract_postal_code(text):
    postal_patterns = [
        r'\b(?:postal\s*code|zip\s*code|postcode|zip)\s*[:\-]?\s*(\d{3,10}[A-Za-z]{0,2}|\w{3,10})\b',
        r'\b(\d{5}(-\d{4})?)\b',  # US zip code (12345 or 12345-6789)
        r'\b([A-Za-z0-9]{3,10}[ ]?[A-Za-z0-9]{0,10})\b',  # Alphanumeric codes (UK, Canada)
        r'\b(\d{4,6})\b'  # Generic 4-6 digits postal codes (Germany, India)
    ]
    
    for pattern in postal_patterns:
        match = re.search(pattern, text)
        if match:
            return match.group(1)
    
    return "None"

def fill_missing_answers(answers_dict, company_name):
    headers = {
        "X-API-KEY": SERPER_API_KEY,
        "Content-Type": "application/json"
    }
    
    for question, answer in answers_dict.items():
        if answer == "None":
            try:
                query = f"{company_name} {question}"
                payload = {"q": query, "num": 1}
                response = requests.post(SERPER_API_URL, headers=headers, json=payload)

                if response.status_code == 200:
                    data = response.json()
                    if "organic" in data and data["organic"]:
                        snippet = data["organic"][0].get("snippet", "").strip()
                        if snippet:
                            answers_dict[question] = snippet
            except Exception as e:
                print(f"Error fetching from Serper.dev: {e}")
    
    return answers_dict

def save_to_excel(all_results, filename="company_data1.xlsx"):
    column_order = [
        'Company Name',
        'Website',
        'Description',
        'Industry',
        'Software Classification',
        'Enterprise Grade Classification',
        'Geography',
        'Street Address',
        'City',
        'Postal Code',
        'Country',
        'Phone',
        'Email',
        'Employee Count',
        'Customers',
        'Investors',
        'Parent Company',
        'Financial Info'
    ]
    
    df = pd.DataFrame(all_results)
    
    for col in column_order:
        if col not in df.columns:
            df[col] = "None"
    
    df = df[column_order]
    df = df.map(lambda x: "None" if pd.isna(x) or str(x).strip() == "" else x)
    
    df.to_excel(filename, index=False)
    print(f"Results saved to {filename} with {len(df)} records")

# === Main Execution ===

if __name__ == "__main__":
    driver = None
    all_results = []  # List to store the results for all companies
    try:
        input_df = pd.read_excel('Book1.xlsx')  # Load the input Excel file
        driver = init_driver()

        for index, row in input_df.iterrows():
            company_name = row['Company']  # Get the company name from the Excel row
            company_url = row['URL']  # Get the company URL from the Excel row

            print(f"\nProcessing {company_name}...")

            # Fetch the final URL (handles redirects if needed)
            final_url = get_final_url(driver, company_url)

            # Fetch the content of the company page (main content)
            raw_content = fetch_content(driver, final_url)

            if raw_content:
                # Fetch structured answers using Gemini (AI-based responses)
                gemini_response = ask_gemini(raw_content)

                # Parse the raw answers into a structured format
                answers = parse_answers(gemini_response)

                # Fill in missing answers using external sources (Serper API)
                filled_answers = fill_missing_answers(answers, company_name)

                # Collect the result
                result = {'Company Name': company_name, 'Website': company_url}
                result.update(filled_answers)  # Add all other parsed answers
                all_results.append(result)

                # Save data to Supabase (pass the correct arguments individually)
                save_to_supabase(company_name, final_url, company_url, raw_content)

            time.sleep(2)

        # Once all companies are processed, save the results to an Excel file
        save_to_excel(all_results)

    except Exception as e:
        print(f"Error: {str(e)}")
    finally:
        if driver:
            driver.quit()
        print("Processing complete")
