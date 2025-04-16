# final_script.py
import asyncio
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup
import pandas as pd
import re
import os
from urllib.parse import urlparse, urljoin, quote
import time
from supabase import create_client
import google.generativeai as genai
from tqdm import tqdm
import logging
import json
from fake_useragent import UserAgent
from concurrent.futures import ThreadPoolExecutor

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configuration
GEMINI_API_KEY = "AIzaSyBivI06bNv07KQi2CO74cr9DCwLnmdR13g"
SUPABASE_URL = "https://edekginkagyjntqetfej.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImVkZWtnaW5rYWd5am50cWV0ZmVqIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NDQ3MDk2NTMsImV4cCI6MjA2MDI4NTY1M30.4mQBtWI4QnPSiclCjCWQBlI69Mb98AbxxYrdNo01cSc"
GOOGLE_CSE_API_KEY = "AIzaSyBoidP9gH7eUACzPnlKckRRFH0gIjyHcPQ"
GOOGLE_CSE_ID = "97fabb6b99b4c4998"

ua = UserAgent()
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
genai.configure(api_key=GEMINI_API_KEY)
model = genai.GenerativeModel('gemini-2.0-flash')

# Rate limit map
domain_last_request = {}
REQUEST_DELAY = 1.0

async def async_scrape_url(url):
    try:
        domain = urlparse(url).netloc
        now = time.time()
        if domain in domain_last_request and (now - domain_last_request[domain] < REQUEST_DELAY):
            await asyncio.sleep(REQUEST_DELAY - (now - domain_last_request[domain]))

        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context(user_agent=ua.random)
            page = await context.new_page()
            await page.goto(url, timeout=30000)
            content = await page.content()
            await browser.close()

            domain_last_request[domain] = time.time()

            soup = BeautifulSoup(content, 'html.parser')
            for tag in soup(["script", "style"]):
                tag.extract()

            text = ' '.join(chunk.strip() for chunk in soup.get_text().splitlines() if chunk.strip())
            return {"url": url, "content": text[:100000]}
    except Exception as e:
        logger.error(f"Playwright scraping failed for {url}: {e}")
        return None

def scrape_url(url):
    try:
        return asyncio.run(async_scrape_url(url))
    except RuntimeError:
        import nest_asyncio
        nest_asyncio.apply()
        return asyncio.run(async_scrape_url(url))

def get_important_pages(company_name, website_url):
    if not website_url.startswith("http"):
        website_url = "https://" + website_url
    base_domain = urlparse(website_url).netloc
    paths = ["", "about", "contact", "team", "investors", "products", "services", "careers"]
    urls = [urljoin(website_url, path) for path in paths]
    valid_urls = []
    with ThreadPoolExecutor(max_workers=5) as executor:
        results = list(executor.map(scrape_url, urls))
    for res in results:
        if res and res.get("content"):
            valid_urls.append(res["url"])
    return valid_urls

def google_cse_search(company_name, fields):
    base_url = "https://www.googleapis.com/customsearch/v1"
    query_map = {
        "Geography": "{} headquarters location",
        "Employee Count": "{} number of employees",
        "Investors": "{} investors or funding",
        "Customers": "{} notable clients",
        "Description": "{} company overview",
        "Contact": "{} contact email phone",
        "Industry": "{} industry sector",
        "Software Classification": "{} software type",
        "Enterprise Grade Classification": "{} smb enterprise classification",
        "Financial Info": "{} revenue funding"
    }
    results = {}
    for field in fields:
        query = query_map.get(field, f"{company_name} {field}").format(company_name)
        logger.info(f"Searching CSE: {query}")
        params = {
            "key": GOOGLE_CSE_API_KEY,
            "cx": GOOGLE_CSE_ID,
            "q": query,
            "num": 3
        }
        try:
            time.sleep(1)
            response = requests.get(base_url, params=params)
            items = response.json().get("items", [])
            results[field] = [item["link"] for item in items]
        except Exception as e:
            logger.error(f"CSE error for {field}: {e}")
            results[field] = []
    return results

def extract_gemini_field(company_name, field, content):
    prompt = f"""
    Extract the {field} for {company_name} from the following text.
    Be brief and specific. If unknown, reply only with 'Not found'.
    Text:
    {content[:5000]}
    """
    try:
        response = model.generate_content(prompt)
        result = response.text.strip()
        if "not found" in result.lower():
            return "Not found"
        return result
    except Exception as e:
        logger.error(f"Gemini error for {field}: {e}")
        return "Not found"

def extract_company_info(company_name, urls):
    all_content = ""
    for url in urls:
        scraped = scrape_url(url)
        if scraped:
            all_content += scraped["content"] + "\n"
    fields = [
        "Description", "Software Classification", "Enterprise Grade Classification", "Industry",
        "Customers", "Employee Count", "Investors", "Geography", "Contact",
        "Financial Info"
    ]
    result = {"Company Name": company_name, "Website": urls[0] if urls else "Not found"}
    for field in fields:
        result[field] = extract_gemini_field(company_name, field, all_content)
    return result

def process_company(company_name, website_url):
    logger.info(f"Processing {company_name}")
    try:
        important_pages = get_important_pages(company_name, website_url)
        info = extract_company_info(company_name, important_pages)
        supabase.table("company_information").insert(info).execute()
        return info
    except Exception as e:
        logger.error(f"Failed to process {company_name}: {e}")
        return {"Company Name": company_name, "Error": str(e)}

def main(input_file):
    df = pd.read_excel(input_file)
    results = []
    for _, row in tqdm(df.iterrows(), total=len(df)):
        company_name = row["Company Name"]
        website_url = row["Website"]
        results.append(process_company(company_name, website_url))
    output_df = pd.DataFrame(results)
    output_df.to_excel("company_information_results_gpt.xlsx", index=False)

if __name__ == "__main__":
    print("Starting company data extraction...")
    input_path = input("Enter path to Excel file with 'Company Name' and 'Website' columns: ")
    main(input_path)
