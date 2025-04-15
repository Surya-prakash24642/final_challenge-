import requests
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup
import xml.etree.ElementTree as ET
import google.generativeai as genai
import time
from duckduckgo_search import DDGS

# === Configuration ===
GEMINI_API_KEY = "YOUR_GEMINI_API_KEY"  # Replace with your Gemini API key
genai.configure(api_key=GEMINI_API_KEY)

QUESTIONS = [
    "Website",
    "Description",
    "Software Classification",
    "Enterprise Grade Classification",
    "Industry",
    "Customers names list",
    "Employee Headcount #",
    "Investors",
    "Geography",
    "Parent company",
    "Address 1: Street 1",
    "Address 1: ZIP/Postal Code",
    "Address 1: City",
    "Address 1: Country/Region",
    "Finance",
    "Email",
    "Phone"
]

def get_final_url(url):
    try:
        response = requests.get(url, timeout=10, allow_redirects=True)
        if response.status_code == 200:
            return response.url
    except requests.RequestException:
        pass
    return url

def get_sitemap_or_links(website_url):
    sitemap_variants = [
        "sitemap.xml", "sitemap_index.xml", "sitemap1.xml", "wp-sitemap.xml"
    ]
    parsed = urlparse(website_url)
    base_url = f"{parsed.scheme}://{parsed.netloc}"

    for variant in sitemap_variants:
        sitemap_url = urljoin(base_url, variant)
        try:
            response = requests.get(sitemap_url, timeout=10)
            if response.status_code == 200 and response.content.startswith(b"<?xml"):
                return parse_sitemap(response.content)
        except requests.RequestException:
            continue

    return extract_links_from_homepage(base_url)

def parse_sitemap(xml_content):
    urls = []
    try:
        root = ET.fromstring(xml_content)
        ns = {"ns": "http://www.sitemaps.org/schemas/sitemap/0.9"}
        for url in root.findall("ns:url", ns):
            loc = url.find("ns:loc", ns)
            if loc is not None:
                urls.append(loc.text)
    except ET.ParseError:
        pass
    return urls[:5]

def extract_links_from_homepage(base_url):
    try:
        response = requests.get(base_url, timeout=10)
        if response.status_code == 200:
            soup = BeautifulSoup(response.text, 'html.parser')
            links = set()
            for a in soup.find_all('a', href=True):
                href = a['href']
                full_url = urljoin(base_url, href)
                if urlparse(full_url).netloc == urlparse(base_url).netloc:
                    links.add(full_url)
            return list(links)[:5]
    except requests.RequestException:
        pass
    return []

def fetch_content(urls):
    content = ""
    for url in urls:
        try:
            response = requests.get(url, timeout=10)
            if response.status_code == 200:
                soup = BeautifulSoup(response.text, 'html.parser')
                text = soup.get_text(separator=' ', strip=True)
                content += f"\n\n--- Content from {url} ---\n{text[:3000]}"
                time.sleep(1)
        except:
            continue
    return content

def ask_gemini(content):
    model = genai.GenerativeModel("gemini-pro")
    prompt = f"""Based on the following website content, answer these questions about the company:

{', '.join(QUESTIONS)}

Here is the content:
{content}

Provide the answers in the same order as the questions, separated clearly with bullet points or numbering."""
    
    try:
        response = model.generate_content(prompt)
        return response.text
    except Exception as e:
        return f"Error: {e}"

def fill_missing_answers(answers_dict, company_name):
    for q, a in answers_dict.items():
        if not a or "not available" in a.lower():
            with DDGS() as ddgs:
                print(f"🌐 Searching online for: {q}...")
                query = f"{company_name} {q}"
                results = ddgs.text(query, max_results=1)
                if results:
                    answers_dict[q] = results[0]['body']
    return answers_dict

def parse_answers(raw_text):
    result = {}
    lines = raw_text.strip().splitlines()
    idx = 0
    for q in QUESTIONS:
        if idx < len(lines):
            line = lines[idx].strip(" -•1234567890.").strip()
            result[q] = line
            idx += 1
        else:
            result[q] = ""
    return result

def print_answers(answers_dict):
    print("\n📋 Extracted Company Information:\n")
    for q in QUESTIONS:
        print(f"{q}: {answers_dict.get(q, '')}")

# === MAIN ===
if __name__ == "__main__":
    with open("urls.txt", "r") as file:
        company_url = file.readline().strip()

    final_url = get_final_url(company_url)
    print(f"\n🔗 Cleaned URL: {final_url}")

    links = get_sitemap_or_links(final_url)
    print(f"🔍 Found {len(links)} pages to scrape")

    content = fetch_content(links)
    print("🧠 Asking Gemini AI...")
    raw_answers = ask_gemini(content)

    answers_dict = parse_answers(raw_answers)
    company_name = urlparse(final_url).netloc.replace("www.", "").split(".")[0]

    print("\n🔎 Filling missing answers using online search...")
    completed_answers = fill_missing_answers(answers_dict, company_name)

    print_answers(completed_answers)
