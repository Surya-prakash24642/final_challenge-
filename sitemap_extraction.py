import requests
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup
import xml.etree.ElementTree as ET

def get_sitemap_or_links(website_url):
    sitemap_variants = [
        "sitemap.xml",
        "sitemap_index.xml",
        "sitemap1.xml",
        "wp-sitemap.xml"
    ]

    parsed = urlparse(website_url)
    base_url = f"{parsed.scheme}://{parsed.netloc}"

    # Try sitemap variants
    for variant in sitemap_variants:
        sitemap_url = urljoin(base_url, variant)
        try:
            response = requests.get(sitemap_url, timeout=10)
            if response.status_code == 200 and response.content.startswith(b"<?xml"):
                print(f"✅ Found sitemap: {sitemap_url}")
                return parse_sitemap(response.content)
        except requests.RequestException:
            continue

    print(f"❌ No sitemap found, falling back to <a> tag extraction from homepage...")
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
    return urls

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
            return list(links)
    except requests.RequestException:
        pass
    return []

# === Example usage ===
if __name__ == "__main__":
    company_url = "https://www.microsoft.com"  # Replace with your target URL
    urls = get_sitemap_or_links(company_url)
    print(f"\n🔗 Total URLs extracted: {len(urls)}")
    for url in urls[:10]:
        print("-", url)
