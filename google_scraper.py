import requests
import pandas as pd
import time
from urllib.parse import urlparse

def get_company_url(company_name, api_key, cse_id):
    url = "https://www.googleapis.com/customsearch/v1"
    params = {
        "key": api_key,
        "cx": cse_id,
        "q": f"{company_name} official site"
    }

    response = requests.get(url, params=params)
    if response.status_code == 200:
        data = response.json()
        if 'items' in data:
            for item in data['items']:
                if 'link' in item:
                    domain = urlparse(item['link']).netloc.lower()
                    # Filter out known non-official domains
                    if any(bad in domain for bad in ['github', 'linkedin', 'facebook', 'crunchbase', 'youtube']):
                        continue
                    # Accept domains with typical company TLDs
                    if domain.endswith(('.com', '.org', '.net', '.de', '.co.uk', '.io', '.biz')):
                        return item['link']
        return "No official site found"
    else:
        return f"Error: {response.status_code}"

# === Configuration ===
api_key = "AIzaSyCPr3u1vqFWYlQW_zeN65S7y3W4_QiFHXM"  # Replace with your API key
cse_id = "32ffd6aedf0b34bf3"                        # Replace with your CSE ID
input_file = "companies.txt"
output_file = "company_urls123.xlsx"

# === Step 1: Read company names from text file with proper encoding ===
with open(input_file, 'r', encoding='utf-8') as file:
    company_names = [line.strip() for line in file if line.strip()]

# === Step 2: Get URLs for each company ===
results = []
for company in company_names:
    print(f"🔍 Searching for: {company}")
    url = get_company_url(company, api_key, cse_id)
    results.append({"Company": company, "URL": url})
    time.sleep(2)  # delay to respect rate limits

# === Step 3: Save to Excel ===
df = pd.DataFrame(results)
df.to_excel(output_file, index=False)

print(f"\n✅ URLs saved to {output_file}")
