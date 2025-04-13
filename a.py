import requests
import pandas as pd
import time

def get_company_url(company_name, api_key, cse_id):
    url = "https://www.googleapis.com/customsearch/v1"
    params = {
        "key": api_key,
        "cx": cse_id,
        "q": company_name
    }
    
    response = requests.get(url, params=params)
    if response.status_code == 200:
        data = response.json()
        if 'items' in data:
            for item in data['items']:
                if 'link' in item:
                    return item['link']
        return "No result found"
    else:
        return f"Error: {response.status_code}"

# === Configuration ===
api_key = "AIzaSyCPr3u1vqFWYlQW_zeN65S7y3W4_QiFHXM"    # Replace this
cse_id = "32ffd6aedf0b34bf3"      # Replace this
input_file = "companies.txt"
output_file = "company_urls1212.xlsx"

# === Step 1: Read company names with UTF-8 ===
with open(input_file, 'r', encoding='utf-8') as file:
    company_names = [line.strip() for line in file if line.strip()]

# === Step 2: Get URLs for each company ===
results = []
for company in company_names:
    print(f"Searching for: {company}")
    url = get_company_url(company, api_key, cse_id)
    results.append({"Company": company, "URL": url})
    print(f"Found URL: {url}")
    time.sleep(2)  # Delay to avoid rate limiting

# === Step 3: Save to Excel ===
df = pd.DataFrame(results)
df.to_excel(output_file, index=False)

print(f"✅ URLs saved to {output_file}")
