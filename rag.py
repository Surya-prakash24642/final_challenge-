import pandas as pd
import time
from playwright.sync_api import sync_playwright
from supabase import create_client
from typing import List
import google.generativeai as genai

# Supabase Config
SUPABASE_URL = "https://edekginkagyjntqetfej.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImVkZWtnaW5rYWd5am50cWV0ZmVqIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NDQ3MDk2NTMsImV4cCI6MjA2MDI4NTY1M30.4mQBtWI4QnPSiclCjCWQBlI69Mb98AbxxYrdNo01cSc"
# Gemini Config
GEMINI_API_KEY = "AIzaSyAgE7ofFt6EozMFBqf4odwgauMdIemOI30"


genai.configure(api_key=GEMINI_API_KEY)

# Questions and Titles
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

# Initialize Supabase client
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

def scrape_website(playwright, url):
    """Scrape content from a single website."""
    browser = playwright.chromium.launch(headless=True)
    page = browser.new_page()
    try:
        page.goto(url, timeout=60000)
        time.sleep(3)
        content = page.content()
    except Exception as e:
        print(f"Error scraping {url}: {e}")
        content = ""
    finally:
        browser.close()
    return content

def chunk_content(content, chunk_size=1000):
    """Split content into manageable chunks."""
    return [content[i:i+chunk_size] for i in range(0, len(content), chunk_size)]

def store_in_supabase(company_name, chunks):
    for chunk in chunks:
        # Check if the chunk already exists in the database
        existing_entry = supabase.table('vectors').select('*').eq('content_chunk', chunk).execute()
        
        # If the chunk doesn't already exist, insert it
        if len(existing_entry.data) == 0:
            print(f"Storing chunk for {company_name}")
            supabase.table('vectors').insert({
                'company_name': company_name,
                'content_chunk': chunk
            }).execute()
        else:
            print(f"Chunk already exists for {company_name}, skipping...")
            
    print(f"Finished storing chunks for {company_name}")


def ask_gemini(content_chunk):
    """Ask Gemini LLM to answer the questions based on the content chunk."""
    prompt = f"""
    Extract factual information based on the following questions. If data is unavailable, respond 'None'.
    Questions:
    {', '.join(question_to_title.keys())}

    Content:
    {content_chunk}
    """
    try:
        model = genai.GenerativeModel("gemini-2.0-flash")
        response = model.generate_content(prompt)
        return response.text
    except Exception as e:
        print(f"Error querying Gemini: {e}")
        return ""

def parse_answers(raw_text):
    """Parse answers from Gemini's response."""
    answers = {title: "None" for title in question_to_title.values()}
    lines = raw_text.strip().split("\n")
    for line in lines:
        for question, title in question_to_title.items():
            if line.startswith(question):
                answers[title] = line.split(": ", 1)[-1].strip()
    return answers

def save_to_excel(results, filename="company_data.xlsx"):
    """Save results to an Excel file."""
    df = pd.DataFrame(results)
    column_order = list(question_to_title.values()) + ["Company Name"]
    df = df[column_order]
    df.to_excel(filename, index=False)
    print(f"Results saved to {filename}")

# Main Script
def main():
    # Read input URLs
    input_df = pd.read_excel("Book1.xlsx")
    results = []
    
    with sync_playwright() as playwright:
        for _, row in input_df.iterrows():
            company_name = row["Company"]
            company_url = row["URL"]
            
            print(f"Processing: {company_name} ({company_url})")
            
            # Scrape website
            content = scrape_website(playwright, company_url)
            
            # Chunk and store in Supabase
            chunks = chunk_content(content)
            store_in_supabase(company_name, chunks)
            
            # Query Gemini for each chunk
            aggregated_answers = {title: "None" for title in question_to_title.values()}
            for chunk in chunks:
                raw_response = ask_gemini(chunk)
                parsed_answers = parse_answers(raw_response)
                
                for key, value in parsed_answers.items():
                    if value != "None":
                        aggregated_answers[key] = value
            
            # Add company name and append results
            aggregated_answers["Company Name"] = company_name
            results.append(aggregated_answers)
    
    # Save results to Excel
    save_to_excel(results)

if __name__ == "__main__":
    main()
