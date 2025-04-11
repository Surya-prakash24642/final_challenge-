from duckduckgo_search import ddg

EXCLUDE_DOMAINS = ["linkedin.com", "crunchbase.com", "glassdoor.com", "facebook.com"]

def find_domain_duckduckgo(company_name, max_results=8):
    try:
        query = f"{company_name} official site"
        results = ddg(query, max_results=max_results)

        for result in results:
            url = result.get("href", "")
            title = result.get("title", "").lower()

            if any(ex in url for ex in EXCLUDE_DOMAINS):
                continue  # Skip known third-party sites

            # Prefer URLs with company name and "official", "home", or "site" in title
            if company_name.lower().split()[0] in url and ("official" in title or "home" in title or "site" in title):
                return url

        # Fallback to first non-excluded result
        for result in results:
            url = result.get("href", "")
            if not any(ex in url for ex in EXCLUDE_DOMAINS):
                return url

        return None
    except Exception as e:
        print(f"Error with DuckDuckGo for {company_name}: {e}")
        return None
