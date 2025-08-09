#!/usr/bin/env python3
"""
Script to analyze network requests and find the actual data endpoints.
"""

import os
import sys
import json
import time
from pathlib import Path

# Import the main script functions
from BCOdata_cdp import (
    init_driver_attach, resolve_company_candidates, 
    fetch_company_page_and_ports, score_candidate,
    slugify_company, NetworkCapture
)

def analyze_network_requests(company_name: str = "Kimberly-Clark"):
    """Analyze all network requests to find data endpoints."""
    
    print(f"Analyzing network requests for: {company_name}")
    print("=" * 60)
    
    # Initialize driver
    driver = init_driver_attach()
    
    try:
        # Find company candidates
        print("1. Finding company candidates...")
        cands = resolve_company_candidates(driver, company_name)
        if not cands:
            print("   No candidates found!")
            return
        
        best_url = cands[0]
        print(f"2. Analyzing network for: {best_url}")
        
        # Navigate to the page
        driver.get(best_url)
        time.sleep(5)  # Wait for initial load
        
        # Create capture object
        cap = NetworkCapture(driver)
        
        # Let initial requests fire
        print("3. Capturing initial requests...")
        cap.collect_until_idle(idle_sec=2, total_timeout=10)
        
        # Get summary of all requests
        summary = cap.get_all_requests_summary()
        print(f"   Total requests: {summary['total_requests']}")
        print(f"   API calls: {summary['api_calls']}")
        
        # Show all API calls
        api_calls = [req for req in cap.all_requests if any(api in req["url"] for api in ["/api/", "/graphql", "/data/"])]
        print(f"\n4. All API calls found:")
        for i, req in enumerate(api_calls, 1):
            print(f"   {i}. {req['url']}")
        
        # Show all requests with potential data patterns
        data_patterns = ["port", "lane", "trade", "shipping", "import", "export", "data", "company", "supplier"]
        data_requests = []
        for req in cap.all_requests:
            url = req["url"].lower()
            if any(pattern in url for pattern in data_patterns):
                data_requests.append(req)
        
        print(f"\n5. Requests with data patterns:")
        for i, req in enumerate(data_requests[:10], 1):
            print(f"   {i}. {req['url']}")
        
        # Try to trigger more data loading
        print(f"\n6. Triggering additional data loading...")
        
        # Scroll and click more aggressively
        for i in range(5):
            driver.execute_script("window.scrollBy(0, Math.round(window.innerHeight*0.8));")
            time.sleep(1)
        
        # Look for and click on any data-related elements
        from selenium.webdriver.common.by import By
        from selenium.webdriver.common.action_chains import ActionChains
        
        # Click on any elements that might trigger data loading
        triggers = ["port", "lane", "trade", "shipping", "data", "map", "chart", "graph"]
        for trigger in triggers:
            try:
                elements = driver.find_elements(By.XPATH, f"//*[contains(translate(.,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'{trigger}')]")
                for el in elements[:3]:
                    try:
                        driver.execute_script("arguments[0].scrollIntoView({block:'center'});", el)
                        ActionChains(driver).move_to_element(el).pause(0.5).click(el).perform()
                        time.sleep(1)
                    except:
                        pass
            except:
                pass
        
        # Capture additional requests
        print("7. Capturing additional requests...")
        cap.collect_until_idle(idle_sec=3, total_timeout=15)
        
        # Updated summary
        summary = cap.get_all_requests_summary()
        print(f"   Updated total requests: {summary['total_requests']}")
        print(f"   Updated API calls: {summary['api_calls']}")
        
        # Show new API calls
        api_calls = [req for req in cap.all_requests if any(api in req["url"] for api in ["/api/", "/graphql", "/data/"])]
        print(f"\n8. All API calls (after triggers):")
        for i, req in enumerate(api_calls, 1):
            print(f"   {i}. {req['url']}")
        
        # Analyze JSON payloads
        json_hits = cap.find_json_payloads()
        print(f"\n9. JSON payloads found: {len(json_hits)}")
        
        for i, (url, obj) in enumerate(json_hits, 1):
            print(f"   JSON {i}: {url}")
            if isinstance(obj, dict):
                print(f"      Keys: {list(obj.keys())}")
                # Look for any data-related keys
                data_keys = [k for k in obj.keys() if any(term in k.lower() for term in ['data', 'ports', 'lanes', 'trade', 'shipping', 'import', 'export', 'company', 'supplier'])]
                if data_keys:
                    print(f"      Data-related keys: {data_keys}")
            elif isinstance(obj, list) and obj:
                print(f"      List with {len(obj)} items")
                if isinstance(obj[0], dict):
                    print(f"      First item keys: {list(obj[0].keys())}")
        
        # Save detailed analysis
        analysis_file = f"network_analysis_{slugify_company(company_name)}.json"
        analysis_data = {
            "company": company_name,
            "url": best_url,
            "total_requests": summary['total_requests'],
            "api_calls": summary['api_calls'],
            "json_payloads": len(json_hits),
            "all_requests": cap.all_requests,
            "json_payloads_detail": [(url, type(obj).__name__, list(obj.keys()) if isinstance(obj, dict) else len(obj) if isinstance(obj, list) else "other") for url, obj in json_hits]
        }
        
        with open(analysis_file, "w", encoding="utf-8") as f:
            json.dump(analysis_data, f, indent=2, ensure_ascii=False)
        
        print(f"\n10. Analysis saved to: {analysis_file}")
        
    finally:
        driver.quit()

def main():
    if len(sys.argv) > 1:
        company = sys.argv[1]
    else:
        company = "Kimberly-Clark"
    
    analyze_network_requests(company)

if __name__ == "__main__":
    main()
