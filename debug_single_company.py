#!/usr/bin/env python3
"""
Debug script to test port/lane extraction with a single company.
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
    slugify_company, NetworkCapture, extract_top_info_from_any
)

def debug_single_company(company_name: str = "Kimberly-Clark"):
    """Debug the extraction process for a single company."""
    
    print(f"Debugging extraction for: {company_name}")
    print("=" * 60)
    
    # Initialize driver
    driver = init_driver_attach()
    
    try:
        # Find company candidates
        print("1. Finding company candidates...")
        cands = resolve_company_candidates(driver, company_name)
        print(f"   Found {len(cands)} candidates:")
        for i, url in enumerate(cands[:3], 1):
            score = score_candidate(company_name, url)
            print(f"   {i}. {url} (score: {score['total']:.2f})")
        
        if not cands:
            print("   No candidates found!")
            return
        
        # Test with the best candidate
        best_url = cands[0]
        print(f"\n2. Testing with best candidate: {best_url}")
        
        # Fetch data
        slug_hint = slugify_company(company_name)
        html, topinfo, cap = fetch_company_page_and_ports(driver, best_url, slug_hint=slug_hint)
        
        print(f"   HTML length: {len(html)} characters")
        print(f"   Network requests captured: {len(cap.bodies)}")
        
        # Show network request summary
        summary = cap.get_all_requests_summary()
        print(f"   Total requests: {summary['total_requests']}")
        print(f"   API calls: {summary['api_calls']}")
        print(f"   JSON payloads: {summary['json_payloads']}")
        
        if summary['sample_apis']:
            print(f"   Sample API calls:")
            for api in summary['sample_apis'][:5]:
                print(f"     - {api}")
        
        # Analyze captured JSON
        json_hits = cap.find_json_payloads()
        print(f"\n3. JSON payloads found: {len(json_hits)}")
        
        for i, (url, obj) in enumerate(json_hits[:8], 1):
            print(f"   JSON {i}: {url}")
            print(f"      Type: {type(obj)}")
            if isinstance(obj, dict):
                print(f"      Keys: {list(obj.keys())[:10]}")
                # Look for any keys that might contain data
                data_keys = [k for k in obj.keys() if any(term in k.lower() for term in ['data', 'ports', 'lanes', 'trade', 'shipping', 'import', 'export'])]
                if data_keys:
                    print(f"      Potential data keys: {data_keys}")
            elif isinstance(obj, list):
                print(f"      Length: {len(obj)}")
                if obj and isinstance(obj[0], dict):
                    print(f"      First item keys: {list(obj[0].keys())[:5]}")
        
        # Test extraction on each JSON payload
        print(f"\n4. Testing extraction on each JSON payload...")
        found_data = False
        for i, (url, obj) in enumerate(json_hits, 1):
            print(f"   Testing JSON {i}...")
            info = extract_top_info_from_any(obj, topn=5)
            if info["exit_ports"] or info["entry_ports"] or info["lanes"]:
                print(f"   ✓ Found data in JSON {i}!")
                print(f"      Exit ports: {len(info['exit_ports'])}")
                print(f"      Entry ports: {len(info['entry_ports'])}")
                print(f"      Lanes: {len(info['lanes'])}")
                if info["exit_ports"]:
                    print(f"      Sample exit port: {info['exit_ports'][0]}")
                if info["entry_ports"]:
                    print(f"      Sample entry port: {info['entry_ports'][0]}")
                if info["lanes"]:
                    print(f"      Sample lane: {info['lanes'][0]}")
                found_data = True
                break
            else:
                print(f"   ✗ No port/lane data found in JSON {i}")
        
        # Show final results
        print(f"\n5. Final extraction results:")
        print(f"   Exit ports: {len(topinfo['exit_ports'])}")
        print(f"   Entry ports: {len(topinfo['entry_ports'])}")
        print(f"   Lanes: {len(topinfo['lanes'])}")
        
        if not (topinfo["exit_ports"] or topinfo["entry_ports"] or topinfo["lanes"]):
            print("   ✗ No port/lane data extracted!")
            print("\n   Debugging suggestions:")
            print("   - Check if you're logged into ImportYeti")
            print("   - The page might need more time to load")
            print("   - The data might be in a different format")
            print("   - Check the captures/ directory for raw JSON")
            print("   - The API endpoints might be different than expected")
            print("   - Try manually navigating to the page and checking Network tab")
        else:
            print("   ✓ Successfully extracted port/lane data!")
        
        # Additional debugging info
        print(f"\n6. Additional debugging info:")
        print(f"   Captures saved: {len(list(Path('captures').glob(f'{slug_hint}-*.json')))} files")
        if summary['api_calls'] > 0:
            print(f"   API calls detected but no data found - might need different parsing")
        else:
            print(f"   No API calls detected - might need different triggers")
        
    finally:
        driver.quit()

def main():
    if len(sys.argv) > 1:
        company = sys.argv[1]
    else:
        company = "Kimberly-Clark"
    
    debug_single_company(company)

if __name__ == "__main__":
    main()
