#!/usr/bin/env python3
"""Quick test of enhanced scraping capabilities"""

import time
from selenium import webdriver
from selenium.webdriver.chrome.options import Options as ChromeOptions
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from cloudflare_helper import (
    is_cloudflare_challenge, enhanced_wait_for_page, 
    enhance_driver_stealth, better_data_extraction
)

def test_company(company_url="https://www.importyeti.com/company/apple"):
    """Test enhanced scraping on a single company"""
    
    print("🧪 Testing Enhanced ImportYeti Scraper")
    print("=" * 50)
    
    # Setup driver
    opts = ChromeOptions()
    opts.add_experimental_option("debuggerAddress", "127.0.0.1:9222")
    
    try:
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=opts)
        
        # Apply stealth enhancements
        enhance_driver_stealth(driver)
        
        print(f"📍 Testing URL: {company_url}")
        
        # Test enhanced page loading
        if enhanced_wait_for_page(driver, company_url):
            print("✅ Page loaded successfully")
            
            # Check for Cloudflare
            if is_cloudflare_challenge(driver):
                print("❌ Cloudflare challenge still present")
            else:
                print("✅ No Cloudflare challenge detected")
            
            # Test data extraction
            html = driver.page_source
            print(f"📄 Page HTML length: {len(html)} characters")
            
            # Look for data patterns
            data = better_data_extraction(html)
            print(f"🔍 Data extraction results: {data}")
            
            # Look for specific ImportYeti patterns
            if "importyeti" in html.lower():
                print("✅ On ImportYeti domain")
                
                # Look for data indicators
                indicators = ["port", "lane", "shipping", "trade", "map"]
                found = [ind for ind in indicators if ind in html.lower()]
                print(f"📊 Data indicators found: {found}")
                
                # Look for JavaScript data
                if "window.__NEXT_DATA__" in html:
                    print("✅ Next.js data found")
                if "map_table" in html:
                    print("✅ Map table data found")
                if "exit_ports" in html:
                    print("✅ Exit ports data found")
                if "entry_ports" in html:
                    print("✅ Entry ports data found")
                    
        else:
            print("❌ Failed to load page")
    
    except Exception as e:
        print(f"❌ Error: {e}")
    
    finally:
        try:
            driver.quit()
        except:
            pass

if __name__ == "__main__":
    test_company()
