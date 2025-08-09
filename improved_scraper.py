# improved_scraper.py
# Enhanced ImportYeti scraper with advanced Cloudflare bypass and data extraction

import os
import sys
import time
import json
import random
from pathlib import Path
from urllib.parse import urlparse

# Import our bypass utilities
from bypass_cloudflare import (
    setup_cloudflare_bypass, smart_page_load, is_cloudflare_page,
    wait_for_cloudflare_bypass, human_like_behavior, 
    get_enhanced_extraction_patterns
)

from selenium import webdriver
from selenium.webdriver.chrome.options import Options as ChromeOptions
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

class EnhancedImportYetiScraper:
    def __init__(self, debugger_addr="127.0.0.1:9222"):
        self.debugger_addr = debugger_addr
        self.driver = None
        self.extraction_patterns = get_enhanced_extraction_patterns()
        
    def init_driver(self):
        """Initialize driver with enhanced stealth and bypass capabilities"""
        opts = ChromeOptions()
        
        # Attach to existing Chrome with debugging
        opts.add_experimental_option("debuggerAddress", self.debugger_addr)
        
        # Enhanced stealth options
        opts.add_argument("--disable-blink-features=AutomationControlled")
        opts.add_argument("--disable-dev-shm-usage")
        opts.add_argument("--no-sandbox")
        opts.add_argument("--disable-extensions")
        opts.add_argument("--disable-plugins")
        opts.add_argument("--disable-images")  # Faster loading
        opts.add_argument("--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
        
        # Performance logging for network capture
        opts.set_capability("goog:loggingPrefs", {"performance": "ALL"})
        
        try:
            service = Service(ChromeDriverManager().install())
            self.driver = webdriver.Chrome(service=service, options=opts)
            
            # Setup Cloudflare bypass
            setup_cloudflare_bypass(self.driver)
            
            print("[init] ✅ Enhanced driver initialized successfully")
            return True
            
        except Exception as e:
            print(f"[init] ❌ Failed to initialize driver: {e}")
            return False
    
    def enhanced_api_fetch(self, company_slug):
        """Try multiple API endpoints with different approaches"""
        api_results = []
        
        for endpoint_pattern in self.extraction_patterns["api_endpoints"]:
            for domain in ["https://data.importyeti.com", "https://www.importyeti.com"]:
                api_url = f"{domain}{endpoint_pattern}{company_slug}"
                
                try:
                    # Use browser's fetch to maintain session
                    result = self.driver.execute_async_script("""
                        const apiUrl = arguments[0];
                        const callback = arguments[arguments.length - 1];
                        
                        fetch(apiUrl, {
                            method: 'GET',
                            credentials: 'include',
                            headers: {
                                'Accept': 'application/json',
                                'X-Requested-With': 'XMLHttpRequest'
                            }
                        })
                        .then(response => response.text())
                        .then(text => {
                            try {
                                const data = JSON.parse(text);
                                callback({success: true, data: data, url: apiUrl});
                            } catch(e) {
                                callback({success: false, error: 'Parse error', url: apiUrl});
                            }
                        })
                        .catch(error => {
                            callback({success: false, error: error.toString(), url: apiUrl});
                        });
                    """, api_url)
                    
                    if result.get("success") and result.get("data"):
                        api_results.append(result)
                        print(f"[api] ✅ Success: {api_url}")
                    
                except Exception as e:
                    print(f"[api] ❌ Failed {api_url}: {e}")
                    continue
        
        return api_results
    
    def extract_data_from_apis(self, api_results):
        """Extract port/lane data from API responses"""
        extracted_data = {"exit_ports": [], "entry_ports": [], "lanes": []}
        
        for result in api_results:
            data = result.get("data", {})
            
            # Try various data extraction patterns
            for key_group in ["ports", "lanes", "map_data"]:
                for key in self.extraction_patterns["json_keys"][key_group]:
                    if self._extract_by_key(data, key, extracted_data):
                        print(f"[extract] ✅ Found data via key: {key}")
        
        return extracted_data
    
    def _extract_by_key(self, data, key, extracted_data):
        """Helper to extract data by specific key"""
        def find_nested(obj, target_key):
            if isinstance(obj, dict):
                if target_key in obj:
                    return obj[target_key]
                for v in obj.values():
                    result = find_nested(v, target_key)
                    if result:
                        return result
            elif isinstance(obj, list):
                for item in obj:
                    result = find_nested(item, target_key)
                    if result:
                        return result
            return None
        
        found_data = find_nested(data, key)
        if found_data:
            # Process based on key type
            if "exit" in key or "from" in key or "origin" in key:
                extracted_data["exit_ports"].extend(self._normalize_ports(found_data))
            elif "entry" in key or "to" in key or "destination" in key:
                extracted_data["entry_ports"].extend(self._normalize_ports(found_data))
            elif "lane" in key or "route" in key:
                extracted_data["lanes"].extend(self._normalize_lanes(found_data))
            return True
        return False
    
    def _normalize_ports(self, data):
        """Normalize port data to standard format"""
        ports = []
        if isinstance(data, dict):
            for name, info in data.items():
                if isinstance(info, dict):
                    ports.append({
                        "port": name,
                        "shipments": info.get("shipments", 0),
                        "lat": info.get("lat"),
                        "lon": info.get("lon")
                    })
                elif isinstance(info, (int, float)):
                    ports.append({"port": name, "shipments": int(info)})
        elif isinstance(data, list):
            for item in data:
                if isinstance(item, dict):
                    ports.append({
                        "port": item.get("port", item.get("name", "Unknown")),
                        "shipments": item.get("shipments", item.get("count", 0)),
                        "lat": item.get("lat"),
                        "lon": item.get("lon")
                    })
        return ports[:5]  # Top 5
    
    def _normalize_lanes(self, data):
        """Normalize lane data to standard format"""
        lanes = []
        if isinstance(data, list):
            for item in data:
                if isinstance(item, dict):
                    lanes.append({
                        "exit_port": item.get("exit_port", item.get("from", "Unknown")),
                        "entry_port": item.get("entry_port", item.get("to", "Unknown")), 
                        "shipments": item.get("shipments", item.get("count", 0))
                    })
        return lanes[:5]  # Top 5
    
    def enhanced_html_extraction(self, html):
        """Enhanced HTML extraction using multiple selectors"""
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(html, 'html.parser')
        
        extracted_data = {"exit_ports": [], "entry_ports": [], "lanes": []}
        
        # Try CSS selectors
        for selector in self.extraction_patterns["html_selectors"]:
            try:
                elements = soup.select(selector)
                if elements:
                    print(f"[html] Found elements with selector: {selector}")
                    # Process elements...
            except Exception as e:
                continue
        
        # Look for tables with port/lane data
        tables = soup.find_all('table')
        for table in tables:
            table_text = table.get_text().lower()
            if any(keyword in table_text for keyword in ['port', 'lane', 'shipping', 'trade']):
                print(f"[html] Found relevant table")
                # Extract table data...
        
        return extracted_data
    
    def scrape_company(self, company_name, company_url):
        """Main scraping method for a company"""
        print(f"\n{'='*60}")
        print(f"🏢 Scraping: {company_name}")
        print(f"🌐 URL: {company_url}")
        print(f"{'='*60}")
        
        # Extract company slug
        try:
            slug = urlparse(company_url).path.strip('/').split('/')[-1]
        except:
            slug = company_name.lower().replace(' ', '-')
        
        # Step 1: Load page with Cloudflare bypass
        if not smart_page_load(self.driver, company_url):
            print(f"[scrape] ❌ Failed to load page for {company_name}")
            return None
        
        # Step 2: Human-like behavior
        human_like_behavior(self.driver)
        
        # Step 3: Try API extraction first
        print("[scrape] 🔍 Attempting API extraction...")
        api_results = self.enhanced_api_fetch(slug)
        
        if api_results:
            extracted_data = self.extract_data_from_apis(api_results)
            if any(extracted_data.values()):
                print("[scrape] ✅ API extraction successful!")
                return extracted_data
        
        # Step 4: Fallback to HTML extraction
        print("[scrape] 🔍 Falling back to HTML extraction...")
        html = self.driver.page_source
        extracted_data = self.enhanced_html_extraction(html)
        
        if any(extracted_data.values()):
            print("[scrape] ✅ HTML extraction successful!")
            return extracted_data
        
        print("[scrape] ❌ No data found")
        return {"exit_ports": [], "entry_ports": [], "lanes": []}
    
    def run_scraping(self, companies_file="consumerBCO.txt"):
        """Run the enhanced scraping process"""
        if not self.init_driver():
            return
        
        try:
            # Load companies
            with open(companies_file, 'r') as f:
                companies = [line.strip() for line in f if line.strip()]
            
            print(f"[run] 📋 Loaded {len(companies)} companies")
            
            results = []
            
            for i, company in enumerate(companies[:5], 1):  # Test with first 5
                print(f"\n[run] Processing {i}/{min(5, len(companies))}: {company}")
                
                # Generate company URL (you might want to improve this)
                company_url = f"https://www.importyeti.com/company/{company.lower().replace(' ', '-').replace('&', 'and')}"
                
                result = self.scrape_company(company, company_url)
                
                if result:
                    results.append({
                        "company": company,
                        "url": company_url,
                        "data": result,
                        "timestamp": time.time()
                    })
                
                # Random delay between companies
                delay = random.uniform(5, 10)
                print(f"[run] ⏱️ Waiting {delay:.1f}s before next company...")
                time.sleep(delay)
            
            # Save results
            output_file = "enhanced_results.json"
            with open(output_file, 'w') as f:
                json.dump(results, f, indent=2)
            
            print(f"\n[run] ✅ Scraping complete! Results saved to {output_file}")
            print(f"[run] 📊 Successfully scraped {len(results)} companies")
            
        except Exception as e:
            print(f"[run] ❌ Error during scraping: {e}")
        
        finally:
            if self.driver:
                self.driver.quit()
                print("[run] 🚪 Driver closed")

if __name__ == "__main__":
    scraper = EnhancedImportYetiScraper()
    scraper.run_scraping()
