# cloudflare_helper.py
# Simple helper functions to improve Cloudflare bypass for the existing scraper

import time
import random
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

def is_cloudflare_challenge(driver):
    """Detect if current page is a Cloudflare challenge"""
    try:
        html = driver.page_source.lower()
        indicators = [
            "verifying you are human",
            "checking your browser", 
            "cloudflare",
            "just a moment",
            "needs to review the security"
        ]
        return any(indicator in html for indicator in indicators)
    except:
        return False

def enhanced_wait_for_page(driver, url, max_wait=120):
    """Enhanced page loading with better Cloudflare handling"""
    print(f"[load] Loading: {url}")
    
    # Load the page
    driver.get(url)
    
    # Wait for body
    try:
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.TAG_NAME, "body"))
        )
    except:
        pass
    
    # Check for Cloudflare
    if is_cloudflare_challenge(driver):
        print("[cf] 🛡️ Cloudflare challenge detected")
        print("[cf] ⏳ Waiting for automatic bypass...")
        
        start_time = time.time()
        while time.time() - start_time < max_wait:
            time.sleep(2)
            
            # Check if challenge is resolved
            if not is_cloudflare_challenge(driver):
                elapsed = time.time() - start_time
                print(f"[cf] ✅ Challenge bypassed after {elapsed:.1f}s")
                time.sleep(1)  # Extra wait for stability
                return True
            
            # Show progress
            elapsed = time.time() - start_time
            if int(elapsed) % 10 == 0:  # Every 10 seconds
                print(f"[cf] Still waiting... {elapsed:.0f}s")
        
        # Manual intervention required
        print("[cf] ⚠️ Automatic bypass failed")
        print("[cf] Please solve the challenge manually in the browser")
        input("Press Enter after solving the challenge...")
        
        # Verify manual bypass
        if not is_cloudflare_challenge(driver):
            print("[cf] ✅ Manual bypass confirmed")
            return True
        else:
            print("[cf] ❌ Challenge still present")
            return False
    
    print("[load] ✅ Page loaded successfully")
    return True

def add_realistic_delays():
    """Add realistic human-like delays"""
    delay = random.uniform(2, 5)
    time.sleep(delay)

def enhance_driver_stealth(driver):
    """Add basic stealth enhancements to existing driver"""
    try:
        # Execute stealth script
        driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
            "source": """
                Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
                Object.defineProperty(navigator, 'languages', {get: () => ['en-US', 'en']});
                Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4, 5]});
            """
        })
        print("[stealth] ✅ Basic stealth enhancements applied")
    except Exception as e:
        print(f"[stealth] ⚠️ Could not apply stealth: {e}")

def better_data_extraction(html_content):
    """Enhanced data extraction patterns"""
    from bs4 import BeautifulSoup
    
    if not html_content:
        return {"exit_ports": [], "entry_ports": [], "lanes": []}
    
    soup = BeautifulSoup(html_content, 'html.parser')
    
    # Look for data in various places
    data_sources = []
    
    # 1. Look for JSON in script tags
    scripts = soup.find_all('script')
    for script in scripts:
        if script.string:
            text = script.string
            # Look for common data patterns
            if any(pattern in text.lower() for pattern in ['port', 'lane', 'shipping', 'trade']):
                data_sources.append(('script', text[:500]))  # First 500 chars
    
    # 2. Look for tables with port data
    tables = soup.find_all('table')
    for table in tables:
        table_text = table.get_text().lower()
        if any(keyword in table_text for keyword in ['port', 'lane', 'shipping']):
            data_sources.append(('table', table_text[:200]))
    
    # 3. Look for divs with data attributes
    divs = soup.find_all('div', {'data-testid': True})
    for div in divs:
        testid = div.get('data-testid', '').lower()
        if any(keyword in testid for keyword in ['port', 'lane', 'map']):
            data_sources.append(('div', div.get_text()[:100]))
    
    if data_sources:
        print(f"[extract] 🔍 Found {len(data_sources)} potential data sources")
        for source_type, content in data_sources[:3]:  # Show first 3
            print(f"[extract] - {source_type}: {content[:50]}...")
    
    # Return empty for now - this is where you'd implement actual extraction
    return {"exit_ports": [], "entry_ports": [], "lanes": []}

# Quick integration functions for existing code
def patch_existing_scraper():
    """Patches to improve the existing web_ports_extractor.py scraper"""
    
    improvements = {
        "cloudflare_detection": is_cloudflare_challenge,
        "enhanced_loading": enhanced_wait_for_page,
        "stealth_enhancements": enhance_driver_stealth,
        "better_extraction": better_data_extraction,
        "human_delays": add_realistic_delays
    }
    
    return improvements

if __name__ == "__main__":
    print("Cloudflare Helper Utilities")
    print("===========================")
    print("Available functions:")
    for name, func in patch_existing_scraper().items():
        print(f"- {name}: {func.__doc__.split('.')[0] if func.__doc__ else 'No description'}")
