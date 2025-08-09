# bypass_cloudflare.py
# Advanced Cloudflare bypass strategies for ImportYeti scraping

import time
import random
import json
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains

def setup_stealth_driver(driver):
    """Enhanced stealth configuration for bypassing Cloudflare"""
    
    # Execute stealth scripts in the browser context
    stealth_script = """
    // Remove webdriver property
    Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
    
    // Override plugins
    Object.defineProperty(navigator, 'plugins', {
        get: () => [1, 2, 3, 4, 5]
    });
    
    // Override languages
    Object.defineProperty(navigator, 'languages', {
        get: () => ['en-US', 'en']
    });
    
    // Override chrome property
    Object.defineProperty(window, 'chrome', {
        get: () => ({
            runtime: {},
            loadTimes: function() {},
            csi: function() {},
            app: {}
        })
    });
    
    // Override permissions
    const originalQuery = window.navigator.permissions.query;
    window.navigator.permissions.query = (parameters) => (
        parameters.name === 'notifications' ?
            Promise.resolve({ state: Cypress.config('userAgent') }) :
            originalQuery(parameters)
    );
    
    // Add mouse and keyboard events
    window.addEventListener('mousemove', () => {});
    window.addEventListener('keydown', () => {});
    """
    
    try:
        driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
            "source": stealth_script
        })
        print("[stealth] Enhanced stealth scripts applied")
    except Exception as e:
        print(f"[stealth] Failed to apply stealth scripts: {e}")

def human_like_behavior(driver):
    """Simulate human-like browsing behavior"""
    try:
        # Random mouse movements
        actions = ActionChains(driver)
        
        # Get window size
        window_size = driver.get_window_size()
        width, height = window_size['width'], window_size['height']
        
        # Random movements
        for _ in range(3):
            x = random.randint(50, width - 50)
            y = random.randint(50, height - 50)
            actions.move_by_offset(x - width//2, y - height//2)
            actions.pause(random.uniform(0.1, 0.3))
        
        actions.perform()
        
        # Random scroll
        scroll_amount = random.randint(100, 500)
        driver.execute_script(f"window.scrollBy(0, {scroll_amount});")
        time.sleep(random.uniform(0.5, 1.5))
        
        # Random pause
        time.sleep(random.uniform(1, 3))
        
    except Exception as e:
        print(f"[behavior] Human-like behavior failed: {e}")

def is_cloudflare_page(driver):
    """Detect various Cloudflare challenge types"""
    try:
        html = driver.page_source.lower()
        url = driver.current_url.lower()
        
        cf_indicators = [
            "verifying you are human",
            "checking your browser",
            "cloudflare",
            "cf-please-wait", 
            "challenge-platform",
            "turnstile",
            "just a moment",
            "needs to review the security",
            "enable javascript and cookies",
            "cf-browser-verification"
        ]
        
        # Check HTML content
        html_has_cf = any(indicator in html for indicator in cf_indicators)
        
        # Check URL patterns
        url_has_cf = any(pattern in url for pattern in ["challenge", "captcha", "verify"])
        
        # Check for specific Cloudflare elements
        cf_elements = False
        try:
            cf_elements = (
                driver.find_elements(By.CSS_SELECTOR, "[data-cf-settings]") or
                driver.find_elements(By.CSS_SELECTOR, ".cf-browser-verification") or
                driver.find_elements(By.CSS_SELECTOR, "#cf-please-wait") or
                driver.find_elements(By.CSS_SELECTOR, ".challenge-platform")
            )
        except:
            pass
        
        return html_has_cf or url_has_cf or bool(cf_elements)
        
    except Exception:
        return False

def wait_for_cloudflare_bypass(driver, max_wait=120):
    """Intelligent waiting for Cloudflare bypass with progress indication"""
    print("[cf] Cloudflare challenge detected, waiting for bypass...")
    
    start_time = time.time()
    last_progress = 0
    
    while time.time() - start_time < max_wait:
        try:
            elapsed = time.time() - start_time
            progress = int((elapsed / max_wait) * 100)
            
            # Show progress every 10%
            if progress - last_progress >= 10:
                print(f"[cf] Waiting... {progress}% ({elapsed:.1f}s)")
                last_progress = progress
            
            # Check if challenge is solved
            if not is_cloudflare_page(driver):
                print(f"[cf] ✅ Challenge bypassed after {elapsed:.1f}s")
                # Extra wait to ensure page is fully loaded
                time.sleep(2)
                return True
            
            # Simulate human behavior while waiting
            if elapsed > 10 and elapsed % 15 < 1:  # Every 15 seconds after 10s
                human_like_behavior(driver)
            
            time.sleep(1)
            
        except Exception as e:
            print(f"[cf] Error during wait: {e}")
            time.sleep(2)
    
    print(f"[cf] ❌ Timeout after {max_wait}s")
    return False

def smart_page_load(driver, url, max_retries=3):
    """Smart page loading with Cloudflare handling"""
    
    for attempt in range(max_retries):
        try:
            print(f"[load] Loading {url} (attempt {attempt + 1}/{max_retries})")
            
            # Add random delay between attempts
            if attempt > 0:
                delay = random.uniform(3, 8)
                print(f"[load] Waiting {delay:.1f}s before retry...")
                time.sleep(delay)
            
            # Load page
            driver.get(url)
            
            # Wait for basic page load
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
            
            # Check for Cloudflare
            if is_cloudflare_page(driver):
                print("[load] Cloudflare challenge detected")
                
                # Try automatic bypass first
                if wait_for_cloudflare_bypass(driver, max_wait=60):
                    print("[load] ✅ Automatic bypass successful")
                    return True
                else:
                    print("[load] ⚠️ Manual intervention required")
                    print("Please solve the Cloudflare challenge in the browser window")
                    input("Press Enter after solving the challenge...")
                    
                    # Verify challenge is solved
                    if not is_cloudflare_page(driver):
                        print("[load] ✅ Manual bypass confirmed")
                        return True
                    else:
                        print("[load] ❌ Challenge still present")
                        continue
            else:
                print("[load] ✅ Page loaded successfully")
                return True
                
        except Exception as e:
            print(f"[load] Error on attempt {attempt + 1}: {e}")
            if attempt == max_retries - 1:
                print("[load] ❌ All attempts failed")
                return False
    
    return False

def setup_cloudflare_bypass(driver):
    """Main setup function for Cloudflare bypass"""
    print("[cf] Setting up advanced Cloudflare bypass...")
    
    # Apply stealth configuration
    setup_stealth_driver(driver)
    
    # Set realistic viewport
    driver.set_window_size(1366, 768)
    
    # Clear any existing data
    try:
        driver.delete_all_cookies()
        driver.execute_script("localStorage.clear(); sessionStorage.clear();")
    except:
        pass
    
    print("[cf] ✅ Cloudflare bypass setup complete")

# Enhanced data extraction patterns
def get_enhanced_extraction_patterns():
    """Return enhanced patterns for extracting port/lane data"""
    return {
        "api_endpoints": [
            "/api/company/",
            "/data/v1.0/company/",
            "/graphql",
            "/api/search/company/",
            "/api/company-data/",
            "/v2/company/",
            "/company-api/"
        ],
        "json_keys": {
            "ports": [
                "exit_ports", "entry_ports", "top_ports", "ports",
                "origin_ports", "destination_ports", "from_ports", "to_ports",
                "shipping_from", "shipping_to", "port_data", "port_analytics"
            ],
            "lanes": [
                "lane_permutations", "lanes", "trade_lanes", "shipping_lanes",
                "routes", "trade_routes", "shipping_routes", "top_lanes",
                "lane_data", "route_data"
            ],
            "map_data": [
                "map_table", "map_data", "geo_data", "geographic_data",
                "port_map", "lane_map", "shipping_map"
            ]
        },
        "html_selectors": [
            '[data-testid*="port"]',
            '[data-testid*="lane"]', 
            '[class*="port"]',
            '[class*="lane"]',
            '[class*="shipping"]',
            '[class*="trade"]',
            'table[class*="port"]',
            'table[class*="lane"]',
            '.port-list',
            '.lane-list',
            '.shipping-data',
            '.trade-data'
        ]
    }
