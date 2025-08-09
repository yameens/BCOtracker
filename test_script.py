#!/usr/bin/env python3
"""
Simple test script to verify the setup and debug port/lane extraction.
"""

import os
import sys
import json
from pathlib import Path

def test_imports():
    """Test if all required modules can be imported."""
    try:
        import selenium
        print("✓ selenium imported successfully")
    except ImportError as e:
        print(f"✗ selenium import failed: {e}")
        return False
    
    try:
        import bs4
        print("✓ beautifulsoup4 imported successfully")
    except ImportError as e:
        print(f"✗ beautifulsoup4 import failed: {e}")
        return False
    
    return True

def test_chromedriver():
    """Test if chromedriver is available."""
    import subprocess
    try:
        result = subprocess.run(['chromedriver', '--version'], 
                              capture_output=True, text=True, timeout=5)
        if result.returncode == 0:
            print(f"✓ chromedriver found: {result.stdout.strip()}")
            return True
        else:
            print(f"✗ chromedriver failed: {result.stderr}")
            return False
    except FileNotFoundError:
        print("✗ chromedriver not found in PATH")
        return False
    except Exception as e:
        print(f"✗ chromedriver test failed: {e}")
        return False

def test_files():
    """Test if required files exist."""
    files_to_check = [
        "consumerBCO.txt",
        "BCOdata_cdp.py"
    ]
    
    all_good = True
    for file_path in files_to_check:
        if Path(file_path).exists():
            print(f"✓ {file_path} exists")
        else:
            print(f"✗ {file_path} not found")
            all_good = False
    
    return all_good

def test_chrome_debugger():
    """Test if Chrome debugger is accessible."""
    import requests
    try:
        debugger_addr = os.getenv("CHROME_DEBUGGER", "127.0.0.1:9222")
        host, port = debugger_addr.split(":")
        url = f"http://{host}:{port}/json/version"
        response = requests.get(url, timeout=5)
        if response.status_code == 200:
            data = response.json()
            print(f"✓ Chrome debugger accessible: {data.get('Browser', 'Unknown')}")
            return True
        else:
            print(f"✗ Chrome debugger returned status {response.status_code}")
            return False
    except Exception as e:
        print(f"✗ Chrome debugger test failed: {e}")
        print("  Make sure Chrome is running with --remote-debugging-port=9222")
        return False

def main():
    print("Testing ImportYeti scraper setup...\n")
    
    tests = [
        ("Module imports", test_imports),
        ("ChromeDriver", test_chromedriver),
        ("Required files", test_files),
        ("Chrome debugger", test_chrome_debugger),
    ]
    
    results = []
    for test_name, test_func in tests:
        print(f"Testing {test_name}...")
        try:
            result = test_func()
            results.append((test_name, result))
        except Exception as e:
            print(f"✗ {test_name} test failed with exception: {e}")
            results.append((test_name, False))
        print()
    
    print("Test Results:")
    print("=" * 50)
    all_passed = True
    for test_name, passed in results:
        status = "PASS" if passed else "FAIL"
        print(f"{test_name:20} {status}")
        if not passed:
            all_passed = False
    
    print("=" * 50)
    if all_passed:
        print("✓ All tests passed! You can run the main script.")
        print("\nTo run the scraper:")
        print("1. Start Chrome with: open -na 'Google Chrome' --args --remote-debugging-port=9222 --user-data-dir='$HOME/ChromeScrapeProfile'")
        print("2. Log in to ImportYeti in the Chrome window")
        print("3. Run: python3 BCOdata_cdp.py")
    else:
        print("✗ Some tests failed. Please fix the issues above before running the main script.")
    
    return all_passed

if __name__ == "__main__":
    main()
