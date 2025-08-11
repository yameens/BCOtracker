#!/usr/bin/env python3
"""
Flask backend for Forum Mobility ESG Port Analytics
"""

import os
import json
import time
from pathlib import Path
from flask import Flask, render_template, request, jsonify
from flask_cors import CORS

# Import the scraper functions
from web_ports_extractor import (
    init_driver_attach, resolve_company_candidates, 
    fetch_company_page_and_ports, score_candidate,
    slugify_company, NetworkCapture, extract_top_info_from_any
)

app = Flask(__name__)
CORS(app)

# Configuration
DEBUGGER_ADDR = os.getenv("CHROME_DEBUGGER", "127.0.0.1:9222")
COMPANY_TXT = "consumerBCO.txt"

# Cache for company data
company_cache = {}
cache_timeout = 3600  # 1 hour

def get_company_list():
    """Get list of companies from the text file."""
    try:
        with open(COMPANY_TXT, encoding="utf-8") as f:
            companies = [line.strip() for line in f if line.strip()]
        return companies
    except FileNotFoundError:
        return []

def format_port_data(topinfo):
    """Format port data for frontend consumption."""
    export_ports = []
    import_ports = []
    trade_lanes = []
    
    # Format export ports (ports shipped from)
    for port in topinfo.get("exit_ports", []):
        export_ports.append({
            "port": port.get("port", "Unknown"),
            "shipments": port.get("shipments", 0)
        })
    
    # Format import ports (ports shipped to)
    for port in topinfo.get("entry_ports", []):
        import_ports.append({
            "port": port.get("port", "Unknown"),
            "shipments": port.get("shipments", 0)
        })
    
    # Format trade lanes
    for lane in topinfo.get("lanes", []):
        trade_lanes.append({
            "exit_port": lane.get("exit_port", "Unknown"),
            "entry_port": lane.get("entry_port", "Unknown"),
            "shipments": lane.get("shipments", 0)
        })
    
    return {
        "export_ports": export_ports,
        "import_ports": import_ports,
        "trade_lanes": trade_lanes
    }

def scrape_company_data(company_name):
    """Scrape port data for a specific company."""
    try:
        # Initialize driver
        driver = init_driver_attach(DEBUGGER_ADDR)
        
        # Find company candidates
        cands = resolve_company_candidates(driver, company_name)
        if not cands:
            return None
        
        # Use the best candidate
        best_url = cands[0]
        slug_hint = slugify_company(company_name)
        
        # Fetch data
        html, topinfo, cap = fetch_company_page_and_ports(driver, best_url, slug_hint=slug_hint)
        
        # Format data for frontend
        formatted_data = format_port_data(topinfo)
        
        driver.quit()
        return formatted_data
        
    except Exception as e:
        print(f"Error scraping {company_name}: {e}")
        return None

@app.route('/')
def index():
    """Serve the main page."""
    return render_template('index.html')

@app.route('/api/companies')
def get_companies():
    """Get list of available companies."""
    companies = get_company_list()
    return jsonify({
        "companies": companies,
        "total": len(companies)
    })

@app.route('/api/company/<company_name>')
def get_company_data(company_name):
    """Get port data for a specific company."""
    # Check cache first
    cache_key = company_name.lower()
    current_time = time.time()
    
    if cache_key in company_cache:
        cached_data, cache_time = company_cache[cache_key]
        if current_time - cache_time < cache_timeout:
            return jsonify({
                "success": True,
                "data": cached_data,
                "cached": True
            })
    
    # Scrape fresh data
    print(f"Scraping data for: {company_name}")
    data = scrape_company_data(company_name)
    
    if data:
        # Cache the result
        company_cache[cache_key] = (data, current_time)
        
        return jsonify({
            "success": True,
            "data": data,
            "cached": False
        })
    else:
        return jsonify({
            "success": False,
            "error": "No data available for this company"
        }), 404

@app.route('/api/health')
def health_check():
    """Health check endpoint."""
    return jsonify({
        "status": "healthy",
        "timestamp": time.time(),
        "cache_size": len(company_cache)
    })

@app.route('/api/cache/clear')
def clear_cache():
    """Clear the company data cache."""
    global company_cache
    company_cache.clear()
    return jsonify({
        "success": True,
        "message": "Cache cleared"
    })

# Create templates directory and move index.html
def setup_templates():
    """Setup templates directory and move index.html."""
    templates_dir = Path("templates")
    templates_dir.mkdir(exist_ok=True)
    
    # Move index.html to templates if it exists
    if Path("index.html").exists():
        import shutil
        shutil.move("index.html", "templates/index.html")

if __name__ == '__main__':
    setup_templates()
    
    print("Forum Mobility ESG Port Analytics")
    print("=" * 40)
    print("Starting Flask server...")
    print("Make sure Chrome is running with remote debugging enabled:")
    print("open -na 'Google Chrome' --args --remote-debugging-port=9222 --user-data-dir='$HOME/ChromeScrapeProfile'")
    print("\nAccess the application at: http://localhost:8081")
    
    app.run(debug=True, host='0.0.0.0', port=8081)
