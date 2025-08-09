
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
analyzeData.py
Reads consumerData.json and extracts:
  1) "Most Recent Sea Shipments" rows
  2) Top entry ports (when an ImportYeti API-style JSON block is present AND company matches)

Outputs CSVs into an --out directory:
  out/<company_sanitized>_recent_shipments.csv
  out/<company_sanitized>_top_entry_ports.csv

Usage:
  python analyzeData.py consumerData.json --out out/ --log INFO
"""
import argparse
import csv
import json
import logging
import os
import re
import sys
from typing import Dict, List, Any, Optional

# --- Utilities ----------------------------------------------------------------

COUNTRIES = set("""
Afghanistan, Albania, Algeria, Andorra, Angola, Antigua and Barbuda, Argentina, Armenia, Australia, Austria,
Azerbaijan, Bahamas, Bahrain, Bangladesh, Barbados, Belarus, Belgium, Belize, Benin, Bhutan, Bolivia,
Bosnia and Herzegovina, Botswana, Brazil, Brunei, Bulgaria, Burkina Faso, Burundi, Cabo Verde, Cambodia,
Cameroon, Canada, Central African Republic, Chad, Chile, China, Colombia, Comoros, Congo, Costa Rica, Cote d'Ivoire,
Croatia, Cuba, Cyprus, Czech Republic, Denmark, Djibouti, Dominica, Dominican Republic, Ecuador, Egypt, El Salvador,
Equatorial Guinea, Eritrea, Estonia, Eswatini, Ethiopia, Fiji, Finland, France, Gabon, Gambia, Georgia, Germany, Ghana,
Greece, Grenada, Guatemala, Guinea, Guinea-Bissau, Guyana, Haiti, Honduras, Hungary, Iceland, India, Indonesia, Iran,
Iraq, Ireland, Israel, Italy, Jamaica, Japan, Jordan, Kazakhstan, Kenya, Kiribati, Kuwait, Kyrgyzstan, Laos, Latvia,
Lebanon, Lesotho, Liberia, Libya, Liechtenstein, Lithuania, Luxembourg, Madagascar, Malawi, Malaysia, Maldives, Mali,
Malta, Marshall Islands, Mauritania, Mauritius, Mexico, Micronesia, Moldova, Monaco, Mongolia, Montenegro, Morocco,
Mozambique, Myanmar, Namibia, Nauru, Nepal, Netherlands, New Zealand, Nicaragua, Niger, Nigeria, North Korea,
North Macedonia, Norway, Oman, Pakistan, Palau, Panama, Papua New Guinea, Paraguay, Peru, Philippines, Poland,
Portugal, Qatar, Romania, Russia, Rwanda, Saint Kitts and Nevis, Saint Lucia, Saint Vincent and the Grenadines,
Samoa, San Marino, Sao Tome and Principe, Saudi Arabia, Senegal, Serbia, Seychelles, Sierra Leone, Singapore, Slovakia,
Slovenia, Solomon Islands, Somalia, South Africa, South Korea, South Sudan, Spain, Sri Lanka, Sudan, Suriname, Sweden,
Switzerland, Syria, Taiwan, Tajikistan, Tanzania, Thailand, Timor-Leste, Togo, Tonga, Trinidad and Tobago, Tunisia,
Turkey, Turkmenistan, Tuvalu, Uganda, Ukraine, United Arab Emirates, United Kingdom, United States, Uruguay, Uzbekistan,
Vanuatu, Vatican City, Venezuela, Vietnam, Yemen, Zambia, Zimbabwe, Netherlands Antilles, Puerto Rico, Norway
""".replace("\n", "").split(", "))

COUNTRY_ALIASES = {
    "uk": "United Kingdom",
    "u.k.": "United Kingdom",
    "england": "United Kingdom",
    "scotland": "United Kingdom",
    "wales": "United Kingdom",
    "northern ireland": "United Kingdom",
    "us": "United States",
    "u.s.": "United States",
    "usa": "United States",
    "u.s.a.": "United States",
    "south korea": "South Korea",
    "north korea": "North Korea",
    "czechia": "Czech Republic",
    "united states of america": "United States",
}

def normalize_country(s: str) -> str:
    low = s.strip().lower()
    if low in COUNTRY_ALIASES:
        return COUNTRY_ALIASES[low]
    # Try exact in COUNTRIES (case-insensitive)
    for c in COUNTRIES:
        if c.lower() == low:
            return c
    return s.strip()

def sanitize_company(name: str) -> str:
    s = re.sub(r"[^A-Za-z0-9._-]+", "_", name.strip())
    return s.strip("_") or "company"

def ensure_dir(p: str) -> None:
    os.makedirs(p, exist_ok=True)

def to_int(s: str) -> Optional[int]:
    try:
        return int(re.sub(r"[^\d]", "", s))
    except Exception:
        return None

def to_float(s: str) -> Optional[float]:
    try:
        return float(re.sub(r"[,]", "", s))
    except Exception:
        return None

# --- Input loader --------------------------------------------------------------

def load_records(path: str) -> List[Dict[str, Any]]:
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        data = json.load(f)

    if isinstance(data, list):
        items = data
    elif isinstance(data, dict):
        # Flexible keys
        if "records" in data and isinstance(data["records"], list):
            items = data["records"]
        elif "companies" in data and isinstance(data["companies"], list):
            items = data["companies"]
        else:
            # maybe a dict of id -> record
            items = list(data.values())
    else:
        raise ValueError("Unsupported JSON top-level structure.")

    norm = []
    for it in items:
        c = {
            "company": it.get("company") or it.get("name") or it.get("title") or "",
            "url": it.get("url") or it.get("link") or "",
            "raw_text": it.get("raw_text") or it.get("text") or it.get("html") or "",
        }
        if not any(c.values()):
            # skip unusable entries
            continue
        norm.append(c)
    return norm

# --- Shipments parser ----------------------------------------------------------

DATE_RE = r"(0[1-9]|1[0-2])/(0[1-9]|[12]\d|3[01])/(20\d{2})"
BOL_RE = r"[A-Z0-9]{8,}"
WEIGHT_RE = r"(\d[\d,]*)\s*kg"
MONEY_RE = r"\$\s*([\d,]+(?:\.\d{1,2})?)"
CONTAINERS_RE = r"kg\s*([0-9]+)\b"

ROUTE_STARTS = [
    "Asia", "Eu", "Europe", "South America", "North America", "Africa", "US",
    "US Pacific", "US Transatlantic", "US Transpacific", "Oceania", "Middle East"
]

COUNTRY_PAT = re.compile(
    r"\b(" + "|".join(sorted([re.escape(c) for c in COUNTRIES], key=len, reverse=True)) + r")\b",
    re.IGNORECASE
)

def extract_recent_shipments(raw: str) -> List[Dict[str, Any]]:
    # Find the "DateBill of Lading..." header region
    # Start search near "Most Recent Sea Shipments" if present
    start_idx = -1
    m = re.search(r"Most Recent Sea Shipments", raw, re.IGNORECASE)
    if m:
        start_idx = m.end()
    else:
        m2 = re.search(r"Date\s*Bill of Lading\s*Suppliers\s*Country", raw, re.IGNORECASE)
        if m2:
            start_idx = m2.start()
        else:
            # No recognizable table
            return []

    chunk = raw[start_idx: start_idx + 200000]  # take a large window
    # Split into rows by date start
    rows = []
    for match in re.finditer(rf"{DATE_RE}", chunk):
        rows.append(match.start())
    if not rows:
        return []

    rows.append(len(chunk))
    shipments = []
    for i in range(len(rows) - 1):
        row_txt = chunk[rows[i]:rows[i+1]]
        date_m = re.match(rf"{DATE_RE}", row_txt.strip())
        if not date_m:
            continue
        date = date_m.group(0)

        rest = row_txt[len(date):].strip()

        # BOL (first long all-caps/digit token)
        bol_m = re.search(rf"\b({BOL_RE})\b", rest)
        bol = bol_m.group(1) if bol_m else ""

        master_bol = ""
        if bol_m:
            rest_after_bol = rest[bol_m.end():].strip()
            # Optional second code that looks like BOL-ish (often Master BOL)
            mbol_m = re.match(rf"^({BOL_RE})\b", rest_after_bol)
            if mbol_m:
                master_bol = mbol_m.group(1)
                rest_after_codes = rest_after_bol[mbol_m.end():].strip()
            else:
                rest_after_codes = rest_after_bol
        else:
            rest_after_codes = rest

        # Supplier + Country: find the first country mention after codes
        supp_country_txt = rest_after_codes[:400]  # the supplier/country block is usually early
        c_m = COUNTRY_PAT.search(supp_country_txt)
        supplier, country = "", ""
        if c_m:
            country = normalize_country(c_m.group(1))
            supplier = supp_country_txt[:c_m.start()].strip()
            # supplier may have trailing route tokens; keep it readable
            supplier = re.sub(r"\s{2,}", " ", supplier).strip()

        # Weight (kg)
        w_m = re.search(WEIGHT_RE, row_txt, re.IGNORECASE)
        weight_kg = to_int(w_m.group(1)) if w_m else None

        # Containers count (immediately after kg)
        c_m2 = re.search(CONTAINERS_RE, row_txt, re.IGNORECASE)
        containers = to_int(c_m2.group(1)) if c_m2 else None

        # Route tag (e.g., "Asia US Pacific", "Eu US Transatlantic", "South America US Transatlantic")
        route = ""
        for rseed in ROUTE_STARTS:
            r_m = re.search(rseed + r".{0,40}", row_txt)
            if r_m:
                frag = row_txt[r_m.start(): r_m.end()]
                # Trim at $ or 'No Data' or a weight
                frag = re.split(r"(\$|No Data|kg\b)", frag)[0].strip()
                route = " ".join(frag.split())
                break

        # Est. freight (USD)
        money_m = re.search(MONEY_RE, row_txt)
        est_freight_usd = to_float(money_m.group(1)) if money_m else None

        shipments.append({
            "date": date,
            "bill_of_lading": bol,
            "master_bol": master_bol,
            "supplier": supplier,
            "supplier_country": country,
            "weight_kg": weight_kg,
            "containers": containers,
            "route_tag": route,
            "est_freight_usd": est_freight_usd,
        })

    return shipments

# --- Entry ports parser (API-like JSON block) ---------------------------------

TITLE_RE = re.compile(r'"title"\s*:\s*"([^"]+)"', re.IGNORECASE | re.DOTALL)
ENTRY_PORTS_RE = re.compile(r'"entry_ports"\s*:\s*(\{.*?\})\s*,\s*"shipments_by_country"', re.IGNORECASE | re.DOTALL)

def extract_entry_ports(raw: str, company: str) -> List[Dict[str, Any]]:
    # Find title
    t_m = TITLE_RE.search(raw)
    if not t_m:
        return []
    api_title = t_m.group(1).strip()

    # Only accept if title looks like the same company to avoid cross-contamination
    if api_title.lower() != (company or "").strip().lower():
        return []

    e_m = ENTRY_PORTS_RE.search(raw)
    if not e_m:
        return []
    entry_ports_json = e_m.group(1).strip()
    try:
        ports = json.loads(entry_ports_json)
    except Exception:
        # Try to fix trailing commas if any (very light fix)
        fixed = re.sub(r",\s*}", "}", entry_ports_json)
        fixed = re.sub(r",\s*]", "]", fixed)
        try:
            ports = json.loads(fixed)
        except Exception:
            logging.debug("Failed to parse entry_ports JSON block")
            return []

    out = []
    for name, meta in ports.items():
        shipments = meta.get("shipments")
        lat = None
        lon = None
        if isinstance(meta.get("port_location"), dict):
            lat = meta["port_location"].get("lat")
            lon = meta["port_location"].get("lon")
        out.append({
            "entry_port": name,
            "shipments": shipments,
            "lat": lat,
            "lon": lon,
        })

    # Sort descending by shipments when possible
    out.sort(key=lambda x: (x["shipments"] is None, -(x["shipments"] or 0)))
    return out

# --- Writer -------------------------------------------------------------------

def write_csv(path: str, rows: List[Dict[str, Any]], fieldnames: List[str]) -> None:
    if not rows:
        return
    ensure_dir(os.path.dirname(path) or ".")
    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k) for k in fieldnames})

# --- Main ---------------------------------------------------------------------

def run(input_path: str, out_dir: str) -> None:
    ensure_dir(out_dir)
    records = load_records(input_path)
    logging.info("Loaded %d records", len(records))

    total_shipments = 0
    total_ports = 0

    for rec in records:
        company = (rec.get("company") or "").strip() or "Unknown"
        raw = rec.get("raw_text") or ""
        safe_name = sanitize_company(company)

        # Shipments
        shipments = extract_recent_shipments(raw)
        s_fields = ["date", "bill_of_lading", "master_bol", "supplier",
                    "supplier_country", "weight_kg", "containers", "route_tag", "est_freight_usd"]
        s_out = os.path.join(out_dir, f"{safe_name}_recent_shipments.csv")
        write_csv(s_out, shipments, s_fields)
        logging.info("[%s] recent_shipments: %d -> %s", company, len(shipments), s_out)
        total_shipments += len(shipments)

        # Entry ports (only when title matches company in API block)
        ports = extract_entry_ports(raw, company)
        p_fields = ["entry_port", "shipments", "lat", "lon"]
        p_out = os.path.join(out_dir, f"{safe_name}_top_entry_ports.csv")
        write_csv(p_out, ports, p_fields)
        logging.info("[%s] top_entry_ports: %d -> %s", company, len(ports), p_out)
        total_ports += len(ports)

    logging.info("Done. Total rows -> shipments: %d, ports: %d", total_shipments, total_ports)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("input", help="Path to consumerData.json")
    ap.add_argument("--out", default="out", help="Output directory (default: out)")
    ap.add_argument("--log", default="INFO", help="Logging level (DEBUG, INFO, WARNING, ERROR)")
    args = ap.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log.upper(), logging.INFO),
        format="%(levelname)s: %(message)s"
    )

    run(args.input, args.out)

if __name__ == "__main__":
    main()
