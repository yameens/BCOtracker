## web ports extractor information. cross reference the CSV sheet (import into pycharm) to align data and understand how it works.
## game plan. create a new csv sheet, with scope target information, brief description of esg goals, and company name.
## core. scrape top lanes per company and add top lanes + cargo tons (to see if it qualiifies as a potential target).

def main():
    # Minimal, no-argparse main() that:
    # - reads west_coast_companies.jsonl and bco_ports_80.jsonl
    # - keeps companies with considerable traffic to West Coast ports
    # - prints JSONL with {company, top_west_coast_ports:[{port, shipments}, ...]}
    import json, re, sys
    from pathlib import Path

    input_files = ["west_coast_companies.jsonl", "bco_ports_80.jsonl"]

    # West Coast ports of interest
    WEST_COAST = ("Los Angeles", "Long Beach", "Oakland", "Tacoma")

    # Robust matching on messy labels (e.g., "Port of Los Angeles", "LA/LB")
    PORT_PATTERNS = {
        "Los Angeles": re.compile(r"\bport\s+of\s+los\s+angeles\b|\blos\s+angeles\b|\bla\s*/\s*lb\b|\bla\s*-\s*long\s*beach\b", re.I),
        "Long Beach":  re.compile(r"\bport\s+of\s+long\s+beach\b|\blong\s+beach\b|\bla\s*/\s*lb\b|\bla\s*-\s*long\s*beach\b", re.I),
        "Oakland":     re.compile(r"\bport\s+of\s+oakland\b|\boakland\b", re.I),
        "Tacoma":      re.compile(r"\bport\s+of\s+tacoma\b|\btacoma\b", re.I),
    }

    WORD_TO_NUM = {
        "one":1,"two":2,"three":3,"four":4,"five":5,
        "six":6,"seven":7,"eight":8,"nine":9,"ten":10
    }

    def canonical_port(name: str):
        if not name:
            return None
        s = str(name).strip().lower()
        for canon, pat in PORT_PATTERNS.items():
            if pat.search(s):
                return canon
        return None

    def parse_shipments_from_notes(notes):
        if not notes:
            return None
        s = str(notes).strip().lower()
        m = re.search(r"\b(\d+)\s+(?:shipment|shipments|record|records|import\s+records)\b", s)
        if m:
            try:
                return int(m.group(1))
            except ValueError:
                pass
        m = re.search(r"\b(one|two|three|four|five|six|seven|eight|nine|ten)\s+(?:shipment|shipments|record|records|import\s+records)\b", s)
        if m:
            return WORD_TO_NUM.get(m.group(1))
        return None

    def extract_shipments(item: dict) -> int:
        # prefer explicit integer
        val = item.get("shipments")
        if isinstance(val, int):
            return val
        # infer from notes if possible
        n = parse_shipments_from_notes(item.get("notes"))
        return int(n) if n is not None else 0

    def collect_counts(list_of_ports):
        counts = {k: 0 for k in WEST_COAST}
        if not isinstance(list_of_ports, list):
            return counts
        for obj in list_of_ports:
            canon = canonical_port(obj.get("port", ""))
            if canon:
                counts[canon] += extract_shipments(obj)
        return counts

    # Define "considerable movement"
    MIN_SINGLE = 10   # at least this many at any one WC port
    MIN_TOTAL  = 25   # or at least this many combined across WC ports

    seen = set()
    total = 0
    kept = 0

    for path in input_files:
        p = Path(path)
        if not p.exists():
            continue
        with p.open("r", encoding="utf-8") as fh:
            for line in fh:
                total += 1
                s = line.strip()
                if not s:
                    continue
                try:
                    obj = json.loads(s)
                except json.JSONDecodeError:
                    continue

                company = obj.get("company") or obj.get("name") or ""
                if not company or company in seen:
                    continue

                # Aggregate entry + exit counts to capture labeling variance
                entry_counts = collect_counts(obj.get("top_entry_ports"))
                exit_counts  = collect_counts(obj.get("top_exit_ports"))
                counts = {k: entry_counts[k] + exit_counts[k] for k in WEST_COAST}

                total_wc = sum(counts.values())
                has_single = any(v >= MIN_SINGLE for v in counts.values())
                if total_wc >= MIN_TOTAL or has_single:
                    seen.add(company)
                    kept += 1
                    top_ports = [
                        {"port": k, "shipments": v}
                        for k, v in sorted(counts.items(), key=lambda kv: kv[1], reverse=True)
                        if v > 0
                    ]
                    # Output only the essentials you requested
                    print(json.dumps({
                        "company": company,
                        "top_west_coast_ports": top_ports
                    }, ensure_ascii=False))

    print(f"Processed {total} lines; kept {kept} companies.", file=sys.stderr)


if __name__ == "__main__":
    main()
