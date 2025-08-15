#!/usr/bin/env python3
# update_west_coast_companies.py
# Reads ONLY bco_ports_80.jsonl and updates west_coast_companies.jsonl
# with {company, top_west_coast_ports:[{port, shipments}, ...]} for companies
# that have "considerable" West Coast traffic based on entry+exit ports.

# companies + ports + summary of esg goals

def main():
    import json, re, sys, os
    from pathlib import Path
    from tempfile import NamedTemporaryFile

    # ---- File locations ----
    SRC = Path("bco_ports_80.jsonl")              # read ONLY from here
    DST = Path("west_coast_companies.jsonl")      # update/overwrite this file

    # ---- West Coast scope (major gateways; extend if needed) ----
    WEST_COAST = ("Los Angeles", "Long Beach", "Oakland", "Seattle", "Tacoma", "Portland")

    # ---- Regex for single-port labels (do NOT include combined labels here) ----
    PORT_PATTERNS = {
        "Los Angeles": re.compile(r"\bport\s+of\s+los\s+angeles\b|\blos\s+angeles\b", re.I),
        "Long Beach":  re.compile(r"\bport\s+of\s+long\s+beach\b|\blong\s+beach\b", re.I),
        "Oakland":     re.compile(r"\bport\s+of\s+oakland\b|\boakland\b", re.I),
        "Seattle":     re.compile(r"\bport\s+of\s+seattle\b|\bseattle\b", re.I),
        "Tacoma":      re.compile(r"\bport\s+of\s+tacoma\b|\btacoma\b", re.I),
        "Portland":    re.compile(r"\bport\s+of\s+portland\b|\bportland(,\s*or)?\b", re.I),
    }

    # ---- Explicit combined labels -> split across multiple ports ----
    COMBINED_PATTERNS = [
        # San Pedro Bay complex
        (re.compile(r"\b(?:la\s*/\s*lb|los\s+angeles\s*/\s*long\s+beach|los\s+angeles\s*-\s*long\s+beach|san\s+pedro\s+bay)\b", re.I),
         ["Los Angeles", "Long Beach"]),
        # Northwest Seaport Alliance
        (re.compile(r"\b(?:seattle\s*/\s*tacoma|seattle\s*-\s*tacoma|nwsa|northwest\s+seaport\s+alliance)\b", re.I),
         ["Seattle", "Tacoma"]),
    ]

    WORD_TO_NUM = {"one":1,"two":2,"three":3,"four":4,"five":5,"six":6,"seven":7,"eight":8,"nine":9,"ten":10}

    def canonical_ports(name: str):
        """Return list of canonical WC ports from a label; handles combined forms."""
        if not name:
            return []
        s = str(name).strip().lower()

        # 1) Combined labels first
        for pat, ports in COMBINED_PATTERNS:
            if pat.search(s):
                return ports[:]  # copy

        # 2) Single-port hits (could be more than one in odd cases)
        hits = []
        for canon, pat in PORT_PATTERNS.items():
            if pat.search(s):
                hits.append(canon)
        # dedupe but preserve order
        return list(dict.fromkeys(hits))

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
        """Sum shipments across matched WC ports for a list like top_entry_ports or top_exit_ports."""
        counts = {k: 0 for k in WEST_COAST}
        if not isinstance(list_of_ports, list):
            return counts
        for obj in list_of_ports:
            ports = canonical_ports(obj.get("port", ""))
            if not ports:
                continue  # ignore non-WC or unlabeled ports
            qty = extract_shipments(obj)
            if qty <= 0:
                continue
            if len(ports) == 1:
                counts[ports[0]] += qty
            else:
                # even split with remainder distributed to first ports
                base, rem = divmod(qty, len(ports))
                for i, p in enumerate(ports):
                    counts[p] += base + (1 if i < rem else 0)
        return counts

    # ---- Thresholds for "considerable" ----
    MIN_SINGLE = 10   # at least this many at any one WC port
    MIN_TOTAL  = 25   # or at least this many combined across WC ports

    # ---- Build results from SRC only ----
    if not SRC.exists():
        print(f"ERROR: source file not found: {SRC}", file=sys.stderr)
        sys.exit(1)

    merged_counts = {}   # company -> {port: shipments}
    lines_read = 0

    with SRC.open("r", encoding="utf-8") as fh:
        for line in fh:
            s = line.strip()
            if not s:
                continue
            lines_read += 1
            try:
                obj = json.loads(s)
            except json.JSONDecodeError:
                continue

            company = obj.get("company") or obj.get("name") or ""
            if not company:
                continue

            entry_counts = collect_counts(obj.get("top_entry_ports"))
            exit_counts  = collect_counts(obj.get("top_exit_ports"))
            combined = {k: entry_counts[k] + exit_counts[k] for k in WEST_COAST}

            if company not in merged_counts:
                merged_counts[company] = {k: 0 for k in WEST_COAST}
            for k in WEST_COAST:
                merged_counts[company][k] += combined[k]

    # ---- Filter results by thresholds; also emit to stdout for inspection ----
    new_records = {}  # company -> record dict to write into DST
    for company, counts in merged_counts.items():
        total_wc = sum(counts.values())
        has_single = any(v >= MIN_SINGLE for v in counts.values())
        if total_wc >= MIN_TOTAL or has_single:
            top_ports = [
                {"port": k, "shipments": v}
                for k, v in sorted(counts.items(), key=lambda kv: kv[1], reverse=True)
                if v > 0
            ]
            rec = {"company": company, "top_west_coast_ports": top_ports}
            new_records[company] = rec
            print(json.dumps(rec, ensure_ascii=False))

    # ---- Merge into west_coast_companies.jsonl (in place overwrite) ----
    # Load existing records (if any) into a map
    existing = {}
    if DST.exists():
        with DST.open("r", encoding="utf-8") as f:
            for line in f:
                s = line.strip()
                if not s:
                    continue
                try:
                    obj = json.loads(s)
                except json.JSONDecodeError:
                    continue
                c = obj.get("company")
                if c:
                    existing[c] = obj

    # Overwrite/insert with new records
    existing.update(new_records)

    # Write atomically
    with NamedTemporaryFile("w", delete=False, dir=str(DST.parent), encoding="utf-8") as tmp:
        tmp_path = Path(tmp.name)
        # optional: keep output deterministically ordered by company
        for company in sorted(existing.keys(), key=lambda x: x.lower()):
            tmp.write(json.dumps(existing[company], ensure_ascii=False) + "\n")

    # Replace destination
    os.replace(tmp_path, DST)

    print(
        f"Processed {lines_read} lines from {SRC.name}; "
        f"kept {len(new_records)} companies; "
        f"wrote {len(existing)} total records to {DST.name}.",
        file=sys.stderr
    )


if __name__ == "__main__":
    main()
