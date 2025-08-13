#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
normalizeData.py

Reads a CSV that uses a two-row header (top: sections like 'Entity type and location',
second: field names like 'Name'), finds the 'Entity type and location|Name' column,
and filters rows to only those whose Name matches your consumer list.

Usage (strict exact matching on normalized names):
  python3 normalizeData.py \
    --in-csv "ZEROTRACKERFULL(Sheet1).csv" \
    --out-csv consumer_only.csv \
    --names consumerBCO.txt \
    --strict \
    --debug

If you hit encoding issues, add for example: --encoding cp1252
"""

import argparse
import re
import sys
import unicodedata
from typing import List, Set, Optional, Tuple
import pandas as pd

# -------- helpers for normalization --------
def strip_accents(s: str) -> str:
    if s is None:
        return ""
    return "".join(
        c for c in unicodedata.normalize("NFKD", s)
        if not unicodedata.combining(c)
    )

def normalize(s: str) -> str:
    """Lowercase, strip accents, replace & with 'and', remove non-alnum, collapse spaces."""
    if s is None:
        return ""
    s = strip_accents(s)
    s = s.lower().replace("&", " and ")
    s = re.sub(r"[^a-z0-9]+", " ", s)
    return re.sub(r"\s+", " ", s).strip()

def load_names(path: Optional[str]) -> List[str]:
    if path:
        # Try a couple encodings just in case
        for enc in ("utf-8", "utf-8-sig", "cp1252", "latin1"):
            try:
                with open(path, "r", encoding=enc) as f:
                    return [line.strip() for line in f if line.strip()]
            except UnicodeDecodeError:
                continue
    raise SystemExit("ERROR: --names file is required and could not be read.")

def to_norm_set(names: List[str]) -> Set[str]:
    return {normalize(n) for n in names if normalize(n)}

def make_matcher(targets: Set[str], strict: bool):
    if strict:
        return lambda val: normalize(val) in targets
    # fuzzy-ish containment either way
    def match(val: str) -> bool:
        nv = normalize(val)
        if not nv:
            return False
        if nv in targets:
            return True
        for t in targets:
            if t in nv or nv in t:
                return True
        return False
    return match

# -------- CSV header detection --------
def read_head(path: str, encoding: Optional[str], nrows: int = 80) -> pd.DataFrame:
    encs = [encoding, "utf-8-sig", "utf-8", "cp1252", "latin1"]
    last_err = None
    for enc in [e for e in encs if e]:
        try:
            return pd.read_csv(path, header=None, dtype=str, nrows=nrows, encoding=enc)
        except Exception as e:
            last_err = e
            continue
    raise last_err or RuntimeError("Failed to read CSV head")

def read_full(path: str, encoding: Optional[str]) -> pd.DataFrame:
    encs = [encoding, "utf-8-sig", "utf-8", "cp1252", "latin1"]
    last_err = None
    for enc in [e for e in encs if e]:
        try:
            return pd.read_csv(path, header=None, dtype=str, encoding=enc)
        except Exception as e:
            last_err = e
            continue
    raise last_err or RuntimeError("Failed to read CSV")

def find_two_row_header_positions(head: pd.DataFrame) -> Optional[Tuple[int, int, int]]:
    """
    Look for a column where a row has 'Entity type and location' and the NEXT row (same column) has 'Name'.
    Return (top_header_row_index, sub_header_row_index, column_index) or None.
    """
    target_top = "entity type and location"
    target_sub = "name"

    # Iterate rows/cols in the small head sample
    for r in range(len(head) - 1):
        row_top = head.iloc[r].astype(str).fillna("").str.strip()
        row_sub = head.iloc[r + 1].astype(str).fillna("").str.strip()

        for c in range(head.shape[1]):
            top_val = str(row_top.iloc[c]).strip().lower()
            sub_val = str(row_sub.iloc[c]).strip().lower()
            if target_top == top_val and target_sub == sub_val:
                return r, r + 1, c
    return None

def build_columns_from_two_rows(df: pd.DataFrame, top_r: int, sub_r: int) -> List[str]:
    top = df.iloc[top_r].astype(str).fillna("").tolist()
    sub = df.iloc[sub_r].astype(str).fillna("").tolist()

    def clean(v: str) -> str:
        v = v.strip()
        if v.lower().startswith("unnamed:"):
            return ""
        return v

    new_cols = []
    for a, b in zip(top, sub):
        a, b = clean(a), clean(b)
        if a and b:
            new_cols.append(f"{a}|{b}")
        elif a:
            new_cols.append(a)
        else:
            new_cols.append(b)
    return new_cols

def load_dataframe_with_detected_two_row_header(path: str, encoding: Optional[str], debug=False) -> Tuple[pd.DataFrame, Optional[str]]:
    """
    Try to detect the 2-row header and return (df, detected_name_col).
    detected_name_col will be 'Entity type and location|Name' if found, else None.
    """
    head = read_head(path, encoding, nrows=100)
    pos = find_two_row_header_positions(head)
    if not pos:
        return None, None

    top_r, sub_r, c_idx = pos
    if debug:
        print(f"[debug] two-row header detected at rows {top_r}/{sub_r}, anchor column index {c_idx}")

    raw = read_full(path, encoding)
    new_cols = build_columns_from_two_rows(raw, top_r, sub_r)

    df = raw.iloc[sub_r + 1:].copy()
    df.columns = new_cols

    # Drop fully empty-named columns
    df = df.loc[:, [c for c in df.columns if str(c).strip() != ""]]

    # Our target combined column:
    target_col = None
    for c in df.columns:
        cl = str(c).strip().lower()
        if cl == "entity type and location|name":
            target_col = c
            break
    # Fallback: any column whose right side is '|Name' and left contains 'entity type and location'
    if target_col is None:
        for c in df.columns:
            parts = [p.strip().lower() for p in str(c).split("|")]
            if len(parts) == 2 and parts[1] == "name" and "entity type and location" in parts[0]:
                target_col = c
                break

    if debug:
        print(f"[debug] constructed {len(df.columns)} columns")
        if target_col:
            print(f"[debug] detected name column: {target_col!r}")
        else:
            print(f"[debug] name column not found in constructed headers; columns include e.g.: {df.columns[:12].tolist()}")

    return df, target_col

# -------- main --------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in-csv", required=True, help="Path to input CSV")
    ap.add_argument("--out-csv", required=True, help="Path to write filtered CSV")
    ap.add_argument("--names", required=True, help="Path to consumerBCO.txt (one company per line)")
    ap.add_argument("--strict", action="store_true", help="Exact (normalized) match only (default fuzzy-ish if omitted)")
    ap.add_argument("--encoding", help="CSV file encoding hint (utf-8, cp1252, etc.)")
    ap.add_argument("--debug", action="store_true", help="Print detection details")
    args = ap.parse_args()

    # 1) Try the two-row header path
    df = None
    name_col = None
    try:
        df, name_col = load_dataframe_with_detected_two_row_header(args.in_csv, args.encoding, debug=args.debug)
    except Exception as e:
        print(f"[warn] two-row header detection failed: {e}", file=sys.stderr)

    # 2) Fallbacks
    if df is None:
        # Try reading normally; maybe it's already single-header with 'Name'
        if args.debug:
            print("[debug] falling back to single-row header read")
        try:
            df = read_full(args.in_csv, args.encoding)
            # assume first row is header
            # re-read as header=0 using same encoding as succeeded above
            df = None
            for enc in (args.encoding, "utf-8-sig", "utf-8", "cp1252", "latin1"):
                if not enc:
                    continue
                try:
                    df = pd.read_csv(args.in_csv, encoding=enc, dtype=str)
                    break
                except Exception:
                    continue
            if df is None:
                raise RuntimeError("Could not load CSV with a single header row")
        except Exception as e:
            print(f"Failed to read CSV: {e}", file=sys.stderr)
            sys.exit(1)

    # Find a suitable name column if we didn't detect the combined one
    if not name_col:
        # Prefer exact 'Name'
        for c in df.columns:
            if str(c).strip().lower() == "name":
                name_col = c
                break
    if not name_col:
        # Any column whose name contains '|Name'
        for c in df.columns:
            if str(c).strip().lower().endswith("|name"):
                name_col = c
                break
    if not name_col:
        print(f"ERROR: Could not determine a Name column. Found columns: {list(df.columns)}", file=sys.stderr)
        sys.exit(1)

    if args.debug:
        print(f"[debug] using Name column: {name_col!r}")

    # Load list of companies to keep
    names = load_names(args.names)
    targets = to_norm_set(names)
    match_fn = make_matcher(targets, strict=args.strict)

    # Filter
    kept = df[df[name_col].astype(str).apply(match_fn)].copy()

    # Write
    try:
        kept.to_csv(args.out_csv, index=False)
    except Exception as e:
        print(f"Failed to write output CSV: {e}", file=sys.stderr)
        sys.exit(1)

    print(f"Input rows: {len(df):,}")
    print(f"Kept rows:  {len(kept):,}")
    print(f"Name col:   {name_col}")
    print(f"Strict:     {args.strict}")
    print(f"Wrote:      {args.out_csv}")

if __name__ == "__main__":
    main()
