#!/usr/bin/env python3
"""
web_ports_extractor.py
Web-only workflow: Tavily search + OpenAI extraction (no ImportYeti API calls).

Input: a text file of company names (one per line)
Output: JSONL with structured results + a flat CSV view

Example:

    python3 web_ports_extractor.py --input consumerBCO.txt \
      --out-json bco_ports.jsonl --out-csv bco_ports.csv \
      --top 5 --max 50 --sleep 0.5 --allow-importyeti \
      --model gpt-5-mini --search-depth basic

"""

import os, argparse, json, time, re, sys, hashlib, pathlib
from typing import List, Dict, Any, Tuple
from dotenv import load_dotenv
from tqdm import tqdm
import pandas as pd
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from tavily import TavilyClient
from openai import OpenAI

# ----------------- utils -----------------
def clean(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()

def chunk_text(s: str, hard_cap: int = 180_000, step: int = 14_000) -> List[str]:
    s = s[: hard_cap]
    return [s[i:i+step] for i in range(0, len(s), step)]

def pick_urls(results: Dict[str, Any], max_urls: int = 4) -> List[str]:
    urls = []
    for r in (results or {}).get("results", []):
        u = r.get("url")
        if u and u not in urls:
            urls.append(u)
        if len(urls) >= max_urls:
            break
    return urls

def score_page_for_ports(txt: str) -> float:
    """Tiny heuristic to bias pages w/ logistics words."""
    t = (txt or "").lower()
    needles = [
        "entry port","exit port","port of","bill of lading","shipments","teu",
        "import","export","lane","origin port","load port","discharge port",
        "los angeles","long beach","savannah","new york","newark","oakland",
        "charleston","tacoma","seattle","houston","norfolk","philadelphia",
        "yantian","shanghai","ningbo","qingdao","busan","rotterdam","antwerp","hamburg"
    ]
    hits = sum(t.count(w) for w in needles)
    return hits + min(len(t) / 5_000, 10)

def build_queries(name: str, allow_importyeti: bool) -> List[Tuple[str, List[str]]]:
    """Domain-first queries to reduce wasted extracts."""
    lane_domains = ["importinfo.com", "importkey.com", "importgenius.com", "usimportdata.com"]
    q: List[Tuple[str, List[str]]] = []
    # 1) Lane-heavy domains first
    q.append((f'"{name}"', lane_domains))
    q.append((f'"{name}" "bill of lading"', lane_domains))
    # 2) Optional: ImportYeti PUBLIC pages (not API)
    if allow_importyeti:
        q.append((f'"{name}" site:importyeti.com/company', ["importyeti.com"]))
    # 3) Broad fallback
    q.append((f'"{name}" (import OR shipments OR "bill of lading" OR "entry port" OR "port of entry")', []))
    return q

# ----------------- tiny file cache -----------------
CACHE_DIR = pathlib.Path(".cache")
CACHE_DIR.mkdir(exist_ok=True)

def _key(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()

def cache_get(kind: str, key: str):
    p = CACHE_DIR / f"{kind}-{_key(key)}.json"
    if p.exists():
        try:
            return json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            return None
    return None

def cache_set(kind: str, key: str, data: Any):
    p = CACHE_DIR / f"{kind}-{_key(key)}.json"
    try:
        p.write_text(json.dumps(data), encoding="utf-8")
    except Exception:
        pass

# ----------------- Tavily helpers -----------------
@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception_type(Exception),
    reraise=True
)
def tavily_search(tv: TavilyClient, query: str, include_domains: List[str] | None, max_results=4, depth: str = "basic") -> Dict[str, Any]:
    return tv.search(
        query=query,
        search_depth=depth,                # 'basic' by default (cheaper); use 'advanced' if needed
        max_results=max_results,          # smaller page count
        include_answer=False,
        include_raw_content=False,
        include_domains=include_domains or [],
    )

def tavily_search_cached(tv: TavilyClient, query: str, include_domains: List[str] | None, max_results=4, depth: str = "basic") -> Dict[str, Any]:
    key = json.dumps({"q": query, "d": include_domains or [], "m": max_results, "depth": depth}, sort_keys=True)
    hit = cache_get("search", key)
    if hit is not None:
        return hit
    res = tavily_search(tv, query, include_domains, max_results=max_results, depth=depth)
    cache_set("search", key, res)
    return res

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception_type(Exception),
    reraise=True
)
def tavily_extract(tv: TavilyClient, urls: List[str]) -> List[Dict[str, Any]]:
    if not urls:
        return []
    ex = tv.extract(urls=urls)
    return (ex or {}).get("results", []) or []

def tavily_extract_cached(tv: TavilyClient, urls: List[str]) -> List[Dict[str, Any]]:
    """Cache extracts per-URL to avoid paying twice."""
    results: List[Dict[str, Any]] = []
    to_fetch: List[str] = []
    for u in urls:
        hit = cache_get("extract", u)
        if hit is not None:
            results.append(hit)
        else:
            to_fetch.append(u)
    if to_fetch:
        ex = tavily_extract(tv, to_fetch)
        for r in ex:
            url = r.get("url", "")
            if url:
                cache_set("extract", url, r)
                results.append(r)
    return results

# ----------------- OpenAI helpers -----------------
def model_extract_json(client: OpenAI, model: str, system: str, prompt: str) -> Dict[str, Any]:
    """
    Use Chat Completions with JSON object output (stable across SDKs).
    No temperature override (some models only allow default=1).
    """
    resp = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": system},
            {"role": "user", "content": prompt},
        ],
        response_format={"type": "json_object"},
    )
    raw = resp.choices[0].message.content
    try:
        return json.loads(raw)
    except Exception:
        m = re.search(r"\{.*\}", raw, re.DOTALL)
        if m:
            return json.loads(m.group(0))
        raise

# ----------------- main per-company flow -----------------
def run_one_company(name: str, tv: TavilyClient, client: OpenAI, model: str, top_n: int, allow_importyeti: bool, search_depth: str) -> Dict[str, Any]:
    out = {
        "company": name,
        "status": "not_found",
        "sources": [],
        "top_entry_ports": [],
        "top_exit_ports": [],
        "top_lanes": [],
        "confidence": 0.0,
        "error": None
    }

    # 1) Search (domain-first, cached, fewer results)
    urls_all: List[str] = []
    for q, domains in build_queries(name, allow_importyeti):
        try:
            sr = tavily_search_cached(tv, q, include_domains=domains, max_results=4, depth=search_depth)
            urls = pick_urls(sr, max_urls=4)
            for u in urls:
                if u not in urls_all:
                    urls_all.append(u)
        except Exception:
            continue
        time.sleep(0.15)  # gentle

    if not urls_all:
        out["error"] = "no_search_hits"
        return out

    # 2) Extract a few pages first; expand only if needed (cached)
    initial_cap = 4
    try:
        extracted = tavily_extract_cached(tv, urls_all[:initial_cap])
    except Exception as e:
        out["error"] = f"extract_failed: {e}"
        return out

    pages: List[Tuple[str, str]] = []
    for r in extracted:
        url = r.get("url")
        txt = r.get("raw_content") or r.get("content") or ""
        if url and txt and len(clean(txt)) > 500:
            pages.append((url, clean(txt)))

    # Lazy expansion if not enough usable text
    if len(pages) < 2 and len(urls_all) > initial_cap:
        try:
            more = tavily_extract_cached(tv, urls_all[initial_cap:initial_cap+4])
            for r in more:
                url = r.get("url")
                txt = r.get("raw_content") or r.get("content") or ""
                if url and txt and len(clean(txt)) > 500:
                    pages.append((url, clean(txt)))
        except Exception:
            pass

    if not pages:
        out["error"] = "no_pages_extracted"
        return out

    # 3) Keep the best few
    pages.sort(key=lambda t: score_page_for_ports(t[1]), reverse=True)
    pages = pages[:4]
    out["sources"] = [u for (u, _) in pages]

    # 4) Build model context
    combined_text = "\n\n".join([f"URL: {u}\nCONTENT:\n{txt}" for (u, txt) in pages])
    chunks = chunk_text(combined_text, hard_cap=180_000, step=14_000)

    system = (
        "You are a meticulous logistics analyst. From the provided web text, extract explicit, source-supported facts "
        "about a company's trade activity. Return ONLY JSON. If a detail isn't clearly supported, omit it."
    )

    # Running aggregation
    agg = {
        "company": name,
        "sources": out["sources"],
        "top_entry_ports": [],
        "top_exit_ports": [],
        "top_lanes": [],
        "confidence": 0.0,
    }

    # 5) Feed chunks sequentially and merge
    for idx, ch in enumerate(chunks, start=1):
        prompt = f"""
Extract structured data for: {name}

Return a single JSON object with exactly these keys:
- company (string)
- sources (array of strings; reuse/echo the URLs you relied on)
- top_entry_ports (array of up to {top_n} objects: {{ "port": str, "shipments": int|null, "notes": str|null }})
- top_exit_ports  (array of up to {top_n} objects: {{ "port": str, "country": str|null, "shipments": int|null, "notes": str|null }})
- top_lanes       (array of up to {top_n} objects: {{ "exit_port": str, "exit_country": str|null, "entry_port": str, "entry_region": str|null, "shipments": int|null, "teu": int|null, "confidence": number 0-1 }})
- confidence      (number 0-1; overall)

Only include items that the text explicitly supports. If nothing is explicit, return empty arrays and confidence 0.

TEXT CHUNK {idx}/{len(chunks)}:
{ch}
""".strip()

        try:
            js = model_extract_json(client, model, system, prompt)
        except Exception as e:
            out["error"] = f"openai_parse_fail_chunk_{idx}: {e}"
            continue

        # Merge helpers
        def uniq_merge(dst: List[Dict[str, Any]], src: List[Dict[str, Any]], key_fields: List[str], cap: int):
            seen = {tuple((d.get(k) or "").lower() for k in key_fields) for d in dst}
            for s in (src or []):
                tup = tuple((s.get(k) or "").lower() for k in key_fields)
                if tup and tup not in seen and s.get(key_fields[0]):
                    dst.append(s)
                    seen.add(tup)
                if len(dst) >= cap:
                    break

        uniq_merge(agg["top_entry_ports"], js.get("top_entry_ports", []), ["port"], top_n)
        uniq_merge(agg["top_exit_ports"],  js.get("top_exit_ports", []),  ["port"], top_n)
        uniq_merge(agg["top_lanes"],       js.get("top_lanes", []),       ["exit_port","entry_port"], top_n)

        try:
            agg["confidence"] = max(float(agg["confidence"]), float(js.get("confidence") or 0))
        except Exception:
            pass

        time.sleep(0.15)

    # 6) Finalize
    out.update(agg)
    out["status"] = "ok" if (agg["top_entry_ports"] or agg["top_exit_ports"] or agg["top_lanes"]) else "not_found"
    return out

# ----------------- flatten for CSV -----------------
def flat_ports(prefix: str, L: List[Dict[str, Any]], N: int) -> Dict[str, Any]:
    out = {}
    for i in range(N):
        it = L[i] if i < len(L) else {}
        out[f"{prefix}_{i+1}_port"]      = it.get("port")
        out[f"{prefix}_{i+1}_shipments"] = it.get("shipments")
    return out

def flat_lanes(L: List[Dict[str, Any]], N: int) -> Dict[str, Any]:
    out = {}
    for i in range(N):
        it = L[i] if i < len(L) else {}
        out[f"lane_{i+1}_exit_port"]  = it.get("exit_port")
        out[f"lane_{i+1}_entry_port"] = it.get("entry_port")
        out[f"lane_{i+1}_shipments"]  = it.get("shipments")
    return out

# ----------------- main -----------------
def main():
    load_dotenv()

    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True, help="text file with company names (one per line)")
    ap.add_argument("--out-json", default="bco_ports.jsonl")
    ap.add_argument("--out-csv",  default="bco_ports.csv")
    ap.add_argument("--top", type=int, default=5)
    ap.add_argument("--max", type=int, default=10**9)
    ap.add_argument("--sleep", type=float, default=0.4, help="delay between companies (seconds)")
    ap.add_argument("--allow-importyeti", action="store_true", help="allow searching ImportYeti public pages")
    ap.add_argument("--model", default=os.getenv("OPENAI_MODEL", "gpt-5-mini"))
    ap.add_argument("--search-depth", choices=["basic","advanced"], default=os.getenv("TAVILY_SEARCH_DEPTH","basic"),
                    help="Tavily search depth; 'basic' is cheaper")
    args = ap.parse_args()

    openai_key = os.getenv("OPENAI_API_KEY")
    tavily_key = os.getenv("TAVILY_API_KEY")
    if not openai_key or not tavily_key:
        print("ERROR: missing OPENAI_API_KEY or TAVILY_API_KEY in environment/.env", file=sys.stderr)
        sys.exit(1)

    client = OpenAI(api_key=openai_key)
    tv = TavilyClient(api_key=tavily_key)

    # load companies
    with open(args.input, "r", encoding="utf-8") as f:
        companies = [clean(x) for x in f.read().splitlines() if clean(x)]
    if args.max < len(companies):
        companies = companies[: args.max]

    rows = []
    for name in tqdm(companies, desc="Companies"):
        try:
            r = run_one_company(
                name=name,
                tv=tv,
                client=client,
                model=args.model,
                top_n=args.top,
                allow_importyeti=args.allow_importyeti,
                search_depth=args.search_depth
            )
        except Exception as e:
            r = {
                "company": name,
                "status": "error",
                "sources": [],
                "top_entry_ports": [],
                "top_exit_ports": [],
                "top_lanes": [],
                "confidence": 0.0,
                "error": str(e)
            }
        rows.append(r)
        time.sleep(args.sleep)

    # write JSONL
    with open(args.out_json, "w", encoding="utf-8") as w:
        for r in rows:
            w.write(json.dumps(r, ensure_ascii=False) + "\n")
    print(f"Wrote {args.out_json}")

    # write flat CSV
    flat = []
    for r in rows:
        base = {
            "company": r.get("company"),
            "status": r.get("status"),
            "confidence": r.get("confidence"),
            "sources": ";".join(r.get("sources", [])),
            "error": r.get("error"),
        }
        base.update(flat_ports("entry", r.get("top_entry_ports", []), args.top))
        base.update(flat_ports("exit",  r.get("top_exit_ports", []),  args.top))
        base.update(flat_lanes(r.get("top_lanes", []), args.top))
        flat.append(base)

    pd.DataFrame(flat).to_csv(args.out_csv, index=False)
    print(f"Wrote {args.out_csv}")

if __name__ == "__main__":
    main()
