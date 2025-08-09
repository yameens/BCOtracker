# scrape_importyeti_playwright_v2.py
# ------------------------------------------------------------
# Robust Playwright scraper for ImportYeti "Top ports / lanes"
# - Persistent Chrome profile (cf_clearance + session reuse)
# - Explicit login gate
# - Strict per-company watchdog
# - Direct slug + bounded search fallback
# - Header-anchored parsing; zero CDP/network spelunking
#
# USAGE:
#   pip install playwright
#   playwright install
#   python scrape_importyeti_playwright_v2.py
#
# INPUT:  consumerBCO.txt  (one company per line)
# OUTPUT: consumerData.json
# ------------------------------------------------------------
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout
import unicodedata, re, time, json, os, sys
from difflib import SequenceMatcher
from contextlib import contextmanager

# ---------- Config ----------
BASE = "https://www.importyeti.com"
DATA = "https://data.importyeti.com"
PROFILE_DIR = "iy_profile"          # persistent user data directory
INPUT_TXT   = "consumerBCO.txt"
OUT_JSON    = "consumerData.json"

SNAP_EVERY           = 5            # write partial results every N companies
PAUSE_SEC            = 2.5          # pacing between companies
READY_TIMEOUT_MS     = 45_000       # general ready timeout after goto
CF_TIMEOUT_MS        = 90_000       # max wait for CF interstitial to clear
COMPANY_WATCHDOG_SEC = 60           # per-company absolute wall-time
SEARCH_MAX_CANDS     = 3            # test at most top-N search candidates

HEADER_FROM   = re.compile(r"(Top ports shipped from|Top ports from|Origin ports)", re.I)
HEADER_TO     = re.compile(r"(Top ports shipped to|Top ports to|Destination ports)", re.I)
HEADER_LANES  = re.compile(r"(Top lanes used|Top lanes|Routes|Trade routes|Shipping routes)", re.I)
CF_RE         = re.compile(r"(verify you are human|checking your browser|hcaptcha|cloudflare|stand by, while we are checking)", re.I)

STOPWORDS = set("""
co company companies corp corporation incorporated inc ltd limited llc plc holdings group international the and & s a de sa spa ag nv se asa nv sa
""".split())

# ---------- Logging ----------
def log(tag, msg):
    ts = time.strftime("%H:%M:%S")
    print(f"[{ts}] [{tag}] {msg}")

# ---------- Utilities ----------
def slugify(name: str) -> str:
    n = unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode()
    n = n.lower().replace("&", " and ")
    n = re.sub(r"[^\w\s-]", " ", n)
    n = re.sub(r"\s+", "-", n).strip("-")
    return n

def tokens(s: str):
    n = unicodedata.normalize("NFKD", s).encode("ascii","ignore").decode()
    n = n.lower()
    n = re.sub(r"[^a-z0-9\s-]", " ", n)
    n = re.sub(r"\s+", " ", n).strip()
    ts = [w for w in n.split() if w and w not in STOPWORDS]
    return set(sorted(ts, key=len, reverse=True)[:3] if len(ts) > 3 else ts)

def score_candidate(name_tokens: set, company_url_path: str) -> float:
    parts = [w for w in re.split(r"[-/_]", company_url_path) if w]
    pset  = set(parts)
    jac = (len(name_tokens & pset) / len(name_tokens | pset)) if name_tokens and pset else 0.0
    last = parts[-1] if parts else ""
    sratio = SequenceMatcher(None, " ".join(sorted(name_tokens)), last.replace("-", " ")).ratio()
    contain_all = name_tokens.issubset(pset)
    contain_bonus = 0.45 if contain_all else 0.0
    company_bias = 0.12 if "/company/" in company_url_path else 0.0
    return 0.55 * jac + 0.20 * sratio + contain_bonus + company_bias

def read_companies(path=INPUT_TXT):
    with open(path, encoding="utf-8") as f:
        lines = [ln.strip() for ln in f if ln.strip()]
    seen, out = set(), []
    for x in lines:
        xl = x.lower()
        if xl not in seen:
            seen.add(xl)
            out.append(x)
    return out

def load_existing(path=OUT_JSON):
    if not os.path.exists(path):
        return []
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return []

def save_json(path, data):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    os.replace(tmp, path)

# ---------- Navigation / CF / Login ----------
def wait_for_settled(page, timeout_ms=READY_TIMEOUT_MS):
    start = time.time()
    page.wait_for_load_state("domcontentloaded", timeout=timeout_ms)
    try:
        page.wait_for_load_state("networkidle", timeout=min(10_000, timeout_ms))
    except PWTimeout:
        pass
    # ensure the URL stops changing
    url_a = page.url
    page.wait_for_timeout(500)
    url_b = page.url
    if url_a != url_b:
        remaining = max(0, timeout_ms - int((time.time() - start) * 1000))
        if remaining > 0:
            return wait_for_settled(page, timeout_ms=remaining)

def cf_present(page) -> bool:
    try:
        el = page.get_by_text(CF_RE).first
        return el.is_visible(timeout=500)
    except Exception:
        return False

def wait_cf_clear(page, timeout_ms=CF_TIMEOUT_MS) -> bool:
    start = time.time()
    while (time.time() - start) * 1000 < timeout_ms:
        try:
            wait_for_settled(page, timeout_ms=15_000)
        except PWTimeout:
            pass
        if not cf_present(page):
            page.wait_for_timeout(1200)
            return True
        page.wait_for_timeout(800)
    return False

def goto_and_ready(page, url):
    log("nav", url)
    page.goto(url, wait_until="domcontentloaded", timeout=60_000)
    if not wait_cf_clear(page):
        log("guard", f"Cloudflare visible at {url}. Solve in the window, then press Enter.")
        input()
    wait_for_settled(page)

def has_app_session(page) -> bool:
    # Heuristics: session token and/or cf_clearance
    try:
        session_token = page.evaluate("() => window.localStorage?.getItem('NRBA_SESSION') || ''") or ""
    except Exception:
        session_token = ""
    cookie_names = {c.get('name','') for c in page.context.cookies()}
    return ('cf_clearance' in cookie_names) and bool(session_token)

def login_gate(page):
    # Warm both subdomains
    goto_and_ready(page, BASE + "/")
    goto_and_ready(page, DATA + "/")

    if has_app_session(page):
        log("login", "Session detected; proceeding.")
        return

    log("login", "Please log in within the visible browser window.")
    log("login", "Open any company page to clear CF, then press Enter here.")
    input()
    # Re-check
    goto_and_ready(page, BASE + "/")
    goto_and_ready(page, DATA + "/")
    if has_app_session(page):
        log("login", "Session confirmed.")
    else:
        log("login", "Warning: session not confirmed; continuing anyway.")

# ---------- Header presence / parsing ----------
def wait_has_top_headers(page, max_ms=10_000) -> bool:
    # Try to find ANY of the three header types
    try:
        loc = page.get_by_text(re.compile(
            f"{HEADER_FROM.pattern}|{HEADER_TO.pattern}|{HEADER_LANES.pattern}", re.I
        )).first
        loc.wait_for(timeout=max_ms)
        return True
    except PWTimeout:
        return False

def grab_section_texts(page, regex_compiled, limit_rows=18):
    # Find the header node by text (case-insensitive)
    header = page.get_by_text(regex_compiled).first
    header.wait_for(timeout=15_000)

    # The list/table/div right after the header
    container = header.locator("xpath=following::*[self::ul or self::ol or self::table or self::div][1]").first
    rows = container.locator("xpath=.//li|.//tr|.//div")

    count = rows.count()
    n = min(count, limit_rows) if count else 0
    out = []
    for i in range(n):
        try:
            txt = rows.nth(i).inner_text().strip()
            if txt:
                out.append(txt)
        except Exception:
            pass
    return out

def parse_label_count(texts, topn=5):
    out = []
    for t in texts:
        m = re.search(r"(.*?)[\s—–-]*\s(\d[\d,]*)\s*$", t) or re.search(r"(.*?)(\d[\d,]*)\s*$", t)
        if not m:
            continue
        label = re.sub(r"[—–-]\s*$", "", m.group(1).strip())
        try:
            cnt = int(m.group(2).replace(",", ""))
        except ValueError:
            continue
        if label:
            out.append((label, cnt))
    out.sort(key=lambda x: x[1], reverse=True)
    return out[:topn]

# ---------- URL resolution ----------
def search_candidates(page, name: str, max_links=12):
    q = re.sub(r"\s+", "+", name.strip())
    s_url = f"{BASE}/search?q={q}"
    goto_and_ready(page, s_url)

    # collect visible /company/ links
    anchors = page.locator("a[href*='/company/']")
    cnt = min(anchors.count(), max_links)
    out = []
    for i in range(cnt):
        href = anchors.nth(i).get_attribute("href") or ""
        if not href:
            continue
        href = href if href.startswith("http") else (BASE + href)
        if "/company/" in href:
            out.append(href)
    return out

def best_company_url(page, name: str):
    name_tokens = tokens(name)
    direct = f"{BASE}/company/{slugify(name)}"

    # Try direct slug
    goto_and_ready(page, direct)
    if wait_has_top_headers(page, max_ms=8_000):
        return direct

    # Search fallback (bounded)
    cands = search_candidates(page, name, max_links=12)
    if not cands:
        return direct

    ranked = []
    for u in cands:
        path = re.sub(r"^https?://[^/]+", "", u)
        ranked.append((u, score_candidate(name_tokens, path)))
    ranked.sort(key=lambda t: t[1], reverse=True)

    for u, _ in ranked[:SEARCH_MAX_CANDS]:
        goto_and_ready(page, u)
        if wait_has_top_headers(page, max_ms=8_000):
            return u

    # Nothing showed headers; return best guess (parser will likely no-op)
    return ranked[0][0] if ranked else direct

# ---------- Watchdog ----------
@contextmanager
def watchdog(seconds, label="task"):
    start = time.time()
    try:
        yield
    finally:
        dur = time.time() - start
        if dur > seconds:
            # Nothing to do here; actual timeout handling occurs where watchdog is used
            pass

# ---------- Extraction ----------
def extract_company(page, name: str):
    t0 = time.time()
    url = best_company_url(page, name)

    # Small guard if CF pops up during company load
    if not wait_cf_clear(page):
        log("guard", f"CF lingered on {url}. Solve and press Enter.")
        input()
    wait_for_settled(page)

    # Try to grab sections; each has short internal waits
    try:
        top_from_txt = grab_section_texts(page, HEADER_FROM)
    except Exception:
        top_from_txt = []
    try:
        top_to_txt   = grab_section_texts(page, HEADER_TO)
    except Exception:
        top_to_txt = []
    try:
        lanes_txt    = grab_section_texts(page, HEADER_LANES)
    except Exception:
        lanes_txt = []

    res = {
        "company": name,
        "url": url,
        "top_ports_shipped_from": [{"port": p, "shipments": c} for p, c in parse_label_count(top_from_txt)],
        "top_ports_shipped_to":   [{"port": p, "shipments": c} for p, c in parse_label_count(top_to_txt)],
        "top_lanes_used":         [{"lane": p, "shipments": c} for p, c in parse_label_count(lanes_txt)],
        "_elapsed_sec": round(time.time() - t0, 2),
    }
    return res

# ---------- Main ----------
def main():
    if not os.path.exists(INPUT_TXT):
        print(f"Missing {INPUT_TXT}. Create it with one company per line.")
        sys.exit(1)

    companies = read_companies(INPUT_TXT)
    existing  = load_existing(OUT_JSON)
    done = { (rec.get("company","") or "").lower(): True for rec in existing }
    results = list(existing)

    with sync_playwright() as pw:
        browser = pw.chromium.launch_persistent_context(
            user_data_dir=PROFILE_DIR,
            channel="chrome",
            headless=False,
            viewport={"width": 1280, "height": 900},
            args=["--disable-blink-features=AutomationControlled"],
        )
        page = browser.new_page()

        # One-time login gate (no recursion)
        login_gate(page)

        processed = 0
        for name in companies:
            if name.lower() in done:
                processed += 1
                continue

            log("company", f"→ {name}")
            start = time.time()
            try:
                # Per-company watchdog: abort after COMPANY_WATCHDOG_SEC
                rec = None
                try:
                    with watchdog(COMPANY_WATCHDOG_SEC, label=name):
                        rec = extract_company(page, name)
                except Exception as e:
                    # Shouldn’t happen because extract has its own short waits, but keep this
                    log("error", f"{name}: {e}")

                # If extraction took too long, skip
                if (time.time() - start) > COMPANY_WATCHDOG_SEC and rec is None:
                    log("skip", f"{name}: watchdog timeout")
                else:
                    rec = rec or {"company": name, "url": page.url, "top_ports_shipped_from": [], "top_ports_shipped_to": [], "top_lanes_used": [], "_elapsed_sec": round(time.time()-start,2)}
                    results.append(rec)
                    done[name.lower()] = True
                    processed += 1
                    log("ok", f"{name} in {rec.get('_elapsed_sec','?')}s")

            except PWTimeout:
                log("skip", f"{name}: Playwright timeout")
            except Exception as e:
                log("skip", f"{name}: {type(e).__name__}: {e}")

            if processed % SNAP_EVERY == 0:
                save_json(OUT_JSON, results)
                log("save", f"{len(results)} records → {OUT_JSON}")

            # gentle pacing
            time.sleep(PAUSE_SEC)

        save_json(OUT_JSON, results)
        log("done", f"{len(results)} total records → {OUT_JSON}")
        browser.close()

if __name__ == "__main__":
    main()
