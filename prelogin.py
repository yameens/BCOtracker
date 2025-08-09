# BCOdata.py
# -----------------------------------------------------------------------------
# Attach to YOUR live Chrome (remote-debugging) so the exact logged-in session
# is used. If not logged in, script pauses and asks you to complete login
# manually in that same window (works with Captcha/MFA). Then it scrapes one
# highest-scoring ImportYeti URL per company and prints a big snippet.
# -----------------------------------------------------------------------------

import os, sys, time, random, json, re, unicodedata, warnings
from pathlib import Path
from urllib.parse import quote_plus, urlparse
from difflib import SequenceMatcher

import requests
from bs4 import BeautifulSoup

from selenium import webdriver
from selenium.webdriver.chrome.options import Options as ChromeOptions
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException

# -------------------------- configuration --------------------------
BASE = "https://www.importyeti.com"
COMPANY_TXT = "consumerBCO.txt"
OUT_JSON = "consumerData.json"

# IMPORTANT: set this in your shell before running:
#   export CHROME_DEBUGGER=127.0.0.1:9222
# or hardcode here:
DEBUGGER_ADDR = os.getenv("CHROME_DEBUGGER") or "127.0.0.1:9222"  # <-- change if you prefer

WAIT_SECS = 15
PAUSE_MIN, PAUSE_JITTER = 7, 6
MAX_SNIPPET_CHARS = 700
MAX_LINKS = 12

TAG_LIST = ["p", "div", "span", "td", "th", "strong"]

CF_MARKERS = [
    "verifying you are human",
    "needs to review the security of your connection",
    "performance & security by cloudflare",
    "enable javascript and cookies to continue",
    "just a moment",
    "cf-please-wait",
    "challenge-platform",
    "turnstile",
    "managed challenge",
]

NAV_NOISE = set([
    "menu","about","contact","login","log in","sign up","free","privacy","terms","copyright","©",
    "all rights reserved","press","faqs","pricing"
])

warnings.filterwarnings("ignore", message="This package (`duckduckgo_search`) has been renamed")

# ====================== text utils ======================
STOPWORDS = set("""
co company companies corp corporation incorporated inc ltd limited llc plc holdings group international the and & s a de sa spa ag nv se asa
""".split())

def norm(s: str) -> str:
    s = unicodedata.normalize("NFKD", s or "").encode("ascii", "ignore").decode()
    s = s.lower()
    s = re.sub(r"[^a-z0-9\s-]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def tokens(s: str) -> list[str]:
    return [w for w in norm(s).split() if w not in STOPWORDS]

def core_tokens(company_name: str) -> list[str]:
    t = [w for w in tokens(company_name) if len(w) >= 2]
    if len(t) <= 3:
        return t
    return sorted(t, key=len, reverse=True)[:3]

def jaccard(a: set[str], b: set[str]) -> float:
    if not a or not b:
        return 0.0
    inter = len(a & b)
    union = len(a | b)
    return inter / union if union else 0.0

def seq_ratio(a: str, b: str) -> float:
    return SequenceMatcher(None, norm(a), norm(b)).ratio()

def path_tokens(u: str) -> list[str]:
    try:
        p = urlparse(u).path
    except Exception:
        p = ""
    pt = [w for w in re.split(r"[-/_]", p) if w]
    return [w for w in pt if w not in STOPWORDS]

def is_company_url(u: str) -> bool:
    return "/company/" in u

def slugify_company(name: str) -> str:
    n = unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode()
    n = n.lower().replace("&", " and ")
    n = re.sub(r"[^\w\s-]", " ", n)
    n = re.sub(r"\s+", "-", n).strip("-")
    return n

# ====================== driver (attach-only) ======================
def init_driver_attach(debugger_addr: str):
    """Attach to the already-open Chrome you started with --remote-debugging-port."""
    print(f"[init] Trying to attach to Chrome at {debugger_addr} ...")
    opts = ChromeOptions()
    opts.add_experimental_option("debuggerAddress", debugger_addr)
    try:
        driver = webdriver.Chrome(options=opts)  # Selenium Manager finds chromedriver
    except Exception as e:
        raise RuntimeError(
            f"Could not attach to Chrome at {debugger_addr}. "
            f"Make sure you launched Chrome with --remote-debugging-port and set CHROME_DEBUGGER. Error: {e}"
        )
    # small sanity print
    try:
        ver = driver.execute_cdp_cmd("Browser.getVersion", {})
        print(f"[init] Attached. Product={ver.get('product')} UserAgent={ver.get('userAgent','')[:60]}...")
    except Exception:
        print("[init] Attached (version query failed; continuing).")
    return driver

# ====================== login / session checks ======================
def is_logged_in(driver) -> bool:
    """Check if current session is logged in by visiting a known company page and seeing if we get marketing shell."""
    try:
        driver.get(f"{BASE}/company/apple")
        WebDriverWait(driver, WAIT_SECS).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
        if "/login" in driver.current_url.lower():
            return False
        html = driver.page_source or ""
        # Logged-out pages tend to be mostly nav/marketing text (lots of 'login/sign up', no data terms)
        txt = BeautifulSoup(html, "html.parser").get_text(" ", strip=True).lower()
        logins = txt.count("login") + txt.count("log in") + txt.count("sign up")
        has_content = any(k in txt for k in ["total sea shipments", "customers", "hs code", "top 10"])
        return has_content and (logins < 3)
    except Exception:
        return False

def wait_for_manual_login(driver, timeout=240):
    """Ask you to complete login manually in the ATTACHED Chrome, and wait until detected."""
    print("\n⚠️  You are not logged in on the attached Chrome.")
    print("   Please complete login in THAT window (solve any captcha/MFA).")
    print("   I’ll keep checking for up to", timeout, "seconds...")
    end = time.time() + timeout
    while time.time() < end:
        if is_logged_in(driver):
            print("✅ Detected logged-in session.")
            return True
        time.sleep(2)
    print("⛔ Timed out waiting for manual login.")
    return False

# =================== candidates & scoring ===================
def company_url_attempt(name: str) -> str | None:
    """Direct slug guess: /company/<slug>."""
    slug = slugify_company(name)
    url = f"{BASE}/company/{slug}"
    try:
        r = requests.get(url, timeout=15, headers={"User-Agent": "Mozilla/5.0"})
        if r.status_code == 200 and "company not found" not in r.text.lower():
            return url
    except requests.RequestException:
        pass
    return None

def importyeti_search_candidates(name: str, max_links: int = MAX_LINKS) -> list[str]:
    q = quote_plus(name)
    search_url = f"{BASE}/search?q={q}"
    out = []
    try:
        r = requests.get(search_url, timeout=15, headers={"User-Agent": "Mozilla/5.0"})
        if r.status_code != 200:
            return out
        soup = BeautifulSoup(r.text, "html.parser")
        for a in soup.select('a[href^="/company/"], a[href^="/supplier/"]'):
            href = a.get("href")
            if href and (href.startswith("/company/") or href.startswith("/supplier/")):
                out.append(BASE + href)
                if len(out) >= max_links:
                    break
    except requests.RequestException:
        pass
    return out

def score_candidate(target_name: str, url: str) -> dict:
    tgt_core = set(core_tokens(target_name))
    ptk = set(path_tokens(url))
    jac = jaccard(tgt_core, ptk)
    last_seg = urlparse(url).path.strip("/").split("/")[-1]
    sratio = seq_ratio(" ".join(tgt_core), last_seg.replace("-", " "))
    contain_all = tgt_core.issubset(ptk)
    contain_bonus = 0.45 if contain_all else 0.0
    company_bias  = 0.12 if is_company_url(url) else 0.0
    total = 0.55 * jac + 0.20 * sratio + contain_bonus + company_bias
    return {
        "total": total, "jac": jac, "sratio": sratio,
        "contain_all": contain_all, "company_bias": company_bias,
    }

def resolve_company_candidates(name: str) -> list[str]:
    seen, out = set(), []
    u = company_url_attempt(name)
    if u and u not in seen:
        seen.add(u); out.append(u)

    for u in importyeti_search_candidates(name, max_links=MAX_LINKS):
        if u not in seen:
            seen.add(u); out.append(u)

    # Filter: require some token overlap in path; allow very short brands
    core = set(core_tokens(name))
    filtered = []
    for u in out:
        ptk = set(path_tokens(u))
        if core & ptk:
            filtered.append(u)
        else:
            nm = name.strip().lower()
            if any(len(t) <= 2 for t in core) or nm in ["vf", "lg", "3m", "bp", "ge"]:
                filtered.append(u)

    ranked = sorted(filtered, key=lambda x: score_candidate(name, x)["total"], reverse=True)
    return ranked[:MAX_LINKS]

# ================== CF / page utils ==================
def is_cloudflare_challenge(html: str) -> bool:
    h = (html or "").lower()
    return any(m in h for m in CF_MARKERS)

def extract_next_data(html: str) -> dict | None:
    m = re.search(r'window\.__NEXT_DATA__\s*=\s*({.*?});', html or "", re.DOTALL)
    if not m:
        return None
    try:
        return json.loads(m.group(1))
    except json.JSONDecodeError:
        return None

# ===================== scrape helpers =====================
def cleaned_snippet(raw_text: str, limit: int = MAX_SNIPPET_CHARS) -> str:
    if not raw_text:
        return ""
    lines = [ln.strip() for ln in raw_text.split("\n")]
    filt = []
    for ln in lines:
        ln_low = ln.lower()
        if not ln or len(ln) < 3:
            continue
        if any(tok in ln_low for tok in NAV_NOISE):
            continue
        if len(re.sub(r"[^a-zA-Z]", "", ln)) < 5:
            continue
        filt.append(ln)
        if sum(len(x) for x in filt) > (limit * 1.5):
            break
    txt = " ".join(filt) if filt else raw_text.replace("\n", " ")
    txt = re.sub(r"\s+", " ", txt).strip()
    return (txt[:limit] + "…") if len(txt) > limit else txt

def fetch_page(driver, url: str) -> str:
    tries, backoff = 0, 8
    while tries < 3:
        try:
            driver.get(url)
            WebDriverWait(driver, WAIT_SECS).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
            html = driver.page_source or ""
            if not is_cloudflare_challenge(html):
                # Give Next.js a moment
                try:
                    WebDriverWait(driver, 2).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, 'script#__NEXT_DATA__'))
                    )
                    html = driver.page_source or html
                except TimeoutException:
                    pass
                return html
            print("[fetch] Cloudflare challenge detected, backing off...")
            time.sleep(backoff + random.random() * 3)
            backoff *= 2
            tries += 1
        except (TimeoutException, WebDriverException) as e:
            print(f"[fetch] Selenium error: {e}. Backing off...")
            time.sleep(backoff)
            backoff *= 2
            tries += 1
    return ""

def scrape_text_from_html(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    return "\n".join(
        t.get_text(strip=True) for t in soup.find_all(TAG_LIST) if t.get_text(strip=True)
    )

def fetch_company_data(driver, url: str) -> dict:
    html = fetch_page(driver, url)
    if not html:
        try:
            title = driver.title
        except Exception:
            title = ""
        return {"url": url, "raw_text": "", "next_data": None, "cloudflare": True, "title": title}

    try:
        title = driver.title or ""
    except Exception:
        title = ""

    next_data = extract_next_data(html)
    raw_text  = scrape_text_from_html(html)

    return {"url": url, "raw_text": raw_text, "next_data": next_data, "cloudflare": False, "title": title}

# ===================== main =====================
def queries(path: str = COMPANY_TXT):
    # ---- attach to your Chrome
    driver = init_driver_attach(DEBUGGER_ADDR)

    # ---- require you to be logged in (manual, in that same window)
    if not is_logged_in(driver):
        if not wait_for_manual_login(driver):
            print("Exiting: login not completed.")
            try:
                driver.quit()
            except Exception:
                pass
            return

    # ---- read companies
    with open(path, encoding="utf-8") as f:
        companies = [line.strip() for line in f if line.strip()]

    results = []

    try:
        for idx, company in enumerate(companies, 1):
            # candidates → score → best
            cands = resolve_company_candidates(company)
            if not cands:
                print(f"[{idx:03d}] no URL candidates for {company}")
                continue

            scored = [(u, score_candidate(company, u)) for u in cands]
            scored.sort(key=lambda tup: tup[1]["total"], reverse=True)
            chosen, comp = scored[0]

            # fetch
            data = fetch_company_data(driver, chosen)
            data["company"] = company
            results.append(data)

            # progress
            cf = data.get("cloudflare", False)
            raw = (data.get("raw_text") or "")
            snippet = cleaned_snippet(raw, limit=MAX_SNIPPET_CHARS)
            status = "CF" if cf else "OK"

            print(f"[{idx:03d}] {status} | {company} | {chosen}")
            print(f"       score={comp['total']:.2f} (jac={comp['jac']:.2f}, sratio={comp['sratio']:.2f}, contain_all={comp['contain_all']}, bias={comp['company_bias']:.2f})")
            print(f"       title: {data.get('title','')[:160]}")
            print(f"       snippet: {snippet if snippet else '<empty>'}")

            time.sleep(PAUSE_MIN + random.random() * PAUSE_JITTER)

    finally:
        # We are attached to your browser; do NOT close it automatically.
        try:
            driver.quit()
        except Exception:
            pass

    with open(OUT_JSON, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    print(f"Saved {len(results)} records to {OUT_JSON}")

if __name__ == "__main__":
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")
    # Quick sanity echo:
    print(f"[env] CHROME_DEBUGGER={os.getenv('CHROME_DEBUGGER')}")
    queries()