#!/usr/bin/env python3
"""
make_esg_summaries.py

Stable ESG summarizer with:
- Responses API text extraction (no reliance on output_text).
- Automatic fallback to Chat Completions if Responses yields empty text.
- Column re-mapping for 'Unnamed:*' fields into human labels.
- Strong debug logging, retries, schema normalization & safe fallbacks.

Usage:
  python3 make_esg_summaries.py \
    --in-csv west_coast_company_and_esg.csv \
    --out-csv west_coast_company_and_esg_summary.csv \
    --model gpt-5-mini --max-rows 0 --debug 1
"""

import os
import re
import json
import argparse
import logging
from typing import Dict, Any, Tuple, List, Optional

import pandas as pd
from tqdm import tqdm
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

# --- Auto-load .env if present ---
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

# --- OpenAI SDK ---
try:
    import openai
    from openai import OpenAI
except Exception as e:
    raise RuntimeError("Install OpenAI SDK: pip install --upgrade openai python-dotenv") from e

# =========================
# CLI + Logging
# =========================

def setup_logging(debug: int):
    level = logging.DEBUG if debug else logging.INFO
    logging.basicConfig(level=level, format="%(asctime)s | %(levelname)s | %(message)s")

# =========================
# Prompts
# =========================

SYSTEM_INSTRUCTIONS = (
    "You are an analyst. Using only the structured fields provided, write a concise 3–5 sentence "
    "ESG / Net-Zero summary for the company. If a detail is missing, state 'not disclosed' rather than inferring. "
    "Prioritize: (1) net-zero or emissions targets and years; (2) scopes covered (1/2/3) and validation (e.g., SBTi); "
    "(3) interim progress/metrics; (4) key actions or policies; (5) any explicit risks/controversies; "
    "(6) optionally relate West Coast port usage when relevant. "
    "Be neutral and precise. No bullet points—one compact paragraph."
)

USER_TEMPLATE = (
    "Return a SINGLE JSON object with fields EXACTLY:\n"
    '  - "company": the exact company name\n'
    '  - "ports": a short human string summarizing West Coast ports (e.g., "Los Angeles: 1200 | Long Beach: 800"), or "not disclosed"\n'
    '  - "esg_summary": the paragraph summary\n\n'
    "CRITICAL RULES:\n"
    "• Output JSON only (no preface, no markdown, no code fences, no commentary).\n"
    "• Use ONLY the data below. If something is missing, write 'not disclosed'.\n\n"
    "DATA:\n{data}"
)

def build_prompt_input(payload: Dict[str, Any]) -> str:
    return USER_TEMPLATE.format(data=json.dumps(payload, ensure_ascii=False))

# =========================
# CSV helpers (ports + remap)
# =========================

def choose_ports(row: pd.Series) -> Tuple[str, str]:
    ports_json = None
    if "top_west_coast_ports" in row and pd.notna(row["top_west_coast_ports"]):
        ports_json = str(row["top_west_coast_ports"]).strip()

    ports_flat = ""
    if "ports_flat" in row and pd.notna(row["ports_flat"]):
        ports_flat = str(row["ports_flat"]).strip()

    if not ports_json and ports_flat:
        parts: List[Dict[str, Any]] = []
        for seg in ports_flat.split("|"):
            seg = seg.strip()
            if not seg:
                continue
            if ":" in seg:
                p, s = seg.split(":", 1)
                p = p.strip()
                s = s.strip()
                try:
                    s_val = int(s.replace(",", ""))
                except Exception:
                    s_val = s
                parts.append({"port": p, "shipments": s_val})
        ports_json = json.dumps(parts, ensure_ascii=False)

    if not ports_json:
        ports_json = "[]"

    return ports_json, ports_flat

# Map the recurring 'Unnamed:*' columns into clearer labels the model can use
UNNAMED_MAP = {
    # end target block
    "Unnamed: 6": "end_target_percent",
    "Unnamed: 7": "end_target_base_year",
    "Unnamed: 8": "end_target_year",
    "Unnamed: 9": "end_target_status",
    "Unnamed: 10": "last_updated",
    # interim target block
    "Unnamed: 12": "interim_target_year",
    "Unnamed: 13": "interim_target_percent",
    "Unnamed: 14": "interim_base_year",
    # integrity / scopes / extras
    "Unnamed: 16": "covers_scope_1",
    "Unnamed: 17": "covers_scope_2",
    "Unnamed: 18": "covers_scope_3",
    "Unnamed: 19": "notes",
    "Unnamed: 20": "uses_offsets",
    "Unnamed: 21": "measures_detail",
    "Unnamed: 22": "reporting_cycle",
    # context / financials
    "Unnamed: 24": "revenue",
    "Unnamed: 25": "sector",
    "Unnamed: 26": "employees",
    "Unnamed: 28": "reference_year",
}

def relabel_keys(record: Dict[str, Any]) -> Dict[str, Any]:
    out = {}
    for k, v in record.items():
        out[UNNAMED_MAP.get(k, k)] = v
    return out

def row_to_payload(row: pd.Series) -> Dict[str, Any]:
    ports_json, ports_flat = choose_ports(row)
    try:
        ports_list = json.loads(ports_json)
    except Exception:
        ports_list = []

    skip_cols = {"ports_flat", "top_west_coast_ports", "match_method", "matched_name_in_esg"}

    esg_fields: Dict[str, Any] = {}
    for col, val in row.items():
        if col in skip_cols or col == "company":
            continue
        if pd.isna(val):
            continue
        sval = str(val)
        if len(sval) > 1200:
            sval = sval[:1200] + "…"
        esg_fields[col] = sval

    esg_fields = relabel_keys(esg_fields)

    return {
        "company": str(row.get("company", "")).strip(),
        "ports": ports_list,
        "ports_flat": ports_flat,
        "esg_fields": esg_fields,
    }

# =========================
# JSON parsing + normalization
# =========================

def _first_json_object(text: str) -> Optional[str]:
    t = (text or "").strip()
    try:
        json.loads(t); return t
    except Exception:
        pass
    candidates = re.findall(r"\{[\s\S]*\}", t)
    for c in candidates:
        try:
            json.loads(c); return c
        except Exception:
            continue
    start_idx, depth = None, 0
    for i, ch in enumerate(t):
        if ch == "{":
            if depth == 0: start_idx = i
            depth += 1
        elif ch == "}":
            if depth > 0:
                depth -= 1
                if depth == 0 and start_idx is not None:
                    cand = t[start_idx:i+1]
                    try:
                        json.loads(cand); return cand
                    except Exception:
                        start_idx = None
                        continue
    return None

def parse_model_json(text: str) -> Dict[str, Any]:
    t = (text or "").strip()
    try:
        return json.loads(t)
    except Exception:
        cand = _first_json_object(t)
        if cand:
            try:
                return json.loads(cand)
            except Exception:
                pass
        return {"company": None, "ports": None, "esg_summary": t}

def normalize_model_object(obj: Dict[str, Any]) -> Dict[str, Any]:
    kmap = {
        "company": ["company", "Company", "org", "organization", "name"],
        "ports": ["ports", "Ports", "port_summary", "portSummary"],
        "esg_summary": ["esg_summary", "esgSummary", "summary", "ESG_summary", "esg"],
    }
    out: Dict[str, Any] = {}
    for target, aliases in kmap.items():
        val = None
        for a in aliases:
            if a in obj and obj[a] not in (None, ""):
                val = obj[a]; break
        out[target] = val

    v = out.get("esg_summary")
    if isinstance(v, dict) and "text" in v:
        out["esg_summary"] = v["text"]
    elif isinstance(v, list):
        out["esg_summary"] = " ".join(str(x) for x in v if x)
    elif v is not None and not isinstance(v, str):
        out["esg_summary"] = str(v)

    vp = out.get("ports")
    if vp is not None and not isinstance(vp, str):
        try:
            out["ports"] = json.dumps(vp, ensure_ascii=False)
        except Exception:
            out["ports"] = None

    return out

# =========================
# OpenAI calls + fallbacks
# =========================

# Broad retryable exceptions
_RETRY_EXC = (
    getattr(openai, "APIError", Exception),
    getattr(openai, "APIStatusError", Exception),
    getattr(openai, "APIConnectionError", Exception),
    getattr(openai, "RateLimitError", Exception),
    getattr(openai, "APITimeoutError", Exception),
)

def _extract_text_from_responses(resp: Any, debug: bool=False) -> str:
    """
    Robustly extract assistant text from a Responses API object.
    We ignore .output_text and walk the raw structure to collect any text segments.
    """
    # 1) Try the convenience first (cheap if it works in their SDK)
    try:
        t = getattr(resp, "output_text", None)
        if isinstance(t, str) and t.strip():
            return t
    except Exception:
        pass

    # 2) Walk .output -> .content -> text
    try:
        outputs = getattr(resp, "output", None)
        if outputs:
            chunks = []
            for o in outputs:
                content = getattr(o, "content", None) or o.get("content", [])
                for part in content:
                    # SDK objects may expose .type / .text or dicts
                    ptype = getattr(part, "type", None) or part.get("type")
                    if ptype == "output_text" or ptype == "text":
                        text = getattr(part, "text", None) or part.get("text")
                        if text: chunks.append(text)
                    elif ptype in ("message", "assistant"):
                        # nested shapes
                        text = (getattr(part, "content", None) or part.get("content"))
                        if isinstance(text, str) and text.strip():
                            chunks.append(text)
            joined = "\n".join(chunks).strip()
            if joined:
                return joined
    except Exception as e:
        if debug:
            logging.debug("Responses payload walk failed: %s", e)

    # 3) Fall back to raw dict serialization search
    try:
        raw = resp.model_dump() if hasattr(resp, "model_dump") else resp.__dict__
        s = json.dumps(raw, ensure_ascii=False)
        # try to fish out any "text":"..." fragments
        matches = re.findall(r'"text"\s*:\s*"([^"]+)"', s)
        if matches:
            return "\n".join(m for m in matches if m).strip()
    except Exception:
        pass

    return ""

@retry(
    reraise=True,
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1.5, min=1, max=8),
    retry=retry_if_exception_type(_RETRY_EXC),
)
def call_openai_responses(client: OpenAI, model: str, user_input: str, debug: bool) -> str:
    resp = client.responses.create(
        model=model,
        instructions=SYSTEM_INSTRUCTIONS,
        input=user_input,
        max_output_tokens=300,
    )
    text = _extract_text_from_responses(resp, debug)
    return text

@retry(
    reraise=True,
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1.5, min=1, max=8),
    retry=retry_if_exception_type(_RETRY_EXC),
)
def call_openai_chat(client: OpenAI, model: str, user_input: str) -> str:
    """
    Fallback to the classic Chat Completions API.
    """
    resp = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": SYSTEM_INSTRUCTIONS},
            {"role": "user", "content": user_input},
        ],
        max_tokens=350,
        temperature=0,
    )
    try:
        return (resp.choices[0].message.content or "").strip()
    except Exception:
        return ""

def call_openai_with_fallback(client: OpenAI, model: str, user_input: str, debug: bool, chat_fallback_model: Optional[str]) -> str:
    # 1) Try Responses API first
    try:
        text = call_openai_responses(client, model, user_input, debug)
        if text.strip():
            return text
        if debug:
            logging.debug("Responses returned empty text; switching to Chat Completions fallback.")
    except Exception as e:
        if debug:
            logging.debug("Responses call failed (%s); switching to Chat Completions.", e)

    # 2) Try Chat Completions with the same model
    text = ""
    try:
        text = call_openai_chat(client, model, user_input)
    except Exception as e:
        if debug:
            logging.debug("Chat call with model=%s failed: %s", model, e)

    # 3) Optional: fallback to a known chat-safe mini if still empty
    if (not text.strip()) and chat_fallback_model:
        if debug:
            logging.debug("Trying secondary chat model fallback: %s", chat_fallback_model)
        try:
            text = call_openai_chat(client, chat_fallback_model, user_input)
        except Exception as e:
            if debug:
                logging.debug("Secondary chat model failed: %s", e)

    return text or ""

# =========================
# Main
# =========================

def main():
    ap = argparse.ArgumentParser(description="Generate ESG summaries with robust OpenAI extraction & fallbacks.")
    ap.add_argument("--in-csv", type=str, default="west_coast_company_and_esg.csv")
    ap.add_argument("--out-csv", type=str, default="west_coast_company_and_esg_summary.csv")
    ap.add_argument("--fail-csv", type=str, default="summary_failures.csv")
    ap.add_argument("--model", type=str, default="gpt-5-mini", help="Primary model (used for Responses API and chat fallback).")
    ap.add_argument("--chat-fallback-model", type=str, default="gpt-4o-mini", help="Secondary fallback if both Responses and chat (primary) return empty.")
    ap.add_argument("--max-rows", type=int, default=0, help="0 = all rows")
    ap.add_argument("--debug", type=int, default=0, help="1 = verbose")
    ap.add_argument("--stop-on-first-error", action="store_true")
    args = ap.parse_args()

    setup_logging(args.debug)

    api_key = os.environ.get("OPENAI_API_KEY") or ""
    masked = api_key[:6] + "..." if api_key else "<MISSING>"
    logging.info("OpenAI SDK version: %s", getattr(openai, "__version__", "unknown"))
    logging.info("OPENAI_API_KEY: %s", masked)
    logging.info("Model: %s (chat fallback: %s)", args.model, args.chat_fallback_model)
    logging.info("Input CSV: %s", args.in_csv)

    if not api_key:
        raise RuntimeError("OPENAI_API_KEY not found. Put it in .env or export it.")

    df = pd.read_csv(args.in_csv, dtype=str)
    total_len = len(df)
    if args.max_rows and args.max_rows > 0:
        df = df.head(args.max_rows)
    logging.info("CSV rows (requested/total): %d/%d", len(df), total_len)
    logging.debug("CSV columns: %s", list(df.columns))

    client = OpenAI(api_key=api_key)

    # Simple ping to verify access; don't depend on output_text here
    try:
        ping = client.responses.create(
            model=args.model,
            instructions="Return exactly OK.",
            input="OK",
            max_output_tokens=20,
        )
        ping_txt = _extract_text_from_responses(ping, debug=bool(args.debug))
        logging.info("Ping response (truncated): %s", (ping_txt or "")[:100].replace("\n", " "))
    except Exception as e:
        logging.exception("Ping failed. Check key/model access.")
        raise

    if len(df) and args.debug:
        sample_payload = row_to_payload(df.iloc[0])
        sample_user = build_prompt_input(sample_payload)
        logging.debug("Sample company: %s", sample_payload.get("company"))
        logging.debug("Sample prompt bytes: %d", len(sample_user.encode("utf-8")))
        logging.debug("Sample prompt preview:\n%s", sample_user[:800])

    out_rows: List[Dict[str, str]] = []
    fail_rows: List[Dict[str, str]] = []

    for idx, (_, row) in enumerate(tqdm(df.iterrows(), total=len(df), desc="Summarizing")):
        try:
            payload = row_to_payload(row)
            user_input = build_prompt_input(payload)

            text = call_openai_with_fallback(
                client=client,
                model=args.model,
                user_input=user_input,
                debug=bool(args.debug),
                chat_fallback_model=args.chat_fallback_model,
            )

            if args.debug:
                logging.debug("Model raw text (first 300): %r", (text or "")[:300])

            obj = parse_model_json(text) if text.strip().startswith("{") else {"esg_summary": text}
            obj_norm = normalize_model_object(obj)

            company = payload["company"] or (obj_norm.get("company") or "")

            ports_json, ports_flat = choose_ports(row)
            if ports_flat:
                ports_out = ports_flat
            else:
                try:
                    parts = json.loads(ports_json)
                    ports_out = " | ".join(f"{p.get('port')}: {p.get('shipments')}" for p in parts) or "not disclosed"
                except Exception:
                    ports_out = "not disclosed"

            model_ports = obj_norm.get("ports")
            if isinstance(model_ports, str) and model_ports.strip() and model_ports.strip().lower() != "not disclosed":
                ports_out = model_ports.strip()

            esg_summary = (obj_norm.get("esg_summary") or "").strip()
            if not esg_summary:
                esg_summary = "not disclosed" if not text.strip() else text.strip()

            out_rows.append({
                "company": company,
                "ports": ports_out,
                "esg_summary": esg_summary,
            })

            if args.debug and (not esg_summary or esg_summary == "not disclosed"):
                logging.debug("Placeholder summary for %s. Raw preview: %r", company, (text or "")[:200])

        except Exception as e:
            err = {
                "company": str(row.get("company", "")),
                "error_class": e.__class__.__name__,
                "error": str(e),
            }
            if args.debug:
                logging.exception("Row %d FAILED for %s", idx, err["company"])
            fail_rows.append(err)
            if args.stop_on_first_error:
                break

    pd.DataFrame(out_rows).to_csv(args.out_csv, index=False, encoding="utf-8-sig")
    if fail_rows:
        pd.DataFrame(fail_rows).to_csv(args.fail_csv, index=False, encoding="utf-8-sig")

    logging.info("Wrote %d summaries -> %s", len(out_rows), args.out_csv)
    if fail_rows:
        logging.warning("%d failures -> %s", len(fail_rows), args.fail_csv)
        logging.warning("First failures preview: %s", json.dumps(fail_rows[:5], indent=2)[:1200])


if __name__ == "__main__":
    main()
