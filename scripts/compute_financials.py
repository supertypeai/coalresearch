"""
compute_financials.py — Company financial history + LLM narrative pipeline.

Computes the `financial` column on the `company` table by:
1. Querying local SQLite for companies that have a stock symbol
2. Querying Supabase (idx_combine_financials_annual) for the full financial
   history per symbol
3. Normalizing each annual row to a `FinancialAnnual` (mirrors
   `getCompanyFinancials.ts` field-for-field, including derived ratios)
4. Generating a 5-category narrative via a local LLM proxy at
   `http://localhost:20128/v1/chat/completions` (gemini/gemini-3.1-flash-lite-preview),
   mirroring `HistoricalFinancialNarrative.tsx` field-for-field
5. Storing the wrapper `{annual, narrative, generated_at}` as JSON on each
   company row

The stored shape MUST match `financial-restructure-plan.md` §2.1
field-for-field, so the frontend (`company.financial`) can read it directly
without any Supabase call or local LLM call.

Usage:
    python scripts/compute_financials.py

Requires:
    - .env file with NEXT_PUBLIC_SUPABASE_URL, NEXT_PUBLIC_SUPABASE_ANON_KEY
    - Local LLM proxy running at http://localhost:20128
    - db.sqlite (local SQLite database)
"""

import os
import sys
import json
import re
import sqlite3
from datetime import datetime, timezone

from dotenv import load_dotenv
import requests
from pydantic import BaseModel, Field


DB_PATH = "db.sqlite"


# ---------------------------------------------------------------------------
# Pydantic models — must match `financial-restructure-plan.md` §2.1 / §3.2
# ---------------------------------------------------------------------------
# Note on `class`: it's a Python keyword, so the Pydantic field uses `class_`
# with `alias="class"` and `populate_by_name=True`. When serializing back to
# JSON for the column, use `model_dump_json(by_alias=True)` so the stored key
# is `"class"` (matching the frontend type).


class BreakdownItem(BaseModel):
    class_: str = Field(alias="class")
    category: str
    amount: float

    model_config = {"populate_by_name": True}


class FinancialAnnual(BaseModel):
    year: int
    revenue: float
    cost_of_revenue: float
    total_assets: float
    earnings: float
    net_profit: float           # alias of earnings, kept for frontend compat
    ebitda: float
    capital_expenditure: float
    free_cash_flow: float
    net_debt: float
    total_equity: float
    inventories: float
    fixed_assets: float
    # derived ratios
    ebitda_margin: float        # ebitda / revenue
    fcf_yield: float            # free_cash_flow / revenue
    capex_to_revenue: float     # capital_expenditure / revenue
    net_debt_to_ebitda: float   # net_debt / ebitda
    asset_turnover: float       # revenue / total_assets
    # breakdown arrays
    revenue_breakdown: list[BreakdownItem]
    cost_of_revenue_breakdown: list[BreakdownItem]


class FinancialNarrative(BaseModel):
    summary: str
    earnings_power: str
    cash_control: str
    balance_sheet_risk: str
    operational_health: str


class CompanyFinancial(BaseModel):
    annual: list[FinancialAnnual]
    narrative: FinancialNarrative
    generated_at: str | None = None      # ISO 8601


# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------

def get_local_connection():
    """Open a connection to the local SQLite database."""
    return sqlite3.connect(DB_PATH)


# ---------------------------------------------------------------------------
# Step 1 — Query local SQLite for companies with a stock symbol
# ---------------------------------------------------------------------------

def query_companies_with_symbol(cursor):
    """
    Fetch every company row that has a stock ticker.

    Returns:
        list[dict] with {company_id, company_name, slug, symbol}
    """
    query = """
        SELECT id, name, slug, symbol
        FROM company
        WHERE symbol IS NOT NULL AND symbol != ''
        ORDER BY id
    """
    cursor.execute(query)
    rows = cursor.fetchall()
    return [
        {
            "company_id": row[0],
            "company_name": row[1],
            "slug": row[2],
            "symbol": row[3],
        }
        for row in rows
    ]


# ---------------------------------------------------------------------------
# Step 2 — Pull raw rows from Supabase
# ---------------------------------------------------------------------------

def fetch_raw_financials(symbol):
    """
    Pull every annual row from Supabase `idx_combine_financials_annual` for
    the given stock symbol, ascending by date (latest year is last).

    Mirrors the SELECT clause in `financial-restructure-plan.md` §3.4 and
    `getCompanyFinancials.ts`. We pull the FULL history (no date filter,
    no limit) — §3.5 says to store the full `annual` array and let the
    frontend slice to 5 where needed.

    Returns:
        list[dict] of raw Supabase rows (may be empty).
    """
    load_dotenv()
    supabase_url = os.getenv("NEXT_PUBLIC_SUPABASE_URL")
    supabase_key = os.getenv("NEXT_PUBLIC_SUPABASE_ANON_KEY")

    if not supabase_url or not supabase_key:
        print(
            "  WARNING: Supabase credentials not found in .env. "
            "Financial data will be skipped."
        )
        return []

    headers = {
        "apikey": supabase_key,
        "Authorization": f"Bearer {supabase_key}",
        "Accept": "application/json",
    }

    url = f"{supabase_url.rstrip('/')}/rest/v1/idx_combine_financials_annual"

    select_cols = (
        "date, revenue, cost_of_revenue, earnings, revenue_breakdown, "
        "operating_expense_breakdown, total_assets, ebitda, "
        "capital_expenditure, free_cash_flow, net_debt, total_equity, "
        "inventories, fixed_assets"
    )

    try:
        response = requests.get(
            url,
            headers=headers,
            params=[
                ("select", select_cols),
                ("symbol", f"eq.{symbol}"),
                ("order", "date.asc"),
            ],
            timeout=30,
        )
        response.raise_for_status()
        return response.json() or []
    except requests.RequestException as e:
        print(f"  WARNING: Could not fetch financials for {symbol}: {e}")
        return []


# ---------------------------------------------------------------------------
# Step 3 — Normalize each row to `FinancialAnnual`
# ---------------------------------------------------------------------------
# Port the JS transforms from `getCompanyFinancials.ts` verbatim, per
# `financial-restructure-plan.md` §3.5 ("Do not improve, refactor, or 'fix'
# any edge cases — the goal is byte-for-byte parity").

def _breakdown(raw):
    """Normalize a revenue/operating-expense breakdown array."""
    if not isinstance(raw, list):
        return []
    return [
        BreakdownItem(
            **{
                "class": item.get("class") or "Unknown",
                "category": item.get("category") or "Unknown",
                "amount": item.get("amount") or 0,
            }
        )
        for item in raw
    ]


def normalize_row(row):
    """Convert one raw Supabase row into a `FinancialAnnual`."""
    # year from date string ("YYYY-MM-DD" or similar)
    year = datetime.fromisoformat(row["date"]).year

    revenue_breakdown = _breakdown(row.get("revenue_breakdown"))
    cost_of_revenue_breakdown = _breakdown(row.get("operating_expense_breakdown"))

    revenue             = row.get("revenue")             or 0
    earnings            = row.get("earnings")            or 0
    ebitda              = row.get("ebitda")              or 0
    cost_of_revenue     = row.get("cost_of_revenue")     or 0
    capital_expenditure = row.get("capital_expenditure") or 0
    free_cash_flow      = row.get("free_cash_flow")      or 0
    net_debt            = row.get("net_debt")            or 0
    total_equity        = row.get("total_equity")        or 0
    inventories         = row.get("inventories")         or 0
    fixed_assets        = row.get("fixed_assets")        or 0
    total_assets        = row.get("total_assets")        or 0

    # Derived ratios — keep these exact formulas (plan §3.5)
    ebitda_margin      = ebitda / revenue            if revenue      > 0 else 0
    fcf_yield          = free_cash_flow / revenue    if revenue      > 0 else 0
    capex_to_revenue   = capital_expenditure/revenue if revenue      > 0 else 0
    net_debt_to_ebitda = net_debt / ebitda           if ebitda       > 0 else 0
    asset_turnover     = revenue / total_assets      if total_assets > 0 else 0

    return FinancialAnnual(
        year=year,
        revenue=revenue,
        cost_of_revenue=cost_of_revenue,
        total_assets=total_assets,
        earnings=earnings,
        net_profit=earnings,                      # alias — preserve old shape
        ebitda=ebitda,
        capital_expenditure=capital_expenditure,
        free_cash_flow=free_cash_flow,
        net_debt=net_debt,
        total_equity=total_equity,
        inventories=inventories,
        fixed_assets=fixed_assets,
        ebitda_margin=ebitda_margin,
        fcf_yield=fcf_yield,
        capex_to_revenue=capex_to_revenue,
        net_debt_to_ebitda=net_debt_to_ebitda,
        asset_turnover=asset_turnover,
        revenue_breakdown=revenue_breakdown,
        cost_of_revenue_breakdown=cost_of_revenue_breakdown,
    )


# ---------------------------------------------------------------------------
# Step 4 — Build the LLM payload (last 5 years only) and call the LLM
# ---------------------------------------------------------------------------
# Mirrors `HistoricalFinancialNarrative.tsx` field-for-field — system prompt,
# user-prompt layout, request shape, and response parsing all kept verbatim
# so the Python backend produces the same narrative strings as the old frontend
# code.

API_URL = "http://localhost:20128/v1/chat/completions"
MODEL = "gemini/gemini-3.1-flash-lite-preview"

SYSTEM_PROMPT = (
    "You are a financial analyst writing concise, insight-rich narrative summaries "
    "for public-market mining and metals companies. You are given up to 5 years of "
    "annual financial data grouped into four categories: earnings power, cash "
    "control, balance sheet risk, and operational health. Use only the data "
    "provided. Highlight trends, inflection points, and category-level takeaways. "
    "Currency is IDR. Do not invent numbers.\n\n"
    "You MUST respond with ONLY valid JSON (no markdown, no code fences) matching "
    "this exact schema:\n"
    "{\n"
    '  "summary": string,\n'
    '  "earnings_power": string,\n'
    '  "cash_control": string,\n'
    '  "balance_sheet_risk": string,\n'
    '  "operational_health": string\n'
    "}\n\n"
    "Constraint: The 'summary' field MUST be exactly one single sentence. "
    "Each other field should contain 1-3 sentences of prose. "
    "Do not include any other keys or commentary outside the JSON."
)


def build_llm_payload(annual, company_name):
    """Build the 4-category payload over the last 5 years (plan \u00a73.6)."""
    last5 = annual[-5:]
    earnings_power = [
        {
            "year": str(a.year),
            "revenue": a.revenue,
            "cost_of_revenue": a.cost_of_revenue,
            "ebitda": a.ebitda,
            "earnings": a.earnings,
        }
        for a in last5
    ]
    cash_control = [
        {
            "year": str(a.year),
            "capital_expenditure": a.capital_expenditure,
            "free_cash_flow": a.free_cash_flow,
        }
        for a in last5
    ]
    balance_sheet_risk = [
        {
            "year": str(a.year),
            "net_debt": a.net_debt,
            "total_equity": a.total_equity,
        }
        for a in last5
    ]
    operational_health = [
        {
            "year": str(a.year),
            "inventories": a.inventories,
            "fixed_assets": a.fixed_assets,
        }
        for a in last5
    ]
    return {
        "earnings_power": earnings_power,
        "cash_control": cash_control,
        "balance_sheet_risk": balance_sheet_risk,
        "operational_health": operational_health,
    }, company_name


def _build_user_prompt(data, company_name):
    """Build the user prompt. Mirrors `buildUserPrompt` in the TS verbatim."""
    return (
        f"Analyze the following multi-year financial history for {company_name}.\n\n"
        f"Earnings Power (IDR):\n{json.dumps(data['earnings_power'], indent=2)}\n\n"
        f"Cash Control (IDR):\n{json.dumps(data['cash_control'], indent=2)}\n\n"
        f"Balance Sheet Risk (IDR):\n{json.dumps(data['balance_sheet_risk'], indent=2)}\n\n"
        f"Operational Health (IDR):\n{json.dumps(data['operational_health'], indent=2)}\n\n"
        'Respond with JSON only. "summary" is a single sentence overview. Each '
        "category field should cover: earnings_power (revenue/EBITDA trajectory "
        "and margin trends), cash_control (CapEx vs FCF evolution), "
        "balance_sheet_risk (leverage, equity, liquidity), operational_health "
        "(inventory and fixed asset intensity)."
    )


def _parse_narrative_response(raw):
    """
    Parse the LLM's response string into a FinancialNarrative.

    Mirrors `parseNarrativeResponse` in the TS verbatim — strip code fences,
    JSON.parse, validate all 5 string fields.
    """
    # Strip markdown code fences if the model added them despite instructions
    cleaned = raw.strip()
    if cleaned.startswith("```"):
        cleaned = re.sub(r"^```(?:json)?\s*", "", cleaned, flags=re.IGNORECASE)
        cleaned = re.sub(r"\s*```$", "", cleaned)

    parsed = json.loads(cleaned)

    required_keys = [
        "summary",
        "earnings_power",
        "cash_control",
        "balance_sheet_risk",
        "operational_health",
    ]

    for key in required_keys:
        if not isinstance(parsed.get(key), str):
            raise ValueError(f"Missing or invalid field: {key}")

    return FinancialNarrative(**parsed)


def _empty_narrative():
    """
    Per `financial-restructure-plan.md` \u00a76: on LLM failure, never write a
    malformed `narrative` object — write empty strings for all five fields
    so the frontend's `if (!narrative) return null;`-style guards still work
    and the per-year `annual` array is still usable.
    """
    return FinancialNarrative(
        summary="",
        earnings_power="",
        cash_control="",
        balance_sheet_risk="",
        operational_health="",
    )


def generate_narrative(annual, company_name):
    """
    Call the LLM proxy over the last 5 years and return a `FinancialNarrative`.

    Mirrors `useFinancialNarrative` in the TS verbatim — POST to
    `http://localhost:20128/v1/chat/completions` with the exact same request
    body (model, messages, temperature, stream: false), then extract
    `choices[0].message.content`, strip code fences, parse JSON, and validate
    the 5 required string fields.

    On any failure (network, HTTP error, JSON parse, field validation),
    returns a narrative with empty strings (plan \u00a76) rather than crashing
    the run.
    """
    payload, name = build_llm_payload(annual, company_name)
    user_prompt = _build_user_prompt(payload, name)

    request_body = {
        "model": MODEL,
        "messages": [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user_prompt},
        ],
        "temperature": 0.4,
        "stream": False,
    }

    try:
        response = requests.post(
            API_URL,
            headers={"Content-Type": "application/json"},
            json=request_body,
            timeout=60,
        )
        response.raise_for_status()
        response_json = response.json()

        raw = response_json.get("choices", [{}])[0].get("message", {}).get("content", "")
        if not raw:
            print(f"  WARNING: Empty response content for {company_name}")
            return _empty_narrative()

        result = _parse_narrative_response(raw)
        return result

    except Exception as e:  # noqa: BLE001 — broad on purpose, see docstring
        print(f"  WARNING: LLM narrative failed for {company_name}: {e}")
        return _empty_narrative()


# ---------------------------------------------------------------------------
# Main orchestrator
# ---------------------------------------------------------------------------

def compute_all_financials(symbol=None):
    """
    Query, normalize, generate narrative, and write `financial` per company.

    Args:
        symbol: Optional stock symbol to process only that company
                (e.g. "MYOH.JK"). If None, process all companies.
    """
    print("=" * 60)
    print("  COMPANY FINANCIALS + NARRATIVE — Computation Pipeline")
    print("=" * 60)

    conn = get_local_connection()
    cursor = conn.cursor()

    # ---- Step 1: Companies with symbol ----
    print("\n[1/4] Querying companies with a stock symbol...")
    companies = query_companies_with_symbol(cursor)
    print(f"  -> Found {len(companies)} listed companies")

    # ---- Optional filter to a single symbol ----
    if symbol:
        companies = [c for c in companies if c["symbol"] == symbol]
        if not companies:
            print(f"  -> No company found with symbol '{symbol}'; nothing to do.")
            conn.close()
            return
        print(f"  -> Filtered to 1 company: {companies[0]['company_name']} ({symbol})")

    if not companies:
        print("  -> No data to process.")
        conn.close()
        return

    # ---- Step 2 + 3 + 4: Per-company fetch / normalize / narrate / write ----
    print("\n[2/4] Fetching financials from Supabase...")
    print("[3/4] Normalizing rows + generating LLM narrative...")
    print("[4/4] Writing to company.financial...")

    update_cursor = conn.cursor()
    updated_count = 0
    skipped_no_data = 0
    skipped_no_symbol = 0

    for i, company in enumerate(companies, start=1):
        company_id = company["company_id"]
        name = company["company_name"]
        symbol = company["symbol"]

        if not symbol:
            skipped_no_symbol += 1
            continue

        print(f"  [{i}/{len(companies)}] {name} ({symbol})")

        # Step 2 — fetch raw rows
        raw_rows = fetch_raw_financials(symbol)
        if not raw_rows:
            print("     -> no Supabase rows; skipping")
            skipped_no_data += 1
            continue

        # Step 3 — normalize each row
        try:
            annual = [normalize_row(row) for row in raw_rows]
        except Exception as e:  # noqa: BLE001 — never crash the pipeline
            print(f"     -> normalize failed: {e}; skipping")
            skipped_no_data += 1
            continue

        if not annual:
            print("     -> no normalized rows; skipping")
            skipped_no_data += 1
            continue

        # Step 4 — LLM narrative (empty strings on failure, never crash)
        narrative = generate_narrative(annual, name)

        wrapper = CompanyFinancial(
            annual=annual,
            narrative=narrative,
            generated_at=datetime.now(timezone.utc).isoformat(),
        )

        # Serialize with by_alias=True so `class_` becomes `"class"` in JSON
        # (plan §3.2 note + §6 risk) — this is the #1 thing to get right for
        # frontend parity.
        financial_json = wrapper.model_dump_json(by_alias=True)

        update_cursor.execute(
            "UPDATE company SET financial = ? WHERE id = ?",
            (financial_json, company_id),
        )
        updated_count += 1

    conn.commit()
    print(f"\n  -> Updated {updated_count} companies with `financial` JSON")
    print(f"  -> Skipped {skipped_no_data} (no Supabase data / normalize failed)")
    print(f"  -> Skipped {skipped_no_symbol} (no symbol)")

    # Sanity check
    cursor.execute("SELECT COUNT(*) FROM company WHERE financial IS NOT NULL")
    count = cursor.fetchone()[0]
    print(f"  -> Total companies with non-null `financial`: {count}")

    conn.close()
    print("\n✓ Financials computation complete!")


if __name__ == "__main__":
    # Parse optional --symbol argument
    symbol = None
    for i, arg in enumerate(sys.argv[1:], start=1):
        if arg == "--symbol" and i + 1 < len(sys.argv):
            symbol = sys.argv[i + 1]
            break

    compute_all_financials(symbol=symbol)
