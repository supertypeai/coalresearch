"""
compute_peers.py — Peer Performance Insight computation pipeline.

Computes the `peers` column on the `company` table by:
1. Querying local SQLite (company + company_performance)
2. Querying Supabase (idx_combine_financials_annual) for financial data
3. Calculating PeerPerformanceInsight per company per commodity type
4. Storing the result as JSON on each company row, grouped by commodity type

Usage:
    python scripts/compute_peers.py

Requires:
    - .env file with NEXT_PUBLIC_SUPABASE_URL and NEXT_PUBLIC_SUPABASE_ANON_KEY
    - db.sqlite (local SQLite database)
"""

import os
import json
import sqlite3
from collections import defaultdict

from dotenv import load_dotenv
import requests

DB_PATH = "db.sqlite"


# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------

def get_local_connection():
    """Open a connection to the local SQLite database."""
    return sqlite3.connect(DB_PATH)


# ---------------------------------------------------------------------------
# Step 1 — Query local SQLite for companies + performance
# ---------------------------------------------------------------------------

def query_companies_and_performance(cursor):
    """
    Fetch all publicly-traded companies with their latest performance record
    per commodity type.

    Returns a list of dicts:
        {company_id, company_name, slug, symbol, year,
         commodity_type, commodity_sub_type, commodity_stats}
    """
    query = """
        SELECT
          c.id            AS company_id,
          c.name          AS company_name,
          c.slug,
          c.symbol,
          cp.year,
          cp.commodity_type,
          cp.commodity_sub_type,
          cp.commodity_stats
        FROM company c
        INNER JOIN company_performance cp ON cp.company_id = c.id
        WHERE c.symbol IS NOT NULL
          AND c.key_operation != 'Mining Services'
          AND (cp.company_id, cp.commodity_type, cp.year) IN (
            SELECT cp2.company_id, cp2.commodity_type, MAX(cp2.year)
            FROM company_performance cp2
            GROUP BY cp2.company_id, cp2.commodity_type
          )
        ORDER BY c.id, cp.commodity_type
    """
    cursor.execute(query)
    rows = cursor.fetchall()

    results = []
    seen = set()  # track (company_id, commodity_type) to deduplicate
    for row in rows:
        company_id = row[0]
        commodity_type = row[5]
        key = (company_id, commodity_type)
        if key in seen:
            continue
        seen.add(key)

        raw_stats = row[7]
        stats = {}
        if raw_stats:
            try:
                stats = json.loads(raw_stats)
            except json.JSONDecodeError:
                stats = {}

        results.append({
            "company_id": company_id,
            "company_name": row[1],
            "slug": row[2],
            "symbol": row[3],
            "year": row[4],
            "commodity_type": commodity_type,
            "commodity_sub_type": row[6],
            "commodity_stats": stats,
        })
    return results


# ---------------------------------------------------------------------------
# Step 2 — Query Supabase for financial data
# ---------------------------------------------------------------------------

def query_financials_from_supabase(symbols):
    """
    Fetch revenue, earnings, and total_assets from the Supabase
    idx_combine_financials_annual table for the given stock symbols.

    Individual queries per symbol are used because the PostgREST `in.()`
    clause can produce 500 errors for certain symbol combinations.

    Deduplication rule:
        Per symbol, prefer the row where all three fields (revenue, earnings,
        total_assets) are non-null.  Among rows with equal completeness, pick
        the most recent date.

    Returns:
        dict[symbol] \u2192 {revenue, earnings, total_assets}
    """
    load_dotenv()
    supabase_url = os.getenv("NEXT_PUBLIC_SUPABASE_URL")
    supabase_key = os.getenv("NEXT_PUBLIC_SUPABASE_ANON_KEY")

    if not supabase_url or not supabase_key:
        print(
            "  WARNING: Supabase credentials not found in .env. "
            "Financial metrics (grossMargin, roa) will be null."
        )
        return {}

    # Filter out None/empty symbols and deduplicate
    symbols_list = sorted(set(s for s in symbols if s))
    if not symbols_list:
        return {}

    headers = {
        "apikey": supabase_key,
        "Authorization": f"Bearer {supabase_key}",
        "Accept": "application/json",
    }

    url = f"{supabase_url.rstrip('/')}/rest/v1/idx_combine_financials_annual"

    # Query each symbol individually for reliability
    all_data = []
    for sym in symbols_list:
        try:
            response = requests.get(
                    url,
                    headers=headers,
                    params=[
                        ("select", "symbol,date,revenue,earnings,total_assets,cost_of_revenue,gross_profit,ebitda,capital_expenditure,total_debt,net_debt,free_cash_flow"),
                        ("symbol", f"eq.{sym}"),
                        ("date", "gte.2024-01-01"),
                        ("date", "lte.2024-12-31"),
                        ("order", "date.desc"),
                        ("limit", 5),
                    ],
                    timeout=15,
                )
            response.raise_for_status()
            all_data.extend(response.json())
        except requests.RequestException as e:
            print(f"  WARNING: Could not fetch financials for {sym}: {e}")
            continue

    data = all_data
    if not data:
        print("  WARNING: No financial data returned from Supabase.")
        return {}

    # Deduplicate per symbol
    financials = {}

    FINANCIAL_FIELDS = [
        "revenue", "earnings", "total_assets", "cost_of_revenue",
        "gross_profit", "ebitda", "capital_expenditure",
        "total_debt", "net_debt", "free_cash_flow",
    ]

    def completeness(rec):
        return sum(1 for f in FINANCIAL_FIELDS if rec.get(f) is not None)

    for row in data:
        symbol = row.get("symbol")
        if not symbol:
            continue

        current = financials.get(symbol)
        new_comp = completeness(row)

        if current is None or new_comp > completeness(current):
            financials[symbol] = {f: row.get(f) for f in FINANCIAL_FIELDS}

    print(f"  -> Fetched financial data for {len(financials)} symbols from Supabase")
    return financials


# ---------------------------------------------------------------------------
# Step 3 — Calculation helpers
# ---------------------------------------------------------------------------

def calculate_average_grade(commodity_type, stats):
    """
    Derive the average grade value and unit from the first product entry.

    Returns:
        dict with "value" (number) and "unit" (str), or None if unavailable.

    Rules (per PEERS_DATA_PIPELINE.md):
        Coal    \u2192 products[0].calorific_value_kcal  \u2192 avg(min, max) \u2192 value, unit="Kcal/kg"
        Nickel  \u2192 products[0].Ni_pct                \u2192 avg(min, max) \u2192 value, unit="% Ni"
        Gold    \u2192 products[0].Au_g_per_ton          \u2192 use max       \u2192 value, unit="g/t Au"
        Silver  → products[0].Ag_g_per_ton          → use max       → value, unit="g/t Ag"
        Copper  \u2192 products[0].Cu_pct                \u2192 avg(min, max) \u2192 value, unit="% Cu"
    """
    products = stats.get("products")
    if not products or not isinstance(products, list) or len(products) == 0:
        return None

    product = products[0]
    ctype = commodity_type.lower()

    if ctype == "coal":
        cv = product.get("calorific_value_kcal")
        if isinstance(cv, dict):
            lo, hi = cv.get("min"), cv.get("max")
            if lo is not None and hi is not None:
                return {"value": round((lo + hi) / 2), "unit": "Kcal/kg"}

    elif ctype == "nickel":
        ni = product.get("Ni_pct")
        if isinstance(ni, dict):
            lo, hi = ni.get("min"), ni.get("max")
            if lo is not None and hi is not None:
                return {"value": round((lo + hi) / 2, 2), "unit": "% Ni"}

    elif ctype == "gold":
        au = product.get("Au_g_per_ton")
        if isinstance(au, dict):
            hi = au.get("max")
            if hi is not None:
                return {"value": round(hi, 1), "unit": "g/t Au"}

    elif ctype == "silver":
        au = product.get("Ag_g_per_ton")
        if isinstance(au, dict):
            hi = au.get("max")
            if hi is not None:
                return {"value": round(hi, 1), "unit": "g/t Ag"}

    elif ctype == "copper":
        cu = product.get("Cu_pct")
        if isinstance(cu, dict):
            lo, hi = cu.get("min"), cu.get("max")
            if lo is not None and hi is not None:
                return {"value": round((lo + hi) / 2, 2), "unit": "% Cu"}

    return None


def calculate_implied_lom(commodity_type, stats):
    """
    Life of Mine = total_reserves / annual_production (both > 0).

    Commodity-aware dispatch using the correct reserve field and unit
    conversion so that reserves and production are dimensionally matched:

      Coal    total_reserves_Mt (Mt)       / production_volume (Mt)
      Gold    Au_reserves_koz (koz)        / production_volume (koz)
      Silver  Ag_reserves_koz (koz) → kg  / production_volume (kg)
      Copper  Cu_reserves_Mt (Mt) → kton / production_volume (kton)
      Nickel  total_reserves_wmt (WMT)     / production_volume (wmt)
    """
    reserves = stats.get("resources_reserves")
    if not isinstance(reserves, dict):
        return None

    annual_production = stats.get("production_volume")
    if annual_production is None or annual_production <= 0:
        return None

    ctype = commodity_type.lower() if commodity_type else ""

    if ctype == "coal":
        total = reserves.get("total_reserves_Mt")
        if total and total > 0:
            return round(total / annual_production, 2)

    elif ctype == "gold":
        total = reserves.get("Au_reserves_koz")
        if total and total > 0:
            return round(total / annual_production, 2)

    elif ctype == "silver":
        total_koz = reserves.get("Ag_reserves_koz")
        if total_koz and total_koz > 0:
            total_kg = total_koz * 31.1035
            return round(total_kg / annual_production, 2)

    elif ctype == "copper":
        total = reserves.get("Cu_reserves_Mt")
        if total and total > 0:
            total_kton = total * 1_000
            return round(total_kton / annual_production, 2)

    elif ctype == "nickel":
        total = reserves.get("total_reserves_wmt")
        if total and total > 0:
            return round(total / annual_production, 2)

    return None


# ---------------------------------------------------------------------------
# Peer averages
# ---------------------------------------------------------------------------

PEER_AVG_METRICS = [
    "gross_margin", "roa", "strip_ratio", "life_of_mine",
    "cost_per_unit", "revenue_per_unit", "earnings_per_unit", "ebitda_per_unit",
]


def compute_and_attach_peer_averages(insights):
    """
    For each metric, compute the average across the peer group and attach
    it as `peer_avg_{metric}` to every record in the list.
    """
    for metric in PEER_AVG_METRICS:
        values = [i[metric] for i in insights if i.get(metric) is not None]
        avg = round(sum(values) / len(values), 4) if values else None
        for i in insights:
            i[f"peer_avg_{metric}"] = avg


def calculate_peer_insight(record, financials):
    """
    Build a single PeerPerformanceInsight dict from a performance record
    and its matching financial data (matched by symbol).
    """
    stats = record["commodity_stats"]
    fin = financials.get(record["symbol"]) or {}

    # --- Production volume ---
    production_volume = stats.get("production_volume") or None

    # --- Production volume unit ---
    production_volume_unit = stats.get("unit")

    # --- Life of Mine ---
    life_of_mine = calculate_implied_lom(record["commodity_type"], stats)

    # --- Strip ratio ---
    strip_ratio = stats.get("strip_ratio") or None

    # --- Average grade ---
    grade = calculate_average_grade(record["commodity_type"], stats)
    avg_grade = grade["value"] if grade else None
    avg_grade_unit = grade["unit"] if grade else None

    # --- Total reserves (commodity-aware) ---
    reserves = stats.get("resources_reserves")
    total_reserves_mt = None
    total_reserves_unit = None
    if isinstance(reserves, dict):
        ctype = record["commodity_type"].lower() if record["commodity_type"] else ""
        if ctype == "coal":
            total_reserves_mt = reserves.get("total_reserves_Mt")
            total_reserves_unit = "Mt"
        elif ctype == "gold":
            total_reserves_mt = reserves.get("Au_reserves_koz")
            total_reserves_unit = "koz"
        elif ctype == "silver":
            total_reserves_mt = reserves.get("Ag_reserves_koz")
            total_reserves_unit = "koz"
        elif ctype == "copper":
            total_reserves_mt = reserves.get("Cu_reserves_Mt")
            total_reserves_unit = "Mt"
        elif ctype == "nickel":
            total_reserves_mt = reserves.get("total_reserves_wmt")
            total_reserves_unit = "WMT"

    # --- Financial metrics ---
    rev = fin.get("revenue")
    cogs = fin.get("cost_of_revenue")
    earn = fin.get("earnings")
    assets = fin.get("total_assets")
    ebitda = fin.get("ebitda")

    # Gross margin: gross_profit / revenue
    gross_profit = fin.get("gross_profit")
    gross_margin = None
    if gross_profit is not None and rev is not None and rev > 0:
        gross_margin = round(gross_profit / rev, 4)

    # ROA: earnings / total_assets
    roa = None
    if assets is not None and earn is not None and assets > 0:
        roa = round(earn / assets, 4)

    # --- Unit economics (require production_volume) ---
    revenue_per_unit = None
    cost_per_unit = None
    earnings_per_unit = None
    ebitda_per_unit = None

    if production_volume and production_volume > 0:
        if rev is not None:
            revenue_per_unit = round(rev / production_volume, 2)
        if cogs is not None:
            cost_per_unit = round(cogs / production_volume, 2)
        if earn is not None:
            earnings_per_unit = round(earn / production_volume, 2)
        if ebitda is not None:
            ebitda_per_unit = round(ebitda / production_volume, 2)

    return {
        "company_name": record["company_name"],
        "symbol": record["symbol"],
        "slug": record["slug"],
        "production_volume": production_volume,
        "production_volume_unit": production_volume_unit,
        "life_of_mine": life_of_mine,
        "strip_ratio": strip_ratio,
        "avg_grade": avg_grade,
        "avg_grade_unit": avg_grade_unit,
        "revenue": rev,
        "cogs": cogs,
        "earnings": earn,
        "ebitda": ebitda,
        "assets": assets,
        "gross_profit": gross_profit,
        "gross_margin": gross_margin,
        "roa": roa,
        "revenue_per_unit": revenue_per_unit,
        "cost_per_unit": cost_per_unit,
        "earnings_per_unit": earnings_per_unit,
        "ebitda_per_unit": ebitda_per_unit,
        "total_reserves_mt": total_reserves_mt,
        "total_reserves_unit": total_reserves_unit,
        "commodity_sub_type": record.get("commodity_sub_type"),
    }


# ---------------------------------------------------------------------------
# Main orchestrator
# ---------------------------------------------------------------------------

def compute_all_peers():
    """Query, calculate, assemble, and write peer data for every company."""
    print("=" * 60)
    print("  PEER PERFORMANCE INSIGHT — Computation Pipeline")
    print("=" * 60)

    conn = get_local_connection()
    cursor = conn.cursor()

    # ---- Step 1: Companies + performance ----
    print("\n[1/4] Querying companies and performance data...")
    records = query_companies_and_performance(cursor)
    print(f"  -> Found {len(records)} company-commodity records")

    if not records:
        print("  -> No data to process.")
        conn.close()
        return

    # ---- Step 2: Financials from Supabase ----
    unique_symbols = sorted(set(r["symbol"] for r in records if r["symbol"]))
    print(f"\n[2/4] Fetching financial data for {len(unique_symbols)} symbols ...")
    financials = query_financials_from_supabase(unique_symbols)

    # ---- Step 3: Calculate insights & assemble peer groups ----
    print("\n[3/4] Calculating peer insights and grouping by commodity...")

    # commodity_type -> [PeerPerformanceInsight, ...]
    commodity_peers_map = defaultdict(list)
    # company_id -> set of commodity types the company has
    company_commodity_types = defaultdict(set)

    for rec in records:
        insight = calculate_peer_insight(rec, financials)
        commodity_peers_map[rec["commodity_type"]].append(insight)
        company_commodity_types[rec["company_id"]].add(rec["commodity_type"])

    # Compute and attach peer averages per commodity group
    for ctype, insights in commodity_peers_map.items():
        compute_and_attach_peer_averages(insights)

    # company_id -> {commodity_type -> [PeerPerformanceInsight, ...]}
    # Each company's own insight is EXCLUDED from its own peer lists.
    company_peers_map_final = defaultdict(lambda: defaultdict(list))

    for rec in records:
        cid = rec["company_id"]
        ctype = rec["commodity_type"]

        if ctype == "Silver":
            continue

        for peer_insight in commodity_peers_map[ctype]:
            company_peers_map_final[cid][ctype].append(peer_insight)

    # ---- Step 4: Write back to database ----
    print("[4/4] Writing peers to company table...")
    update_cursor = conn.cursor()
    updated_count = 0

    for company_id, peers_by_type in company_peers_map_final.items():
        # Only include commodity types that actually have peer entries
        filtered = {ct: peers for ct, peers in peers_by_type.items() if peers}
        peers_json = json.dumps(filtered, ensure_ascii=False) if filtered else None

        update_cursor.execute(
            "UPDATE company SET peers = ? WHERE id = ?",
            (peers_json, company_id),
        )
        updated_count += 1

    conn.commit()
    print(f"  -> Updated {updated_count} companies with peer data")

    # Sanity check
    cursor.execute("SELECT COUNT(*) FROM company WHERE peers IS NOT NULL")
    count = cursor.fetchone()[0]
    print(f"  -> Total companies with non-null peers: {count}")

    conn.close()
    print("\n✓ Peer computation complete!")


if __name__ == "__main__":
    compute_all_peers()
