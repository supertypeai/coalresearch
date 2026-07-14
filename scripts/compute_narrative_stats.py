"""
compute_narrative_stats.py — Computes the `narrative_stats` column on the
`company` table.

Reads company, company_performance, mining_license, and company_ownership
tables from the local SQLite database and computes three narrative sections
(profile, production, financial) that the frontend's
CompanyNarrativeOverview renders directly.

The stored shape MUST match the `NarrativeStats` interface in
`src/db/schema.ts` field-for-field.

Usage:
    python scripts/compute_narrative_stats.py

    Process a single company:
    python scripts/compute_narrative_stats.py --slug pt-freeport-indonesia
"""

import json
import sqlite3
import sys
from datetime import datetime, timezone
from typing import Any


DB_PATH = "db.sqlite"

# ---------------------------------------------------------------------------
# Coal calorific-value thresholds — mirrors getCoalTypeInfo in
# mining/src/app/.../operational/CoalNarrative.tsx
# ---------------------------------------------------------------------------

COAL_CV_THRESHOLDS = {"Metallurgical": 6400, "Bituminous": 4200, "Sub-bituminous": 3800}


def get_coal_type_info(cv: float) -> str:
    if cv >= COAL_CV_THRESHOLDS["Metallurgical"]:
        return "Metallurgical"
    if cv >= COAL_CV_THRESHOLDS["Bituminous"]:
        return "Bituminous"
    if cv >= COAL_CV_THRESHOLDS["Sub-bituminous"]:
        return "Sub-bituminous"
    return "Lignite"


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

def get_conn() -> sqlite3.Connection:
    return sqlite3.connect(DB_PATH)


def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def parse_json(raw: Any) -> Any:
    """Safely parse a JSON-ish column (could be a string or already a dict)."""
    if raw is None:
        return None
    if isinstance(raw, (dict, list)):
        return raw
    try:
        return json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        return None


def to_title_case(s: str) -> str:
    """Capitalise the first letter of each word. Mirrors toTitleCase in utils.ts."""
    return " ".join(w[0].upper() + w[1:] for w in s.split(" ") if w)


def format_breakdown_label(class_name: str, category_name: str) -> str:
    """
    Mirrors formatBreakdownLabel in src/lib/formatBreakdownLabel.ts exactly.
    """
    c1val = (class_name or "").strip()
    c2val = (category_name or "").strip()

    if not c1val and not c2val:
        return "Other"
    if not c1val:
        return to_title_case(c2val)
    if not c2val:
        return to_title_case(c1val)

    c1 = c1val.lower()
    c2 = c2val.lower()

    # If identical, just return one
    if c1 == c2:
        return to_title_case(c1val)

    # --- REVENUE CLASSES (generic containers -> use category) ---
    if c1 in ("product/service sales", "product sales", "revenue",
              "construction contracts", "commission income"):
        return to_title_case(c2val)

    if "logistic" in c1 or "deliveries" in c1:
        return to_title_case(c2val)

    if "rental" in c1 or "lease" in c1:
        return to_title_case(c2val)

    if c1 == "service and maintenance":
        return to_title_case(c2val)

    if "interest income" in c1:
        return to_title_case(c2val)

    # --- COST CLASSES (broader buckets -> use class label) ---

    if "other" in c1:
        if "other" in c2:
            return "Others"
        return to_title_case(c2val)

    if c1.startswith("general"):
        if "general" in c2 or "admin" in c2:
            return "General & Administrative"
        return to_title_case(c2val)

    if c1.startswith("sales & marketing") or c1 == "selling and marketing expenses":
        if any(kw in c2 for kw in ("selling", "marketing", "sales")):
            return "Sales & Marketing"
        return to_title_case(c2val)

    if c1.startswith("salaries") or c1 == "salaries and employee benefit":
        if any(kw in c2 for kw in ("salari", "employee", "personnel", "benefit", "selling")):
            return "Salaries & Benefits"
        return to_title_case(c2val)

    if c1 == "r&d":
        return to_title_case(c2val)

    if "general and administration expenses" in c1:
        if "general" in c2 or "admin" in c2:
            return "General & Administrative"
        return to_title_case(c2val)

    # Fallback: prefer category
    return to_title_case(c2val)


# ---------------------------------------------------------------------------
# Profile stats
# ---------------------------------------------------------------------------

def compute_profile_stats(
    conn: sqlite3.Connection,
    company_id: int,
) -> dict | None:
    """
    Compute the profile narrative section.

    Mirrors computeProfileStats in mining/src/app/.../overview/narrativeStats.ts
    """
    cursor = conn.cursor()
    cursor.row_factory = dict_factory

    # -- Get company commodity_type and performances --
    cursor.execute("SELECT commodity_type FROM company WHERE id = ?", (company_id,))
    row = cursor.fetchone()
    if not row:
        return None

    company_types = parse_json(row["commodity_type"]) or []

    cursor.execute(
        """
        SELECT commodity_type, commodity_sub_type, commodity_stats
        FROM company_performance
        WHERE company_id = ?
        """,
        (company_id,),
    )
    performances = cursor.fetchall()

    # Gather commodity types from performances
    types_seen: set[str] = set()
    sub_types_seen: set[str] = set()

    for p in performances:
        t = p["commodity_type"]
        if t:
            types_seen.add(t)

        # Coal subtype via calorific value
        if t == "Coal":
            stats = parse_json(p["commodity_stats"])
            if stats and isinstance(stats, dict):
                products = stats.get("products") or []
                for prod in products:
                    cv_obj = prod.get("calorific_value_kcal")
                    if cv_obj and isinstance(cv_obj, dict):
                        cv_min = cv_obj.get("min")
                        cv_max = cv_obj.get("max")
                        if cv_min is not None and cv_max is not None:
                            cv_val = (cv_min + cv_max) / 2
                        else:
                            cv_val = cv_min or cv_max or 0
                    else:
                        cv_val = 0
                    coal_type = get_coal_type_info(cv_val)
                    if coal_type in ("Bituminous", "Sub-bituminous", "Lignite"):
                        sub_types_seen.add("Thermal")
                    else:
                        sub_types_seen.add(coal_type)
        else:
            sub = p.get("commodity_sub_type")
            if sub:
                sub_types_seen.add(sub)

    # Also add company-level commodity_types if not already present
    type_arr = list(types_seen)
    type_str = " & ".join(type_arr[:2]) if type_arr else ""

    sub_arr = list(sub_types_seen)
    sub_types_str = " and ".join(sub_arr[:2]) if sub_arr else ""

    if sub_types_str and len(type_arr) == 1 and type_arr[0] == "Coal":
        sub_types_str += " Coal"

    # -- Mining licenses --
    # Include licenses for this company AND its children
    cursor.execute(
        """
        SELECT ml.*
        FROM mining_license ml
        WHERE ml.company_id = ?
           OR ml.company_id IN (
               SELECT co.company_id
               FROM company_ownership co
               WHERE co.parent_company_id = ?
           )
        """,
        (company_id, company_id),
    )
    licenses = cursor.fetchall()

    total_licenses = len(licenses)

    if total_licenses == 0:
        return None

    provinces = list({lic["province"] for lic in licenses if lic.get("province")})

    if total_licenses == 1:
        first = licenses[0]
        lic_type = first.get("license_type") or "mining license"
        province = first.get("province") or "Indonesia"
        license_str = f", holding a {lic_type} and focusing its operations in {province}"
    elif provinces:
        formatter = __import__("locale").setlocale(__import__("locale").LC_ALL, "")
        # Simple English conjunction join
        if len(provinces) == 1:
            joined = provinces[0]
        elif len(provinces) == 2:
            joined = f"{provinces[0]} and {provinces[1]}"
        else:
            joined = ", ".join(provinces[:-1]) + f", and {provinces[-1]}"
        license_str = f", owning {total_licenses} mining licenses and operating in {joined}"
    else:
        license_str = f", owning {total_licenses} mining licenses"

    return {
        "type_str": type_str,
        "sub_types_str": sub_types_str,
        "total_licenses": total_licenses,
        "license_str": license_str,
    }


# ---------------------------------------------------------------------------
# Production stats
# ---------------------------------------------------------------------------

def compute_production_stats(
    conn: sqlite3.Connection,
    company_id: int,
) -> dict | None:
    """
    Compute the production narrative section.

    Mirrors computeProductionStats in mining/src/app/.../overview/narrativeStats.ts
    """
    cursor = conn.cursor()
    cursor.row_factory = dict_factory

    cursor.execute(
        """
        SELECT commodity_type, commodity_sub_type, year, commodity_stats
        FROM company_performance
        WHERE company_id = ?
        ORDER BY year DESC
        """,
        (company_id,),
    )
    performances = cursor.fetchall()
    if not performances:
        return None

    # Prefer Coal, otherwise use whatever's available
    coal_ps = [p for p in performances if p["commodity_type"] == "Coal" and p["year"]]
    candidates = coal_ps if coal_ps else [p for p in performances if p["year"]]

    if not candidates:
        return None

    # Use only the primary type + sub-type for consistent narrative
    candidates.sort(key=lambda p: p["year"], reverse=True)
    primary_type = candidates[0]["commodity_type"]
    primary_sub_type = candidates[0].get("commodity_sub_type")

    filtered = [
        p
        for p in candidates
        if p["commodity_type"] == primary_type and p.get("commodity_sub_type") == primary_sub_type
    ]
    # Relax if empty — some companies have null sub-types
    if not filtered:
        filtered = candidates

    if not filtered:
        return None

    latest = filtered[0]
    previous = next((p for p in filtered if p["year"] == latest["year"] - 1), None)

    stats = parse_json(latest["commodity_stats"])
    latest_vol = None
    unit = ""
    if stats and isinstance(stats, dict):
        latest_vol = stats.get("production_volume")
        unit = stats.get("unit") or ""

    if latest_vol is None:
        return None

    # Trend
    trend = None
    if previous:
        prev_stats = parse_json(previous["commodity_stats"])
        prev_vol = prev_stats.get("production_volume") if isinstance(prev_stats, dict) else None
        if prev_vol is not None and prev_vol > 0:
            pct = ((latest_vol - prev_vol) / prev_vol) * 100
            if latest_vol > prev_vol:
                direction = "up"
            elif latest_vol < prev_vol:
                direction = "down"
            else:
                direction = "stable"
            trend = {"direction": direction, "pct": round(pct, 6), "previous_vol": prev_vol}

    # Historical
    historical = None
    with_vol = [
        p
        for p in filtered
        if parse_json(p.get("commodity_stats")) and isinstance(parse_json(p.get("commodity_stats")), dict)
        and parse_json(p.get("commodity_stats")).get("production_volume") is not None
    ]
    if len(with_vol) > 1:
        vols = []
        for p in with_vol:
            s = parse_json(p["commodity_stats"])
            v = s["production_volume"]
            vols.append({"year": p["year"], "vol": v})
        avg = sum(v["vol"] for v in vols) / len(vols)
        mx = max(vols, key=lambda x: x["vol"])
        mn = min(vols, key=lambda x: x["vol"])
        historical = {
            "avg": avg,
            "count": len(vols),
            "max_year": mx["year"],
            "max_vol": mx["vol"],
            "min_year": mn["year"],
            "min_vol": mn["vol"],
        }

    return {
        "commodity_type": (primary_type or "").lower(),
        "year": latest["year"],
        "latest_vol": latest_vol,
        "unit": unit,
        "trend": trend,
        "historical": historical,
    }


# ---------------------------------------------------------------------------
# Financial stats
# ---------------------------------------------------------------------------

def compute_financial_stats(company_id: int, cursor) -> dict | None:
    """
    Compute the finance narrative section from the `financial` JSON column.

    Mirrors computeFinancialStats in mining/src/app/.../overview/narrativeStats.ts
    """
    cursor.execute(
        "SELECT financial FROM company WHERE id = ? AND financial IS NOT NULL",
        (company_id,),
    )
    row = cursor.fetchone()
    if not row or not row.get("financial"):
        return None

    financial = parse_json(row["financial"])
    if not financial or not isinstance(financial, dict):
        return None

    annual = financial.get("annual") or []
    if not annual:
        return None

    # Sort by year descending
    sorted_annual = sorted(annual, key=lambda x: x["year"], reverse=True)
    latest = sorted_annual[0]

    if latest.get("revenue") is None or latest["revenue"] <= 0:
        return None

    prev = next((a for a in sorted_annual if a["year"] == latest["year"] - 1), None)

    yoy_pct = None
    if prev and prev.get("revenue") and prev["revenue"] > 0:
        yoy_pct = ((latest["revenue"] - prev["revenue"]) / prev["revenue"]) * 100

    # Latest year with breakdown
    driver = next(
        (a for a in sorted_annual if a.get("revenue_breakdown") and len(a["revenue_breakdown"]) > 0),
        latest,
    )

    rev_breakdown = driver.get("revenue_breakdown") or []
    cost_breakdown = driver.get("cost_of_revenue_breakdown") or []

    total_revenue = driver["revenue"]
    total_costs = driver.get("cost_of_revenue", 0)
    # Revenue entries, sorted desc
    def rev_label(item):
        return format_breakdown_label(item.get("class", ""), item.get("category", ""))
    def cost_label(item):
        return format_breakdown_label(item.get("class", ""), item.get("category", ""))

    # Revenue entries, sort desc
    rev_entries = [
        {
            "label": rev_label(r),
            "pct": (r["amount"] / total_revenue) * 100 if total_revenue > 0 else 0,
            "val": r["amount"],
        }
        for r in rev_breakdown
    ]
    rev_entries.sort(key=lambda x: x["pct"], reverse=True)
    top_rev = rev_entries[:2]

    # Cost entries with Other Costs fallback
    cost_entries = [
        {
            "label": cost_label(c),
            "pct": (c["amount"] / total_revenue) * 100 if total_revenue > 0 else 0,
            "val": c["amount"],
        }
        for c in cost_breakdown
    ]
    known_cost_sum = sum(c["amount"] for c in cost_breakdown)
    other_costs = total_costs - known_cost_sum
    if other_costs > 0:
        cost_entries.append({
            "label": "Other Costs",
            "pct": (other_costs / total_revenue) * 100 if total_revenue > 0 else 0,
            "val": other_costs,
        })
    cost_entries.sort(key=lambda x: x["pct"], reverse=True)
    top_costs = cost_entries[:2]

    cost_ratio = f"{(total_costs / total_revenue * 100):.1f}" if total_revenue > 0 else "0.0"

    return {
        "year": latest["year"],
        "revenue": latest["revenue"],
        "yoy_pct": yoy_pct,
        "net_profit": latest.get("net_profit") or latest.get("earnings"),
        "driver_year": driver["year"],
        "driver_revenue": driver["revenue"],
        "top_rev": top_rev,
        "top_costs": top_costs,
        "cost_ratio": cost_ratio,
    }


# ---------------------------------------------------------------------------
# Main orchestrator
# ---------------------------------------------------------------------------

def compute_all_narrative_stats(slug: str | None = None) -> None:
    print("=" * 60)
    print("  NARRATIVE STATS — Computation Pipeline")
    print("=" * 60)

    conn = get_conn()
    conn.row_factory = dict_factory
    cursor = conn.cursor()

    # ---- Companies to process ----
    if slug:
        cursor.execute(
            "SELECT id, name, slug, symbol FROM company WHERE slug = ?",
            (slug,),
        )
    else:
        cursor.execute("SELECT id, name, slug, symbol FROM company ORDER BY id")

    companies = cursor.fetchall()
    print(f"\n  Found {len(companies)} companies to process")

    update_cursor = conn.cursor()
    updated = 0
    skipped = 0

    for company in companies:
        cid = company["id"]
        name = company["name"]
        cslug = company["slug"]

        profile = compute_profile_stats(conn, cid)
        production = compute_production_stats(conn, cid)
        financial = compute_financial_stats(cid, cursor)

        # Build the narrative_stats JSON matching frontend NarrativeStats type
        # Lay it out as {profile: ..., production: ..., financial: ...}
        # Each section is null when no data is available.
        wrapper = {"profile": profile, "production": production, "financial": financial}

        stats_json = json.dumps(wrapper, ensure_ascii=False)
        update_cursor.execute(
            "UPDATE company SET narrative_stats = ? WHERE id = ?",
            (stats_json, cid),
        )
        updated += 1

        if slug:
            print(f"  -> {name} ({cslug}): profile={'Y' if profile else 'N'}, production={'Y' if production else 'N'}, financial={'Y' if financial else 'N'}")

    conn.commit()
    print(f"\n  -> Updated {updated} companies")
    if slug is None:
        # Verify count
        cursor.execute("SELECT COUNT(*) as cnt FROM company WHERE narrative_stats IS NOT NULL")
        count = cursor.fetchone()["cnt"]
        print(f"  -> Total companies with narrative_stats: {count}")

    conn.close()
    print("\n✓ Narrative stats computation complete!")


if __name__ == "__main__":
    slug = None
    for i, arg in enumerate(sys.argv[1:], start=1):
        if arg == "--slug" and i < len(sys.argv[1:]):
            slug = sys.argv[1:][i]
            break
    compute_all_narrative_stats(slug=slug)
