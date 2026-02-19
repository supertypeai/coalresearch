import pandas as pd
import sqlite3
from db.models import CompanyPerformance
from sheet_api.core.sync import sync_model
from tables.company_performance.commodity import update_commodity_performance

def companyPerformancePreprocess(df: pd.DataFrame, field_types: dict, sheet):
    """Preprocess for company_performance to add placeholder slug and sequential IDs."""
    
    # 1. Generate sequential IDs (replacing empty string IDs from update_commodity_performance)
    df["id"] = range(1, len(df) + 1)

    # 2. Remap company_id from company
    conn = sqlite3.connect("db.sqlite")
    cursor = conn.cursor()

    # Create mapping for lookup: get id, name, and slug from company table
    cursor.execute("SELECT id, name, slug FROM company")
    company_rows = cursor.fetchall()
    
    # Create the requested lookup table (dict)
    # We use name to map to ID, and ID to map to slug
    company_name_to_id = {row[1]: row[0] for row in company_rows}
    company_id_to_slug = {row[0]: row[2] for row in company_rows}
    # Also valid IDs set for quick check
    valid_ids = {row[0] for row in company_rows}

    conn.close()

    # For rows with company_id, verify they exist in company
    # If not, try to map via company name (*company_name column)
    def remap_company_id(row):
        company_id = row.get("company_id")

        # If company_id is null or empty, try to find via name
        if pd.isna(company_id) or company_id == "":
            company_name = row.get("*company_name")
            if company_name and company_name in company_name_to_id:
                return company_name_to_id[company_name]
            return None

        # Convert to int
        try:
            company_id = int(company_id)
        except (ValueError, TypeError):
            # Try name lookup
            company_name = row.get("*company_name")
            if company_name and company_name in company_name_to_id:
                return company_name_to_id[company_name]
            return None

        # Check if this ID exists in company
        if company_id in valid_ids:
            return company_id

        # If not, try name lookup as fallback
        company_name = row.get("*company_name")
        if company_name and company_name in company_name_to_id:
            return company_name_to_id[company_name]

        return None

    df["company_id"] = df.apply(remap_company_id, axis=1)

    # Append df with the company slug using the lookup table
    df["slug"] = df["company_id"].map(company_id_to_slug)

    return df, field_types, sheet

def sync_company_performance():
    """Sync company performance data to company_performance table."""
    update_commodity_performance()
    CompanyPerformance.truncate_table()

    # First sync as usual
    sync_model(
        "company_performance",
        CompanyPerformance,
        preprocess=companyPerformancePreprocess,
    )

    # Then update slugs based on company
    conn = sqlite3.connect("db.sqlite")
    cursor = conn.cursor()

    print("Updating slugs in company_performance...")
    cursor.execute(
        """
        UPDATE company_performance
        SET slug = (
            SELECT slug FROM company 
            WHERE company.id = company_performance.company_id
        )
    """
    )
    conn.commit()
    conn.close()
    print("✓ Slugs updated successfully!")