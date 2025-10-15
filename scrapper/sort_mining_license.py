import pandas as pd
import sqlite3
import re

from scripts.fuzzy_matcher import match_company_by_name
from lib.formatter import normalize_admin, normalize_location

def clean_company_name(name):
    """Removes common corporate prefixes/suffixes and converts to lowercase."""
    if pd.isna(name):
        return None
    return (
        re.sub(r"\b(PT|Tbk|CV|UD|PD|KSU|KUD)\b", "", str(name), flags=re.IGNORECASE)
        .lower()
        .strip()
    )


def load_and_parse(csv_path: str) -> pd.DataFrame:
    """
    Load the scraped CSV, parse tgl_berlaku and tgl_akhir from ms-since-epoch
    into datetime.
    """
    return pd.read_csv(
        csv_path,
        parse_dates=["tgl_berlaku", "tgl_akhir"],
        # date_parser=lambda col: pd.to_datetime(col, unit="ms"),
    )


def prepare_all(df: pd.DataFrame) -> pd.DataFrame:
    """
    Sort by tgl_berlaku descending, exclude rows with missing/invalid fields,
    reformat dates to YYYY-MM-DD, and assign new sequential id 1..len(df).
    """
    # Sort by effective date
    df_sorted = df.sort_values("tgl_berlaku", ascending=False).copy()

    # Drop rows with null in critical columns
    required_cols = [
        "jenis_izin",  # license type
        "sk_iup",  # license number
        "kode_wiup", # wiup_code
        "nama_prov",  # province
        "nama_kab",  # city
        "tgl_berlaku", # permit_effective_date
        "tgl_akhir",  # permit_expiry_date
        "kegiatan",  # activity
        "luas_sk",  # licensed_area
        "lokasi",  # location
        "komoditas_mapped",  # commodity
        "nama_usaha",  # company_name
        "badan_usaha",
    ]
    df_sorted = df_sorted[required_cols]

    # Exclude rows where effective equals expiry date
    df_sorted = df_sorted[df_sorted["tgl_berlaku"] != df_sorted["tgl_akhir"]]
    excluded_columns = ["lokasi"]
    str_cols = df_sorted.select_dtypes(include=[object]).columns.difference(excluded_columns)

    def valid_row(row):
        for col in str_cols:
            val = row[col]
            if pd.isna(val) or str(val).strip() in ("", "-"):
                return False
        return True

    invalid_df = df_sorted[~df_sorted.apply(valid_row, axis=1)]
    print(f"Dropping {len(invalid_df)} invalid rows. Viewing first 5 rows: ")
    print(invalid_df.head(5), "\n")

    df_sorted = df_sorted[df_sorted.apply(valid_row, axis=1)]

    df_sorted["nama_prov"] = df_sorted["nama_prov"].apply(normalize_admin)
    df_sorted["nama_kab"] = df_sorted["nama_kab"].apply(normalize_admin)
    df_sorted["kegiatan"] = df_sorted["kegiatan"].apply(normalize_admin)

    # Temporary fix: exclude 'Wil Penunjang'
    df_sorted = df_sorted[df_sorted['kegiatan'] != 'Wil Penunjang']

    # Reformat dates
    df_sorted["permit_effective_date"] = df_sorted["tgl_berlaku"].dt.strftime(
        "%Y-%m-%d"
    )
    df_sorted["permit_expiry_date"] = df_sorted["tgl_akhir"].dt.strftime("%Y-%m-%d")

    # Assign sequential IDs
    df_sorted["id"] = range(1, len(df_sorted) + 1)
    df_sorted["commodity"] = df_sorted["komoditas_mapped"].astype(str)
    df_sorted["cleaned_company_name_for_match"] = df_sorted["nama_usaha"].apply(
        clean_company_name
    )
    df_sorted["location"] = df_sorted.apply(normalize_location, axis=1)
    df_sorted.drop(columns="lokasi", inplace=True)

    return df_sorted


def create_table(conn: sqlite3.Connection):
    """
    Create mining_license table if it doesn't exist.
    """
    conn.execute(
        """
    CREATE TABLE IF NOT EXISTS mining_license (
        id TEXT PRIMARY KEY NOT NULL,
        license_type TEXT,
        license_number TEXT,
        wiup_code TEXT,
        province TEXT,
        city TEXT,
        permit_effective_date TEXT,
        permit_expiry_date TEXT,
        activity TEXT,
        licensed_area INTEGER,
        location TEXT,
        commodity TEXT,
        company_name TEXT,
        company_id INTEGER,
        FOREIGN KEY (company_id) REFERENCES company(id)
    );
    """
    )
    conn.commit()


def upsert_records(conn: sqlite3.Connection, df: pd.DataFrame):
    """
    Upsert each row in df into mining_license using id as PK.
    Looks up company_id and canonical company_name from the company table,
    then writes those into mining_license, falling back to the scraped name.
    """
    # 1) Prepare our license DataFrame
    df_up = df.rename(
        columns={
            "jenis_izin": "license_type",
            "sk_iup": "license_number",
            "kode_wiup": "wiup_code",
            "nama_prov": "province",
            "nama_kab": "city",
            "kegiatan": "activity",
            "luas_sk": "licensed_area",
            "lokasi": "location",
        }
    ).copy()

    df_up = match_company_by_name(df_up, "nama_usaha")

    # ←── NEW: where there's no match, fall back to the original scraped name
    # df_up["company_name"] = df_up["company_name"].fillna(df_up["nama_usaha"])

    # ←── NEW: combine badan_usaha + nama_usaha, title-case each word as a fallback
    nama_title = df_up["nama_usaha"].fillna("").str.title()
    fallback = df_up["badan_usaha"].fillna("") + " " + nama_title
    fallback = fallback.str.strip()

    df_up["company_name"] = df_up["company_name"].fillna(fallback)

    # 4) Ensure company_id is an integer (nullable dtype) so we don't get 123.0
    df_up["company_id"] = df_up["company_id"].astype("Int64")

    # 5) Perform the upsert
    upsert_sql = """
    INSERT INTO mining_license (
      id, license_type, license_number, wiup_code, province, city,
      permit_effective_date, permit_expiry_date, activity,
      licensed_area, location, commodity, company_name, company_id
    ) VALUES (
      :id, :license_type, :license_number, :wiup_code, :province, :city,
      :permit_effective_date, :permit_expiry_date, :activity,
      :licensed_area, :location, :commodity, :company_name, :company_id
    )
    ON CONFLICT(id) DO UPDATE SET
      license_type          = excluded.license_type,
      license_number        = excluded.license_number,
      wiup_code             = excluded.wiup_code,
      province              = excluded.province,
      city                  = excluded.city,
      permit_effective_date = excluded.permit_effective_date,
      permit_expiry_date    = excluded.permit_expiry_date,
      activity              = excluded.activity,
      licensed_area         = excluded.licensed_area,
      location              = excluded.location,
      commodity             = excluded.commodity,
      company_name          = excluded.company_name,
      company_id            = excluded.company_id;
    """

    cols = [
        "id",
        "license_type",
        "license_number",
        "wiup_code",
        "province",
        "city",
        "permit_effective_date",
        "permit_expiry_date",
        "activity",
        "licensed_area",
        "location",
        "commodity",
        "company_name",
        "company_id",
    ]
    with conn:
        conn.executemany(upsert_sql, df_up[cols].to_dict(orient="records"))


def scrape_and_upsert(csv_path: str, db_path: str):
    """
    Full pipeline:
      1. Load & parse CSV
      2. Prepare all records with sequential ids and filter out invalid rows
      3. Create table if needed
      4. Upsert into SQLite
    """
    df = load_and_parse(csv_path)
    all_df = prepare_all(df)
    conn = sqlite3.connect(db_path)
    create_table(conn)
    upsert_records(conn, all_df)
    conn.close()
    print(f"Upserted {len(all_df)} valid records (IDs 1-{len(all_df)}")
    
if __name__ == "__main__":
    scrape_and_upsert("datasets/modi_mining_license_merge_v2.csv", "db.sqlite")