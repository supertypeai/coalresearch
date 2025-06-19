import pandas as pd
import sqlite3
import json


def load_and_parse(csv_path: str) -> pd.DataFrame:
    """
    Load the scraped CSV, parse tgl_berlaku and tgl_akhir from ms-since-epoch
    into datetime.
    """
    return pd.read_csv(
        csv_path,
        parse_dates=["tgl_berlaku", "tgl_akhir"],
        date_parser=lambda col: pd.to_datetime(col, unit="ms"),
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
        "tgl_berlaku",
        "tgl_akhir",  # dates
        "sk_iup",  # license number
        "jenis_izin",  # license type
        "nama_prov",  # province
        "nama_kab",  # city
        "kegiatan",  # activity
        "luas_sk",  # licensed_area
        "lokasi",  # location
        "komoditas",  # commodity
    ]
    df_sorted = df_sorted.dropna(subset=required_cols)

    # Exclude rows where effective equals expiry date
    df_sorted = df_sorted[df_sorted["tgl_berlaku"] != df_sorted["tgl_akhir"]]

    # Strip and filter out rows with empty or '-' in any string column
    str_cols = df_sorted.select_dtypes(include=[object]).columns

    def valid_row(row):
        for col in str_cols:
            val = str(row[col]).strip()
            if val == "" or val == "-":
                return False
        return True

    df_sorted = df_sorted[df_sorted.apply(valid_row, axis=1)]

    # Reformat dates
    df_sorted["permit_effective_date"] = df_sorted["tgl_berlaku"].dt.strftime(
        "%Y-%m-%d"
    )
    df_sorted["permit_expiry_date"] = df_sorted["tgl_akhir"].dt.strftime("%Y-%m-%d")

    # Assign sequential IDs
    df_sorted["id"] = range(1, len(df_sorted) + 1)

    # Parse and title-case commodity list
    df_sorted["commodity"] = (
        df_sorted["komoditas"]
        .astype(str)
        .apply(
            lambda x: [
                entry.strip().title()
                for entry in x.split(",")
                if entry and entry.strip() and entry.strip() != "-"
            ]
        )
    )
    # JSON-serialize the list for SQLite storage
    df_sorted["commodity"] = df_sorted["commodity"].apply(json.dumps)
    return df_sorted


def create_table(conn: sqlite3.Connection):
    """
    Create mining_license table if it doesn't exist.
    """
    conn.execute(
        """
    CREATE TABLE IF NOT EXISTS mining_license (
        id INTEGER PRIMARY KEY NOT NULL,
        license_type TEXT,
        license_number TEXT,
        province TEXT,
        city TEXT,
        permit_effective_date TEXT,
        permit_expiry_date TEXT,
        activity TEXT,
        licensed_area INTEGER,
        location TEXT,
        commodity TEXT
    );
    """
    )
    conn.commit()


def upsert_records(conn: sqlite3.Connection, df: pd.DataFrame):
    """
    Upsert each row in df into mining_license using id as PK.
    """
    upsert_sql = """
    INSERT INTO mining_license (
        id,
        license_type,
        license_number,
        province,
        city,
        permit_effective_date,
        permit_expiry_date,
        activity,
        licensed_area,
        location,
        commodity
    ) VALUES (
        :id,
        :license_type,
        :license_number,
        :province,
        :city,
        :permit_effective_date,
        :permit_expiry_date,
        :activity,
        :licensed_area,
        :location,
        :commodity
    )
    ON CONFLICT(id) DO UPDATE SET
        license_type        = excluded.license_type,
        license_number      = excluded.license_number,
        province            = excluded.province,
        city                = excluded.city,
        permit_effective_date = excluded.permit_effective_date,
        permit_expiry_date  = excluded.permit_expiry_date,
        activity            = excluded.activity,
        licensed_area       = excluded.licensed_area,
        location            = excluded.location,
        commodity           = excluded.commodity;
    """
    df_upsert = df.rename(
        columns={
            "jenis_izin": "license_type",
            "sk_iup": "license_number",
            "nama_prov": "province",
            "nama_kab": "city",
            "kegiatan": "activity",
            "luas_sk": "licensed_area",
            "lokasi": "location",
        }
    )
    cols = [
        "id",
        "license_type",
        "license_number",
        "province",
        "city",
        "permit_effective_date",
        "permit_expiry_date",
        "activity",
        "licensed_area",
        "location",
        "commodity",
    ]
    with conn:
        conn.executemany(upsert_sql, df_upsert[cols].to_dict(orient="records"))


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
    print(f"Upserted {len(all_df)} valid records (IDs 1-{len(all_df)}).")


if __name__ == "__main__":
    scrape_and_upsert("esdm_minerba_all.csv", "db.sqlite")
