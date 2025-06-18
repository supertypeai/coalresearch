import pandas as pd
import sqlite3


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


def prepare_top_n(df: pd.DataFrame, n: int = 100) -> pd.DataFrame:
    """
    Sort by tgl_berlaku desc, take top n, reformat dates to DD/MM/YYYY,
    and assign new sequential id 1..n.
    """
    df_sorted = df.sort_values("tgl_berlaku", ascending=False).head(n).copy()
    df_sorted["permit_effective_date"] = df_sorted["tgl_berlaku"].dt.strftime(
        "%d/%m/%Y"
    )
    df_sorted["permit_expiry_date"] = df_sorted["tgl_akhir"].dt.strftime("%d/%m/%Y")
    df_sorted["id"] = range(1, len(df_sorted) + 1)
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
        location TEXT
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
        location
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
        :location
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
        location            = excluded.location;
    """
    # map scraped → table columns
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
    ]
    with conn:
        conn.executemany(upsert_sql, df_upsert[cols].to_dict(orient="records"))


def scrape_and_upsert(csv_path: str, db_path: str, top_n: int = 100):
    """
    Full pipeline:
      1. Load & parse CSV
      2. Prepare top_n newest records with sequential ids
      3. Create table if needed
      4. Upsert into SQLite
    """
    df = load_and_parse(csv_path)
    top_df = prepare_top_n(df, n=top_n)
    conn = sqlite3.connect(db_path)
    create_table(conn)
    upsert_records(conn, top_df)
    conn.close()
    print(f"Upserted {len(top_df)} records (IDs 1-{len(top_df)}).")


if __name__ == "__main__":
    # adjust paths as needed
    scrape_and_upsert("esdm_minerba_all.csv", "db.sqlite", top_n=100)
