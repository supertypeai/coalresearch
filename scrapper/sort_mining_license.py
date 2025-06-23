import pandas as pd
import sqlite3
import re


def normalize_admin(name: str) -> str:
    """
    Normalize a province or city string:
     - split on commas, strip each piece
     - expand kab., prov., kota → full words
     - remove stray dots/extra spaces
     - title-case each word, except 'dan'
     - re-join with ', ' (guaranteed space)
    """
    if pd.isna(name):
        return ""
    # 1) break into parts
    parts = [part.strip() for part in str(name).split(",") if part.strip()]
    cleaned = []
    for part in parts:
        s = part
        # 2) expand abbreviations
        exp = {
            r"\bkab\.?\b": "kabupaten",
            r"\bprov\.?\b": "provinsi",
            r"\bkota\b": "kota",
        }
        for pat, sub in exp.items():
            s = re.sub(pat, sub, s, flags=re.IGNORECASE)
        # 3) remove dots & collapse spaces
        s = s.replace(".", "")
        s = re.sub(r"\s{2,}", " ", s).strip()

        # 4) title-case words (except 'dan')
        def _tc(w):
            return w.lower() if w.lower() == "dan" else w.capitalize()

        s = " ".join(_tc(w) for w in s.split())
        cleaned.append(s)
    # 5) re-join with comma+space
    return ", ".join(cleaned)


def normalize_location(row):
    raw = str(row["lokasi"]).strip()

    raw = re.sub(r"^[\.\s]+", "", raw)

    # 1) DIGIT ONLY → "City, Province"
    if raw.isdigit():
        return f"{row['nama_kab'].title()}, {row['nama_prov'].title()}"

    if re.search(r"https?://|goo\.gl/", raw, flags=re.IGNORECASE):
        return f"{row['nama_kab']}, {row['nama_prov']}"

    loc = raw

    loc = re.sub(r"\bdesa/kelurahan\b", "Desa/Kelurahan", loc, flags=re.IGNORECASE)

    # 2) Expand “Ds” or “Ds.” → “Desa ”
    loc = re.sub(r"\bds\.?\b", "desa ", loc, flags=re.IGNORECASE)

    # 3) Ensure “Jl.” and “No.”
    loc = re.sub(r"\bJl\.?\b", "Jl.", loc)
    loc = re.sub(r"\bNo\.?\b", "No.", loc)

    # 4) Uppercase RT/RW
    loc = re.sub(r"\bRt\b", "RT", loc, flags=re.IGNORECASE)
    loc = re.sub(r"\bRw\b", "RW", loc, flags=re.IGNORECASE)

    # 5) Expand other abbreviations
    expansions = {
        r"\bkec\.?\s*": "kecamatan ",
        r"\bkab\.?\s*": "kabupaten ",
        r"\bprov\.?\s*": "provinsi ",
        r"\bkel\.?\s*": "kelurahan ",
        r"\bdesa/kel\.?\s*": "desa/kelurahan ",
    }
    for pat, sub in expansions.items():
        loc = re.sub(pat, sub, loc, flags=re.IGNORECASE)

    # 6) Fix fused words, e.g. “Kecamatanmook”
    loc = re.sub(
        r"(?i)(kecamatan|kabupaten|provinsi|kelurahan)([A-Za-z])",
        lambda m: m.group(1) + " " + m.group(2),
        loc,
    )

    # 7) Normalize comma spacing & collapse multiple spaces
    loc = re.sub(r"\s{2,}", " ", loc)
    loc = re.sub(r"\s*,\s*", ", ", loc).strip(" ,")

    # 8) Title-case words except 'dan'
    def tc(w):
        return w.lower() if w.lower() == "dan" else w.capitalize()

    parts = [p.strip() for p in loc.split(",") if p.strip()]
    cleaned = [" ".join(tc(w) for w in part.split()) for part in parts]
    return ", ".join(cleaned)


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
        "komoditas_mapped",  # commodity
        "nama_usaha",  # company_name
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

    df_sorted["nama_prov"] = df_sorted["nama_prov"].apply(normalize_admin)
    df_sorted["nama_kab"] = df_sorted["nama_kab"].apply(normalize_admin)

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
        province TEXT,
        city TEXT,
        permit_effective_date TEXT,
        permit_expiry_date TEXT,
        activity TEXT,
        licensed_area INTEGER,
        location TEXT,
        commodity TEXT,
        company_name TEXT,
        company_id TEXT,
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
    # 1) Pull the company master list
    company_df = pd.read_sql("SELECT id, name FROM company;", conn)
    company_df["cleaned_company_name"] = company_df["name"].apply(clean_company_name)

    company_id_map = dict(zip(company_df["cleaned_company_name"], company_df["id"]))
    company_name_map = dict(zip(company_df["cleaned_company_name"], company_df["name"]))

    # 2) Prepare our license DataFrame
    df_up = df.rename(
        columns={
            "jenis_izin": "license_type",
            "sk_iup": "license_number",
            "nama_prov": "province",
            "nama_kab": "city",
            "kegiatan": "activity",
            "luas_sk": "licensed_area",
            "lokasi": "location",
        }
    ).copy()

    # 3) Clean the scraped company names and map to id & canonical name
    df_up["cleaned_company_name"] = df_up["nama_usaha"].apply(clean_company_name)
    df_up["company_id"] = df_up["cleaned_company_name"].map(company_id_map)
    df_up["company_name"] = df_up["cleaned_company_name"].map(company_name_map)

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
      id, license_type, license_number, province, city,
      permit_effective_date, permit_expiry_date, activity,
      licensed_area, location, commodity, company_name, company_id
    ) VALUES (
      :id, :license_type, :license_number, :province, :city,
      :permit_effective_date, :permit_expiry_date, :activity,
      :licensed_area, :location, :commodity, :company_name, :company_id
    )
    ON CONFLICT(id) DO UPDATE SET
      license_type          = excluded.license_type,
      license_number        = excluded.license_number,
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
    print(f"Upserted {len(all_df)} valid records (IDs 1-{len(all_df)}).")


if __name__ == "__main__":
    scrape_and_upsert("esdm_minerba_all.csv", "db.sqlite")
