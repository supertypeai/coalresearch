import os
import re
import pandas as pd

from constants import PROVINCE_ALIASES, OFFICIAL_PROVINCES

def normalize_province(name: str) -> str:
    formatted = normalize_admin(name)
    if not formatted:
        return formatted
    parts = [PROVINCE_ALIASES.get(p, p) for p in formatted.split(", ")]
    return ", ".join(parts)


def province_is_valid(province: str) -> bool:
    if not province:
        return False
    return all(p in OFFICIAL_PROVINCES for p in province.split(", "))


def validate_and_filter_province(df: pd.DataFrame, province_col: str = "province", id_col=None) -> pd.DataFrame:
    """
    Normalize df[province_col] via normalize_province, then drop (with a
    warning) any row whose result still isn't one of the 38 official names.
    Rows are dropped in-app (not by a DB constraint) so bad data never syncs.
    Reused by both the mining_license CSV pipeline and the resources_and_reserves
    sheet sync.
    """
    df[province_col] = df[province_col].apply(normalize_province)
    valid_mask = df[province_col].apply(province_is_valid)
    if not valid_mask.all():
        cols = [id_col, province_col] if id_col and id_col in df.columns else [province_col]
        bad_rows = df.loc[~valid_mask, cols]
        msg = (
            f"dropping {len(bad_rows)} row(s) with non-standard {province_col}, "
            f"not inserted: {bad_rows[province_col].tolist()}"
        )
        # GitHub Actions annotation: shows up as a highlighted warning on the run and PR.
        # Plain print outside CI. See constants.OFFICIAL_PROVINCES for the allowed list.
        prefix = "::warning title=Non-standard province::" if os.getenv("GITHUB_ACTIONS") else "WARNING: "
        print(f"{prefix}{msg}")
        print(bad_rows.to_string(index=False))
    return df[valid_mask]


def _title_case_word(w: str) -> str:
    """Title-case a single word, except the Indonesian conjunction 'dan'."""
    return w.lower() if w.lower() == "dan" else w.capitalize()


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
        s = " ".join(_title_case_word(w) for w in s.split())
        cleaned.append(s)
    # 5) re-join with comma+space
    return ", ".join(cleaned)


def normalize_location(row):
    if pd.isna(row["lokasi"]):
        return f"Kab. {row['nama_kab']}, {row['nama_prov']}"

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
    parts = [p.strip() for p in loc.split(",") if p.strip()]
    cleaned = [" ".join(_title_case_word(w) for w in part.split()) for part in parts]
    return ", ".join(cleaned)
