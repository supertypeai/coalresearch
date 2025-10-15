import re
import pandas as pd

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
    def tc(w):
        return w.lower() if w.lower() == "dan" else w.capitalize()

    parts = [p.strip() for p in loc.split(",") if p.strip()]
    cleaned = [" ".join(tc(w) for w in part.split()) for part in parts]
    return ", ".join(cleaned)