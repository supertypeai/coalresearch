import pandas as pd
from lib.formatter import normalize_admin, normalize_location

def prepareMinerbaDf(filename: str = "datasets/modi_mining_license_merge_v2.csv"):
    minerba_df = pd.read_csv(filename)

    minerba_df["nama_prov"] = minerba_df["nama_prov"].apply(normalize_admin)
    minerba_df["nama_kab"] = minerba_df["nama_kab"].apply(normalize_admin)
    minerba_df["kegiatan"] = minerba_df["kegiatan"].apply(normalize_admin)
    minerba_df["lokasi"] = minerba_df.apply(normalize_location, axis=1)

    minerba_df = minerba_df.rename(columns={
        "nama_usaha": "company_name",
        "jenis_izin": "license_type",
        "sk_iup": "license_number",
        "kode_wiup": "wiup_code",
        "nama_prov": "province",
        "nama_kab": "city",
        "tgl_berlaku": "license_effective_date",
        "tgl_akhir": "license_expiry_date",
        "kegiatan": "activity",
        "luas_sk": "licensed_area",
        "cnc": "cnc",
        "generasi": "generation",
        "lokasi": "location",
        "komoditas_mapped": "komoditas_mapped",
        "geometry": "geometry",
    })

    no_geometry_mask = minerba_df["geometry"] == "[]"
    minerba_df_with_no_geometry = minerba_df[no_geometry_mask]
    print(f"Excluding {len(minerba_df_with_no_geometry)} companies with no geometry data")
    for rowid, row in minerba_df_with_no_geometry.head(5).iterrows():
        print(row['company_name'])

    minerba_df = minerba_df[~no_geometry_mask]

    minerba_df["commodity_type"] = (
        minerba_df["komoditas_mapped"].astype(str).str.strip().str.title()
    )
    minerba_df["cnc"] = minerba_df["cnc"].fillna("-").replace("-", None)
    minerba_df["generation"] = minerba_df["generation"].fillna("-").replace("-", None)

    included_columns = [
        "company_name",
        "license_type",
        "license_number",
        "wiup_code",
        "province",
        "city",
        "license_effective_date",
        "license_expiry_date",
        "activity",
        "licensed_area",
        "cnc",
        "generation",
        "location",
        "commodity_type",
        "geometry",
    ]
    minerba_df = minerba_df[included_columns]
    return minerba_df

if __name__ == "__main__":
    df = prepareMinerbaDf()