import pandas as pd

def prepareMinerbaDf(filename: str = "datasets/modi_mining_license_merge.csv"):
    minerba_df = pd.read_csv(filename)
    minerba_df = minerba_df.rename(columns={
        "Unnamed: 0": "row_id",
        "objectid": "object_id",
        "pulau": "island",
        "pejabat": "official",
        "id_prov": "province_id",
        "nama_prov": "province",
        "id_kab": "city_id",
        "nama_kab": "city",
        "jenis_izin": "license_type",
        "badan_usaha": "business_entity",
        "nama_usaha": "company_name",
        "kode_wiup": "wiup_code",
        "sk_iup": "license_number",
        "tgl_berlaku": "permit_effective_date",
        "tgl_akhir": "permit_expiry_date",
        "kegiatan": "activity",
        "luas_sk": "licensed_area",
        "komoditas": "commodity",
        "kode_golongan": "group_code",
        "kode_jnskom": "commodity_type_code",
        "cnc": "cnc",
        "generasi": "generation",
        "kode_wil": "region_code",
        "lokasi": "location",
        "geometry": "geometry",
        "komoditas_mapped": "komoditas_mapped",
        "provinsi_norm": "provinsi_norm",
        "kabupaten_norm": "kabupaten_norm",
        "kegiatan_norm": "kegiatan_norm",
        "lokasi_norm": "lokasi_norm"
    })
    included_columns = [
        "license_type",
        "license_number",
        "province",
        "city",
        "permit_effective_date",
        "permit_expiry_date",
        "activity",
        "licensed_area",
        "cnc",
        "generation",
        "location",
        "commodity",
        "geometry",
    ]
    minerba_df["province"] = minerba_df["provinsi_norm"]
    minerba_df["city"] = minerba_df["kabupaten_norm"]
    minerba_df["activity"] = minerba_df["kegiatan_norm"]
    minerba_df["location"] = minerba_df["lokasi_norm"].fillna("-")
    minerba_df["license_number"] = minerba_df["license_number"].fillna("-").str.strip()
    # minerba_df["permit_effective_date"] = pd.to_datetime(
    #     minerba_df["permit_effective_date"], unit="ms", errors="coerce"
    # )
    # minerba_df["permit_expiry_date"] = pd.to_datetime(
    #     minerba_df["permit_expiry_date"], unit="ms", errors="coerce"
    # )
    # minerba_df["permit_effective_date"] = (
    #     minerba_df["permit_effective_date"].dt.strftime("%Y-%m-%d").fillna("-")
    # )
    # minerba_df["permit_expiry_date"] = (
    #     minerba_df["permit_expiry_date"].dt.strftime("%Y-%m-%d").fillna("-")
    # )
    minerba_df["commodity"] = (
        minerba_df["komoditas_mapped"].astype(str).str.strip().str.title()
    )
    minerba_df["generation"] = minerba_df["generation"].fillna("-")

    return minerba_df, included_columns