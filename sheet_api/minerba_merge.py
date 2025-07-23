import pandas as pd

def prepareMinerbaDf(filename: str = "datasets/esdm_minerba_all.csv"):
    minerba_df = pd.read_csv(filename)
    minerba_df.columns = [
        "row_id",  # 'Unnamed: 0'
        "object_id",  # 'objectid'
        "island",  # 'pulau'
        "official",  # 'pejabat'
        "province_id",  # 'id_prov'
        "province",  # 'nama_prov'
        "city_id",  # 'id_kab'
        "city",  # 'nama_kab'
        "license_type",  # 'jenis_izin'
        "business_entity",  # 'badan_usaha'
        "company_name",  # 'nama_usaha'
        "wiup_code",  # 'kode_wiup'
        "license_number",  # 'sk_iup'
        "permit_effective_date",  # 'tgl_berlaku'
        "permit_expiry_date",  # 'tgl_akhir'
        "activity",  # 'kegiatan'
        "licensed_area",  # 'luas_sk'
        "commodity",  # 'komoditas'
        "group_code",  # 'kode_golongan'
        "commodity_type_code",  # 'kode_jnskom'
        "cnc",  # 'cnc'
        "generation",  # 'generasi'
        "region_code",  # 'kode_wil'
        "location",  # 'lokasi'
        "geometry",  # 'geometry'
        "komoditas_mapped",  # 'komoditas_mapped'
        "provinsi_norm",  # 'provinsi_norm'
        "kabupaten_norm",  # 'kabupaten_norm'
        "kegiatan_norm",  # 'kegiatan_norm'
        "lokasi_norm",  # 'lokasi_norm'
    ]
    included_columns = [
        "license_type",
        "license_number",
        "province",
        "city",
        # "business_entity",
        # "company_name",
        "permit_effective_date",
        "permit_expiry_date",
        "activity",
        "licensed_area",
        "cnc",
        "generation",
        "location",
        "commodity",
        "geometry",
        # "commodity",  # 'komoditas_mapped'
    ]
    minerba_df = minerba_df[minerba_df['kegiatan_norm'] != 'Wil Penunjang']
    # minerba_df["province"] = minerba_df["province"].str.title()
    minerba_df["province"] = minerba_df["provinsi_norm"]
    minerba_df["city"] = minerba_df["kabupaten_norm"]
    minerba_df["activity"] = minerba_df["kegiatan_norm"]
    minerba_df["location"] = minerba_df["lokasi_norm"].fillna("-")
    # minerba_df["company_name"] = minerba_df["company_name"].str.title()
    # minerba_df["city"] = (
    #     minerba_df["city"].str.replace(r"^KAB\.\s*", "", regex=True).str.title()
    # )
    # minerba_df["activity"] = minerba_df["activity"].str.lower()
    # minerba_df["location"] = minerba_df["location"].str.lower()
    minerba_df["license_number"] = minerba_df["license_number"].fillna("-").str.strip()
    minerba_df["permit_effective_date"] = pd.to_datetime(
        minerba_df["permit_effective_date"], unit="ms", errors="coerce"
    )
    minerba_df["permit_expiry_date"] = pd.to_datetime(
        minerba_df["permit_expiry_date"], unit="ms", errors="coerce"
    )
    minerba_df["permit_effective_date"] = (
        minerba_df["permit_effective_date"].dt.strftime("%Y-%m-%d").fillna("-")
    )
    minerba_df["permit_expiry_date"] = (
        minerba_df["permit_expiry_date"].dt.strftime("%Y-%m-%d").fillna("-")
    )
    # minerba_df["commodity"] = (
    #     minerba_df["komoditas_mapped"].astype(str).str.strip().str.title()
    # )
    minerba_df["generation"] = minerba_df["generation"].fillna("-")

    return minerba_df, included_columns

if __name__ == '__main__':
    df, cols = prepareMinerbaDf('datasets/coal_db - minerba (cleansed).csv')
    cop_df = df[df['commodity'].str.contains('TEMB')]
    cop_df.to_csv('cop_df.csv')