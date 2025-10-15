import pandas as pd
import json
from constants import COMMODITY_MAP

def process_mining_license_scraped_from_modi(csv_path: str):
    """
        Process mining license data scraped from modi.
        important note on the excluded data:
        1. perizinan_data: dict == pd.na
        2. jenis_izin: str == 'IUP OPK' (IUP Operasi Produksi Khusus)
        3. jenis_izin: str == 'IPP' (Izin Pengangkutan dan Penjualan)
        4. badan_usaha: str == pd.na
    """

    def _filter_1(df):
        df = df[~pd.isna(df['perizinan_data'])]
        return df

    def _transform_1(df):
        flattened_rows = []
        for _, row in df.iterrows():
            a_data = json.loads(row['profil_perusahaan'])
            b_data = json.loads(row['perizinan_data'])
        
            for item in b_data:
                combined = {**a_data, **item}
                flattened_rows.append(combined)
        return pd.DataFrame(flattened_rows)

    def _filter_2(df):
        df = df.rename(columns={
            "Jenis Badan Usaha": "badan_usaha",
            "Nama Perusahaan": "nama_usaha",
            "JenisPerizinan": "jenis_izin",
            "NomorPerizinan": "sk_iup",
            "TahapanKegiatan": "kegiatan",
            "KodeWIUP": "kode_wiup",
            "Komoditas": "komoditas",
            "Luas(ha)": "luas_sk",
            "TglMulaiBerlaku": "tgl_berlaku",
            "TglBerakhir": "tgl_akhir",
            "Tahapan CNC": "cnc",
            "Lokasi": "lokasi",
            "Kabupaten": "nama_kab",
            "Provinsi": "nama_prov"
        })
        df = df[df['jenis_izin'] != 'IUP OPK']
        df = df[df['jenis_izin'] != 'IPP']
        df = df[~pd.isna(df['badan_usaha'])]
        df = df.drop_duplicates(keep='first')
        return df

    df = pd.read_csv(csv_path)
    # ==== modi_company_all_data_v2 ====
    #
    # profil_perusahaan,perizinan_data,url
    # "{
    # ""Nama Perusahaan"": ""3G TRUST"",
    # ""Jenis Badan Usaha"": ""CV""
    # }","[
    # {
    #     ""JenisPerizinan"": ""IUP"",
    #     ""NomorPerizinan"": ""02201043921450002"",
    #     ""TahapanKegiatan"": ""OPERASI PRODUKSI"",
    #     ""KodeWIUP"": ""2119064052019141"",
    #     ""Komoditas"": ""Pasir Kuarsa"",
    #     ""Luas(ha)"": ""123.90000000"",
    #     ""TglMulaiBerlaku"": ""2023-03-30"",
    #     ""TglBerakhir"": ""2043-03-30"",
    #     ""Tahapan CNC"": ""CNC"",
    #     ""Lokasi"": ""KAB. BELITUNG TIMUR"",
    #     ""Kabupaten"": ""KAB. BELITUNG TIMUR"",
    #     ""Provinsi"": ""Bangka Belitung""
    # }
    # ]",https://minerbaone.esdm.go.id/api/common/v2/publik/badan-usaha/611426735552075729

    df = _filter_1(df)
    df = _transform_1(df)
    df = _filter_2(df)

    return df

def process_mining_license_scraped_from_esdm(csv_path: str):
    df = pd.read_csv(csv_path)
    df = df[['kode_wiup', 'lokasi', 'nama_prov', 'nama_kab', 'generasi', 'geometry']]
    return df

if __name__ == "__main__":
    modi_df = process_mining_license_scraped_from_modi('datasets/modi_company_all_data_v2_20251015_082601.csv')
    minerba_df = process_mining_license_scraped_from_esdm('datasets/esdm_minerba_all.csv')

    def _filter_1(df):
        df_to_drop = df[df['kode_wiup'].isna() | (df['kode_wiup'] == '')]

        print(f"Dropping {len(df_to_drop)} companies with no wiup_code")
        for _, row in df_to_drop.iterrows():
            print(row['nama_usaha'])

        df = df[~df.index.isin(df_to_drop.index)]
        return df

    def _transform_1(df):
        df = df.sort_values(by=['nama_usaha', 'sk_iup'])
        df['generasi'] = df['generasi'].fillna("-")
        
        df.loc[pd.isna(df['geometry']), 'geometry'] = "[]"
        df['luas_sk'] = df['luas_sk'].str.replace('.', '', regex=False).str.replace(',', '.', regex=False)
        df['luas_sk'] = (pd.to_numeric(df['luas_sk']) / (10 ** 8)).round(2)

        df['lokasi'] = df['lokasi_x'].fillna(df['lokasi_y'])
        df['nama_prov'] = df['nama_prov_x'].fillna(df['nama_prov_y'])
        df['nama_kab'] = df['nama_kab_x'].fillna(df['nama_kab_y'])

        komoditas_cleaned = df["komoditas"].str.upper().str.replace(r"\s+DMP$", "", regex=True)
        df["komoditas_mapped"] = komoditas_cleaned.map(COMMODITY_MAP).fillna("Others")        
        return df
    
    def _filter_2(df):
        return df[["nama_usaha", "badan_usaha", "jenis_izin", "sk_iup", "kode_wiup",
                "nama_prov", "nama_kab", "tgl_berlaku", "tgl_akhir", "kegiatan",
                "luas_sk", "cnc", "generasi", "lokasi", "komoditas_mapped","geometry"]]

    df = pd.merge(modi_df, minerba_df, how='left', on='kode_wiup')
    df = _filter_1(df)
    df = _transform_1(df)
    df = _filter_2(df)

    df.reset_index(drop=True, inplace=True)
    df.to_csv("datasets/modi_mining_license_merge_v2.csv", index=False)