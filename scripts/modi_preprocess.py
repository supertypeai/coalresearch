import pandas as pd
import json

df = pd.read_csv('datasets/modi_detailed_company_all.csv')

cols = [
    # 0 - 1: profil_perusahaan
    ("nama_usaha", "Nama Perusahaan"),
    ("badan_usaha", "Jenis Badan Usaha"),

    # 1 - end: perizinan_data
    ("jenis_izin", "JenisPerizinan"),
    ("sk_iup", "NomorPerizinan"),
    ("kegiatan", "TahapanKegiatan"),
    ("kode_wiup", "KodeWIUP"),
    ("komoditas", "Komoditas"),
    ("luas_sk", "Luas(ha)"),
    ("tgl_berlaku", "TglMulaiBerlaku"),
    ("tgl_akhir", "TglBerakhir"),
    ("cnc", "Tahapan CNC"),
    ("lokasi", "Lokasi")
]

df = df[~pd.isna(df['perizinan_data'])]

flattened_rows = []

# Iterate over rows
for _, row in df.iterrows():
    a_data = json.loads(row['profil_perusahaan'])
    b_data = json.loads(row['perizinan_data'])
   
    for item in b_data:
        # Merge a + b[i] into one row
        combined = {**a_data, **item}
        flattened_rows.append(combined)

# Create final DataFrame
flat_df = pd.DataFrame(flattened_rows)
modi_df = flat_df[flat_df['JenisPerizinan'] != 'IUP OPK']

modi_df = modi_df.rename(columns={o:t for t, o in cols})
modi_df = modi_df[[t for t, o in cols]]
modi_df = modi_df.sort_values(by=['nama_usaha', 'sk_iup'])
modi_df = modi_df.drop_duplicates(keep='first')

minerba_df = pd.read_csv('datasets/esdm_minerba_all.csv')
minerba_cols = ['sk_iup', 'kode_wiup', 'objectid', 'pulau', 'pejabat', 'id_prov', 'nama_prov', 'id_kab', 
                'nama_kab', 'kode_golongan', 'kode_jnskom', 'generasi', 'kode_wil', 'geometry',
                'komoditas_mapped', 'provinsi_norm', 'kabupaten_norm', 'kegiatan_norm', 'lokasi_norm']
minerba_df = minerba_df[[c for c in minerba_cols]]

merge = pd.merge(
    modi_df,
    minerba_df,
    how='left',
    on=['sk_iup', 'kode_wiup'],
)
merge = merge.sort_values(by=['nama_usaha', 'sk_iup'])

# No duplicated on merge['kode_wiup']
merge.loc[pd.isna(merge['geometry']), 'geometry'] = "[]"
merge = merge.fillna("-")
merge['luas_sk'] = merge['luas_sk'].str.replace('.', '', regex=False) \
                                   .str.replace(',', '.', regex=False)

merge.to_csv("datasets/modi_mining_license_merge.csv")