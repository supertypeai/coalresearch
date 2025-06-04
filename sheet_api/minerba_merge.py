import pandas as pd

def prepareMinerbaDf():
    minerba_df = pd.read_csv("coal_db - minerba.csv")
    minerba_df.columns = [
        "row_id",                   # 'Unnamed: 0'
        "object_id",                # 'objectid'
        "island",                   # 'pulau'
        "official",                 # 'pejabat'
        "province_id",              # 'id_prov'
        "province",                 # 'nama_prov'
        "city_id",                  # 'id_kab'
        "city",                     # 'nama_kab'
        "license_type",             # 'jenis_izin'
        "business_entity",          # 'badan_usaha'
        "company_name",             # 'nama_usaha'
        "wiup_code",                # 'kode_wiup'
        "license_number",           # 'sk_iup'
        "permit_effective_date",    # 'tgl_berlaku'
        "permit_expiry_date",       # 'tgl_akhir'
        "activity",                 # 'kegiatan'
        "licensed_area",            # 'luas_sk'
        "commodity",                # 'komoditas'
        "group_code",               # 'kode_golongan'
        "commodity_type_code",      # 'kode_jnskom'
        "cnc",                      # 'cnc'
        "generation",               # 'generasi'
        "region_code",              # 'kode_wil'
        "location",                 # 'lokasi'
        "geometry",                 # 'geometry'
    ]
    included_columns = [
        "license_type", "license_number", "province", "city", "business_entity", 
        "company_name", "permit_effective_date", "permit_expiry_date", "activity", 
        "licensed_area", "cnc", "generation", "location", "geometry"
    ]
    minerba_df['province'] = minerba_df['province'].str.title()
    minerba_df['company_name'] = minerba_df['company_name'].str.title()
    minerba_df['city'] = minerba_df['city'].str.replace(r'^KAB\.\s*', '', regex=True).str.title()
    minerba_df['activity'] = minerba_df['activity'].str.lower()
    minerba_df['location'] = minerba_df['location'].str.lower()

    return minerba_df, included_columns

# c_cols = ["operation_province", "operation_kabkot"]
# m_cols = ["nama_prov", "nama_kab"]

# for row_id, row in c_df.iterrows():
#     q = minerba_df[minerba_df['nama_usaha'] == clean_company_name(row['name'])]
    
#     for c, m in zip(c_cols, m_cols):
        
#         op = checkNoneEmpty(row[c])
        
#         if (op is None) and (not q.empty):

#             col_id = c_df.columns.get_loc(c)
            
#             original_value = op
#             new_value = q[m].iloc[0]

#             to_use_value = new_value

#             c_sheet.update_cell(2 + row_id, col_id + 1, to_use_value)

#             print("Updating row number, col number, col name, value:", row_id + 2, col_id, c, to_use_value)