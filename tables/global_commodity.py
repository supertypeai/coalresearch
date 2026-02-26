from db.models import GlobalCommodityData
from sheet_api.core.sync import sync_model

def sync_global_commodity_data():
    from sheet_api import global_commodity_data_merge

    global_commodity_data_merge.main()
    sync_model("global_commodity_data", GlobalCommodityData, "A1:J137")
