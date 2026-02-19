from db.models import TotalCommoditiesProduction
from sheet_api.core.sync import sync_model

def sync_total_commodities_production():
    sync_model("total_commodities_production", TotalCommoditiesProduction)
    