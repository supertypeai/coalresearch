from db.models import ExportDestination
from sheet_api.core.sync import sync_model

def sync_export_destination():
    sync_model("export_destination", ExportDestination)
