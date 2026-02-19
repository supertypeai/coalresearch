import argparse

from tables.company import sync_company
from tables.company_performance import sync_company_performance
from tables.company_financials import sync_company_financials
from tables.company_ownership import sync_process_ownership
from tables.export_destination import sync_export_destination
from tables.global_commodity import sync_global_commodity_data
from tables.mining_site import sync_mining_site
from tables.reserves_resources import sync_resources_and_reserves
from tables.total_commodities_production import sync_total_commodities_production
from tables.sales_destination import sync_sales_destination


MODEL_SYNC_MAP = {
    "company": sync_company,
    "company_performance": sync_company_performance,
    "company_financials": sync_company_financials,
    "company_ownership": sync_process_ownership,
    "export_destination": sync_export_destination,
    "sales_destination": sync_sales_destination,
    "global_commodity_data": sync_global_commodity_data,
    "mining_site": sync_mining_site,
    "resources_and_reserves": sync_resources_and_reserves,
    "total_commodities_production": sync_total_commodities_production
}


def main():
    parser = argparse.ArgumentParser(description="Data sync CLI")
    parser.add_argument(
        "action", choices=["sync"], help="Action to perform (only 'sync' supported)"
    )
    parser.add_argument(
        "model", choices=MODEL_SYNC_MAP.keys(), help="Specify the model to sync"
    )

    args = parser.parse_args()

    if args.action == "sync":
        MODEL_SYNC_MAP[args.model]()
        print(f"{args.model} synced.")


if __name__ == "__main__":
    main()
