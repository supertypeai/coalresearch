import argparse
import importlib

MODEL_SYNC_MAP = {
    "company"                       : ("tables.company"                     , "sync_company"),
    "company_performance"           : ("tables.company_performance"         , "sync_company_performance"),
    "company_financials"            : ("tables.company_financials"          , "sync_company_financials"),
    "company_ownership"             : ("tables.company_ownership"           , "sync_process_ownership"),
    "commodity_price"               : ("tables.commodity_price"             , "sync_commodity_price"),
    "mining_contract"               : ("tables.mining_contract"             , "sync_mining_contract"),
    "export_destination"            : ("tables.export_destination"          , "sync_export_destination"),
    "sales_destination"             : ("tables.sales_destination"           , "sync_sales_destination"),
    "global_commodity_data"         : ("tables.global_commodity"            , "sync_global_commodity_data"),
    "mining_site"                   : ("tables.mining_site"                 , "sync_mining_site"),
    "resources_and_reserves"        : ("tables.reserves_resources"          , "sync_resources_and_reserves"),
    "total_commodities_production"  : ("tables.total_commodities_production", "sync_total_commodities_production")
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
        module_path, func_name = MODEL_SYNC_MAP[args.model]
        module = importlib.import_module(module_path)
        sync_func = getattr(module, func_name)
        
        sync_func()
        print(f"{args.model} synced.")


if __name__ == "__main__":
    main()
