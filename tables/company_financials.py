def sync_company_financials():
    from sheet_api import company_financials
    company_financials.main(table_name="company_financials")
