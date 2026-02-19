from db.models import (
    db,
    Company,
    CompanyOwnership,
    CompanyPerformance,
    MiningSite,
    ResourcesAndReserves,
    TotalCommoditiesProduction,
    ExportDestination,
    GlobalCommodityData,
    CompanyFinancials
)


def create_tables():
    db.connect()
    db.create_tables(
        [
            Company,
            CompanyOwnership,
            CompanyPerformance,
            MiningSite,
            ResourcesAndReserves,
            TotalCommoditiesProduction,
            ExportDestination,
            GlobalCommodityData,
            CompanyFinancials,
        ]
    )
    print("All tables created successfully.")


if __name__ == "__main__":
    create_tables()
