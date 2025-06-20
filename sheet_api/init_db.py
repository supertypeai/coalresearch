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
)


def create_tables():
    db.connect()
    db.create_tables(
        [
            # Company,
            # CompanyOwnership,
            # CompanyPerformance,
            # MiningSite,
            # ResourcesAndReserves,
            # TotalCommoditiesProduction,
            # ExportDestination,
            GlobalCommodityData,
        ]
    )
    print("All tables created successfully.")


if __name__ == "__main__":
    create_tables()
