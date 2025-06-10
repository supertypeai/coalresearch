from db.models import (
    db,
    Company,
    CompanyOwnership,
    CompanyPerformance,
    MiningSite,
    ResourcesAndReserves,
    TotalCommoditiesProduction,
    ExportDestination,
    MiningContract,
)

def create_tables():
    db.connect()
    db.create_tables([
        Company,
        # CompanyOwnership,
        CompanyPerformance,
        # MiningSite,
        # ResourcesAndReserves,
        # TotalCommoditiesProduction,
        # ExportDestination,
        # MiningContract,
    ])
    print("All tables created successfully.")

if __name__ == "__main__":
    create_tables()
