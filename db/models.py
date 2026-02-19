from peewee import (
    Model,
    SqliteDatabase,
    TextField,
    IntegerField,
    DecimalField,
    ForeignKeyField,
    Check,
    CompositeKey,
    DateTimeField,
    DateField,
)

province_constraints = (
    "Aceh",
    "Bali",
    "Kepulauan Bangka Belitung",
    "Banten",
    "Bengkulu",
    "Gorontalo",
    "Jakarta",
    "Jambi",
    "Jawa Barat",
    "Jawa Tengah",
    "Jawa Timur",
    "Kalimantan Barat",
    "Kalimantan Selatan",
    "Kalimantan Tengah",
    "Kalimantan Timur",
    "Kalimantan Utara",
    "Kepulauan Riau",
    "Lampung",
    "Maluku Utara",
    "Maluku",
    "Nusa Tenggara Barat",
    "Nusa Tenggara Timur",
    "Papua",
    "Papua Barat",
    "Papua Barat Daya",
    "Papua Pegunungan",
    "Papua Selatan",
    "Papua Tengah",
    "Riau",
    "Sulawesi Barat",
    "Sulawesi Selatan",
    "Sulawesi Tengah",
    "Sulawesi Tenggara",
    "Sulawesi Utara",
    "Sumatera Barat",
    "Sumatera Selatan",
    "Sumatera Utara",
    "Yogyakarta",
)
company_type_constraints = (
    "Holding",
    "Mine Owner",
    "Consultant",
    "Contractor",
    "Trader",
    "Manufacturer",
)
key_operation_constraints = (
    "Mining",
    "Mining Services",
    "Equipment Rental",
    "Logistic Management",
    "Overburden Removal & Hauling",
    "Dredging",
    "Coal Trading",
    "Barging, Port & Transshipment",
    "Investment",
    "Mineral Refining",
    "Construction",
)
commodity_type_constraints = (
    "Coal",
    "Aluminium",
    "Gold",
    "Zinc and Lead",
    "Oil",
    "Nickel",
    "Copper",
    "Silver",
    "Tin",
    "Cobalt",
)
mineral_type_constraints = (
    "Coal",
    "Aluminium",
    "Gold",
    "Zinc and Lead",
    "Oil",
    "Nickel",
    "Copper",
    "Silver",
)
mining_operation_status_constraints = ("production", "development", "inactive")

db = SqliteDatabase("db.sqlite")


class Company(Model):
    id = IntegerField(primary_key=True)
    name = TextField()
    slug = TextField(unique=True)
    idx_ticker = TextField(null=True)
    operation_province = TextField(
        null=True,
        constraints=[Check(f"operation_province IN {province_constraints}")],
    )
    operation_district = TextField(null=True)
    representative_address = TextField(null=True)
    company_type = TextField(
        null=True, constraints=[Check(f"company_type IN {company_type_constraints}")]
    )
    key_operation = TextField(
        constraints=[Check(f"key_operation IN {key_operation_constraints}")]
    )
    activities = TextField(null=True, constraints=[Check("json_valid(activities)")])
    website = TextField(null=True)
    phone_number = IntegerField(null=True)
    email = TextField(null=True)
    mining_license = TextField(
        null=True, constraints=[Check("json_valid(mining_license)")]
    )
    mining_contract = TextField(
        null=True, constraints=[Check("json_valid(mining_contract)")]
    )
    commodity = TextField(null=True, constraints=[Check("json_valid(commodity)")])

    class Meta:
        database = db
        table_name = "company"


class MiningContract(Model):
    mine_owner = ForeignKeyField(
        Company,
        backref="contracts_as_mine_owner",
        column_name="mine_owner_id",
        on_delete="NO ACTION",
        on_update="NO ACTION",
    )
    contractor = ForeignKeyField(
        Company,
        backref="contracts_as_contractor",
        column_name="contractor_id",
        on_delete="NO ACTION",
        on_update="NO ACTION",
    )
    contract_period_end = DateField(null=True)

    class Meta:
        database = db
        table_name = "mining_contract"
        primary_key = CompositeKey("mine_owner", "contractor")


class CompanyOwnership(Model):
    parent_company = ForeignKeyField(
        Company,
        backref="child_ownerships",
        column_name="parent_company_id",
        on_delete="NO ACTION",
        on_update="NO ACTION",
    )
    company = ForeignKeyField(
        Company,
        backref="parent_ownerships",
        column_name="company_id",
        on_delete="NO ACTION",
        on_update="NO ACTION",
    )
    percentage_ownership = DecimalField()

    class Meta:
        database = db
        table_name = "company_ownership"
        primary_key = CompositeKey("parent_company", "company")


class CompanyPerformance(Model):
    company = ForeignKeyField(
        Company,
        backref="performance_records",
        column_name="company_id",
        on_delete="NO ACTION",
        on_update="NO ACTION",
    )
    year = IntegerField()
    commodity_type = TextField()
    commodity_sub_type = TextField(null=True)
    commodity_stats = TextField()
    slug = TextField()

    class Meta:
        database = db
        table_name = "company_performance"


class ExportDestination(Model):
    country = TextField()
    year = IntegerField()
    commodity_type = TextField()
    export_USD = DecimalField(null=True)
    export_volume_BPS = DecimalField(null=True)
    export_volume_ESDM = DecimalField(null=True)

    class Meta:
        database = db
        table_name = "export_destination"


class ResourcesAndReserves(Model):
    province = TextField()
    year = IntegerField()
    commodity_type = TextField()
    resources_reserves = TextField()

    class Meta:
        database = db
        table_name = "resources_and_reserves"


class TotalCommoditiesProduction(Model):
    commodity_type = TextField()
    production_volume = DecimalField()
    unit = TextField()
    year = IntegerField()

    class Meta:
        database = db
        table_name = "total_commodities_production"


class CommodityPrice(Model):
    name = TextField()
    price = DecimalField()
    date = TextField()

    class Meta:
        database = db
        table_name = "commodity_price"
        primary_key = CompositeKey("name", "date")


class GlobalCommodityData(Model):
    country = TextField()
    resources_reserves = TextField(null=True)
    resources_reserves_share = TextField(null=True)
    export_import = TextField(null=True)
    production_volume = TextField(null=True)
    production_share = TextField(null=True)
    commodity_type = TextField()

    class Meta:
        database = db
        table_name = "global_commodity_data"


class MiningLicense(Model):
    license_type = TextField()
    license_number = TextField()
    wiup_code = TextField()
    province = TextField()
    city = TextField()
    license_effective_date = DateField()
    license_expiry_date = DateField()
    activity = TextField()
    licensed_area = DecimalField()
    location = TextField()
    commodity_type = TextField()
    company_name = TextField()
    company = ForeignKeyField(
        Company,
        backref="licenses",
        column_name="company_id",
        null=True,
        on_delete="NO ACTION",
        on_update="NO ACTION",
    )

    class Meta:
        database = db
        table_name = "mining_license"


class MiningNews(Model):
    title = TextField()
    body = TextField()
    source = TextField()
    timestamp = DateTimeField()
    commodity_type = TextField()
    created_at = DateTimeField()

    class Meta:
        database = db
        table_name = "mining_news"


class SalesDestination(Model):
    company = ForeignKeyField(
        Company,
        backref="sales_destinations",
        column_name="company_id",
        null=True,
        on_delete="NO ACTION",
        on_update="NO ACTION",
    )
    country = TextField()
    idx_ticker = TextField()
    year = IntegerField()
    revenue = DecimalField(null=True)
    percentage_of_total_revenue = DecimalField(null=True)
    volume = DecimalField(null=True)
    percentage_of_sales_volume = DecimalField(null=True)

    class Meta:
        database = db
        table_name = "sales_destination"


class CompanyFinancials(Model):
    company = ForeignKeyField(
        Company,
        backref="financials",
        column_name="company_id",
        on_delete="NO ACTION",
        on_update="NO ACTION",
    )
    idx_ticker = TextField()
    name = TextField()
    year = IntegerField()
    assets = DecimalField()
    revenue = DecimalField()
    revenue_breakdown = TextField()
    cost_of_revenue = DecimalField()
    cost_of_revenue_breakdown = TextField()
    net_profit = DecimalField()
    slug = TextField()

    class Meta:
        database = db
        table_name = "company_financials"
        primary_key = CompositeKey("idx_ticker", "year")


class MiningSite(Model):
    name = TextField()
    project_name = TextField(null=True)
    year = IntegerField()
    mineral_type = TextField(
        constraints=[Check(f"mineral_type IN {mineral_type_constraints}")]
    )
    company = ForeignKeyField(
        Company,
        backref="mining_sites",
        on_delete="NO ACTION",
        on_update="NO ACTION",
    )
    production_volume = DecimalField(max_digits=10, decimal_places=5, null=True)
    overburden_removal_volume = DecimalField(max_digits=10, decimal_places=5, null=True)
    strip_ratio = DecimalField(max_digits=10, decimal_places=5, null=True)
    resources_reserves = TextField(
        null=True, constraints=[Check("json_valid(resources_reserves)")]
    )
    location = TextField(null=True, constraints=[Check("json_valid(location)")])

    class Meta:
        database = db
        table_name = "mining_site"


class MiningLicenseAuction(Model):
    commodity = TextField()
    city = TextField(null=True)
    province = TextField(null=True)
    company_name = TextField(null=True)
    date_winner = TextField(null=True)
    permit_area = DecimalField(null=True)
    number = TextField(null=True, unique=True)
    permit_type = TextField(null=True)
    kdi = TextField(null=True)
    code_wiup = TextField(null=True)
    auction_status = TextField(null=True)
    created_at = DateTimeField(null=True)
    last_modified = DateTimeField(null=True)
    participant_count = IntegerField(null=True)
    phases = TextField(null=True)
    participants = TextField(null=True)
    winner = TextField(null=True)
    company = ForeignKeyField(
        Company,
        backref="license_auctions",
        column_name="company_id",
        null=True,
        on_delete="NO ACTION",
        on_update="NO ACTION",
    )

    class Meta:
        database = db
        table_name = "mining_license_auctions"
