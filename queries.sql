SELECT
	c.id as company_id,
    c.name AS company_name,
    c.company_type,
    co.parent_company_id, 
    parent.name AS parent_company_name,
    COALESCE(parent3.name, parent2.name, parent.name) AS company_group
FROM
    company c
LEFT JOIN 
    company_ownership co ON co.company_id = c.id
LEFT JOIN
	company parent ON parent.id = co.parent_company_id 
LEFT JOIN
	company_ownership co2 ON co2.company_id= co.parent_company_id
LEFT JOIN
	company parent2 ON parent2.id = co2.parent_company_id
LEFT JOIN
	company_ownership co3 ON co3.company_id = co2.parent_company_id
LEFT JOIN
	company parent3 ON parent3.id = co3.parent_company_id
	
SELECT 
	c.id AS company_id,
    c.name AS company_name,
    c.operation_location,
    c.key_operation,
    c.company_type,
    p.year,
    p.production_volume,
    p.strip_ratio,
    p.overburden_removal_volume,
    p.sales_volume,
    p.mineral_type,
    p.calorific_value,
    p.mining_operation_status,
    p.reserve,
    p.resource
FROM
    company c
LEFT JOIN
    coal_company_performance p ON c.id = p.company_id
	