QUERY_ETAT_STOCK = """
SELECT DISTINCT 
    processing_periods.enddate AS enddate,
    facilities.code AS code_site,
    facilities.name AS site,
    requisition_line_items.productcode as code_produit,
    requisition_line_items.beginningbalance AS Stock_initial, --MxwO32EmLkm : SIGL-Quantité reçue au cours du  mois
    requisition_line_items.quantityreceived AS Quantite_recue, -- VpsWXngJn8m : SIGL-Quantité disponible et utilisable en début mois
    requisition_line_items.quantitydispensed AS Quantite_distribuee, -- r4Y2vAZNFJr : SIGL-Quantité consommée au cours du  mois
    requisition_line_items.totallossesandadjustments AS Perte_ajustement, -- DaYWwwQWpzO : SIGL-Pertes et ajustements au cours du  mois
    requisition_line_items.stockinhand AS SDU, -- MsVzBFeQy98 : SIGL-Stock Disponible et Utilisatble 
    requisition_line_items.amc AS cmm, -- tAviNwTJA69 : SIGL-Consommation Moyenne Mensuel
    requisition_line_items.stockoutdays AS NbreJrsRupture, -- lmIvSiYc80L : SIGL-Nbre de jours de rupture de stock au cours du  mois
    requisition_line_items.calculatedorderquantity AS Quantite_proposee, -- cpDZa6GSME2 : SIGL-Quantité suggérée
    requisition_line_items.quantityrequested AS Quantite_commandee, -- qz4cXueOt5p : SIGL-Quantité commander
    requisition_line_items.quantityapproved AS Quantite_approuvee -- TnEwztOelac : SIGL-Quantité approuvée 
FROM requisition_line_items
JOIN requisitions ON requisition_line_items.rnrid = requisitions.id
JOIN products ON requisition_line_items.productcode::text = products.code::text
JOIN programs ON requisitions.programid = programs.id
JOIN program_products ON products.id = program_products.productid AND program_products.programid = programs.id
JOIN processing_periods ON requisitions.periodid = processing_periods.id
JOIN product_categories ON program_products.productcategoryid = product_categories.id
JOIN processing_schedules ON processing_periods.scheduleid = processing_schedules.id
JOIN facilities ON requisitions.facilityid = facilities.id
JOIN facility_operators ON facilities.operatedbyid = facility_operators.id
JOIN facility_types ON facilities.typeid = facility_types.id
JOIN vw_districts  ON facilities.geographiczoneid = vw_districts.district_id
JOIN geographic_zones ON facilities.geographiczoneid = geographic_zones.id
LEFT JOIN product_forms ON products.formid = product_forms.id
LEFT JOIN dosage_units ON products.dosageunitid = dosage_units.id
WHERE
    {processing_periods}
    AND programs.id IN (19, 23)
    AND requisitions.status NOT IN ('INITIATED', 'SUBMITTED')
    AND requisitions.emergency = false
"""  # noqa: E501

# doit être ajouté s'il existe AND facilities.code IN {facilities_code}

QUERY_ETAT_STOCK_OLD = """
SELECT
    programs.name AS Programme,
    processing_periods.startdate AS startdate,
    geographic_zones.name AS District,
    requisition_line_items.productcode AS Code_produit,
    SUM(requisition_line_items.beginningbalance) AS Stock_initial, --MxwO32EmLkm : SIGL-Quantité reçue au cours du  mois
    SUM(requisition_line_items.quantityreceived) AS Quantite_recue, -- VpsWXngJn8m : SIGL-Quantité disponible et utilisable en début mois
    SUM(requisition_line_items.quantitydispensed) AS Quantite_distribuee, -- r4Y2vAZNFJr : SIGL-Quantité consommée au cours du  mois
    SUM(requisition_line_items.totallossesandadjustments) AS Perte_ajustement, -- DaYWwwQWpzO : SIGL-Pertes et ajustements au cours du  mois
    SUM(requisition_line_items.stockinhand) AS SDU, -- MsVzBFeQy98 : SIGL-Stock Disponible et Utilisatble 
    SUM(requisition_line_items.amc) AS cmm, -- tAviNwTJA69 : SIGL-Consommation Moyenne Mensuel
    SUM(requisition_line_items.stockoutdays) AS NbreJrsRupture, -- lmIvSiYc80L : SIGL-Nbre de jours de rupture de stock au cours du  mois
    SUM(requisition_line_items.calculatedorderquantity) AS Quantite_proposee, -- cpDZa6GSME2 : SIGL-Quantité suggérée
    SUM(requisition_line_items.quantityrequested) AS Quantite_commandee, -- qz4cXueOt5p : SIGL-Quantité commander
    SUM(requisition_line_items.quantityapproved) AS Quantite_approuvee -- TnEwztOelac : SIGL-Quantité approuvée 
FROM requisition_line_items
JOIN requisitions 
    ON requisition_line_items.rnrid = requisitions.id
    AND requisitions.status NOT IN ('INITIATED', 'SUBMITTED')
    AND requisitions.emergency = FALSE
    AND requisitions.programid = '23'
JOIN processing_periods 
    ON requisitions.periodid = processing_periods.id
    AND {processing_periods}
JOIN programs 
    ON requisitions.programid = programs.id
JOIN products 
    ON requisition_line_items.productcode = products.code
    AND requisition_line_items.productcode IN {products_code}
JOIN facilities 
    ON requisitions.facilityid = facilities.id
JOIN geographic_zones 
    ON facilities.geographiczoneid = geographic_zones.id
WHERE
    requisition_line_items.skipped = FALSE
    AND requisition_line_items.fullsupply = TRUE
GROUP BY 
    programs.name, 
    processing_periods.startdate, 
    geographic_zones.name, 
    requisition_line_items.productcode
ORDER BY 
    processing_periods.startdate DESC
"""  # noqa: E501

QUERY_DISTRICT = """
SELECT DISTINCT
    vw_districts.region_name AS region,
    geographic_zones.name AS district,
    geographic_zones.id AS id_district,
    facilities.code AS code_etablissement,
    facilities.name AS etablissement
FROM vw_districts
    JOIN facilities ON vw_districts.district_id  = facilities.geographiczoneid
    JOIN geographic_zones ON facilities.geographiczoneid = geographic_zones.id
ORDER BY region
"""

# processing_periods.startdate >= DATE_TRUNC('month', CURRENT_DATE)
#  - INTERVAL '{lookback_months} months'
