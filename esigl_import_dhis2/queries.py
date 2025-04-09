QUERY_ETAT_STOCK = """
WITH requisition_aggregates AS (
    SELECT
        programs.name AS Programme,
        processing_periods.startdate AS startdate,
        geographic_zones.name AS District, 
        requisition_line_items.productcode as Code_produit,
        requisition_line_items.skipped,
        requisition_line_items.beginningbalance,
        requisition_line_items.quantityreceived,
        requisition_line_items.quantitydispensed,
        requisition_line_items.totallossesandadjustments,
        requisition_line_items.stockinhand,
        requisition_line_items.amc,
        requisition_line_items.stockoutdays,
        requisition_line_items.calculatedorderquantity,
        requisition_line_items.quantityrequested,
        requisition_line_items.quantityapproved
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
    requisition_line_items.skipped = FALSE
    AND requisition_line_items.productcode in {products_code}
    AND processing_periods.startdate >= date_trunc('month', current_date) - INTERVAL '{lookback_months} months'
    AND programs.id = '23'
    AND requisitions.status <> 'INITIATED'
    AND requisitions.status <> 'SUBMITTED'
    AND requisitions.emergency = FALSE
    AND requisition_line_items.fullsupply = TRUE
)
SELECT
    Programme,
    startdate,
    District, 
    Code_produit,
    SUM(beginningbalance) AS Stock_initial, --MxwO32EmLkm : SIGL-Quantité reçue au cours du  mois
    SUM(quantityreceived) AS Quantite_recue, -- VpsWXngJn8m : SIGL-Quantité disponible et utilisable en début mois
    SUM(quantitydispensed) AS Quantite_distribuee, -- r4Y2vAZNFJr : SIGL-Quantité consommée au cours du  mois
    SUM(totallossesandadjustments) AS Perte_ajustement, -- DaYWwwQWpzO : SIGL-Pertes et ajustements au cours du  mois
    SUM(stockinhand) AS SDU, -- MsVzBFeQy98 : SIGL-Stock Disponible et Utilisatble 
    SUM(amc) AS cmm, -- tAviNwTJA69 : SIGL-Consommation Moyenne Mensuel
    SUM(stockoutdays) AS NbreJrsRupture, -- lmIvSiYc80L : SIGL-Nbre de jours de rupture de stock au cours du  mois
    SUM(calculatedorderquantity) AS Quantite_proposee, -- cpDZa6GSME2 : SIGL-Quantité suggérée
    SUM(quantityrequested) AS Quantite_commandee, -- qz4cXueOt5p : SIGL-Quantité commander
    SUM(quantityapproved) AS Quantite_approuvee -- TnEwztOelac : SIGL-Quantité approuvée 
FROM requisition_aggregates
GROUP BY 
    Programme, 
    startdate, 
    District, 
    Code_produit
order by startdate desc
"""  # noqa: E501

QUERY_DISTRICT = """
SELECT DISTINCT
    vw_districts.region_name AS region,
    geographic_zones.name AS district,
    geographic_zones.id AS id_district,
    facilities.code AS code,
    facilities.name AS etablissement
FROM vw_districts
    JOIN facilities ON vw_districts.district_id  = facilities.geographiczoneid
    JOIN geographic_zones ON facilities.geographiczoneid = geographic_zones.id
ORDER BY region
"""