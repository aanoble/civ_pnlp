QUERY_ETAT_STOCK = """
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
    AND processing_periods.startdate >= DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '{lookback_months} months'
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
    facilities.code AS code,
    facilities.name AS etablissement
FROM vw_districts
    JOIN facilities ON vw_districts.district_id  = facilities.geographiczoneid
    JOIN geographic_zones ON facilities.geographiczoneid = geographic_zones.id
ORDER BY region
"""