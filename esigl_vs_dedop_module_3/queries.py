QUERY_ETAT_STOCK = """
SELECT DISTINCT
    TO_CHAR(processing_periods.enddate, 'YYYYMM') AS period,
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

QUERY_ETAT_STOCK_GTC = """
WITH tb_gtc AS (
    SELECT DISTINCT
        TO_CHAR(pp.startdate, 'YYYYMM') AS period,
        pp.startdate,
        pp.enddate,
        pp.name,
        f.code AS code_site,
        f.name AS site,
        fo.text AS type_structure,
        gz.name AS district,
        rli.productcode AS code_produit,
        rli.skipped,
        rli.beginningbalance AS stock_initial,
        rli.quantityreceived AS quantite_recue,
        rli.quantitydispensed AS quantite_distribuee,
        rli.totallossesandadjustments AS perte_ajustement,
        rli.stockinhand AS sdu,
        rli.amc AS cmm,
        rli.stockoutdays AS nbrejrsrupture,
        rli.calculatedorderquantity AS quantite_proposee,
        rli.quantityrequested AS quantite_commandee,
        rli.quantityapproved AS quantite_approuvee
    FROM requisition_line_items rli
    INNER JOIN requisitions r ON rli.rnrid = r.id
    INNER JOIN processing_periods pp ON r.periodid = pp.id
    INNER JOIN facilities f ON r.facilityid = f.id
    INNER JOIN programs p ON r.programid = p.id
    INNER JOIN products pr ON rli.productcode = pr.code
    INNER JOIN program_products ppd ON (pr.id = ppd.productid AND ppd.programid = p.id)
    INNER JOIN facility_operators fo ON f.operatedbyid = fo.id
    INNER JOIN geographic_zones gz ON f.geographiczoneid = gz.id
    INNER JOIN vw_districts vd ON f.geographiczoneid = vd.district_id
    WHERE rli.skipped = FALSE
        AND {products_code}
        AND {processing_periods}
        AND p.id IN (19, 23)
        AND r.status NOT IN ('INITIATED', 'SUBMITTED')
        AND r.emergency = FALSE
        AND rli.fullsupply = TRUE
),
latest_records AS (
    SELECT
        code_site,
        code_produit,
        MAX(startdate) AS max_date
    FROM tb_gtc
    GROUP BY code_site, code_produit
)
SELECT
    tgtc.*,
    lr.max_date
FROM tb_gtc tgtc
INNER JOIN latest_records lr
    ON tgtc.code_site = lr.code_site
    AND tgtc.code_produit = lr.code_produit
    AND tgtc.startdate = lr.max_date
ORDER BY tgtc.code_site, tgtc.code_produit
"""

NEW_QUERY_ETAT_STOCK_GTC = """
SELECT
    TO_CHAR(pp.enddate, 'YYYYMM') AS period,
    pp.enddate AS enddate,
    f.code AS code_site,
    rli.productcode AS code_produit,
    rli.beginningbalance AS stock_initial,
    rli.quantityreceived AS quantite_recue,
    rli.quantitydispensed AS quantite_distribuee,
    rli.stockoutdays AS nbrejrsrupture,
    rli.totallossesandadjustments AS perte_ajustement,
    -- rli.calculatedorderquantity AS quantite_proposee,
    -- rli.quantityrequested AS quantite_commandee,
    -- rli.quantityapproved AS quantite_approuvee,
    rli.stockinhand AS sdu
FROM requisition_line_items rli
INNER JOIN requisitions r ON rli.rnrid = r.id
INNER JOIN processing_periods pp ON r.periodid = pp.id
INNER JOIN facilities f ON r.facilityid = f.id
INNER JOIN programs p ON r.programid = p.id
INNER JOIN products pr ON rli.productcode = pr.code
INNER JOIN program_products ppd ON (pr.id = ppd.productid AND ppd.programid = p.id)
INNER JOIN geographic_zones gz ON f.geographiczoneid = gz.id
WHERE rli.skipped = FALSE
    AND {products_code}
    AND {processing_periods}
    AND p.id = 19
    AND r.status NOT IN ('INITIATED', 'SUBMITTED')
    AND r.emergency = FALSE
    AND rli.fullsupply = TRUE
ORDER BY enddate, code_site, code_produit
"""
