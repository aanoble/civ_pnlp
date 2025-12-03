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
# processing_periods.startdate >= DATE_TRUNC('month', CURRENT_DATE)
#  - INTERVAL '{lookback_months} months'
# pp.startdate BETWEEN '2025-01-01' AND '2025-01-31'
#  rli.productcode IN ('3050055', '3050058', '3050075', '3050345', '3050346', '3050349')
