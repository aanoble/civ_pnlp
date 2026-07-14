"""Requêtes SQL Metabase (base eSIGL) du pipeline d'import Gestion de Stock.

Chaque requête expose des emplacements `{...}` formatés côté pipeline :
- ``{processing_periods}`` : clause de bornage temporel (sur ``pp.enddate``).
- ``{products_code}`` : clause de filtrage des produits (``rli.productcode IN (...)``).
"""

# Extraction unifiée des états de stock (routine + GTC), grain ligne de réquisition.
# Le split routine/GTC est effectué en aval (classification produit), la requête est
# volontairement identique aux deux flux pour limiter la charge Metabase.
QUERY_ETAT_STOCK = """
SELECT
    TO_CHAR(pp.enddate, 'YYYYMM') AS period,
    pp.enddate AS enddate,
    pp.startdate AS startdate,
    f.code AS code_site,
    rli.productcode AS code_produit,
    rli.beginningbalance AS stock_initial,
    rli.quantityreceived AS quantite_recue,
    rli.quantitydispensed AS quantite_distribuee,
    rli.stockoutdays AS nbrejrsrupture,
    rli.totallossesandadjustments AS perte_ajustement,
    rli.calculatedorderquantity AS quantite_proposee,
    rli.quantityrequested AS quantite_commandee,
    rli.quantityapproved AS quantite_approuvee,
    rli.amc AS cmm,
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
    AND {processing_periods}
    AND {products_code}
    AND {facilities}
    AND p.id IN (19, 23)
    AND r.status NOT IN ('INITIATED', 'SUBMITTED')
    AND r.emergency = FALSE
    AND rli.fullsupply = TRUE
ORDER BY enddate, code_site, code_produit
"""

# Promptitude des rapports : date de soumission vs date-limite par structure.
# On récupère la première soumission (SUBMITTED) et la première autorisation (AUTHORIZED)
# par réquisition, avec le type de structure servant à déterminer le délai autorisé.
QUERY_PROMPTITUDE = """
WITH submission_dates AS (
    SELECT
        rnrid,
        MIN(createddate) FILTER (WHERE status = 'SUBMITTED')::date AS date_soumission,
        MIN(createddate) FILTER (WHERE status = 'AUTHORIZED')::date AS date_autorisation
    FROM requisition_status_changes
    WHERE status IN ('SUBMITTED', 'AUTHORIZED')
    GROUP BY rnrid
)
SELECT
    TO_CHAR(pp.enddate, 'YYYYMM') AS period,
    pp.enddate AS enddate,
    f.code AS code_site,
    fo.text AS type_structure,
    sd.date_soumission,
    sd.date_autorisation
FROM requisitions r
JOIN facilities f ON f.id = r.facilityid
JOIN facility_operators fo ON f.operatedbyid = fo.id
JOIN vw_districts vd ON f.geographiczoneid = vd.district_id
JOIN geographic_zones gz ON gz.id = f.geographiczoneid
JOIN processing_periods pp ON pp.id = r.periodid
JOIN programs pr ON pr.id = r.programid
LEFT JOIN submission_dates sd ON r.id = sd.rnrid
WHERE pr.id = {promptitude_program_id}
    AND {processing_periods}
    AND {facilities}
    AND r.emergency = FALSE
    AND r.status IN ('AUTHORIZED', 'APPROVED', 'RELEASED')
ORDER BY period ASC
"""

# Liste des codes de site (pour la validation des paramètres facilities_code).
QUERY_FACILITIES = "SELECT code FROM facilities"
