select
    --programs.name AS Programme,
    TO_CHAR(processing_periods.enddate, 'YYYYMM') AS period,
    facilities.code AS code_site,
    --vw_districts.region_name AS Region,
    --geographic_zones.name AS District,
    facilities.name AS site,
    --facility_operators.text as Type_structure,
    -- geographic_zones.name AS District,
    requisition_line_items.productcode as Code_produit,
    --requisition_line_items.skipped,
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
from
	facilities
left join geographic_zones on
	facilities.geographiczoneid = geographic_zones.id
left join vw_districts on
	facilities.geographiczoneid = vw_districts.district_id
left join requisitions on
	facilities.id = requisitions.facilityid
left join requisition_line_items on
	requisitions.id = requisition_line_items.rnrid
left join programs on
	requisitions.programid = programs.id
left join processing_periods on
	requisitions.periodid = processing_periods.id
left join products on
	requisition_line_items.productcode = products.code
left join program_products on
	products.id = program_products.productid
left join product_categories on
	program_products.productcategoryid = product_categories.id
where
	programs.id in (19, 23)
	and to_char(processing_periods.enddate, 'YYYY') = '2017'
	and requisition_line_items.productcode in (
	'AY13071', 'AY13115', 'AY13075', 'AY02020', 'AM02015', 'AY02015', 'AY02027', 'AM02028', 'AY02028', 'AM01033',
	'AY02252', 'AM02254', 'AY02254', 'AM02253', 'AY02253', 'AM02251', 'AY02251', 'AY02177', 'AY02172', 'AM02172',
	'AY02175', 'AM02275', 'AY02275', 'AM02276', 'AY02276', 'AM02272', 'AY02272', 'AY02271', 'AM02270', 'AY02270',
	'AM02274', 'AY02274', 'AY02277', 'AM02277', 'AY02068', 'AY02069', 'AY23050', 'AY23020', 'AM18180', 'AY18180',
	'AM18181', 'AY18181', 'AY47075', 'AY47076', 'AY42150', 'AY42140', 'AY24350', 'AY24355', 'AY02140', 'AY42050',
	'AY03410', 'AY23060', 'AY24230', 'AM26156', 'AY26156', 'AY42051', 'AY23238', 'AY23237', 'AY23239', 'AY23236',
	'AY23230', 'AY23235', 'AY42350', 'AY42365', 'AM02172', '4150556', '3050040', '3050398', 'AY01033', 'AM02252',
	'4030177', '4150558', '4060061', '3010062', '3080086', '3050342', '3050016', '4150562',
    '4030209', '4030032', '4030247', '4150557', '3050340', '3050062', '3050200', '4150954',
    '3050071', '4030266', '3050070', '3050074', '3050383', '4150554', '3050012', '3050198',
    '3050064', '4150555', '4030224', '4150091', '3050343', '4030462', '3050339', '4030456',
    '3050061', '3050013', '4030208', '3080129', '3050015', '3050199', '3010077', '3050347',
    '3050348', '3230032', '3050063', '3010049', '3130041', '4150958', '4030170', '4030021')
	and requisitions.emergency = false
	and requisitions.status <> 'INITIATED'
	and requisitions.status <> 'SUBMITTED'
