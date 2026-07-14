"""Constantes DHIS2 du module Gestion de Stock (refonte eSIGL → DHIS2).

Le modèle de données cible porte le **produit** dans le `categoryOptionCombo` (COC)
et la **métrique** dans le `dataElement` (DE). Les UID des 21 dataElements sont donc
figés ici (source : `other files/module 3 refonte/payload_dataElements.json`), au lieu
d'être résolus dynamiquement depuis DHIS2 comme dans l'ancien modèle.
"""

# --- Cibles DHIS2 définitives -------------------------------------------------

#: Dataset unique du module Gestion de Stock (routine + GTC + promptitude).
TARGET_DATASET_ID = "KyO2eSxVW4q"

#: Le mapping produit → COC est versionné dans le module ``coc_mapping.py`` (``COC_MAPPING``),
#: pas dans un ``.json`` : la CLI OpenHEXA n'embarque que .py/.ipynb/.txt/.md/.r/.sql au push.

#: Nom de l'``categoryOptionCombo`` par défaut d'une instance DHIS2 (résolution AOC auto).
DEFAULT_COC_NAME = "default"

#: `categoryCombo` attendu des DE portant un produit (familles métriques + dérivés).
PRODUCT_CATEGORY_COMBO = "UxWRWV9jfqG"

#: `categoryCombo` attendu des DE de promptitude des rapports.
REPORT_CATEGORY_COMBO = "bjDvmb4bfuf"

# --- Programmes eSIGL ---------------------------------------------------------

#: Programmes eSIGL inclus dans l'extraction des états de stock (19 = GTC, 23 = routine).
STOCK_PROGRAM_IDS = (19, 23)

#: Programme eSIGL utilisé pour la promptitude des rapports.
PROMPTITUDE_PROGRAM_ID = 23

# --- Mapping métrique → UID dataElement (COC = produit) -----------------------

#: Métriques brutes eSIGL + indicateurs dérivés (categoryCombo = PRODUCT_CATEGORY_COMBO).
DE_MAPPING: dict[str, str] = {
    # Métriques brutes issues de requisition_line_items
    "stock_initial": "dQvauonTgfA",
    "quantite_recue": "Szsz0XHYiAe",
    "quantite_distribuee": "WqNI4Edv8WM",
    "quantite_proposee": "fpEMLfiW8Uz",
    "quantite_commandee": "YfGFeJSQ7p9",
    "quantite_approuvee": "hE28ONCxXcM",
    "perte_ajustement": "kbn3GYfJXXF",
    "sdu": "VshSCHiBPgE",
    "cmm": "V1s6mq1YEMO",
    "nbrejrsrupture": "OG31bo2TLYg",
    # Indicateurs dérivés calculés par le pipeline
    "nbrejrsdumois": "IcRli8NshIf",
    "produit_gere": "cKAxI1SPpmj",
    "categorie_produit_traceur": "i8cadk3Qmqz",
    "produit_non_traceur": "zGCY3xVNFTY",
    "rupture_stock": "dsXnvvMNB0f",
    "rupture_traceur_stock": "dFOhiqVwkGL",
    "rupture_non_traceur_stock": "ubzT7coWWiW",
    "cmm_gestionnaire": "tlYrqLFWMqW",
    "bien_stocke": "ZIVrMUyAN0Q",
}

#: Promptitude des rapports (categoryCombo = REPORT_CATEGORY_COMBO, COC = AOC par défaut).
PROMPTITUDE_DE_MAPPING: dict[str, str] = {
    "rapport_prompt": "E60usFLi06D",
    "rapport_attendu": "v7dA6nznX1m",
}

#: DE dont les zéros sont significatifs (zeroIsSignificant=true) → à pousser explicitement.
#: Toutes les autres métriques ont zeroIsSignificant=false ; leurs zéros sont ignorés par
#: DHIS2 à l'import, on les filtre donc à la construction du payload.
ZERO_SIGNIFICANT_METRICS: frozenset[str] = frozenset({"rapport_prompt", "rapport_attendu"})

#: Colonnes calculées comme des flags 0/1 (dérivés booléens).
FLAG_METRICS: frozenset[str] = frozenset(
    {
        "produit_gere",
        "categorie_produit_traceur",
        "produit_non_traceur",
        "rupture_stock",
        "rupture_traceur_stock",
        "rupture_non_traceur_stock",
    }
)

# --- Promptitude : délais de transmission par type de structure ---------------

#: Jour-limite de transmission du mois pour être « prompt », par type de structure.
PROMPTITUDE_DEADLINE_DAYS: dict[str, int] = {
    "DISTRICT SANITAIRE": 10,
    "CHU": 10,
}

#: Délai par défaut (jours) si le type de structure n'est pas dans PROMPTITUDE_DEADLINE_DAYS.
PROMPTITUDE_DEFAULT_DEADLINE_DAY = 7

# --- Classification produit (fallback) ----------------------------------------

#: Type de produit « GTC » (par opposition à « ROUTINE ») dans la feuille de mapping.
GTC_PRODUCT_TYPE = "GTC"

#: Liste de repli des codes produits GTC si la classification externe est absente.
#: Source : notebook de refonte (`extract_data_from_metabase.ipynb`).
FALLBACK_GTC_PRODUCT_CODES: frozenset[str] = frozenset(
    {
        "3010050",
        "3010064",
        "3010074",
        "3020013",
        "3050055",
        "3050058",
        "3050075",
        "3050345",
        "3050346",
        "3050349",
        "3050380",
        "3060027",
        "3230027",
        "3230056",
        "3230059",
        "4150146",
        "4150564",
        "4150567",
        "4150752",
        "4150763",
    }
)
