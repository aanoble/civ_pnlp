"""Tests de garde sur le module de mapping produit → COC."""

from coc_mapping import COC_MAPPING
from product_aliases import PRODUCT_CODE_ALIASES


def test_coc_mapping_nonempty_and_valid_uids() -> None:
    """Le mapping est non vide, à codes numériques et COC = UID DHIS2 (11 caractères)."""
    assert COC_MAPPING, "COC_MAPPING vide"
    for code, coc in COC_MAPPING.items():
        assert code.isdigit(), f"code_produit non numérique : {code}"
        assert isinstance(coc, str), f"COC non textuel pour {code} : {coc!r}"
        assert len(coc) == 11, f"COC (UID DHIS2) de longueur invalide pour {code} : {coc!r}"


def test_product_aliases_point_to_known_products() -> None:
    """Chaque ancien code pointe vers un code produit présent dans COC_MAPPING."""
    assert PRODUCT_CODE_ALIASES, "PRODUCT_CODE_ALIASES vide"
    for old, new in PRODUCT_CODE_ALIASES.items():
        assert old not in COC_MAPPING, f"ancien code {old} ne doit pas être un code actuel"
        assert new in COC_MAPPING, f"cible inconnue pour l'ancien code {old} : {new}"
