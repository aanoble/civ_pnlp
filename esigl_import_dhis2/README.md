# eSIGL → DHIS2 — Module Gestion de Stock (pipeline unifié)

Pipeline OpenHEXA unifié qui extrait les données de gestion de stock d'**eSIGL** (via Metabase)
et les publie dans le module **Gestion de Stock** de DHIS2 (workspace `nmdr-civ`).

Il **remplace** les deux anciens pipelines `esigl_import_dhis2` (routine) et
`esigl_import_dhis2_gtc` (GTC), désormais fusionnés. La promptitude des rapports y est intégrée.

## Modèle de données cible

Le modèle porte le **produit** dans le `categoryOptionCombo` (COC) et la **métrique** dans le
`dataElement` (DE) — l'inverse de l'ancien modèle. Les 21 DE sont figés dans `constants.py`
(source : `other files/module 3 refonte/payload_dataElements.json`).

| Famille | categoryCombo | COC utilisé | zeroIsSignificant |
|---|---|---|---|
| Métriques brutes + dérivés produit | `UxWRWV9jfqG` | COC **produit** (mapping) | non (zéros filtrés) |
| Promptitude (`rapport_prompt/attendu`) | `bjDvmb4bfuf` | COC **par défaut** (AOC) | oui (zéros poussés) |

## Flux (DAG)

```
read_mappings ─► resolve_aoc ─► validate_target_metadata ─► add_missing_orgunits ─┐
                                                                                 ▼
extract_stock ─► compute_derived ─┐
extract_promptitude ──────────────┴─► build_payload ─► push_data_to_dhis2
                                                          └─► align_stale_values
                                                                └─► write_import_report ─► cleanup
```

- **validate_target_metadata** : vérifie l'existence et le `categoryCombo` des 21 DE, l'AOC,
  le dataset, et que **chaque COC produit ∈ le categoryCombo produit** (sinon `ignored` silencieux).
- **compute_derived** : agrège routine (CMM eSIGL) et GTC (CMM en moyenne glissante 3 mois),
  puis calcule tous les indicateurs dérivés (traceur, ruptures, `cmm_gestionnaire`, `bien_stocke`).
- **push_data_to_dhis2** : chunks + retry/backoff (429/5xx), agrège `importCount`/`conflicts`,
  et **échoue** si `ignored/total > max_ignored_ratio`.
- **align_stale_values** (opt-in) : DELETE ciblé des flags dérivés obsolètes (correction d'un
  passage 1→0, impossible via un simple upsert car DHIS2 ignore les zéros non significatifs).

## Paramètres principaux

| Paramètre | Défaut | Rôle |
|---|---|---|
| `dhis2_connection` | `dedop-nouvelle-instance` | Instance DHIS2 cible (définitive) |
| `metabase_connection` | `metabase-esigl` | Source eSIGL |
| `fp_ou_mapping` | `.../Fichier mapping OrgUnit eSIGL DHIS2.xlsx` | Feuilles `OrgUnit`, `Traceurs`, `R ou G` |
| `fp_site_attendus` | `.../site_attendus/sites_attendus.csv` | Sites attendus consolidés (promptitude) |
| `dataset_id` | `KyO2eSxVW4q` | Dataset Gestion de Stock (sélecteur DHIS2) |
| `dhis2_aoc` | *(vide)* | AOC (et COC promptitude). **Facultatif** : si vide, l'AOC par défaut de l'instance (`default`) est résolu automatiquement |
| `start_date` / `end_date` | (mois courant) | Fenêtre d'extraction |
| `months_back` | `3` | Historique republié avant `start_date` |
| `enable_promptitude` | `True` | Branche promptitude |
| `enable_bien_stocke` | `False` | Indicateur `bien_stocke` (règle **provisoire**) |
| `delete_stale_values` | `False` | Alignement des suppressions (DELETE) |
| `max_ignored_ratio` | `0.05` | Seuil d'échec sur les `ignored` |
| `import_mode` | `CREATE_AND_UPDATE` | Stratégie d'import |
| `dry_run` | `False` | Simulation |

## Mapping produit → COC (module versionné)

Le mapping `code_produit → coc` vit dans **`coc_mapping.py`** (`COC_MAPPING`, clé-valeur),
versionné avec le pipeline. C'est un fichier `.py` : il est **embarqué au push OpenHEXA**
(la CLI n'inclut que `.py/.ipynb/.txt/.md/.r/.sql` — un `.json` serait exclu du bundle).
Aucun `@parameter File` à importer, aucun upload workspace.

```python
COC_MAPPING = {
    "3010016": "yFj3lQaotI8",  # AMODIAQUINE 153 MG + SULFADOX/PYRIMETHA …
}
```

Pour ajouter/modifier un produit : éditer `coc_mapping.py` puis committer.

### Anciens codes produits (< 2020)

Avant 2020, certains produits avaient une **codification différente**. eSIGL renvoie donc
l'ancien code pour les périodes antérieures. Le module **`product_aliases.py`**
(`PRODUCT_CODE_ALIASES`, `ancien_code → code_actuel`) sert à :

1. **étendre le filtre produit** (`rli.productcode IN (...)`) aux anciens codes quand des
   produits sont explicitement demandés, pour ne pas manquer l'historique ;
2. **normaliser** `code_produit` vers le code actuel à l'extraction, avant la jointure COC.

## Fichiers de ressources (workspace, non versionnés)

Sous `metabase eSIGL/data/ressources/` :

- **`Fichier mapping OrgUnit eSIGL DHIS2.xlsx`** — feuilles :
  - `OrgUnit` : `New_Code` (code site) → `ID_Dhis2` (orgUnit).
  - `Traceurs` (en-tête ligne 3) : `ANNEE`, `Nvo code` (code produit).
  - `R ou G` : `code_produit`, `Type produit` (`ROUTINE`/`GTC`). Repli sur liste en dur si absente.
- **`site_attendus/sites_attendus.csv`** — fichier **consolidé** `annee,code_site,rapport_attendu`.
  À maintenir en concaténant les fichiers annuels PNLP (les formats Excel annuels diffèrent ;
  la normalisation est faite une fois pour toutes hors pipeline).

## Développement

```bash
ruff check esigl_import_dhis2/ && ruff format esigl_import_dhis2/
python -m pytest esigl_import_dhis2/tests/ -q      # logique pure (transforms)
python esigl_import_dhis2/pipeline.py              # run local (workspace.yaml requis)
```

## Points ouverts (voir `refonte-pipelines/PLAN_AMELIORATION.md`)

1. Règle métier exacte de `bien_stocke` (implémentation provisoire, désactivée par défaut).
2. Justification du diviseur `4` dans `cmm_gestionnaire`.
3. Validation en prod de `align_stale_values` (DELETE) avant activation.
