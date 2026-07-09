# CLAUDE.md

Guide pour travailler dans ce dépôt. Lis-le avant toute modification.

## Vue d'ensemble

Monorepo de **pipelines OpenHEXA** pour le projet **NMDR – Côte d'Ivoire** (workspace OpenHEXA `nmdr-civ`).
La majorité des pipelines synchronisent des données/métadonnées **entre instances DHIS2** (SNIS → DEDOP/NMDR), plus quelques pipelines d'import (eSIGL, ERA5) et de comparaison/QA.

Chaque sous-dossier de premier niveau (hors utilitaires) = **un pipeline OpenHEXA indépendant**.

## Pipelines (dossiers principaux)

| Dossier | Rôle |
|---------|------|
| `snis_to_dedop_sync/` | Synchro `dataValues` SNIS → DEDOP à partir des datasets. **Voir `PLAN_AMELIORATION.md`** (revue + roadmap). |
| `dhis2_to_nmdr_sync_datavalues/` | Synchro générique `dataValues` DHIS2 → NMDR, avec mappings externes (OU/COC/AOC). |
| `dhis2_to_nmdr_sync_datasets/` | Synchro des métadonnées `dataSets`. |
| `dhis2_to_nmdr_sync_dataelements/` | Synchro des métadonnées `dataElements`. |
| `dhis2_to_nmdr_sync_shared/` | **Utilitaires partagés** (pas un pipeline) — helpers réutilisables (`utils.py`). |
| `snis_to_nmdr_sync_orgunits/` | Synchro des `organisationUnits`. |
| `snis_vs_dedop_module_1/`, `esigl_vs_dedop_module_3/` | Pipelines de **comparaison/QA** entre instances. |
| `esigl_import_dhis2/`, `esigl_import_dhis2_gtc/` | Import eSIGL → DHIS2. |
| `era5_load_dhis2/`, `era5-sync/` | Import données climatiques ERA5 → DHIS2. |

Dossiers **non-pipeline** (ignorés par l'outillage) : `superset_dashboard/`, `other files/`, `notebooks/`, `enquête et recherches/`, `IASO CPS/`.

## Anatomie d'un pipeline

```
<pipeline>/
  pipeline.py       # point d'entrée : @pipeline + tâches @<pipeline>.task
  utils.py          # helpers spécifiques au pipeline
  constants.py      # IDs DHIS2 en dur (datasets, etc.)
  requirements.txt  # deps runtime OpenHEXA (souvent juste openhexa-toolbox)
  workspace.yaml    # config workspace locale (git-ignorée en pratique)
  mappings/         # (optionnel) fichiers de mapping JSON
  README.md         # (recommandé) doc du pipeline
```

`workspace.yaml` et `workspace/` sont **git-ignorés** au niveau pipeline (`.gitignore` local). Ne pas committer de secrets ni de contenu de workspace.

## Structure OpenHEXA SDK (patterns imposés)

- Fonction principale décorée `@pipeline("<code>", timeout=...)` puis empilement de `@parameter(...)`.
- Tâches décorées `@<nom_pipeline>.task` ; le **DAG se construit via le passage des valeurs de retour** entre tâches (une tâche qui dépend d'une autre reçoit son résultat en argument — cf. le paramètre `_write` factice utilisé pour chaîner `cleanup` après `write`).
- Logs via `current_run.log_info/log_warning/log_error/log_critical` — **messages en français** (convention du dépôt).
- Fichiers de sortie : `current_run.add_file_output(path)`, écrits sous `workspace.files_path`.
- Connexions DHIS2 : paramètre `type=DHIS2Connection`, client `DHIS2(connection=...)` du `openhexa.toolbox.dhis2`.
- Lancement local : `python pipeline.py` (nécessite un `workspace.yaml` configuré).

## Commandes

```bash
# Lint + format (config dans pyproject.toml, line-length=100, preview=true)
ruff check .
ruff check --fix .
ruff format .

# Type-check (config [tool.pyright] dans pyproject.toml, mode basic, py3.13)
pyright

# Hooks pré-commit (ruff-check --fix + ruff-format + hygiène fichiers)
pre-commit run --all-files

# Déploiement : automatique via GitHub Actions au push sur main
# (workflows .github/workflows/push_<pipeline>.yml → `openhexa pipelines push`)
```

Il n'y a **pas de suite de tests globale** ; seuls certains pipelines ont un dossier `tests/` (ex. `era5_load_dhis2/`). Ajouter des tests est encouragé pour la logique pure (dates, préparation de payload, mappings).

## Conventions de code

- **Python ≥ 3.11** (CI de déploiement en 3.11, pyright en 3.13).
- **Ruff** avec un large set de règles activées (voir `pyproject.toml`) — respecter docstrings (pydocstyle `D`), annotations (`ANN`), `pathlib` (`PTH`), imports triés (`I`).
- **Docstrings obligatoires** sur fonctions/tâches publiques (style NumPy majoritaire).
- Utiliser `pathlib.Path` (pas `os.path`), et `Path(workspace.files_path)` pour les I/O.
- Réutiliser les helpers de `dhis2_to_nmdr_sync_shared/utils.py` quand pertinent (`ensure_list`, `load_mapping_parameter`, `resolve_target_id`, `fetch_existing_ids`, `extract_import_counts`, `check_server_health`, `last_analytics_update`, `parse_cutoff_date`).

## Domaine DHIS2 — règles clés

- **UID identiques** entre SNIS et DEDOP pour `dataSets` et `dataElements` → pas de mapping à ces niveaux. Le risque d'alignement porte sur les **categoryOptionCombos (COC)**, les **attributeOptionCombos (AOC)** et les **periodType**.
- Push de données via l'endpoint `dataValueSets` (chunks + retry/backoff sur 429/5xx). Vérifier `importCount` (imported/updated/ignored/deleted) et les `conflicts` dans la réponse.
- Les valeurs `ignored` signalent souvent des métadonnées manquantes côté cible (COC/AOC/OU inexistants). Toujours reporter ces compteurs.
- Pour les **suppressions**, extraire avec `includeDeleted=True` et propager côté cible (alignement source↔cible attendu).
- Écrire un `payload.json` + `report.json` par run/dataset ; ne pas supprimer trop agressivement l'historique d'audit.

## Déploiement (CI)

- Un workflow `push_<pipeline>.yml` par pipeline, déclenché au **push sur `main`** touchant le dossier du pipeline.
- Il exécute `openhexa pipelines push <dir> --code "<kebab-code>" ...` vers le workspace `nmdr-civ` (secret `OH_TOKEN`).
- **Créer un workflow de push dédié** lors de l'ajout d'un nouveau pipeline (copier un `push_*.yml` existant, adapter dossier + `--code`).
- Lint (`ruff`) et types (`pyright`) tournent sur chaque PR et push.

## Git / workflow

- Ne pas committer/pusher sans demande explicite. Si sur `main`, créer une branche d'abord.
- Convention de messages : `type(scope): message` (ex. `fix:`, `feat(snis_to_dedop_sync):`, `ci(...)`).
- Ne jamais committer `workspace/`, `workspace.yaml`, ni de secrets/tokens DHIS2.
