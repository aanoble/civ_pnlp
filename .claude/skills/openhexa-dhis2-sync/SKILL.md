---
name: openhexa-dhis2-sync
description: >-
  Créer, réviser ou déboguer un pipeline OpenHEXA de synchronisation DHIS2 (SNIS →
  DEDOP/NMDR) de ce dépôt. À utiliser dès qu'on écrit/modifie un `pipeline.py`
  OpenHEXA, qu'on ajoute un nouveau pipeline, qu'on fait la revue d'une synchro DHIS2
  (dataValues, dataSets, dataElements, orgUnits, COC/AOC/periodType), ou qu'on prépare
  son déploiement CI. Déclencheurs : "pipeline OpenHEXA", "synchro DHIS2", "dataValueSets",
  "categoryOptionCombo", "revue de pipeline", "@pipeline", "openhexa pipelines push".
---

# Pipelines OpenHEXA de synchronisation DHIS2

Skill pour travailler sur les pipelines de ce monorepo (`nmdr-civ`). Lire aussi le
`CLAUDE.md` racine et, pour `snis_to_dedop_sync`, son `PLAN_AMELIORATION.md`.

## 1. Quand l'utiliser

- Écrire un **nouveau pipeline** de synchro/import DHIS2.
- **Réviser** un `pipeline.py` existant (correction, alignement métadonnées, robustesse).
- **Déboguer** une synchro (valeurs `ignored`, COC/AOC manquants, périodes, suppressions).
- Préparer le **déploiement** (workflow CI `push_<pipeline>.yml`).

## 2. Structure d'un pipeline (à respecter)

```
<pipeline>/
  pipeline.py       # @pipeline + tâches @<pipeline>.task
  utils.py          # helpers du pipeline (réutiliser dhis2_to_nmdr_sync_shared/utils.py)
  constants.py      # IDs DHIS2 en dur documentés par commentaire
  requirements.txt  # deps runtime (openhexa-toolbox~=2.x, + psycopg2-binary si BD)
  workspace.yaml    # git-ignoré ; config workspace locale
  README.md         # objectif, paramètres, mappings, flow, sorties
  mappings/         # optionnel : mappings JSON
```

Squelette minimal :

```python
from pathlib import Path
from openhexa.sdk import DHIS2Connection, current_run, parameter, pipeline, workspace
from openhexa.toolbox.dhis2 import DHIS2

@pipeline("<code_snake>", timeout=43200)
@parameter("source_connection", type=DHIS2Connection, name="...", required=True)
@parameter("target_connection", type=DHIS2Connection, name="...", required=True)
# ... autres @parameter (exposer dry_run, import_mode, post_batch_size !)
def my_pipeline(source_connection, target_connection, ..., dry_run: bool = True):
    """Docstring NumPy obligatoire."""
    source = DHIS2(connection=source_connection)
    target = DHIS2(connection=target_connection)
    # enchaîner des tâches @my_pipeline.task ; le DAG se construit par passage de retours

@my_pipeline.task
def fetch(...): ...

if __name__ == "__main__":
    my_pipeline()
```

Règles SDK :
- Le **DAG se construit via le passage des valeurs de retour** entre tâches (pas d'appels implicites). Pour forcer un ordre sans dépendance de données, passer le retour d'une tâche en argument factice.
- Logs `current_run.log_info/log_warning/log_error/log_critical` — **en français**.
- Sorties : écrire sous `Path(workspace.files_path)`, puis `current_run.add_file_output(...)`.
- Exposer en **`@parameter`** tout ce qui doit être configurable (dont `dry_run`, `import_mode`, `post_batch_size`) — ne pas laisser d'argument caché non exposé. Éviter les défauts contradictoires entre `@parameter` et la signature.

## 3. Règles métier DHIS2 (ce dépôt)

- **UID identiques** SNIS↔cible pour `dataSets`/`dataElements` → pas de mapping à ces niveaux.
  Le risque d'alignement est sur **COC**, **AOC**, **periodType**.
- **Racine d'extraction paramétrable, pas codée en dur** : extraire depuis une racine large
  (niveau national) + `include_children` puis **post-filtrer** sur `org_unit_id` est un **choix
  de performance** (plus rapide qu'extraire OU par OU). Exposer cette racine en `@parameter`
  (défaut = racine nationale) plutôt qu'une constante en dur.
- **COC manquant côté cible** (catégorie nouvelle/ajoutée) → **créer** les métadonnées dans la
  cible en préservant les UID source (categoryOptions → categories → categoryCombos →
  régénération des COC), puis pousser — **uniquement si un paramètre booléen de consentement**
  (`create_missing_metadata`, défaut `False`) est activé ; sinon **ignorer + reporter**.
- **AOC** : utiliser l'**AOC de la cible (DEDOP)** pour toutes les valeurs (paramètre `dhis2_aoc`
  / `dedop_target_aoc`), **pas** l'AOC source SNIS. Vérifier que cet AOC existe dans la cible.
- **periodType** : convertir explicitement (au niveau **dataSet**) si source ≠ cible (agréger
  selon `aggregationType`) ; ne pas retourner silencieusement vide.
- **Suppressions** : extraire avec `includeDeleted=True` et **propager** (upserts vs deletes
  partitionnés) pour l'alignement source↔cible. Ne pas `fill_null("0")` (masque le problème).
- **Extraction bornée par la période** : `data_value_sets.get(start_date, end_date)` filtre sur
  le champ `period` (le filtre `lastUpdated` s'y ajoute). Garder une fenêtre de périodes large
  (`months_back`) pour capter les corrections rétroactives.
- **Incrémental** : pas de watermark persistant ; en mode automatisé quotidien, `lastUpdated =
  date du jour`. Garder un paramètre `last_updated` pour le backfill manuel.
- **Existence des OU** : assurée en amont par un pipeline OU dédié (`snis_to_nmdr_sync_orgunits`)
  planifié **avant** la synchro des valeurs ; la synchro de valeurs ne gère que l'assignation
  dataset↔OU.

## 4. Push vers DHIS2 (endpoint `dataValueSets`)

- Découper en **chunks** (`post_batch_size`, ~5000) ; **retry + backoff exponentiel** sur
  `429` et `5xx` ; erreurs non-retryables → stop.
- Lire `response.json()["response"]` → `importCount` (imported/updated/ignored/deleted) et
  `conflicts`. **Agréger et reporter** ces compteurs ; `ignored` élevé = métadonnées cibles
  manquantes → investiguer.
- Toujours supporter `dryRun` et `importStrategy` (`CREATE`, `UPDATE`, `CREATE_AND_UPDATE`, `DELETE`).
- **Ne pas avaler les erreurs** : distinguer « échec » de « vide » ; propager un statut d'échec
  par dataset et global (faire échouer le run au-delà d'un seuil).
- Écrire `payload.json` + `report.json` par dataset ; rétention d'audit raisonnable (pas 1 jour).

## 5. Checklist de revue

- [ ] Racine d'extraction **paramétrable** (pas codée en dur) ; post-filtre `org_unit_id` conservé.
- [ ] Création de métadonnées côté cible **gardée par un flag de consentement** (`create_missing_metadata`).
- [ ] AOC écrit = **AOC cible (DEDOP)**, pas l'AOC source.
- [ ] Comparaisons de métadonnées faites sur les **vrais objets** (pas `set()` sur une chaîne d'ID).
- [ ] COC/AOC/OU cibles vérifiés/créés avant push ; `ignored` reporté.
- [ ] Suppressions propagées ; pas de `fill_null("0")` non justifié.
- [ ] `periodType` source vs cible géré explicitement.
- [ ] `dry_run`/`import_mode`/`post_batch_size` exposés en `@parameter`, défauts cohérents.
- [ ] Retry/backoff sur le push ; statut d'échec propagé (pas de faux « success »).
- [ ] Logique de périodes correcte (`months_back` seulement si dates absentes ; pas de forçage fin de mois sur date explicite).
- [ ] Docstrings + types ; `ruff check` et `pyright` passent.
- [ ] Connexion cible = instance définitive (pas une connexion « temp »).

## 6. Outillage & déploiement

```bash
ruff check --fix . && ruff format .   # config pyproject.toml (line-length=100, preview)
pyright                                # [tool.pyright], mode basic
pre-commit run --all-files
python <pipeline>/pipeline.py         # run local (workspace.yaml requis)
```

**Nouveau pipeline** → créer `.github/workflows/push_<pipeline>.yml` (copier un
`push_*.yml` existant ; adapter le path du dossier et `--code "<kebab-code>"`). Le push sur
`main` déclenche `openhexa pipelines push` vers le workspace `nmdr-civ`.

## 7. Conventions

- Python ≥ 3.11 ; `pathlib` (pas `os.path`) ; docstrings NumPy ; logs en français.
- Messages git : `type(scope): message`. Ne pas committer `workspace/`, `workspace.yaml`, secrets.
- Réutiliser `dhis2_to_nmdr_sync_shared/utils.py` (`ensure_list`, `load_mapping_parameter`,
  `resolve_target_id`, `fetch_existing_ids`, `extract_import_counts`, `check_server_health`,
  `last_analytics_update`, `parse_cutoff_date`).
