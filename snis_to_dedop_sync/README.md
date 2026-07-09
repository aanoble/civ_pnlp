# Synchronisation des dataValues — instance source → instance cible

Pipeline OpenHEXA synchronisant les `dataValues` d'une **instance source** vers une **instance
cible** à partir des datasets. Par défaut, la source est l'instance SNIS (`snis-dhis2`) et la
cible l'instance DEDOP/NMDR (`dhis2-nmdr-temp`).
Voir [`PLAN_AMELIORATION.md`](./PLAN_AMELIORATION.md) pour le détail des choix de conception.

## Hypothèses

- Les **UID** des `dataSets` et `dataElements` sont **identiques** entre l'instance source et
  l'instance cible (pas de mapping).
- L'**existence des organisation units** dans l'instance cible est assurée en amont par le
  pipeline `snis_to_nmdr_sync_orgunits`, planifié **quotidiennement avant** cette synchro.

## Flux

```
check santé + validation AOC cible
  └─ pour chaque dataset :
       sync assignation dataset⇄orgUnits (suppressions gardées par flag)
       détection COC manquants dans la cible → création optionnelle (consentement)
       extraction source (racine + enfants, includeDeleted) → post-filtre orgUnits + COC valides
       conversion de periodType si nécessaire (agrégation)
       préparation payload (AOC cible) → partition upserts / deletes
       push DHIS2 (chunks + retry) ; propagation des suppressions
       écriture rapport (payload.json + report.json) + purge des anciens rapports
```

Un échec sur un dataset est isolé (les autres continuent) ; le run se termine en **erreur** si au
moins un dataset a échoué.

## Paramètres principaux

| Paramètre | Type | Défaut | Description |
|-----------|------|--------|-------------|
| `source_connection` / `target_connection` | DHIS2Connection | `snis-dhis2` / `dhis2-nmdr-temp` | Connexions source / cible |
| `dataset_id` | list[str] | tous (`DATASET_IDS`) | Datasets à synchroniser |
| `org_unit_id` | list[str] | — | **Post-filtre** optionnel sur les org units |
| `extraction_root_org_unit` | str | `ZD44Asc0bAk` | Racine d'extraction (enfants inclus) — choix de performance |
| `start_date` / `end_date` | str | — | Fenêtre d'extraction (YYYY-MM-DD) |
| `months_back` | int | 24 | Recul en mois (appliqué **seulement** si `start_date` absente) |
| `last_updated` | str | — | Cutoff `lastUpdated` (backfill manuel) |
| `target_aoc` | str | `HllvX50cXC0` | **AOC de l'instance cible** appliqué à toutes les valeurs |
| `create_missing_metadata` | bool | `False` | Créer les COC manquants dans la cible (UID préservés) — sinon ignorés + reportés |
| `sync_orgunit_deletions` | bool | `False` | Autoriser la désassignation d'org units (destructif) |
| `automate_sync` | bool | `False` | Mode quotidien incrémental (`lastUpdated` = aujourd'hui) |
| `dry_run` | bool | `False` | Simuler sans écrire (import + création de métadonnées) |
| `import_mode` | str | `CREATE_AND_UPDATE` | Stratégie d'import des upserts |
| `post_batch_size` | int | 5000 | Taille des chunks POST |
| `use_cache` | bool | `False` | Cache des réponses API de la source |
| `retention_days` | int | 30 | Rétention des rapports d'import |

## Comportements clés

- **Racine d'extraction** : extraire depuis une racine large + enfants est plus rapide qu'unité par
  unité ; `org_unit_id` sert de post-filtre.
- **Extraction bornée par la période** : la fenêtre `[start, end]` filtre le champ `period`. Garder
  `months_back` suffisamment large pour capter les corrections rétroactives.
- **Suppressions** : extraites via `includeDeleted=true` et propagées à la cible (payload `deleted`).
- **COC manquants** : détectés par comparaison des vrais categoryOptionCombos ; créés uniquement si
  `create_missing_metadata=True`, sinon les valeurs concernées sont ignorées et reportées.
- **periodType** : conversion au niveau dataSet (agrégation fine → grossière selon
  `aggregationType`) ; les paires non supportées font échouer le dataset.

## Sorties

Par dataset et par run : `payload.json` (upserts + deletes) et `report.json` (résumé d'import,
compteurs, rapport métadonnées).

## Tests

```bash
pytest snis_to_dedop_sync/tests
```
