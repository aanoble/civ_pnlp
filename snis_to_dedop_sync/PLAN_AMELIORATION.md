# Plan de revue et d'amélioration — Pipeline `snis_to_dedop_sync`

> Revue technique du pipeline OpenHEXA de synchronisation DHIS2 → DHIS2 (SNIS → DEDOP).
> Objectif : documenter les **bugs**, **manquements** et **axes d'amélioration** à implémenter ultérieurement.
> Date de revue : 2026-07-09

---

## 1. Résumé exécutif

Le pipeline fait le travail de base (extraction `dataValueSets`, préparation d'un payload, push par chunks avec retry, rapport d'import). Cependant il repose sur **plusieurs hypothèses fortes non vérifiées** et contient **des bugs de correction fonctionnelle** qui peuvent provoquer une synchronisation silencieusement incorrecte (données manquantes, valeurs écrasées, périmètre erroné).

**Contexte confirmé (métier)** : les **UID sont identiques** entre SNIS et DEDOP pour les `dataSets` et `dataElements` → **pas de couche de mapping** requise à ce niveau. Le vrai risque d'alignement se situe au niveau des **categoryOptionCombos (COC)** : catégories absentes du DEDOP ou nouvellement créées pour un dataElement côté SNIS.

Les problèmes les plus critiques à traiter en priorité :

1. **Org unit codé en dur** (`ZD44Asc0bAk`) dans l'extraction → le paramètre `org_unit_id` de l'utilisateur est ignoré à l'extraction.
2. **Détection + création des COC/métadonnées de désagrégation manquants dans DEDOP** (remplace le contrôle `coc_equiv` bugué). Décision métier : **créer automatiquement** les métadonnées manquantes côté DEDOP en préservant les UID source, puis pousser la valeur.
3. **Propager les suppressions** SNIS → DEDOP (confirmé) pour garder l'alignement/cohérence source↔cible ; supprimer le `fill_null("0")` qui masque le problème.
4. **Utiliser l'AOC de la cible (DEDOP)** pour toutes les valeurs, et **implémenter la conversion `periodType`** (au niveau dataset) au lieu de retourner vide.

---

## 2. Bugs de correction (priorité HAUTE)

### 2.1 Org unit d'extraction codé en dur → le rendre paramétrable
- **Où** : `fetch_dhis2_data`, lignes ~462 et ~488 (`org_units=["ZD44Asc0bAk"]` et `"orgUnit": "ZD44Asc0bAk"`).
- **Constat de perf (confirmé par l'équipe)** : extraire depuis l'**OU racine national** + `include_children` est **plus rapide** qu'extraire OU par OU au niveau régional/site (comportement observé de l'API/toolbox `dataValueSets`). L'extraction « racine large puis **post-filtre** sur `org_unit_ids` » est donc un **choix délibéré de performance**, pas seulement un défaut.
- **Problème restant** : la racine est **codée en dur** → non configurable, non documentée, et risque de retourner 0 enregistrement si un `org_unit_id` demandé n'est pas sous cette racine.
- **À faire** :
  - **Exposer la racine d'extraction en `@parameter`** (ex. `extraction_root_org_unit`, défaut = racine nationale) au lieu de la constante en dur.
  - Conserver la stratégie « extraction depuis la racine + `include_children` puis **post-filtre** `org_unit_ids` » pour la performance.
  - Valider que les `org_unit_ids` demandés sont bien **descendants** de la racine (sinon warning explicite plutôt que résultat vide silencieux).
  - Optionnel : permettre de basculer sur une extraction ciblée par OU si un jour c'est plus efficace, mais garder la racine large par défaut.

### 2.2 Détection des COC : le contrôle `coc_equiv` est bugué
- **Où** : `fetch_dhis2_data`, lignes ~427-434.
- **Problème** : `set(row["categoryCombo"]) - set(row["categoryCombo_right"])` opère sur des **chaînes d'IDs**, donc compare des ensembles de **caractères**, pas les combos réels. Deux IDs différents partageant les mêmes lettres seraient jugés « équivalents ». Ce contrôle est censé détecter les divergences de désagrégation mais est inopérant.
- **À faire** : remplacer par une vraie comparaison, au niveau **categoryOptionCombo (COC)** et non au niveau de la chaîne d'ID. Pour chaque dataElement, comparer l'ensemble des COC réels du SNIS vs DEDOP (via le `categoryCombo` → `categoryOptionCombos[id]`). Les COC présents en SNIS et absents en DEDOP alimentent la synchro de métadonnées (§2.3). Supprimer le `map_elements` sur chaînes.

### 2.3 Synchronisation des métadonnées de désagrégation (COC) — **création dans DEDOP**
- **Contexte** : UID identiques pour dataSets/dataElements → **pas de mapping**. Mais une **category option / COC** peut exister en SNIS et être **absente** du DEDOP (catégorie nouvellement créée, option d'âge/sexe ajoutée, `categoryCombo` d'un dataElement modifié). Les valeurs portant ce COC sont alors rejetées (comptées en `ignored`).
- **Décision métier** : **créer** les métadonnées manquantes dans DEDOP, en **préservant les UID source**, puis pousser la valeur — **mais uniquement sur consentement explicite de l'utilisateur**.
- ⚠️ **Point d'attention — garde-fou obligatoire** : la création de catégories/COC côté cible est une **écriture de métadonnées** qui peut occasionner des problèmes si elle est faite sans le consentement de l'utilisateur. Exposer un **paramètre booléen** dédié (ex. `create_missing_metadata`, défaut `False`) :
  - `True` → le pipeline crée les métadonnées manquantes puis pousse.
  - `False` (défaut) → aucune création ; les valeurs dont le COC est absent sont **ignorées + reportées** clairement (dataElement, COC, nb) pour correction manuelle. Combiner avec le garde `dry_run`.
- **Conception proposée** — nouvelle tâche `ensure_disaggregation_metadata(snis, dedop, dataset_id, create_missing_metadata)` exécutée **avant** l'extraction/push (no-op si le flag est `False`) :
  1. Récupérer côté SNIS, pour les dataElements du dataset, la chaîne complète : `categoryOptions` → `categories` → `categoryCombos` → `categoryOptionCombos` (définitions complètes avec UID).
  2. Diff avec DEDOP (par UID) pour identifier les objets manquants à chaque niveau.
  3. **Importer dans DEDOP en préservant les UID** via l'API metadata (`POST /api/metadata?importStrategy=CREATE_AND_UPDATE&identifier=UID`), dans l'ordre : `categoryOptions` → `categories` (mettre à jour la liste d'options) → `categoryCombos` (mettre à jour la liste de catégories).
  4. **Régénérer les COC** : `POST /api/maintenance/categoryOptionComboUpdate` (ou importer explicitement les `categoryOptionCombos` avec leur UID source pour garantir l'égalité d'UID — DHIS2 régénère sinon avec de nouveaux UID).
  5. Re-vérifier que les COC cibles existent avant push ; journaliser les objets créés (niveau + UID + nom).
- **Points de vigilance** :
  - Nécessite les **droits d'écriture métadonnées** sur DEDOP.
  - Modifier le `categoryCombo` d'un dataElement existant impacte les données déjà présentes → tracer et sécuriser (garde `dry_run`).
  - La régénération des COC peut être coûteuse → la lancer une fois par run après tous les ajouts.
  - Idempotence : ne créer que le delta ; l'import `CREATE_AND_UPDATE` doit être sûr en réexécution.

### 2.4 Propagation des suppressions et suppression du `fill_null("0")`
- **Où** : branche `automate_sync`, ligne ~506 (`fill_null("0")`) et `includeDeleted=True`.
- **Décision métier** : les **suppressions doivent être propagées** vers DEDOP pour l'alignement source↔cible.
- **Problèmes actuels** :
  - `fill_null("0")` **écrase des données réelles** par des zéros non déclarés → à **supprimer**.
  - `includeDeleted=True` récupère les valeurs supprimées mais le push (`CREATE_AND_UPDATE`) ne supprime **jamais** côté cible → dérive de données.
  - Incohérence : le `fill_null` n'existe pas dans la branche non automatisée.
- **Conception proposée** :
  - Extraire avec `includeDeleted=True` dans **les deux branches**.
  - **Partitionner** le payload : *upserts* (valeurs actives) vs *deletes* (valeurs `deleted=true` en source).
  - Upserts : `dataValueSets` avec `importStrategy=CREATE_AND_UPDATE`.
  - Deletes : soit `dataValueSets` avec le flag `deleted: true` par valeur, soit `importStrategy=DELETE` par lot, soit `DELETE /api/dataValues` par valeur (à benchmarker). Retenir l'approche par lot pour la perf.
  - Ne plus forcer `"0"` ; conserver les nulls tels quels (ou règle métier explicite).
  - Rapporter le nb de suppressions propagées séparément des upserts.

### 2.5 `months_back` appliqué même avec dates explicites
- **Où** : `process_periods`, lignes ~258-262.
- **Problème** : si l'utilisateur fournit `start_date` et `end_date`, on soustrait quand même `months_back` (défaut 24) à la date de début → fenêtre bien plus large que demandée. `end_dt = end_dt + relativedelta(day=31)` force aussi toujours la fin de mois même pour une date de fin explicite.
- **À faire** : n'appliquer `months_back` que lorsque `start_date` est absent ; respecter une `end_date` explicite.

### 2.6 `process_periods` génère une liste journalière inutile
- **Où** : lignes ~274-275 ; seuls `periods_range[0]` et `[-1]` sont utilisés en aval.
- **À faire** : retourner simplement `[start, end]` (ou des périodes DHIS2 réelles selon `periodType`). Supprimer la génération `rrule.DAILY`.
- ⚠️ **Point d'attention — extraction bornée par le champ période** : l'extraction repose sur `dataframe.extract_dataset` → `dhis2.data_value_sets.get(start_date, end_date, ...)` (confirmé dans la toolbox `openhexa/toolbox/dhis2/dataframe.py`). L'export DHIS2 `dataValueSets` ne renvoie que les valeurs **dont la `period` tombe dans la fenêtre `[start_date, end_date]`** (le filtre `lastUpdated` s'ajoute à ce filtre, il ne le remplace pas). Conséquences :
  - La fenêtre `[start, end]` retournée par `process_periods` doit **couvrir toutes les périodes visées** (borne basse = `months_back` inclus, borne haute = fin de période courante). Une fenêtre trop étroite exclut silencieusement des périodes.
  - En mode automatisé basé sur `lastUpdated`, il faut quand même une fenêtre de périodes suffisamment large pour capter les corrections rétroactives (une saisie faite aujourd'hui peut concerner une période ancienne).

---

## 3. Manquements fonctionnels (priorité HAUTE / MOYENNE)

### 3.1 Conversion de type de période — **à implémenter**
- Actuel : si `periodType` source ≠ cible, le pipeline **log et retourne un DataFrame vide** (lignes ~525-533). Aucune agrégation.
- **Décision métier** : implémenter la conversion `periodType`, **au niveau du dataSet** (le `periodType` est une propriété du dataSet — la conversion se raisonne par dataSet, pas par dataElement).
- **Conception proposée** — tâche `convert_periods(df, period_type_source, period_type_target, data_elements)` :
  - Convertir les identifiants de période source vers le format cible (ex. `Weekly`→`Monthly`, `Monthly`→`Quarterly`/`Yearly`).
  - Agréger par `(dataElement, orgUnit, COC, AOC, période cible)` selon l'`aggregationType` de chaque dataElement (SUM / AVERAGE / etc.) récupéré des métadonnées.
  - Gérer les cas non agrégeables (désagrégation plus fine en cible) → reporter/ignorer avec log clair.
  - Documenter les paires `periodType` supportées ; sortir en échec explicite pour les paires non gérées.

### 3.2 `attributeOptionCombo` — **utiliser l'AOC de la cible (DEDOP)**
- **Où** : `prepare_data_for_dhis2`, ligne ~581 (`pl.lit(dhis2_aoc)`).
- **Décision métier** : **ne pas** utiliser l'AOC source du SNIS. Toutes les valeurs sont écrites avec l'**AOC de la cible DEDOP** (le paramètre `dhis2_aoc`, ex. AOC par défaut `HllvX50cXC0`). Le comportement actuel (forcer une constante) est donc **correct dans son principe**.
- **À faire (clarifications)** :
  - Renommer le paramètre pour lever l'ambiguïté (ex. `dedop_target_aoc`) et documenter qu'il s'agit de l'AOC **cible**, pas source.
  - Vérifier que cet AOC **existe bien dans DEDOP** au démarrage (sinon échec explicite plutôt que rejet silencieux à l'import).
  - Documenter la conséquence : si les données SNIS portent plusieurs AOC, elles sont **volontairement collapsées** sur l'AOC cible unique (comportement voulu). S'assurer qu'aucune agrégation involontaire de valeurs distinctes n'en résulte (clé d'unicité `(dataElement, orgUnit, COC, période)` après réaffectation de l'AOC).

### 3.3 Fenêtre incrémentale : simplifier via les saisies du jour (pas de watermark)
- La logique `automate_sync` déduit `lastUpdated` d'une **heuristique de planification** fragile (`jour ∈ {1,15,fin de mois}` et `heure ∈ {6,18}`).
- **Décision métier** : **pas de watermark persistant** (fichier d'état jugé peu utile). Préférer récupérer simplement les **dernières saisies du jour** : `lastUpdated = date du jour` (avec `includeDeleted=True`).
- **À faire** :
  - Remplacer l'heuristique jour/heure par un `lastUpdated` = **date du jour** en mode automatisé (le pipeline tourne quotidiennement).
  - Conserver une **fenêtre de périodes suffisamment large** (`months_back`) pour capter les corrections rétroactives (cf. §2.6 : filtre `period` ≠ filtre `lastUpdated`).
  - Garder le paramètre `last_updated` pour un rattrapage manuel ponctuel (backfill).
  - Cadence : quotidienne (aligne avec la synchro OU journalière, cf. §3.4).

### 3.4 `sync_dataset_orgunits` : logique et risques
- **Contexte confirmé** : l'**existence des OU** dans DEDOP est déjà assurée en amont par un **pipeline dédié de synchronisation des organisationUnits** (`snis_to_nmdr_sync_orgunits`), **programmé quotidiennement avant** la synchro des valeurs. Le risque « OU inexistant côté cible » est donc **couvert en amont** — cette tâche ne gère que l'**assignation** dataset↔OU.
- **Points restants** :
  - Ne s'exécute **que** si `org_unit_ids is None` (ligne ~186) : quand l'utilisateur cible des OU, la synchro d'assignation est ignorée.
  - **Suppression destructive** : retire des OU du dataset DEDOP s'ils sont absents du dataset SNIS — sans garde `dry_run`. Risque de supprimer de la configuration légitime.
- **À faire** : conditionner les suppressions à un flag explicite + dry-run ; permettre la synchro d'assignation même avec un périmètre restreint ; s'appuyer sur le pipeline OU journalier pour l'existence (documenter l'ordre d'exécution : OU d'abord, valeurs ensuite).

### 3.5 Erreurs avalées → faux « succès »
- `fetch_dhis2_data` capture toute exception et retourne un DataFrame **vide** (lignes ~535-540). Impossible de distinguer « pas de données » d'un « échec d'extraction ». Le pipeline continue et rapporte un import « completed ».
- De même, `push_data_to_dhis2` continue après un chunk en échec et renvoie un statut agrégé qui ne remonte pas l'échec global.
- **À faire** : différencier explicitement échec vs vide ; propager un statut d'échec (par dataset et global) ; faire échouer le run si un seuil d'erreurs est dépassé.

### 3.6 Vérifications pré-import manquantes
- Aucun contrôle d'existence des dataElements / COC / OU **dans la cible** avant push (on s'appuie sur le rejet DHIS2). Aucun traitement des périodes verrouillées/approuvées (elles seront rejetées).
- Pas de synchro des `completeDataSetRegistrations` (statut de complétude des rapports).
- **À faire** : pré-valider les métadonnées cibles ; gérer/documenter les périodes verrouillées ; envisager la synchro de complétude si nécessaire au métier.

---

## 4. Architecture & performances (priorité MOYENNE)

### 4.1 Pas de parallélisme entre datasets
- Boucle Python séquentielle sur `dataset_ids` dans la fonction principale. Avec `timeout=43200` (12 h) et `include_children` sur tout l'arbre, l'extraction peut être très lourde.
- **À faire** : exploiter le DAG OpenHEXA (tâches en parallèle par dataset), chunker l'extraction par OU racine et par fenêtre de périodes.

### 4.2 Usage d'API privée du toolbox
- `dataframe._data_values_to_dataframe(...)` (préfixe `_`) est une API interne, susceptible de casser lors des montées de version.
- **À faire** : utiliser une API publique équivalente ou isoler l'appel derrière une fonction utilitaire testée.

### 4.3 Nettoyage des rapports trop agressif
- `cleanup_old_directory_files` avec `retention_days=1` supprime l'historique d'audit au bout d'un jour.
- **À faire** : augmenter la rétention (configurable), rendre le paramètre exposé, conserver au moins N derniers rapports.

---

## 5. Paramètres & configuration (priorité MOYENNE)

### 5.1 Paramètres non exposés / incohérents
- `dry_run`, `import_mode`, `post_batch_size` sont des **arguments de fonction** avec valeurs par défaut mais **pas déclarés `@parameter`** → non configurables depuis l'UI OpenHEXA (notamment `dry_run` pour tester sans risque).
- **Conflit `use_cache`** : `@parameter` défaut `False` (ligne ~109) vs signature défaut `True` (ligne ~133). Le décorateur l'emporte mais l'incohérence prête à confusion.
- **À faire** : déclarer `dry_run`, `import_mode` (avec choix contrôlés), `post_batch_size` en paramètres ; aligner les défauts de `use_cache`.

### 5.2 Connexion cible « temp » par défaut
- Défaut `dedop_connection = "dhis2-nmdr-temp"` (temporaire) : risque d'écrire dans une instance de test/temporaire par mégarde en prod.
- **À faire** : pointer vers la connexion cible définitive et documenter.

### 5.3 Typage des paramètres multiples
- `dataset_id` et `org_unit_id` sont `multiple=True` mais typés `str` dans la signature ; le code fait `list(dataset_id)`. La variable de boucle `dataset_id` **masque** le paramètre.
- **À faire** : typer `list[str] | None`, renommer la variable de boucle (`current_dataset_id`).

### 5.4 Dépendances
- `requirements.txt` : `psycopg2-binary` alors que `workspace.yaml` déclare une base mais le pipeline n'utilise pas la BD → dépendance et config potentiellement inutiles.
- **À faire** : nettoyer les dépendances/configs non utilisées ou documenter leur usage prévu.

---

## 6. Qualité de code & observabilité (priorité BASSE)

- Docstring de la fonction principale **tronquée** (section Parameters incomplète, lignes ~139-148).
- Conflits/`ignored` : logués mais non remontés comme **métriques** exploitables ni comme alertes ; pas de seuil d'échec.
- Journalisation à enrichir : nb d'OU traités, nb d'IDs non résolus dans la cible, ventilation imported/updated/ignored/deleted par dataset.
- **Aucun test unitaire** (`process_periods`, `prepare_data_for_dhis2`, logique de mapping, découpage en chunks sont testables sans DHIS2).
- **À faire** : ajouter une suite de tests ; produire un résumé de run structuré (metrics) ; envisager une notification (échec/seuil de conflits).

---

## 7. Priorisation proposée

| # | Item | Sévérité | Effort | Sprint |
|---|------|----------|--------|--------|
| 2.1 | Racine d'extraction paramétrable (garder post-filtre) | Critique | Faible | 1 |
| 2.2 | Détection COC (fix `coc_equiv`) | Critique | Faible | 1 |
| 2.4 | Suppressions + retrait `fill_null("0")` | Critique | Moyen | 1 |
| 3.5 | Erreurs avalées / statut d'échec | Haute | Moyen | 1 |
| 2.3 | Création métadonnées COC dans DEDOP (+ flag consentement) | Critique | Élevé | 2 |
| 3.2 | AOC = AOC cible DEDOP (clarifier/renommer) | Haute | Faible | 2 |
| 3.3 | Fenêtre incrémentale = saisies du jour (pas de watermark) | Haute | Faible | 2 |
| 2.5/2.6 | Logique des périodes (+ extraction bornée par période) | Haute | Faible | 2 |
| 3.1 | Conversion periodType | Moyenne | Élevé | 3 |
| 3.4 | sync_dataset_orgunits | Moyenne | Moyen | 3 |
| 5.x | Paramètres/config | Moyenne | Faible | 3 |
| 4.x | Perf/DAG/API privée | Moyenne | Moyen | 3 |
| 6 | Tests/observabilité | Basse | Moyen | 3 |

---

## 8. Décisions métier (arbitrées)

1. **UID identiques** SNIS↔DEDOP pour `dataSets` et `dataElements` → **pas de couche de mapping** à ces niveaux. ✅
2. **Racine d'extraction** : garder la stratégie « racine nationale large + `include_children` + post-filtre » (plus **performante** qu'une extraction OU par OU), mais rendre la racine **paramétrable** (§2.1). ✅
3. **COC absent en DEDOP** → **créer** les métadonnées (UID source préservés) puis pousser, **uniquement si l'utilisateur y consent** via un **paramètre booléen** `create_missing_metadata` (défaut `False` → ignorer + reporter) (§2.3). ✅
4. **Suppressions** SNIS → DEDOP → **à propager** pour l'alignement source↔cible ; retirer le `fill_null("0")` (§2.4). ✅
5. **AOC** → utiliser l'**AOC de la cible (DEDOP)** pour toutes les valeurs (le comportement actuel de forçage est correct ; clarifier le nommage du paramètre) (§3.2). ✅
6. **periodType** → **conversion à implémenter au niveau dataSet** (§3.1). ✅
7. **Fenêtre incrémentale** → **pas de watermark** ; utiliser `lastUpdated = date du jour` (run quotidien) + fenêtre de périodes large pour les corrections rétroactives (§3.3). ✅
8. **Existence des OU** dans DEDOP → **assurée en amont** par le pipeline `snis_to_nmdr_sync_orgunits` **exécuté quotidiennement avant** la synchro des valeurs ; cette synchro ne gère que l'assignation dataset↔OU (§3.4). ✅

> Note : l'UID `dQZRj9ecqdd` évoqué en revue est un simple exemple de catégorie sans correspondant en DEDOP relevé lors d'un run — ignoré (cas couvert par §2.3).

### Points de conception restant à trancher lors de l'implémentation
- Modifier le `categoryCombo` d'un dataElement existant côté DEDOP : garde `dry_run` obligatoire + traçabilité (impact sur données existantes).
- Méthode de propagation des suppressions : flag `deleted:true` en lot vs `importStrategy=DELETE` vs `DELETE /api/dataValues` (à benchmarker).
- Paires `periodType` réellement à supporter (lesquelles se produisent en pratique ?).
- Ordonnancement inter-pipelines : garantir que la synchro OU journalière se termine **avant** la synchro des valeurs (dépendance de planification).
