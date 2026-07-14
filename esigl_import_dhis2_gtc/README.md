# ⚠️ DÉPRÉCIÉ — `esigl_import_dhis2_gtc`

Ce pipeline est **déprécié** depuis la refonte du module Gestion de Stock.

Il est **remplacé par le pipeline unifié** [`esigl_import_dhis2/`](../esigl_import_dhis2/),
qui traite désormais **routine + GTC + promptitude** dans un seul flux, aligné sur le nouveau
modèle de données (produit = `categoryOptionCombo`, métrique = `dataElement`).

## Conséquences

- Le workflow de déploiement `push_esigl_import_dhis2_gtc.yml` a été **supprimé** :
  ce dossier n'est plus poussé vers OpenHEXA automatiquement.
- Le code reste conservé à titre de référence historique uniquement.

## Action ops restante

Le pipeline déjà déployé sur le workspace `nmdr-civ`
(code `esigl-import-dhis2-1c1596`) doit être **archivé / supprimé manuellement**
dans OpenHEXA une fois le pipeline unifié validé en production.
