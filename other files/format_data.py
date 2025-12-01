import json
from pathlib import Path

import pandas as pd

# 📁 Définition du chemin racine
fp_root = Path("/Volumes/Naud Space/Bluesquare/Pipeline OpenHexa/nmdr_pnlp/")

# 📄 Chargement du fichier Excel
df_etat_stock = pd.read_excel(fp_root / "data/extract-periode-janv-2025.xlsx")

df_etat_stock["period"] = df_etat_stock["period"].astype(str)  # Format YYYYMM

# 📌 Dictionnaire de mapping des codes vers les colonnes
mapping_coc = {
    "MxwO32EmLkm": "stock_initial",
    "VpsWXngJn8m": "quantite_recue",
    "r4Y2vAZNFJr": "quantite_distribuee",
    "DaYWwwQWpzO": "perte_ajustement",
    "MsVzBFeQy98": "sdu",
    "tAviNwTJA69": "cmm",
    "lmIvSiYc80L": "nbrejrsrupture",
    "cpDZa6GSME2": "quantite_proposee",
    "qz4cXueOt5p": "quantite_commandee",
    "TnEwztOelac": "quantite_approuvee",
}

# 📦 Construction du payload
payload = []

for coc, col_name in mapping_coc.items():
    if col_name not in df_etat_stock.columns:
        raise KeyError(f"Colonne manquante : {col_name}")

    df_tmp = pd.DataFrame({
        "dataElement": df_etat_stock["dataElement"],
        "categoryOptionCombo": coc,
        "attributeOptionCombo": "HllvX50cXC0",
        "orgUnit": df_etat_stock["orgUnit"],
        "period": df_etat_stock["period"],
        "value": df_etat_stock[col_name].fillna(0).round(2).astype(str),
    })

    payload.extend(df_tmp.to_dict(orient="records"))

# 💾 Sauvegarde en JSON
with Path(fp_root / "payload.json").open("w", encoding="utf-8") as f:
    json.dump({"dataValues": payload}, f, indent=2, ensure_ascii=False)

# print(f"✅ Payload exporté avec {len(payload)} lignes.")
# Ajouter au résumé
