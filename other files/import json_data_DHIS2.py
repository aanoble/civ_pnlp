import json
import tkinter as tk
from pathlib import Path
from tkinter import filedialog, messagebox, ttk

import pandas as pd


class ExcelToJsonConverter:  # noqa: D101
    def __init__(self, root):  # noqa: ANN001
        self.root = root
        self.root.title("Convertisseur Excel vers JSON - NMDR PNLP")
        self.root.geometry("600x500")

        # Variables
        self.excel_file_path = tk.StringVar()
        self.output_folder_path = tk.StringVar(value=str(Path.home()))

        # Dictionnaire de mapping des codes vers les colonnes
        self.mapping_coc = {
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

        self.setup_ui()

    def setup_ui(self):  # noqa: D102
        # Frame principal
        main_frame = ttk.Frame(self.root, padding="10")
        main_frame.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))  # type: ignore

        # Configuration du grid
        self.root.columnconfigure(0, weight=1)
        self.root.rowconfigure(0, weight=1)
        main_frame.columnconfigure(1, weight=1)

        # Titre
        title_label = ttk.Label(
            main_frame, text="Convertisseur Excel vers JSON", font=("Arial", 16, "bold")
        )
        title_label.grid(row=0, column=0, columnspan=3, pady=(0, 20))

        # Section sélection fichier Excel
        ttk.Label(main_frame, text="Fichier Excel:", font=("Arial", 10, "bold")).grid(
            row=1, column=0, sticky=tk.W, pady=(0, 5)
        )

        ttk.Entry(main_frame, textvariable=self.excel_file_path, width=50).grid(
            row=2, column=0, columnspan=2, sticky=(tk.W, tk.E), padx=(0, 5)
        )

        ttk.Button(main_frame, text="Parcourir", command=self.browse_excel_file).grid(
            row=2, column=2, sticky=tk.W
        )

        # Section dossier de sortie
        ttk.Label(main_frame, text="Dossier de sortie:", font=("Arial", 10, "bold")).grid(
            row=3, column=0, sticky=tk.W, pady=(20, 5)
        )

        ttk.Entry(main_frame, textvariable=self.output_folder_path, width=50).grid(
            row=4, column=0, columnspan=2, sticky=(tk.W, tk.E), padx=(0, 5)
        )

        ttk.Button(main_frame, text="Parcourir", command=self.browse_output_folder).grid(
            row=4, column=2, sticky=tk.W
        )

        # Section informations sur le mapping
        info_frame = ttk.LabelFrame(
            main_frame, text="Colonnes requises dans le fichier Excel", padding="10"
        )
        info_frame.grid(row=5, column=0, columnspan=3, sticky=(tk.W, tk.E), pady=(20, 10))
        info_frame.columnconfigure(0, weight=1)

        # Liste des colonnes attendues
        columns_text = """Colonnes requises: dataElement, orgUnit, period,
stock_initial, quantite_recue, quantite_distribuee, perte_ajustement,
sdu, cmm, nbrejrsrupture, quantite_proposee, quantite_commandee, quantite_approuvee"""

        ttk.Label(info_frame, text=columns_text, wraplength=500, justify=tk.LEFT).grid(
            row=0, column=0, sticky=(tk.W, tk.E)
        )

        # Bouton conversion
        convert_btn = ttk.Button(
            main_frame,
            text="Convertir en JSON",
            command=self.convert_to_json,
            style="Accent.TButton",
        )
        convert_btn.grid(row=6, column=0, columnspan=3, pady=20)

        # Zone de log
        log_frame = ttk.LabelFrame(main_frame, text="Journal", padding="5")
        log_frame.grid(row=7, column=0, columnspan=3, sticky=(tk.W, tk.E, tk.N, tk.S), pady=(10, 0))
        log_frame.columnconfigure(0, weight=1)
        log_frame.rowconfigure(0, weight=1)
        main_frame.rowconfigure(7, weight=1)

        self.log_text = tk.Text(log_frame, height=8, wrap=tk.WORD)
        scrollbar = ttk.Scrollbar(log_frame, orient=tk.VERTICAL, command=self.log_text.yview)
        self.log_text.configure(yscrollcommand=scrollbar.set)

        self.log_text.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        scrollbar.grid(row=0, column=1, sticky=(tk.N, tk.S))

    def log_message(self, message):  # noqa: ANN001
        """Ajoute un message au journal."""
        self.log_text.insert(tk.END, f"{message}\n")
        self.log_text.see(tk.END)
        self.root.update()

    def browse_excel_file(self):
        """Ouvre le dialogue pour sélectionner le fichier Excel."""
        file_path = filedialog.askopenfilename(
            title="Sélectionner le fichier Excel",
            filetypes=[("Fichiers Excel", "*.xlsx *.xls"), ("Tous les fichiers", "*.*")],
        )
        if file_path:
            self.excel_file_path.set(file_path)
            self.log_message(f"📄 Fichier sélectionné: {Path(file_path).name}")

    def browse_output_folder(self):
        """Ouvre le dialogue pour sélectionner le dossier de sortie."""
        folder_path = filedialog.askdirectory(title="Sélectionner le dossier de sortie")
        if folder_path:
            self.output_folder_path.set(folder_path)
            self.log_message(f"📁 Dossier de sortie: {folder_path}")

    def convert_to_json(self):
        """Convertit le fichier Excel en JSON."""
        try:
            # Vérifications
            if not self.excel_file_path.get():
                messagebox.showerror("Erreur", "Veuillez sélectionner un fichier Excel")
                return

            if not self.output_folder_path.get():
                messagebox.showerror("Erreur", "Veuillez sélectionner un dossier de sortie")
                return

            excel_path = Path(self.excel_file_path.get())
            output_folder = Path(self.output_folder_path.get())

            if not excel_path.exists():
                messagebox.showerror("Erreur", "Le fichier Excel n'existe pas")
                return

            self.log_message("🔄 Début de la conversion...")

            # Chargement du fichier Excel
            self.log_message("📥 Chargement du fichier Excel...")
            df_etat_stock = pd.read_excel(excel_path)
            self.log_message(f"✅ Fichier chargé: {len(df_etat_stock)} lignes")

            # Vérification des colonnes requises
            required_columns = ["dataElement", "orgUnit", "period"] + list(
                self.mapping_coc.values()
            )
            missing_columns = [col for col in required_columns if col not in df_etat_stock.columns]

            if missing_columns:
                error_msg = f"Colonnes manquantes: {', '.join(missing_columns)}"
                self.log_message(f"❌ {error_msg}")
                messagebox.showerror("Erreur", error_msg)
                return

            # Traitement des données
            self.log_message("🔧 Traitement des données...")
            df_etat_stock["period"] = df_etat_stock["period"].astype(str)
            df_etat_stock["dataElement"] = df_etat_stock["dataElement"].str.strip()

            # Construction du payload
            self.log_message("📦 Construction du payload...")
            payload = []

            for coc, col_name in self.mapping_coc.items():
                df_new = df_etat_stock.loc[df_etat_stock[col_name].notna()]

                df_tmp = pd.DataFrame(
                    {
                        "dataElement": df_new["dataElement"],
                        "categoryOptionCombo": coc,
                        "attributeOptionCombo": "HllvX50cXC0",
                        "orgUnit": df_new["orgUnit"],
                        "period": df_new["period"],
                        "value": df_new[col_name].astype(int).astype(str),
                    }
                )

                payload.extend(df_tmp.to_dict(orient="records"))

            chunk_size = 40000
            total = len(payload)
            num_chunks = (total + chunk_size - 1) // chunk_size
            self.log_message(f"📊 Total des enregistrements: {total}")

            for i in range(num_chunks):
                chunk = payload[i * chunk_size : (i + 1) * chunk_size]
                chunk_payload = {"dataValues": chunk}
                chunk_file = output_folder / f"payload_chunk_{i + 1:03}.json"

                self.log_message(f"💾 Sauvegarde vers: {chunk_file.as_posix()}")

                with open(chunk_file, "w", encoding="utf-8") as f_out:
                    json.dump(chunk_payload, f_out, indent=2, ensure_ascii=False)

                self.log_message(f"✅ Fichier {chunk_file.name} créé avec {len(chunk)} éléments")

            # with open(output_file, "w", encoding="utf-8") as f:
            #    json.dump(output_data, f, indent=2, ensure_ascii=False)

            self.log_message(f"✅ Conversion terminée! {len(payload)} enregistrements créés")
            # self.log_message(f"📄 Fichier sauvegardé: {output_file}")
            # f"Fichier: {output_file}",
            messagebox.showinfo(
                "Succès", f"Conversion réussie!\n{len(payload)} enregistrements créés\n"
            )

        except Exception as e:
            error_msg = f"Erreur lors de la conversion: {e!s}"
            self.log_message(f"❌ {error_msg}")
            messagebox.showerror("Erreur", error_msg)


def main():  # noqa: D103
    root = tk.Tk()
    app = ExcelToJsonConverter(root)  # noqa: F841
    root.mainloop()


if __name__ == "__main__":
    main()
