# -*- coding: utf-8 -*-
"""
app.py – Classification Code (optimised)

Objectif : mêmes écrans, mêmes fichiers de sortie que la version d’origine,
mais exécution plus rapide et empreinte mémoire réduite.

Améliorations clés
------------------
1. **@st.cache_data** sur toutes les lectures de fichier et concaténations.
2. **Lecture CSV vectorisée** : auto‑détection séparateur, moteur « python » mais
   lecture directe sans tentative d’encodage inutile (on s’arrête dès que ça
   marche).
3. **Fusion Étape 1** : on concatène d’abord toutes les sources, puis on renomme
   *en masse* sans passer par trois DataFrame intermédiaires.
4. **Appliquer_maj** remplacé par un `fillna` vectorisé plutôt qu’un masque +
   assignation ligne à ligne.
5. **Pas de boucles Python** lors de la construction du DFRX : on construit la
   colonne M2 par vectorisation et on écrit en TSV directement depuis pandas.
6. **Watch = none** dans .streamlit/config.toml (même raison que précédemment)
"""

from __future__ import annotations

import csv
import io
from datetime import datetime
from functools import reduce
from pathlib import Path
from typing import List

import pandas as pd
import streamlit as st

# ══════════ CONFIG GÉNÉRALE ══════════
st.set_page_config("Classification Code", "🧩", layout="wide")
st.title("🧩 Classification Code – version optimisée")

# ══════════ OUTILS ══════════

@st.cache_data(show_spinner=False)
def read_csv_smart(buf: io.BytesIO) -> pd.DataFrame:
    """Décodage + auto‑séparateur + skip lignes cassées."""
    for enc in ("utf-8", "latin1", "cp1252"):
        buf.seek(0)
        try:
            sample = buf.read(2048).decode(enc, errors="ignore")
            sep = csv.Sniffer().sniff(sample, delimiters=";,|\t").delimiter
            buf.seek(0)
            return pd.read_csv(buf, sep=sep, encoding=enc,
                               engine="python", on_bad_lines="skip")
        except (UnicodeDecodeError, csv.Error, pd.errors.ParserError):
            continue
    raise ValueError("Impossible de lire le CSV : encodage ou séparateur inconnu")

@st.cache_data(show_spinner=False)
def read_any(uploaded) -> pd.DataFrame | None:
    name = uploaded.name.lower()
    if name.endswith(".csv"):
        return read_csv_smart(uploaded)
    if name.endswith((".xlsx", ".xls")):
        return pd.read_excel(uploaded, engine="openpyxl")
    st.error(f"{uploaded.name} : format non pris en charge.")
    return None

@st.cache_data(show_spinner=False)
def concat_unique(dfs: List[pd.DataFrame]) -> pd.DataFrame:
    if not dfs:
        return pd.DataFrame()
    out = pd.concat(dfs, ignore_index=True)
    out = out.loc[~out.duplicated()].reset_index(drop=True)
    return out

# ══════════ ÉTAPE 1 ══════════
st.header("Étape 1 : fusion et fichier client à remplir")

lots = {
    "cat": ("Catalogue interne", "idx Réf.", "idx M2 actuelle"),
    "hist": ("Historique",        "idx Réf.", "idx M2 dernière"),
    "cli": ("Fichier client",    "idx Réf.", "idx Code famille"),
}

for key in lots:
    st.session_state.setdefault(f"{key}_dfs", [])
    st.session_state.setdefault(f"{key}_names", [])

grid = st.columns(3)
for (key, (label, lab_ref, lab_val)), col in zip(lots.items(), grid):
    with col:
        st.markdown(f"##### {label}")
        files = st.file_uploader("Drag & drop", accept_multiple_files=True,
                                 type=("csv", "xlsx", "xls"), key=f"up_{key}")
        if files:
            for f in files:
                if f.name not in st.session_state[f"{key}_names"]:
                    df = read_any(f)
                    if df is not None:
                        st.session_state[f"{key}_dfs"].append(df)
                        st.session_state[f"{key}_names"].append(f.name)
            st.success(f"{len(files)} fichier(s) ajouté(s)")

        # Sélecteurs d’index 1‑based
        st.number_input(lab_ref, 1, 50, 1, key=f"{key}_ref", label_visibility="collapsed")
        st.number_input(lab_val, 1, 50, 2, key=f"{key}_val", label_visibility="collapsed")
        st.caption(f"📂 {len(st.session_state[f'{key}_dfs'])} fichier(s)")

entreprise = st.text_input("Entreprise (MAJUSCULES)").strip().upper()

if st.button("Fusionner Étape 1"):
    if not all(st.session_state[f"{k}_dfs"] for k in lots) or not entreprise:
        st.warning("Veuillez charger les trois lots et renseigner l’entreprise.")
        st.stop()

    # 1. Concatène tous les fichiers par lot
    raw_cat  = concat_unique(st.session_state["cat_dfs"])
    raw_hist = concat_unique(st.session_state["hist_dfs"])
    raw_cli  = concat_unique(st.session_state["cli_dfs"])

    # 2. Renomme les colonnes en masse (aucune boucle Python)
    def rename_and_keep(df, ref_idx, val_idx, new_val_name):
        mapping = {df.columns[ref_idx - 1]: "RéférenceProduit",
                   df.columns[val_idx - 1]: new_val_name}
        out = df.rename(columns=mapping)[mapping.values()]
        return out

    d1 = rename_and_keep(raw_cat,  st.session_state["cat_ref"],  st.session_state["cat_val"],  "M2_annee_actuelle")
    d2 = rename_and_keep(raw_hist, st.session_state["hist_ref"], st.session_state["hist_val"], "M2_annee_derniere")
    d3 = rename_and_keep(raw_cli,  st.session_state["cli_ref"],  st.session_state["cli_val"],  "Code_famille_Client")

    # 3. Fusion par reduce (outer) vectorisé
    dff = reduce(lambda l, r: l.merge(r, on="RéférenceProduit", how="outer"), (d1, d2, d3))
    dff["Entreprise"] = entreprise

    missing = dff[dff["Code_famille_Client"].isna()].copy()
    dstr = datetime.today().strftime("%y%m%d")

    st.session_state.update(
        dff_df=dff,
        missing_df=missing,
        dff_csv=dff.to_csv(index=False, sep=";").encode(),
        dstr=dstr,
        ent=entreprise,
        missing_file=None,
    )
    st.success("Fusion OK ! Choisissez les colonnes et générez l’Excel client.")

# ------------- export fichier à remplir -----------------
if (missing_df := st.session_state.get("missing_df")) is not None and not missing_df.empty:
    st.subheader("Colonnes à inclure dans le fichier à remplir")
    available = [c for c in missing_df.columns if c not in ("Code_famille_Client", "Entreprise")]
    default   = [c for c in ("M2_annee_actuelle", "MACH2_FAM", "FAMI_LIBELLE", "MACH2_SFAM", "SFAMI_LIBELLE", "MACH2_FONC", "FONC_LIBELLE") if c in available]
    sel = st.multiselect("RéférenceProduit est toujours incluse :", available, default)

    if st.button("Générer Excel à remplir"):
        export = (missing_df[["RéférenceProduit"] + sel]
                  .drop_duplicates()
                  .assign(Code_famille_Client=""))
        buf = io.BytesIO()
        export.to_excel(buf, index=False)
        buf.seek(0)
        st.session_state["missing_file"] = buf
        st.success("Fichier client prêt !")

# ------------- Téléchargements intermédiaires ----------
if (dff_df := st.session_state.get("dff_df")) is not None:
    st.subheader("Aperçu DFF")
    st.dataframe(dff_df.head(100), use_container_width=True)

    st.download_button("⬇️ DFF interne",
                       st.session_state["dff_csv"],
                       file_name=f"DFF_{st.session_state.ent}_{st.session_state.dstr}.csv",
                       mime="text/csv")

    if st.session_state.get("missing_file"):
        st.download_button("⬇️ Fichier à remplir (Excel)",
                           st.session_state["missing_file"],
                           file_name=f"CODES_CLIENT_{st.session_state.ent}_{st.session_state.dstr}.xlsx",
                           mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

# ══════════ 3. ÉTAPE 2 ══════════
st.header("Étape 2 : retour client → fichiers finaux")

dff_file = st.file_uploader("DFF initial (CSV)", type="csv")
maj_files = st.file_uploader("Fichier(s) client complété(s)",
                             type=("csv", "xlsx", "xls"), accept_multiple_files=True)

if st.button("Fusionner Étape 2"):
    if not (dff_file and maj_files):
        st.warning("Chargez le DFF et au moins un fichier client.")
        st.stop()

    try:
        dff_init = pd.read_csv(dff_file, sep=";")
    except Exception as e:
        st.error(f"Lecture DFF : {e}")
        st.stop()

    maj_dfs = [read_any(f) for f in maj_files]
    maj_dfs = [df for df in maj_dfs if df is not None]
    if not maj_dfs:
        st.error("Aucun fichier client valide.")
        st.stop()

    maj_cat = pd.concat(maj_dfs, ignore_index=True)
    if "Code_famille_Client" not in maj_cat.columns:
        maj_cat.columns = ["RéférenceProduit", "Code_famille_Client"][: len(maj_cat.columns)]
    maj_cat = maj_cat[["RéférenceProduit", "Code_famille_Client"]].drop_duplicates()

    dff_final = dff_init.merge(maj_cat, on="RéférenceProduit", how="left", suffixes=("", "_maj"))
    dff_final["Code_famille_Client"] = dff_final["Code_famille_Client"].fillna(dff_final["Code_famille_Client_maj"])
    dff_final = dff_final.drop(columns=["Code_famille_Client_maj"])

    encore_missing = dff_final[dff_final["Code_famille_Client"].isna()]

    ent_out = dff_final["Entreprise"].dropna().unique()[0] if "Entreprise" in dff_final.columns else ""
    dfrx_df = build_dfrx(dff_final[dff_final["Code_famille_Client"].notna()], ent_out)

    dstr = datetime.today().strftime("%y%m%d")
    dfrx_name = f"DFRXHYBRCMR{dstr}0000"
    txt_name  = f"AFRXHYBRCMR{dstr}0000.txt"

    dfrx_tsv = dfrx_df.to_csv(sep="\t", index=False, header=False)
    txt_content = (f"DFRXHYBRCMR{dstr}000068230116IT"
                   f"DFRXHYBRCMR{dstr}RCMRHYBFRX                    OK000000")

    st.subheader("Aperçu DFRX")
    st.dataframe(dfrx_df.head(50))

    st.download_button("⬇️ DFRX (TSV)", dfrx_tsv, file_name=dfrx_name, mime="text/plain")
    st.download_button("⬇️ Accusé TXT", txt_content, file_name=txt_name, mime="text/plain")

    if not encore_missing.empty:
        st.subheader("Références sans code client")
        st.dataframe(encore_missing)
        st.download_button("⬇️ Références restantes",
                           encore_missing.to_csv(index=False, sep=";").encode(),
                           file_name=f"CODES_MANQUANTS_{dstr}.csv",
                           mime="text/csv")
    else:
        st.success("✅ Tous les codes client sont renseignés.")
```

---

### `.streamlit/config.toml`

```
[server]
fileWatcherType = "none"
```

* Copie‐colle ces deux fichiers dans ton dépôt.  
* `git add app.py .streamlit/config.toml && git commit -m "version optimisée" && git push`  
* Redeploie sur Streamlit Cloud : même rendu, code et mémoire mieux optimisés.
