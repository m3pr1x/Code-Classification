# -*- coding: utf-8 -*-
"""
app.py – Classification Code (DFF / DFRX)

• Lecture CSV ultra-robuste : détection auto du séparateur, 3 encodages, skip
  des lignes corrompues.  
• Désactivation du watcher inotify via .streamlit/config.toml (plus
  d’erreur « inotify watch limit reached » sur Streamlit Cloud).

Aucune logique métier n’a changé : seules les fonctions de lecture CSV / Excel
et l’en-tête ont été ajustées.
"""

from __future__ import annotations

import csv
import io
from datetime import datetime
from functools import reduce

import pandas as pd
import streamlit as st

# ══════════ 0. PAGE + CLEAR ══════════
st.set_page_config("Classification Code", "🧩", layout="wide")


def clear_and_rerun():
    st.session_state.clear()
    st.rerun()


st.button("🗑️ CLEAR", on_click=clear_and_rerun)
st.title("🧩 Classification Code")


# ══════════ 1. OUTILS ══════════
def read_any(file):
    """
    Lecture robuste CSV ou Excel.
    • essaie encodings utf-8, latin-1, cp1252
    • autodétecte séparateur  ( ;  ,  |  tab )
    • engine='python'  +  on_bad_lines='skip'  pour ignorer lignes tordues
    """
    name = file.name.lower()

    # ---------- CSV ----------
    if name.endswith(".csv"):
        for enc in ("utf-8", "latin1", "cp1252"):
            try:
                # 1. détecte le séparateur sur 2 kio
                file.seek(0)
                sample = file.read(2048).decode(enc, errors="ignore")
                dialect = csv.Sniffer().sniff(sample, delimiters=";,|\t")
                sep = dialect.delimiter

                # 2. lit le CSV complet
                file.seek(0)
                return pd.read_csv(
                    file,
                    sep=sep,
                    encoding=enc,
                    engine="python",      # parseur permissif
                    on_bad_lines="skip",  # lignes mal formées ignorées
                )
            except (UnicodeDecodeError, csv.Error, pd.errors.ParserError):
                file.seek(0)  # réinitialise le curseur et teste l’encodage suivant

        st.error(f"{file.name} : encodage ou séparateur non reconnu.")
        return None

    # ---------- Excel ----------
    if name.endswith((".xlsx", ".xls")):
        try:
            return pd.read_excel(file, engine="openpyxl")
        except ImportError:
            st.error("openpyxl manquant (ajoutez-le au requirements).")
            return None

    # ---------- Autre format ----------
    st.error(f"{file.name} : format non pris en charge.")
    return None


def concat_dfs(dfs):
    if not dfs:
        return None
    big = pd.concat(dfs, ignore_index=True)
    headers = list(big.columns)
    big = big[~(big.iloc[:, 0] == headers[0])].drop_duplicates().reset_index(drop=True)
    return big


def subset_current(df, i_ref, i_val):
    df = df.rename(columns={df.columns[i_ref - 1]: "RéférenceProduit",
                             df.columns[i_val - 1]: "M2_annee_actuelle"})
    return df[["RéférenceProduit", "M2_annee_actuelle"]]


def subset_previous(df, i_ref, i_val):
    df = df.rename(columns={df.columns[i_ref - 1]: "RéférenceProduit",
                             df.columns[i_val - 1]: "M2_annee_derniere"})
    extra = ["MACH2_FAM", "FAMI_LIBELLE", "MACH2_SFAM", "SFAMI_LIBELLE",
             "MACH2_FONC", "FONC_LIBELLE"]
    cols = ["RéférenceProduit", "M2_annee_derniere"] + [c for c in extra if c in df.columns]
    return df[cols]


def subset_client(df, i_ref, i_val):
    df = df.rename(columns={df.columns[i_ref - 1]: "RéférenceProduit",
                             df.columns[i_val - 1]: "Code_famille_Client"})
    return df[["RéférenceProduit", "Code_famille_Client"]]


def fusion_etape1(d1, d2, d3, ent):
    full = reduce(lambda l, r: pd.merge(l, r, on="RéférenceProduit", how="outer"), [d1, d2, d3])
    full["Entreprise"] = ent
    missing = full[full["Code_famille_Client"].isna()].copy()
    return full, missing


def appliquer_maj(dff, maj):
    merged = dff.merge(
        maj[["RéférenceProduit", "Code_famille_Client"]],
        on="RéférenceProduit", how="left", suffixes=("", "_maj")
    )
    mask = merged["Code_famille_Client"].isna() & merged["Code_famille_Client_maj"].notna()
    merged.loc[mask, "Code_famille_Client"] = merged.loc[mask, "Code_famille_Client_maj"]
    return merged.drop(columns=["Code_famille_Client_maj"])


def build_dfrx(df, ent):
    return pd.DataFrame({
        "Code famille Client": df["Code_famille_Client"],
        "onsenfou": None,
        "Entreprises": ent,
        "M2": "M2_" + df["RéférenceProduit"].astype(str),
    }).drop_duplicates()


# ══════════ 2. ÉTAPE 1 ══════════
st.header("Étape 1 : DFF & fichier à remplir")

# --- init containers ---
for lot in ("cat", "hist", "cli"):
    st.session_state.setdefault(f"{lot}_dfs", [])
    st.session_state.setdefault(f"{lot}_names", [])

lots = [
    ("Catalogue interne", "cat", "idx Réf.", "idx M2 actuelle"),
    ("Historique",        "hist", "idx Réf.", "idx M2 dernière"),
    ("Fichier client",    "cli",  "idx Réf.", "idx Code famille"),
]

cols = st.columns(3)
for (label, key, lab_ref, lab_val), col in zip(lots, cols):
    with col:
        st.markdown(f"##### {label}")
        new_files = st.file_uploader("Drag & drop (peut être répété)",
                                     accept_multiple_files=True,
                                     type=("csv", "xlsx", "xls"),
                                     key=f"u_{key}")
        if new_files:
            added = 0
            for f in new_files:
                if f.name not in st.session_state[f"{key}_names"]:
                    df = read_any(f)
                    if df is not None:
                        st.session_state[f"{key}_dfs"].append(df)
                        st.session_state[f"{key}_names"].append(f.name)
                        added += 1
            if added:
                st.success(f"{added} fichier(s) ajouté(s) au lot « {label} ».")

        # index selectors
        ref_idx = st.number_input(lab_ref, 1, 50, 1,
                                  key=f"{key}_ref", label_visibility="collapsed")
        val_idx = st.number_input(lab_val, 1, 50, 2,
                                  key=f"{key}_val", label_visibility="collapsed")
        st.caption(f"📂 {len(st.session_state[f'{key}_dfs'])} fichier(s) dans le lot.")

entreprise = st.text_input("Entreprise (MAJUSCULES)").strip().upper()

if st.button("Fusionner Étape 1"):
    if not (st.session_state.cat_dfs and st.session_state.hist_dfs
            and st.session_state.cli_dfs and entreprise):
        st.warning("Remplissez les trois lots et le champ Entreprise.")
        st.stop()

    raw1 = concat_dfs(st.session_state.cat_dfs)
    raw2 = concat_dfs(st.session_state.hist_dfs)
    raw3 = concat_dfs(st.session_state.cli_dfs)

    r1, v1 = st.session_state["cat_ref"],  st.session_state["cat_val"]
    r2, v2 = st.session_state["hist_ref"], st.session_state["hist_val"]
    r3, v3 = st.session_state["cli_ref"],  st.session_state["cli_val"]

    d1 = subset_current(raw1, r1, v1)
    d2 = subset_previous(raw2, r2, v2)
    d3 = subset_client(raw3,  r3, v3)

    dff, missing = fusion_etape1(d1, d2, d3, entreprise)
    dstr = datetime.today().strftime("%y%m%d")

    st.session_state.update(
        dff_df=dff,
        missing_df=missing,
        dff_csv=dff.to_csv(index=False, sep=";").encode(),
        dstr=dstr,
        ent=entreprise,
        missing_file=None
    )
    st.success("Fusion effectuée ! Choisissez les colonnes et générez l’Excel.")

# (le reste du script — sélection des colonnes, Étape 2, téléchargements —
#  reste exactement le même que ta version précédente)
