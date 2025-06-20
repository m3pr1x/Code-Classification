# app.py
import io
from functools import reduce
from datetime import datetime

import pandas as pd
import streamlit as st

# ─────────────── 1. PAGE & CLEAR ────────────────
st.set_page_config(page_title="Fusion familles produit", page_icon="🧩", layout="wide")

# petit hack : colonne droite pour le bouton CLEAR
clear_col, title_col = st.columns([1, 9])
with clear_col:
    if st.button("🗑️ CLEAR", type="primary", help="Réinitialiser l'application"):
        st.session_state.clear()
        st.experimental_rerun()

with title_col:
    st.title("🧩 Classification Code")

# ─────────────── 2. TEXTE INTRO ────────────────
st.markdown(
    """
1. Chargez vos **trois fichiers**  
2. Indiquez les indices de colonnes  
3. Saisissez le nom de l’**entreprise**  
4. Cliquez sur **Fusionner** → téléchargements persistants
"""
)

# ─────────────── 3. UTILITAIRES ────────────────
def read_any(file):
    name = file.name.lower()
    if name.endswith(".csv"):
        for enc in ("utf-8", "latin1", "cp1252"):
            try:
                return pd.read_csv(file, encoding=enc)
            except UnicodeDecodeError:
                file.seek(0)
        st.error("❌ Encodage CSV non reconnu.")
    elif name.endswith((".xlsx", ".xls")):
        try:
            return pd.read_excel(file, engine="openpyxl")
        except ImportError:
            st.error("❌ Ajoutez `openpyxl` à requirements.txt.")
    else:
        st.error("❌ Format non pris en charge.")
    return None


def classification_class(df1, df2, df3, entreprise):
    dfs = [
        df1[["RéférenceProduit", "M2_annee_actuelle"]],
        df2[["RéférenceProduit", "M2_annee_derniere"]],
        df3[["RéférenceProduit", "Code_famille_Client"]],
    ]
    dff = reduce(lambda l, r: pd.merge(l, r, on="RéférenceProduit", how="outer"), dfs)
    dff["Entreprise"] = entreprise
    missing = dff[dff["Code_famille_Client"].isna()].copy()
    return dff, missing


def store_outputs(dff, missing, entreprise):
    dstr = datetime.today().strftime("%y%m%d")

    st.session_state.dstr = dstr
    st.session_state.entreprise = entreprise

    st.session_state.DFF_csv = dff.to_csv(index=False, sep=";").encode()
    st.session_state.missing_csv = missing.to_csv(index=False, sep=";").encode()

    serie_m2 = missing["M2_annee_actuelle"].dropna().drop_duplicates()
    if serie_m2.empty:
        st.session_state.buffer_excel = None
    else:
        buf = io.BytesIO()
        serie_m2.to_excel(buf, index=False, header=False)
        buf.seek(0)
        st.session_state.buffer_excel = buf

    st.session_state.dff_df = dff
    st.session_state.missing_df = missing

# ─────────────── 4. UPLOADS & INPUTS ────────────────
st.subheader("Fichiers")

def num(label, key):
    return st.number_input(label, 1, step=1, value=1, key=key)

file1 = st.file_uploader("📄 Catalogue interne (M2 actuelle)", type=("csv", "xlsx", "xls"))
if file1:
    ref1 = num("Colonne référence (file1)", "r1")
    val1 = num("Colonne *M2 actuelle*", "v1")

file2 = st.file_uploader("📄 Historique (M2 dernière)", type=("csv", "xlsx", "xls"))
if file2:
    ref2 = num("Colonne référence (file2)", "r2")
    val2 = num("Colonne *M2 dernière*", "v2")

file3 = st.file_uploader("📄 Client (Code famille)", type=("csv", "xlsx", "xls"))
if file3:
    ref3 = num("Colonne référence (file3)", "r3")
    val3 = num("Colonne *Code famille*", "v3")

entreprise = st.text_input("🏢 Entreprise (MAJUSCULES)").strip().upper()

# ─────────────── 5. FUSION ────────────────
if st.button("🚀 Fusionner"):
    if not (file1 and file2 and file3 and entreprise):
        st.warning("🛈 Chargez 3 fichiers + entreprise.")
        st.stop()

    # lecture
    dfs_raw = [read_any(f) for f in (file1, file2, file3)]
    if any(obj is None for obj in dfs_raw):
        st.stop()

    # extraction/renommage
    def trim(df, ref_idx, val_idx, new_col):
        try:
            tmp = df.iloc[:, [ref_idx - 1, val_idx - 1]].copy()
            tmp.columns = ["RéférenceProduit", new_col]
            return tmp
        except IndexError:
            st.error("❌ Indice de colonne hors plage.")
            return None

    df1 = trim(dfs_raw[0], ref1, val1, "M2_annee_actuelle")
    df2 = trim(dfs_raw[1], ref2, val2, "M2_annee_derniere")
    df3 = trim(dfs_raw[2], ref3, val3, "Code_famille_Client")
    if any(obj is None for obj in (df1, df2, df3)):
        st.stop()

    dff, missing = classification_class(df1, df2, df3, entreprise)
    store_outputs(dff, missing, entreprise)
    st.success("✅ Fusion terminée !")

# ─────────────── 6. AFFICHAGE / DOWNLOADS ────────────────
if "dff_df" in st.session_state:
    st.subheader("Dataset complet (DFF)")
    st.dataframe(st.session_state.dff_df, use_container_width=True)

    st.subheader("Références à valider")
    st.dataframe(st.session_state.missing_df, use_container_width=True)

    st.download_button(
        "📥 Télécharger DFF",
        st.session_state.DFF_csv,
        file_name=f"DFF_{st.session_state.entreprise}_{st.session_state.dstr}.csv",
        mime="text/csv",
    )

    st.download_button(
        "📥 Télécharger références à valider",
        st.session_state.missing_csv,
        file_name=f"REFS_A_VALIDER_{st.session_state.entreprise}_{st.session_state.dstr}.csv",
        mime="text/csv",
    )

    if st.session_state.buffer_excel:
        st.download_button(
            "📥 Télécharger M2 sans matching (Excel)",
            st.session_state.buffer_excel,
            file_name=f"M2_SANS_MATCH_{st.session_state.entreprise}_{st.session_state.dstr}.xlsx",
            mime=("application/vnd.openxmlformats-officedocument"
                  ".spreadsheetml.sheet"),
        )
