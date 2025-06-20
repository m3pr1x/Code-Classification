# app.py
import io
import pandas as pd
from datetime import datetime
import streamlit as st
from functools import reduce

# ───────────────────── PARAMÈTRES PAGE ─────────────────────
st.set_page_config(page_title="Fusion familles produit", page_icon="🧩", layout="wide")
st.title("🧩 Classification Code")

st.markdown(
    """
1. Chargez vos **trois fichiers**  
   • Catalogue interne (*M2 année actuelle*)  
   • Historique (*M2 année dernière*)  
   • Fichier client (*Code famille Client*)  

2. Indiquez les **indices de colonnes** (1 = première) pour :  
   - la référence produit *(pivot)*  
   - la valeur spécifique de chaque fichier  

3. Renseignez le **nom de l’entreprise** (en majuscules).  

4. Cliquez sur **Fusionner** pour obtenir :  
   - `DFF` → vue complète interne  
   - `Réfs à valider` → références dont **Code famille Client** est vide  
   - fichier Excel des **M2 sans matching**
"""
)

# ───────────────────── UTILITAIRES ─────────────────────
def read_any(file):
    "Lit un CSV ou un Excel en essayant les encodages courants."
    name = file.name.lower()
    if name.endswith(".csv"):
        for enc in ("utf-8", "latin1", "cp1252"):
            try:
                return pd.read_csv(file, encoding=enc)
            except UnicodeDecodeError:
                file.seek(0)
        st.error("❌ Encodage CSV non reconnu.")
        return None
    else:
        return pd.read_excel(file, engine="openpyxl")

def today_yyMMdd() -> str:
    return datetime.today().strftime("%y%m%d")

def classification_class(df1, df2, df3, entreprise,
                          col_ref="RéférenceProduit",
                          col_m2_last="M2_annee_derniere",
                          col_m2_now="M2_annee_actuelle",
                          col_client="Code_famille_Client"):
    """Fusionne les trois sources et renvoie DFF + références sans code client."""
    dfs = [
        df1[[col_ref, col_m2_now]],
        df2[[col_ref, col_m2_last]],
        df3[[col_ref, col_client]],
    ]
    DFF = reduce(lambda l, r: pd.merge(l, r, on=col_ref, how="outer"), dfs)
    DFF["Entreprise"] = entreprise
    df_missing = DFF[DFF[col_client].isna()].copy()
    return DFF, df_missing

def build_trimmed_df(df, ref_idx, value_idx, value_col_name):
    """Extrait deux colonnes par indice (1-based) et les renomme uniformément."""
    try:
        trimmed = df.iloc[:, [ref_idx - 1, value_idx - 1]].copy()
    except IndexError:
        st.error("❌ Indice de colonne hors plage.")
        return None
    trimmed.columns = ["RéférenceProduit", value_col_name]
    return trimmed

# ───────────────────── UPLOADS & INPUTS ─────────────────────
st.subheader("1. Fichiers source")

with st.container():
    file1 = st.file_uploader("📄 Catalogue interne (M2 année **actuelle**)", type=("csv", "xlsx", "xls"))
    if file1:
        ref1 = st.number_input("🔢 Colonne référence produit (Dataset 1)", min_value=1, value=1, step=1, key="ref1")
        val1 = st.number_input("🔢 Colonne **M2 année actuelle**",         min_value=1, value=2, step=1, key="val1")

with st.container():
    file2 = st.file_uploader("📄 Historique (M2 année **dernière**)", type=("csv", "xlsx", "xls"))
    if file2:
        ref2 = st.number_input("🔢 Colonne référence produit (Dataset 2)", min_value=1, value=1, step=1, key="ref2")
        val2 = st.number_input("🔢 Colonne **M2 année dernière**",          min_value=1, value=2, step=1, key="val2")

with st.container():
    file3 = st.file_uploader("📄 Fichier client (**Code famille Client**)", type=("csv", "xlsx", "xls"))
    if file3:
        ref3 = st.number_input("🔢 Colonne référence produit (Dataset 3)", min_value=1, value=1, step=1, key="ref3")
        val3 = st.number_input("🔢 Colonne **Code famille Client**",        min_value=1, value=2, step=1, key="val3")

st.subheader("2. Paramètres complémentaires")
entreprise_input = st.text_input("🏢 Nom de l’entreprise (MAJUSCULES)", placeholder="ACME SA")
entreprise = entreprise_input.strip().upper()

# ───────────────────── BOUTON ACTION ─────────────────────
st.subheader("3. Fusion & export")

if st.button("🚀 Fusionner"):
    if not (file1 and file2 and file3 and entreprise):
        st.warning("🛈 Merci de joindre les trois fichiers **et** de saisir le nom de l’entreprise.")
        st.stop()

    # Lecture
    df_raw1 = read_any(file1)
    df_raw2 = read_any(file2)
    df_raw3 = read_any(file3)
    if None in (df_raw1, df_raw2, df_raw3):
        st.stop()

    # Extraction / renommage
    df1 = build_trimmed_df(df_raw1, ref1, val1, "M2_annee_actuelle")
    df2 = build_trimmed_df(df_raw2, ref2, val2, "M2_annee_derniere")
    df3 = build_trimmed_df(df_raw3, ref3, val3, "Code_famille_Client")
    if None in (df1, df2, df3):
        st.stop()

    # Fusion
    DFF, df_missing = classification_class(df1, df2, df3, entreprise)

    # ── Fichier Excel : M2 sans matching
    serie_m2_sans_match = (
        df_missing["M2_annee_actuelle"]
        .dropna()
        .drop_duplicates()
    )
    buffer_excel = None
    if not serie_m2_sans_match.empty:
        buffer_excel = io.BytesIO()
        serie_m2_sans_match.to_excel(buffer_excel, index=False, header=False)
        buffer_excel.seek(0)

    # ── Affichage interactif
    st.success("✅ Fusion terminée.")
    st.subheader("Dataset complet (DFF)")
    st.dataframe(DFF, use_container_width=True)

    st.subheader("Références à faire valider (sans Code famille Client)")
    st.dataframe(df_missing, use_container_width=True)

    # ── Téléchargements
    dstr = today_yyMMdd()
    DFF_csv     = DFF.to_csv(index=False, sep=";").encode("utf-8")
    missing_csv = df_missing.to_csv(index=False, sep=";").encode("utf-8")

    st.download_button(
        "📥 Télécharger DFF",
        data=DFF_csv,
        file_name=f"DFF_{entreprise}_{dstr}.csv",
        mime="text/csv",
    )
    st.download_button(
        "📥 Télécharger références à valider",
        data=missing_csv,
        file_name=f"REFS_A_VALIDER_{entreprise}_{dstr}.csv",
        mime="text/csv",
    )
    if buffer_excel:
        st.download_button(
            "📥 Télécharger M2 sans matching (Excel)",
            data=buffer_excel,
            file_name=f"M2_SANS_MATCH_{entreprise}_{dstr}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
    else:
        st.info("👍 Aucun code M2 sans correspondance client !")
