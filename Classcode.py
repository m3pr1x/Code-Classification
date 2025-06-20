# app.py
import io
from functools import reduce
from datetime import datetime

import pandas as pd
import streamlit as st

# ───────────────────── PAGE ─────────────────────
st.set_page_config(page_title="Fusion familles produit", page_icon="🧩", layout="wide")
st.title("🧩 Classification Code")

st.markdown(
    """
1. Chargez vos **trois fichiers**  
   • Catalogue interne (*M2 année actuelle*)  
   • Historique (*M2 année dernière*)  
   • Fichier client (*Code famille Client*)  

2. Indiquez les **indices de colonnes** (1 = première) :

| Fichier | Col. pivot (Réf. produit) | Col. valeur |
|---------|---------------------------|-------------|
| Catalogue interne | Réf. produit | **M2 année actuelle** |
| Historique        | Réf. produit | **M2 année dernière** |
| Fichier client    | Réf. produit | **Code famille Client** |

3. Renseignez le **nom de l’entreprise** (en MAJUSCULES).

4. Cliquez sur **Fusionner** pour obtenir :  
   • `DFF` (vue complète)  
   • `Réfs à valider` (codes client manquants)  
   • fichier Excel des **M2 sans matching**
"""
)

# ─────────────── UTILITAIRES ────────────────
def read_any(file):
    """
    Lecture d'un CSV ou Excel.

    - CSV : teste encodages courants.
    - XLSX/XLS : nécessite openpyxl. Affiche un message clair si absent.
    - Retourne un DataFrame ou None si erreur.
    """
    name = file.name.lower()

    # CSV ------------------------------------------------------------------
    if name.endswith(".csv"):
        for enc in ("utf-8", "latin1", "cp1252"):
            try:
                return pd.read_csv(file, encoding=enc)
            except UnicodeDecodeError:
                file.seek(0)
        st.error("❌ Impossible de lire le CSV : encodages testés = UTF-8, Latin-1, CP1252.")
        return None

    # EXCEL ----------------------------------------------------------------
    elif name.endswith((".xlsx", ".xls")):
        try:
            return pd.read_excel(file, engine="openpyxl")
        except ImportError:
            st.error(
                "❌ openpyxl n’est pas installé. "
                "Ajoutez `openpyxl` à requirements.txt puis redeployez."
            )
            return None
        except Exception as e:
            st.error(f"❌ Erreur de lecture Excel : {e}")
            return None

    else:
        st.error("❌ Format non pris en charge (CSV ou Excel uniquement).")
        return None


def today_yyMMdd() -> str:
    return datetime.today().strftime("%y%m%d")


def classification_class(df1, df2, df3, entreprise,
                         col_ref="RéférenceProduit",
                         col_m2_last="M2_annee_derniere",
                         col_m2_now="M2_annee_actuelle",
                         col_client="Code_famille_Client"):
    """Outer-merge des trois sources + ajout colonne Entreprise. Retourne DFF et df_missing."""
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
    """Garde deux colonnes par indice (1-based) et les renomme uniformément."""
    try:
        trimmed = df.iloc[:, [ref_idx - 1, value_idx - 1]].copy()
    except IndexError:
        st.error("❌ Indice de colonne hors plage.")
        return None
    trimmed.columns = ["RéférenceProduit", value_col_name]
    return trimmed


# ─────────────── UPLOADS & INPUTS ────────────────
st.subheader("1. Fichiers source")

with st.container():
    file1 = st.file_uploader("📄 Catalogue interne (M2 année **actuelle**)", type=("csv", "xlsx", "xls"))
    if file1:
        ref1 = st.number_input("🔢 Colonne référence produit (Dataset 1)", 1, step=1, value=1, key="ref1")
        val1 = st.number_input("🔢 Colonne **M2 année actuelle**",          1, step=1, value=2, key="val1")

with st.container():
    file2 = st.file_uploader("📄 Historique (M2 année **dernière**)", type=("csv", "xlsx", "xls"))
    if file2:
        ref2 = st.number_input("🔢 Colonne référence produit (Dataset 2)", 1, step=1, value=1, key="ref2")
        val2 = st.number_input("🔢 Colonne **M2 année dernière**",          1, step=1, value=2, key="val2")

with st.container():
    file3 = st.file_uploader("📄 Fichier client (**Code famille Client**)", type=("csv", "xlsx", "xls"))
    if file3:
        ref3 = st.number_input("🔢 Colonne référence produit (Dataset 3)", 1, step=1, value=1, key="ref3")
        val3 = st.number_input("🔢 Colonne **Code famille Client**",        1, step=1, value=2, key="val3")

st.subheader("2. Paramètre Entreprise")
entreprise_input = st.text_input("🏢 Nom de l’entreprise (MAJUSCULES)", placeholder="ACME SA")
entreprise = entreprise_input.strip().upper()

# ─────────────── ACTION ────────────────
st.subheader("3. Fusion & export")

if st.button("🚀 Fusionner"):
    if not (file1 and file2 and file3 and entreprise):
        st.warning("🛈 Joignez les trois fichiers **et** saisissez l’entreprise.")
        st.stop()

    # Lecture
    df_raw1, df_raw2, df_raw3 = [read_any(f) for f in (file1, file2, file3)]

    # Si l’un des DataFrames n’a pas été lu correctement → arrêt
    if any(obj is None for obj in (df_raw1, df_raw2, df_raw3)):
        st.stop()

    # Extraction
    df1 = build_trimmed_df(df_raw1, ref1, val1, "M2_annee_actuelle")
    df2 = build_trimmed_df(df_raw2, ref2, val2, "M2_annee_derniere")
    df3 = build_trimmed_df(df_raw3, ref3, val3, "Code_famille_Client")
    if any(obj is None for obj in (df1, df2, df3)):
        st.stop()

    # Fusion
    DFF, df_missing = classification_class(df1, df2, df3, entreprise)

    # Excel des M2 sans matching
    serie_m2 = df_missing["M2_annee_actuelle"].dropna().drop_duplicates()
    buffer_excel = None
    if not serie_m2.empty:
        buffer_excel = io.BytesIO()
        serie_m2.to_excel(buffer_excel, index=False, header=False)
        buffer_excel.seek(0)

    # Affichage
    st.success("✅ Fusion terminée.")
    st.subheader("Dataset complet (DFF)")
    st.dataframe(DFF, use_container_width=True)

    st.subheader("Références à faire valider (sans Code famille Client)")
    st.dataframe(df_missing, use_container_width=True)

    # Téléchargements
    dstr = today_yyMMdd()
    DFF_csv     = DFF.to_csv(index=False, sep=";").encode()
    missing_csv = df_missing.to_csv(index=False, sep=";").encode()

    st.download_button("📥 Télécharger DFF",
                       DFF_csv,
                       file_name=f"DFF_{entreprise}_{dstr}.csv",
                       mime="text/csv")

    st.download_button("📥 Télécharger références à valider",
                       missing_csv,
                       file_name=f"REFS_A_VALIDER_{entreprise}_{dstr}.csv",
                       mime="text/csv")

    if buffer_excel:
        st.download_button("📥 Télécharger M2 sans matching (Excel)",
                           buffer_excel,
                           file_name=f"M2_SANS_MATCH_{entreprise}_{dstr}.xlsx",
                           mime=("application/vnd.openxmlformats-officedocument"
                                 ".spreadsheetml.sheet"))
    else:
        st.info("👍 Aucun code M2 sans correspondance client !")
