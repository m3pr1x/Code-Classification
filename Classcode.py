# ────────────────────────────────────────────────────────────
# fichier : classification_code.py
# ────────────────────────────────────────────────────────────
import csv, io
from datetime import datetime

import pandas as pd
import streamlit as st

# ---------- Page ----------
st.set_page_config("Classification Code", "🧩", layout="wide")
st.title("🧩 Classification Code")

# ---------- Helpers ----------
@st.cache_data(show_spinner=False)
def read_csv(buf: io.BytesIO) -> pd.DataFrame:
    """Lecture robuste .csv (encodage + séparateur)."""
    for enc in ("utf-8", "latin1", "cp1252"):
        buf.seek(0)
        try:
            sample = buf.read(2048).decode(enc, errors="ignore")
            sep = csv.Sniffer().sniff(sample, delimiters=";,|\t").delimiter
            buf.seek(0)
            return pd.read_csv(buf, sep=sep, encoding=enc, engine="python", on_bad_lines="skip")
        except (UnicodeDecodeError, csv.Error, pd.errors.ParserError):
            continue
    raise ValueError("Impossible de lire le fichier")

@st.cache_data(show_spinner=False)
def read_any(upload) -> pd.DataFrame | None:
    name = upload.name.lower()
    if name.endswith(".csv"):
        return read_csv(upload)
    if name.endswith((".xlsx", ".xls")):
        return pd.read_excel(upload, engine="openpyxl")
    return None

def to_m2(s: pd.Series) -> pd.Series:
    return s.astype(str).str.zfill(6)

# ---------- Upload de l'appairage ----------
st.subheader("1) Charger l'appairage M2 ➜ Code famille")
pair_file = st.file_uploader("CSV exporté depuis l'app 'Mise à jour M2'", type="csv")

if pair_file:
    pair_df = read_csv(pair_file)
    expected_cols = {"M2", "Code_famille_Client"}
    if not expected_cols.issubset(pair_df.columns):
        st.error(f"Le fichier doit contenir au moins les colonnes : {expected_cols}")
        st.stop()
    pair_df["M2"] = to_m2(pair_df["M2"])
    st.success(f"{len(pair_df)} lignes d'appairage chargées")
    st.dataframe(pair_df.head())

# ---------- Upload des jeux de données à classer ----------
st.subheader("2) Charger les fichiers à classifier")
data_files = st.file_uploader(
    "Glisse‑dépose (CSV, XLSX, XLS)…",
    accept_multiple_files=True,
    type=("csv", "xlsx", "xls"),
)

if pair_file and data_files:
    result_frames = []
    for upl in data_files:
        df = read_any(upl)
        if df is None:
            st.warning(f"⚠️ Impossible de lire {upl.name}, ignoré.")
            continue

        st.markdown(f"###### {upl.name}")
        # Sélection de la colonne contenant M2
        cols = [f"{i+1} – {c}" for i, c in enumerate(df.columns)]
        idx = st.selectbox(
            "Colonne M2",
            options=cols,
            key=f"m2col_{upl.name}",
            index=0,
        )
        m2_col = df.columns[int(idx.split(" –")[0]) - 1]

        # Normalisation & jointure
        df["M2"] = to_m2(df[m2_col])
        merged = df.merge(pair_df[["M2", "Code_famille_Client"]], on="M2", how="left")

        nbr_found = merged["Code_famille_Client"].notna().sum()
        st.write(f"→ {nbr_found} lignes appariées sur {len(df)}")

        result_frames.append(merged)

        # Aperçu
        with st.expander("Aperçu", expanded=False):
            st.dataframe(merged.head())

    # ---------- Export global ----------
    if result_frames:
        final = pd.concat(result_frames, ignore_index=True)
        dstr = datetime.today().strftime("%y%m%d_%H%M%S")
        dl_name = f"DATA_CLASSIFIEE_{dstr}.csv"
        st.download_button(
            "⬇️ Télécharger toutes les données classifiées (CSV)",
            final.to_csv(index=False, sep=";"),
            file_name=dl_name,
            mime="text/csv",
        )
        st.success("Classification terminée !")
elif pair_file and not data_files:
    st.info("Ajoute un ou plusieurs fichiers à classifier.")
else:
    st.info("Commence par charger l'appairage M2.")
