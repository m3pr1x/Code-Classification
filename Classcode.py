# app.py
import io
from functools import reduce
from datetime import datetime

import pandas as pd
import streamlit as st

# ═════════════════════ 0. PAGE + CLEAR ═════════════════════
st.set_page_config(page_title="Classification Code", page_icon="🧩", layout="wide")


def clear_and_rerun():
    st.session_state.clear()
    st.rerun()


clear_col, title_col = st.columns([1, 9])
with clear_col:
    st.button("🗑️ CLEAR", type="primary", on_click=clear_and_rerun)
with title_col:
    st.title("🧩 Classification Code")

# ═════════════════════ 1. OUTILS ═════════════════════
def read_any(file):
    name = file.name.lower()
    if name.endswith(".csv"):
        for enc in ("utf-8", "latin1", "cp1252"):
            try:
                return pd.read_csv(file, encoding=enc)
            except UnicodeDecodeError:
                file.seek(0)
        st.error(f"❌ {file.name} : encodage CSV non reconnu.")
    elif name.endswith((".xlsx", ".xls")):
        try:
            return pd.read_excel(file, engine="openpyxl")
        except ImportError:
            st.error("❌ openpyxl manquant (ajoutez-le à requirements.txt).")
    else:
        st.error(f"❌ {file.name} : format non pris en charge.")
    return None


def concat_files(files):
    dfs = [df for f in files if (df := read_any(f)) is not None]
    if not dfs:
        return None
    big = pd.concat(dfs, ignore_index=True)
    headers = list(big.columns)
    big = big[~(big.iloc[:, 0] == headers[0])].drop_duplicates(keep="first").reset_index(drop=True)
    return big


def subset_current(df, ref_idx, val_idx):
    df = df.copy()
    df = df.rename(columns={
        df.columns[ref_idx - 1]: "RéférenceProduit",
        df.columns[val_idx - 1]: "M2_annee_actuelle",
    })
    return df[["RéférenceProduit", "M2_annee_actuelle"]]


def subset_previous(df, ref_idx, val_idx):
    df = df.copy()
    df = df.rename(columns={
        df.columns[ref_idx - 1]: "RéférenceProduit",
        df.columns[val_idx - 1]: "M2_annee_derniere",
    })
    keep = ["RéférenceProduit", "M2_annee_derniere"]
    extra = ["MACH2_FAM", "FAMI_LIBELLE", "MACH2_SFAM", "SFAMI_LIBELLE",
             "MACH2_FONC", "FONC_LIBELLE"]
    keep += [c for c in extra if c in df.columns]
    return df[keep]


def subset_client(df, ref_idx, val_idx):
    df = df.copy()
    df = df.rename(columns={
        df.columns[ref_idx - 1]: "RéférenceProduit",
        df.columns[val_idx - 1]: "Code_famille_Client",
    })
    return df[["RéférenceProduit", "Code_famille_Client"]]


def fusion_etape1(df1, df2, df3, ent):
    dff = reduce(lambda l, r: pd.merge(l, r, on="RéférenceProduit", how="outer"), [df1, df2, df3])
    dff["Entreprise"] = ent
    missing = dff[dff["Code_famille_Client"].isna()].copy()
    return dff, missing


def appliquer_mise_a_jour(dff, maj):
    joined = dff.merge(
        maj[["RéférenceProduit", "Code_famille_Client"]],
        on="RéférenceProduit",
        how="left",
        suffixes=("", "_maj"),
    )
    mask = joined["Code_famille_Client"].isna() & joined["Code_famille_Client_maj"].notna()
    joined.loc[mask, "Code_famille_Client"] = joined.loc[mask, "Code_famille_Client_maj"]
    return joined.drop(columns=["Code_famille_Client_maj"])


# ═════════════════════ 2. ÉTAPE 1 ═════════════════════
st.header("Étape 1 : générer le DFF et le fichier à remplir")

c1, c2, c3 = st.columns(3)
with c1:
    files1 = st.file_uploader("Catalogue interne (M2 actuelle)", type=("csv", "xlsx", "xls"),
                              accept_multiple_files=True)
    if files1:
        r1 = st.number_input("Réf.", 1, key="r1", value=1)
        v1 = st.number_input("M2 actuelle", 1, key="v1", value=2)
with c2:
    files2 = st.file_uploader("Historique (M2 dernière)", type=("csv", "xlsx", "xls"),
                              accept_multiple_files=True)
    if files2:
        r2 = st.number_input("Réf.", 1, key="r2", value=1)
        v2 = st.number_input("M2 dernière", 1, key="v2", value=2)
with c3:
    files3 = st.file_uploader("Fichier client (Code famille)", type=("csv", "xlsx", "xls"),
                              accept_multiple_files=True)
    if files3:
        r3 = st.number_input("Réf.", 1, key="r3", value=1)
        v3 = st.number_input("Code famille", 1, key="v3", value=2)

entreprise = st.text_input("Entreprise (MAJUSCULES)").strip().upper()

if st.button("Fusionner Étape 1"):
    if not (files1 and files2 and files3 and entreprise):
        st.warning("Chargez les trois blocs de fichiers + entreprise.")
        st.stop()

    raw1, raw2, raw3 = [concat_files(x) for x in (files1, files2, files3)]
    if any(df is None for df in (raw1, raw2, raw3)):
        st.stop()

    df1 = subset_current(raw1, r1, v1)
    df2 = subset_previous(raw2, r2, v2)
    df3 = subset_client(raw3,  r3, v3)
    if any(df is None for df in (df1, df2, df3)):
        st.stop()

    dff, missing = fusion_etape1(df1, df2, df3, entreprise)

    dstr = datetime.today().strftime("%y%m%d")
    st.session_state.update(
        dff_df=dff,
        missing_df=missing,
        dff_csv=dff.to_csv(index=False, sep=";").encode(),
        dstr=dstr,
        ent=entreprise,
    )

    if missing.empty:
        st.session_state["missing_file"] = None
    else:
        cols_export = ["M2_annee_actuelle", "MACH2_FAM", "FAMI_LIBELLE",
                       "MACH2_SFAM", "SFAMI_LIBELLE", "MACH2_FONC", "FONC_LIBELLE"]
        export_df = missing[[c for c in cols_export if c in missing.columns]].drop_duplicates()
        buf = io.BytesIO()
        export_df.to_excel(buf, index=False)  # header inclus
        buf.seek(0)
        st.session_state["missing_file"] = buf

    st.success("Étape 1 terminée !")

if "dff_df" in st.session_state:
    st.subheader("Aperçu DFF")
    st.dataframe(st.session_state.dff_df, use_container_width=True)

    st.download_button(
        "Télécharger DFF",
        st.session_state.dff_csv,
        file_name=f"DFF_{st.session_state.ent}_{st.session_state.dstr}.csv",
        mime="text/csv",
    )

    if st.session_state.get("missing_file"):
        st.download_button(
            "Télécharger fichier à remplir (Excel)",
            st.session_state.missing_file,
            file_name=f"CODES_CLIENT_{st.session_state.ent}_{st.session_state.dstr}.xlsx",
            mime=("application/vnd.openxmlformats-officedocument."
                  "spreadsheetml.sheet"),
        )
    else:
        st.info("Aucun code client manquant.")

# ═════════════════════ 3. ÉTAPE 2 ═════════════════════
st.header("Étape 2 : intégrer le fichier client complété")

col_dff, col_cli = st.columns(2)
with col_dff:
    dff_file = st.file_uploader("DFF initial (CSV)", type="csv")
with col_cli:
    maj_file = st.file_uploader("Fichier client complété", type=("csv", "xlsx", "xls"))

if st.button("Fusionner Étape 2"):
    if not (dff_file and maj_file):
        st.warning("Chargez les deux fichiers.")
        st.stop()

    try:
        dff_initial = pd.read_csv(dff_file, sep=";")
    except Exception as e:
        st.error(f"Lecture DFF : {e}")
        st.stop()

    maj_df = read_any(maj_file)
    if maj_df is None:
        st.stop()
    if "Code_famille_Client" not in maj_df.columns:
        maj_df.columns = ["RéférenceProduit", "Code_famille_Client"][: len(maj_df.columns)]

    dff_final = appliquer_mise_a_jour(dff_initial, maj_df)
    encore_missing = dff_final[dff_final["Code_famille_Client"].isna()]

    date_txt = datetime.today().strftime("%y%m%d")
    txt_name = f"AFRXHYBRCMR{date_txt}0000.txt"
    txt_content = f"DFRXHYBRCMR{date_txt}000068230116ITDFRXHYBRCMR{date_txt}RCMRHYBFRX                    OK000000"

    st.subheader("DFF final")
    st.dataframe(dff_final, use_container_width=True)

    st.download_button("Télécharger DFF final",
                       dff_final.to_csv(index=False, sep=";").encode(),
                       file_name=f"DFF_FINAL_{date_txt}.csv",
                       mime="text/csv")
    st.download_button(f"Télécharger {txt_name}",
                       txt_content,
                       file_name=txt_name,
                       mime="text/plain")

    if not encore_missing.empty:
        st.subheader("Références encore sans code client")
        st.dataframe(encore_missing, use_container_width=True)
        st.download_button("Télécharger références restantes",
                           encore_missing.to_csv(index=False, sep=";").encode(),
                           file_name=f"CODES_MANQUANTS_{date_txt}.csv",
                           mime="text/csv")
    else:
        st.success("Tous les codes client sont renseignés !")

