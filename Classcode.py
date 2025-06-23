# app.py
import io
from functools import reduce
from datetime import datetime

import pandas as pd
import streamlit as st


# ═════ 0. PAGE + CLEAR ═════
st.set_page_config(page_title="Classification Code", page_icon="🧩", layout="wide")


def clear_and_rerun():
    st.session_state.clear()
    st.rerun()


c_clear, c_title = st.columns([1, 9])
with c_clear:
    st.button("🗑️ CLEAR", type="primary", on_click=clear_and_rerun)
with c_title:
    st.title("🧩 Classification Code")


# ═════ 1. OUTILS ═════
def read_any(file):
    """Lecture CSV / Excel avec détection d’encodage ou moteur openpyxl."""
    name = file.name.lower()
    if name.endswith(".csv"):
        for enc in ("utf-8", "latin1", "cp1252"):
            try:
                return pd.read_csv(file, encoding=enc)
            except UnicodeDecodeError:
                file.seek(0)
        st.error(f"{file.name} : encodage CSV non reconnu.")
    elif name.endswith((".xlsx", ".xls")):
        try:
            return pd.read_excel(file, engine="openpyxl")
        except ImportError:
            st.error("openpyxl manquant (ajoutez-le au requirements).")
    else:
        st.error(f"{file.name} : format non pris en charge.")
    return None


def concat_files(files):
    """Concatène plusieurs fichiers lus via read_any()."""
    dfs = [df for f in files if (df := read_any(f)) is not None]
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
        on="RéférenceProduit", how="left", suffixes=("", "_maj"))
    mask = merged["Code_famille_Client"].isna() & merged["Code_famille_Client_maj"].notna()
    merged.loc[mask, "Code_famille_Client"] = merged.loc[mask, "Code_famille_Client_maj"]
    return merged.drop(columns=["Code_famille_Client_maj"])


def build_dfrx(df, entreprise):
    return pd.DataFrame({
        "Code famille Client": df["Code_famille_Client"],
        "onsenfou": None,
        "Entreprises": entreprise,
        "M2": "M2_" + df["RéférenceProduit"].astype(str),
    }).drop_duplicates()


# ═════ 2. ÉTAPE 1 ═════
st.header("Étape 1 : DFF & fichier à remplir")

c1, c2, c3 = st.columns(3)
with c1:
    files1 = st.file_uploader("Catalogue interne (M2 actuelle)",
                              type=("csv", "xlsx", "xls"), accept_multiple_files=True)
    if files1:
        r1 = st.number_input("Idx Réf.", 1, key="r1", value=1)
        v1 = st.number_input("Idx M2 actuelle", 1, key="v1", value=2)
with c2:
    files2 = st.file_uploader("Historique (M2 dernière)",
                              type=("csv", "xlsx", "xls"), accept_multiple_files=True)
    if files2:
        r2 = st.number_input("Idx Réf.", 1, key="r2", value=1)
        v2 = st.number_input("Idx M2 dernière", 1, key="v2", value=2)
with c3:
    files3 = st.file_uploader("Fichier client (Code famille)",
                              type=("csv", "xlsx", "xls"), accept_multiple_files=True)
    if files3:
        r3 = st.number_input("Idx Réf.", 1, key="r3", value=1)
        v3 = st.number_input("Idx Code famille", 1, key="v3", value=2)

entreprise = st.text_input("Entreprise (MAJUSCULES)").strip().upper()

# ---------- 2.1 : fusion initiale ----------
if st.button("Fusionner Étape 1"):
    if not (files1 and files2 and files3 and entreprise):
        st.warning("Chargez les trois groupes de fichiers + l’entreprise.")
        st.stop()

    raw1, raw2, raw3 = [concat_files(x) for x in (files1, files2, files3)]
    if any(df is None for df in (raw1, raw2, raw3)):
        st.stop()

    d1 = subset_current(raw1, r1, v1)
    d2 = subset_previous(raw2, r2, v2)
    d3 = subset_client(raw3,  r3, v3)

    dff, missing = fusion_etape1(d1, d2, d3, entreprise)

    dstr = datetime.today().strftime("%y%m%d")

    # persistance (PAS de fichier à remplir généré ici)
    st.session_state.update(
        dff_df=dff,
        missing_df=missing,
        dff_csv=dff.to_csv(index=False, sep=";").encode(),
        dstr=dstr,
        ent=entreprise,
        missing_file=None  # réinitialise
    )
    st.success("Fusion terminée ! Sélectionnez maintenant les colonnes du fichier à remplir puis cliquez sur « Générer ».")

# ---------- 2.2 : sélection des colonnes & génération fichier client ----------
if "missing_df" in st.session_state and not st.session_state.missing_df.empty:
    st.subheader("Colonnes à inclure dans le fichier à remplir")

    available_cols = [c for c in st.session_state.missing_df.columns
                      if c not in ("Code_famille_Client", "Entreprise")]

    default_cols = ["M2_annee_actuelle", "MACH2_FAM", "FAMI_LIBELLE",
                    "MACH2_SFAM", "SFAMI_LIBELLE", "MACH2_FONC", "FONC_LIBELLE"]

    selected_cols = st.multiselect(
        "Choisissez autant de colonnes que nécessaire",
        options=available_cols,
        default=[c for c in default_cols if c in available_cols],
        key="cols_select"
    )

    if st.button("Générer fichier à remplir", key="btn_generate_missing"):
        export = st.session_state.missing_df[
            ["RéférenceProduit"] + selected_cols
        ].drop_duplicates()

        buf = io.BytesIO()
        export.to_excel(buf, index=False)
        buf.seek(0)
        st.session_state["missing_file"] = buf
        st.success("Fichier à remplir mis à jour !")

# ---------- 2.3 : aperçus + téléchargements ----------
if "dff_df" in st.session_state:
    st.subheader("Aperçu DFF")
    st.dataframe(st.session_state.dff_df, use_container_width=True)

    st.download_button("Télécharger DFF (interne)",
                       st.session_state.dff_csv,
                       file_name=f"DFF_{st.session_state.ent}_{st.session_state.dstr}.csv",
                       mime="text/csv")

    if st.session_state.get("missing_file"):
        st.download_button("Télécharger fichier à remplir (Excel)",
                           st.session_state.missing_file,
                           file_name=f"CODES_CLIENT_{st.session_state.ent}_{st.session_state.dstr}.xlsx",
                           mime=("application/vnd.openxmlformats-"
                                 "officedocument.spreadsheetml.sheet"))
    elif "missing_df" in st.session_state and not st.session_state.missing_df.empty:
        st.info("Sélectionnez les colonnes ci-dessus puis cliquez sur « Générer fichier à remplir ».")
    else:
        st.info("Aucun code client manquant.")


# ═════ 3. ÉTAPE 2 ═════
st.header("Étape 2 : retour client → fichiers finaux")

c_dff, c_cli = st.columns(2)
with c_dff:
    dff_file = st.file_uploader("DFF initial (CSV)", type="csv")
with c_cli:
    maj_file = st.file_uploader("Fichier client complété", type=("csv", "xlsx", "xls"))

if st.button("Fusionner Étape 2"):
    if not (dff_file and maj_file):
        st.warning("Chargez les deux fichiers.")
        st.stop()

    # lecture
    try:
        dff_init = pd.read_csv(dff_file, sep=";")
    except Exception as e:
        st.error(f"Lecture DFF : {e}")
        st.stop()

    maj_df = read_any(maj_file)
    if maj_df is None:
        st.stop()
    if "Code_famille_Client" not in maj_df.columns:
        maj_df.columns = ["RéférenceProduit", "Code_famille_Client"][: len(maj_df.columns)]

    # mise à jour
    dff_final = appliquer_maj(dff_init, maj_df)
    encore_missing = dff_final[dff_final["Code_famille_Client"].isna()]

    # fichier DFRX
    ent_out = (dff_final["Entreprise"].dropna().unique() or [""])[0]
    dfrx_df = build_dfrx(dff_final[dff_final["Code_famille_Client"].notna()], ent_out)

    buf_tsv = io.StringIO()
    dfrx_df.to_csv(buf_tsv, sep="\t", index=False, header=False)
    dfrx_content = buf_tsv.getvalue()

    # accusé TXT
    dstr = datetime.today().strftime("%y%m%d")
    txt_name = f"AFRXHYBRCMR{dstr}0000.txt"
    txt_content = (f"DFRXHYBRCMR{dstr}000068230116IT"
                   f"DFRXHYBRCMR{dstr}RCMRHYBFRX                    OK000000")

    dfrx_name = f"DFRXHYBRCMR{dstr}0000"

    # affichage + downloads
    st.subheader("Aperçu DFRX (TSV)")
    st.dataframe(dfrx_df.head(50))

    st.download_button(f"Télécharger {dfrx_name}",
                       dfrx_content,
                       file_name=dfrx_name,
                       mime="text/plain")

    st.download_button(f"Télécharger {txt_name}",
                       txt_content,
                       file_name=txt_name,
                       mime="text/plain")

    if not encore_missing.empty:
        st.subheader("Références encore sans code client")
        st.dataframe(encore_missing, use_container_width=True)
        st.download_button("Télécharger références restantes",
                           encore_missing.to_csv(index=False, sep=";").encode(),
                           file_name=f"CODES_MANQUANTS_{dstr}.csv",
                           mime="text/csv")
    else:
        st.success("Tous les codes client sont renseignés !")
