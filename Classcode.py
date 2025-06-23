# app.py ─ version sans bouton “Ajouter”
import io
from functools import reduce
from datetime import datetime

import pandas as pd
import streamlit as st

# ═══ 0. PAGE + CLEAR ═══
st.set_page_config("Classification Code", "🧩", layout="wide")


def clear_and_rerun():
    st.session_state.clear()
    st.rerun()


st.button("🗑️ CLEAR", on_click=clear_and_rerun)
st.title("🧩 Classification Code")


# ═══ 1. OUTILS ═══
def read_any(file):
    """Lire CSV ou Excel avec tentative d’encodages."""
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
        on="RéférenceProduit", how="left", suffixes=("", "_maj"))
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


# ═══ 2. ÉTAPE 1 ═══
st.header("Étape 1 : DFF & fichier à remplir")

# --- initialisation des conteneurs ---
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
        new_files = st.file_uploader("Drag & drop (peut être répété)", accept_multiple_files=True,
                                     type=("csv", "xlsx", "xls"), key=f"u_{key}")
        # — ajout auto
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

        # — index sélecteurs —
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

    r1, v1 = st.session_state["cat_ref"], st.session_state["cat_val"]
    r2, v2 = st.session_state["hist_ref"], st.session_state["hist_val"]
    r3, v3 = st.session_state["cli_ref"], st.session_state["cli_val"]

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
    st.success("Fusion effectuée ! Choisissez les colonnes et générez le fichier Excel.")

# — sélection colonnes & génération fichier Excel —
if "missing_df" in st.session_state and not st.session_state.missing_df.empty:
    st.subheader("Colonnes à inclure dans le fichier à remplir")
    avail = [c for c in st.session_state.missing_df.columns
             if c not in ("Code_famille_Client", "Entreprise")]
    default = ["M2_annee_actuelle", "MACH2_FAM", "FAMI_LIBELLE",
               "MACH2_SFAM", "SFAMI_LIBELLE", "MACH2_FONC", "FONC_LIBELLE"]
    sel = st.multiselect("RéférenceProduit sera toujours là :",
                         avail, default=[c for c in default if c in avail])

    if st.button("Générer Excel à remplir"):
        export = st.session_state.missing_df[["RéférenceProduit"] + sel].drop_duplicates()
        export.insert(1, "Code_famille_Client", "")
        buf = io.BytesIO()
        export.to_excel(buf, index=False)
        buf.seek(0)
        st.session_state["missing_file"] = buf
        st.success("Fichier prêt !")

# — téléchargements —
if "dff_df" in st.session_state:
    st.subheader("Aperçu DFF")
    st.dataframe(st.session_state.dff_df, use_container_width=True)

    st.download_button("⬇️ DFF interne",
                       st.session_state.dff_csv,
                       file_name=f"DFF_{st.session_state.ent}_{st.session_state.dstr}.csv",
                       mime="text/csv")

    if st.session_state.get("missing_file"):
        st.download_button("⬇️ Fichier à remplir (Excel)",
                           st.session_state.missing_file,
                           file_name=f"CODES_CLIENT_{st.session_state.ent}_{st.session_state.dstr}.xlsx",
                           mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

# ═══ 3. ÉTAPE 2 ═══
st.header("Étape 2 : retour client → fichiers finaux")

dff_file = st.file_uploader("DFF initial (CSV)", type="csv")
maj_files = st.file_uploader("Fichier(s) client complété(s)",
                             type=("csv", "xlsx", "xls"), accept_multiple_files=True)

if st.button("Fusionner Étape 2"):
    if not (dff_file and maj_files):
        st.warning("Chargez le DFF et au moins un fichier client.")
        st.stop()

    try:
        dff_init = pd.read_csv(dff_file, sep=";")
    except Exception as e:
        st.error(f"Lecture DFF : {e}")
        st.stop()

    maj_dfs = []
    for f in maj_files:
        tmp = read_any(f)
        if tmp is None:
            continue
        if "Code_famille_Client" not in tmp.columns:
            tmp.columns = ["RéférenceProduit", "Code_famille_Client"][: len(tmp.columns)]
        maj_dfs.append(tmp[["RéférenceProduit", "Code_famille_Client"]])

    if not maj_dfs:
        st.error("Aucun fichier client valide.")
        st.stop()

    maj_df = pd.concat(maj_dfs, ignore_index=True).drop_duplicates("RéférenceProduit")
    dff_final = appliquer_maj(dff_init, maj_df)
    encore_missing = dff_final[dff_final["Code_famille_Client"].isna()]

    ent_out = (dff_final["Entreprise"].dropna().unique() or [""])[0]
    dfrx_df = build_dfrx(dff_final[dff_final["Code_famille_Client"].notna()], ent_out)

    buf_tsv = io.StringIO()
    dfrx_df.to_csv(buf_tsv, sep="\t", index=False, header=False)
    dfrx_content = buf_tsv.getvalue()

    dstr = datetime.today().strftime("%y%m%d")
    txt_name = f"AFRXHYBRCMR{dstr}0000.txt"
    txt_content = (f"DFRXHYBRCMR{dstr}000068230116IT"
                   f"DFRXHYBRCMR{dstr}RCMRHYBFRX                    OK000000")
    dfrx_name = f"DFRXHYBRCMR{dstr}0000"

    st.subheader("Aperçu DFRX")
    st.dataframe(dfrx_df.head(50))

    st.download_button("⬇️ DFRX", dfrx_content, file_name=dfrx_name, mime="text/plain")
    st.download_button("⬇️ Accusé TXT", txt_content, file_name=txt_name, mime="text/plain")

    if not encore_missing.empty:
        st.subheader("Références sans code client")
        st.dataframe(encore_missing, use_container_width=True)
        st.download_button("⬇️ Références restantes",
                           encore_missing.to_csv(index=False, sep=";").encode(),
                           file_name=f"CODES_MANQUANTS_{dstr}.csv",
                           mime="text/csv")
    else:
        st.success("✅ Tous les codes client sont renseignés.")
