# app.py
import io
from functools import reduce
from datetime import datetime

import pandas as pd
import streamlit as st

# ════════════════════════════════════════════
# 0. PAGE + CLEAR (callback)
# ════════════════════════════════════════════
st.set_page_config(page_title="Classification Code", page_icon="🧩", layout="wide")


def clear_and_rerun():
    st.session_state.clear()
    st.rerun()


clear_col, title_col = st.columns([1, 9])
with clear_col:
    st.button("🗑️ CLEAR", type="primary", on_click=clear_and_rerun)
with title_col:
    st.title("🧩 Classification Code")

# ════════════════════════════════════════════
# 1. OUTILS
# ════════════════════════════════════════════
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
            st.error("❌ openpyxl manquant (ajoutez au requirements).")
    else:
        st.error("❌ Format non pris en charge.")
    return None


def trim(df, ref_idx, val_idx, new_col):
    try:
        out = df.iloc[:, [ref_idx - 1, val_idx - 1]].copy()
    except IndexError:
        st.error("❌ Indice de colonne hors plage.")
        return None
    out.columns = ["RéférenceProduit", new_col]
    return out


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


def save_to_session(dff, missing, ent):
    dstr = datetime.today().strftime("%y%m%d")
    st.session_state.update(
        dff_df=dff,
        missing_df=missing,
        dff_csv=dff.to_csv(index=False, sep=";").encode(),
        missing_csv=missing.to_csv(index=False, sep=";").encode() if not missing.empty else None,
        missing_excel=None,
        dstr=dstr,
        ent=ent,
    )
    if not missing.empty:
        buf = io.BytesIO()
        (
            missing["M2_annee_actuelle"]
            .dropna()
            .drop_duplicates()
            .to_excel(buf, index=False, header=False)
        )
        buf.seek(0)
        st.session_state["missing_excel"] = buf


# ════════════════════════════════════════════
# 2. ÉTAPE 1 : DFF + fichier codes client
# ════════════════════════════════════════════
st.header("Étape 1 : construire le DFF et le fichier des codes client manquants")

c1, c2, c3 = st.columns(3)
with c1:
    f1 = st.file_uploader("📄 Catalogue interne (M2 actuelle)", type=("csv", "xlsx", "xls"))
    if f1:
        r1 = st.number_input("Réf. produit", 1, key="r1", value=1)
        v1 = st.number_input("Col. M2 actuelle", 1, key="v1", value=2)
with c2:
    f2 = st.file_uploader("📄 Historique (M2 dernière)", type=("csv", "xlsx", "xls"))
    if f2:
        r2 = st.number_input("Réf. produit", 1, key="r2", value=1)
        v2 = st.number_input("Col. M2 dernière", 1, key="v2", value=2)
with c3:
    f3 = st.file_uploader("📄 Fichier client (Code famille)", type=("csv", "xlsx", "xls"))
    if f3:
        r3 = st.number_input("Réf. produit", 1, key="r3", value=1)
        v3 = st.number_input("Col. Code famille", 1, key="v3", value=2)

entreprise = st.text_input("🏢 Entreprise (MAJUSCULES)").strip().upper()

if st.button("🚀 Fusionner (Étape 1)"):
    if not (f1 and f2 and f3 and entreprise):
        st.warning("🛈 Chargez les 3 fichiers + entreprise.")
        st.stop()

    raws = [read_any(f) for f in (f1, f2, f3)]
    if any(df is None for df in raws):
        st.stop()

    df1 = trim(raws[0], r1, v1, "M2_annee_actuelle")
    df2 = trim(raws[1], r2, v2, "M2_annee_derniere")
    df3 = trim(raws[2], r3, v3, "Code_famille_Client")
    if any(df is None for df in (df1, df2, df3)):
        st.stop()

    dff, missing = fusion_etape1(df1, df2, df3, entreprise)
    save_to_session(dff, missing, entreprise)
    st.success("✅ Étape 1 terminée !")

if "dff_df" in st.session_state:
    st.subheader("Aperçu DFF")
    st.dataframe(st.session_state.dff_df, use_container_width=True)

    dstr = st.session_state.dstr
    ent = st.session_state.ent
    label_dff = "📥 Télécharger DFF COMPLET" if st.session_state.missing_df.empty else "📥 Télécharger DFF (à conserver)"
    st.download_button(label_dff, st.session_state.dff_csv, file_name=f"DFF_{ent}_{dstr}.csv", mime="text/csv")

    if not st.session_state.missing_df.empty:
        data_missing = (
            st.session_state.missing_excel
            if st.session_state.missing_excel
            else st.session_state.missing_csv
        )
        st.download_button(
            "📥 Télécharger fichier codes client **à remettre dans l’étape 2**",
            data_missing,
            file_name=f"CODES_CLIENT_{ent}_{dstr}.{'xlsx' if st.session_state.missing_excel else 'csv'}",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            if st.session_state.missing_excel
            else "text/csv",
        )
    else:
        st.info("👍 Aucun code client manquant – le DFF est complet.")

# ════════════════════════════════════════════
# 3. ÉTAPE 2 : DFF final + TXT accusé
# ════════════════════════════════════════════
st.header("Étape 2 : intégrer le fichier client complété")

st.markdown(
    """
Chargez le **DFF** généré à l’étape 1 et le fichier que le client a complété.  
La fusion produit&nbsp;:
* un **DFF final**,
* un fichier TXT « AFRXHYBRCMRYYMMDD0000.txt » d’accusé,
* éventuellement les références encore sans code.
"""
)

col_dff, col_cli = st.columns(2)
with col_dff:
    dff_file = st.file_uploader("📄 DFF initial (CSV)", type="csv")
with col_cli:
    maj_file = st.file_uploader("📄 Fichier client complété", type=("csv", "xlsx", "xls"))

if st.button("🔄 Fusionner (Étape 2)"):
    if not (dff_file and maj_file):
        st.warning("🛈 Chargez les deux fichiers.")
        st.stop()

    try:
        dff_initial = pd.read_csv(dff_file, sep=";")
    except Exception as e:
        st.error(f"❌ Lecture DFF : {e}")
        st.stop()

    maj_df = read_any(maj_file)
    if maj_df is None:
        st.stop()

    if "Code_famille_Client" not in maj_df.columns:
        maj_df.columns = ["RéférenceProduit", "Code_famille_Client"][: len(maj_df.columns)]

    dff_final = appliquer_mise_a_jour(dff_initial, maj_df)
    encore_missing = dff_final[dff_final["Code_famille_Client"].isna()]

    # ── Fichier TXT (accusé)
    date_txt = datetime.today().strftime("%y%m%d")
    txt_name = f"AFRXHYBRCMR{date_txt}0000.txt"
    txt_content = f"DFRXHYBRCMR{date_txt}000068230116ITDFRXHYBRCMR{date_txt}RCMRHYBFRX                    OK000000"

    # ── Affichage & téléchargements
    st.subheader("DFF final")
    st.dataframe(dff_final, use_container_width=True)

    st.download_button(
        "📥 Télécharger DFF final",
        dff_final.to_csv(index=False, sep=";").encode(),
        file_name=f"DFF_FINAL_{date_txt}.csv",
        mime="text/csv",
    )

    st.download_button(
        f"📥 Télécharger {txt_name}",
        txt_content,
        file_name=txt_name,
        mime="text/plain",
    )

    if not encore_missing.empty:
        st.subheader("Références encore sans code client")
        st.dataframe(encore_missing, use_container_width=True)
        st.download_button(
            "📥 Télécharger références restantes",
            encore_missing.to_csv(index=False, sep=";").encode(),
            file_name=f"CODES_MANQUANTS_{date_txt}.csv",
            mime="text/csv",
        )
    else:
        st.success("🎉 Tous les codes client sont désormais renseignés !")
