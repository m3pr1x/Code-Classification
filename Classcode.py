# app.py
import io
from functools import reduce
from datetime import datetime

import pandas as pd
import streamlit as st

# ════════════════════════════════════════════
# 0. PAGE + CLEAR
# ════════════════════════════════════════════
st.set_page_config(page_title="Classification Code", page_icon="🧩", layout="wide")

# CLEAR  (vide la session et relance)
clr_col, title_col = st.columns([1, 9])
with clr_col:
    if st.button("🗑️ CLEAR", type="primary"):
        st.session_state.clear()
        st.experimental_rerun()
with title_col:
    st.title("🧩 Classification Code")

# ════════════════════════════════════════════
# 1. OUTILS GÉNÉRIQUES
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
            st.error("❌ Ajoutez `openpyxl` à requirements.txt.")
    else:
        st.error("❌ Format non pris en charge.")
    return None


def trim(df, ref_idx, val_idx, new_col):
    try:
        out = df.iloc[:, [ref_idx - 1, val_idx - 1]].copy()
        out.columns = ["RéférenceProduit", new_col]
        return out
    except IndexError:
        st.error("❌ Indice de colonne hors plage.")
        return None


def fusion_etape1(df1, df2, df3, ent):
    dfs = [df1, df2, df3]
    dff = reduce(lambda l, r: pd.merge(l, r, on="RéférenceProduit", how="outer"), dfs)
    dff["Entreprise"] = ent
    missing = dff[dff["Code_famille_Client"].isna()].copy()
    return dff, missing


def appliquer_mise_a_jour(dff, maj, col_ref="RéférenceProduit", col_client="Code_famille_Client"):
    merged = dff.merge(
        maj[[col_ref, col_client]],
        on=col_ref,
        how="left",
        suffixes=("", "_maj"),
    )
    mask = merged[col_client].isna() & merged[f"{col_client}_maj"].notna()
    merged.loc[mask, col_client] = merged.loc[mask, f"{col_client}_maj"]
    return merged.drop(columns=[f"{col_client}_maj"])


def save_to_session(dff, missing, ent):
    dstr = datetime.today().strftime("%y%m%d")
    st.session_state.update(
        {
            "dff_df": dff,
            "missing_df": missing,
            "dstr": dstr,
            "ent": ent,
            "dff_csv": dff.to_csv(index=False, sep=";").encode(),
            "missing_csv": missing.to_csv(index=False, sep=";").encode()
            if not missing.empty
            else None,
        }
    )
    if missing.empty:
        st.session_state["missing_excel"] = None
    else:
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
# 2. ÉTAPE 1  – CRÉATION DFF + FICHIER CLIENT
# ════════════════════════════════════════════
st.header("Étape 1 : construire le DFF et le fichier codes client manquants")

# Uploads
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
        st.warning("🛈 Chargez 3 fichiers + Entreprise.")
        st.stop()

    raw1, raw2, raw3 = [read_any(f) for f in (f1, f2, f3)]
    if any(d is None for d in (raw1, raw2, raw3)):
        st.stop()

    df1 = trim(raw1, r1, v1, "M2_annee_actuelle")
    df2 = trim(raw2, r2, v2, "M2_annee_derniere")
    df3 = trim(raw3, r3, v3, "Code_famille_Client")
    if any(d is None for d in (df1, df2, df3)):
        st.stop()

    dff, missing = fusion_etape1(df1, df2, df3, entreprise)
    save_to_session(dff, missing, entreprise)
    st.success("✅ Étape 1 terminée ! Les fichiers sont prêts ci-dessous.")

# Affichage + téléchargements STEP 1
if "dff_df" in st.session_state:
    st.subheader("Aperçu DFF")
    st.dataframe(st.session_state.dff_df, use_container_width=True)

    # Nom & libellé des boutons selon présence missing
    dstr = st.session_state.dstr
    ent = st.session_state.ent
    st.download_button(
        "📥 Télécharger DFF COMPLET" if st.session_state.missing_df.empty else "📥 Télécharger DFF (à conserver)",
        st.session_state.dff_csv,
        file_name=f"DFF_{ent}_{dstr}.csv",
        mime="text/csv",
    )

    if st.session_state.missing_excel:
        st.download_button(
            "📥 Télécharger fichier codes client **à remettre dans l’étape 2**",
            st.session_state.missing_excel,
            file_name=f"CODES_CLIENT_{ent}_{dstr}.xlsx",
            mime=("application/vnd.openxmlformats-officedocument"
                  ".spreadsheetml.sheet"),
        )
    elif not st.session_state.missing_df.empty:
        st.download_button(
            "📥 Télécharger fichier codes client **à remettre dans l’étape 2**",
            st.session_state.missing_csv,
            file_name=f"CODES_CLIENT_{ent}_{dstr}.csv",
            mime="text/csv",
        )
    else:
        st.info("👍 Aucun code client manquant – le DFF est complet.")

# ════════════════════════════════════════════
# 3. ÉTAPE 2  – INTÉGRER LE RETOUR CLIENT
# ════════════════════════════════════════════
st.header("Étape 2 : intégrer le fichier client complété")

st.markdown(
    """
Chargez **le DFF précédent** (CSV) **et** le fichier Excel/CSV que le client a
complété.

La fusion remplira les codes manquants et redonnera :

* un **DFF final**,
* éventuellement les références encore sans code (s’il en reste).
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
        dff_orig = pd.read_csv(dff_file, sep=";")
    except Exception as e:
        st.error(f"❌ Lecture DFF : {e}")
        st.stop()

    maj_df = read_any(maj_file)
    if maj_df is None:
        st.stop()

    # on tente de reconnaître la colonne code client dans le fichier complété
    if "Code_famille_Client" not in maj_df.columns:
        maj_df.columns = ["RéférenceProduit", "Code_famille_Client"][: len(maj_df.columns)]

    dff_final = appliquer_mise_a_jour(dff_orig, maj_df)
    still_missing = dff_final[dff_final["Code_famille_Client"].isna()]

    # Téléchargements résultat étape 2
    dstr2 = datetime.today().strftime("%y%m%d")
    st.subheader("DFF final")
    st.dataframe(dff_final, use_container_width=True)

    st.download_button(
        "📥 Télécharger DFF final",
        dff_final.to_csv(index=False, sep=";").encode(),
        file_name=f"DFF_FINAL_{dstr2}.csv",
        mime="text/csv",
    )

    if not still_missing.empty:
        st.subheader("Références encore sans code client")
        st.dataframe(still_missing, use_container_width=True)
        st.download_button(
            "📥 Télécharger références restantes",
            still_missing.to_csv(index=False, sep=";").encode(),
            file_name=f"CODES_MANQUANTS_{dstr2}.csv",
            mime="text/csv",
        )
    else:
        st.info("🎉 Tous les codes client sont maintenant renseignés !")
