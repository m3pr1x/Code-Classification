from __future__ import annotations
import csv, io
from datetime import datetime
from functools import reduce
import pandas as pd
import streamlit as st

st.set_page_config("Classification Code", "🧩", layout="wide")

def clear_and_rerun():
    st.session_state.clear()
    st.rerun()

st.button("🗑️ CLEAR", on_click=clear_and_rerun)
st.title("🧩 Classification Code")

@st.cache_data(show_spinner=False)
def read_csv(buf: io.BytesIO) -> pd.DataFrame:
    for enc in ("utf-8", "latin1", "cp1252"):
        buf.seek(0)
        try:
            sample = buf.read(2048).decode(enc, errors="ignore")
            sep = csv.Sniffer().sniff(sample, delimiters=";,|\t").delimiter
            buf.seek(0)
            return pd.read_csv(buf, sep=sep, encoding=enc, engine="python", on_bad_lines="skip")
        except (UnicodeDecodeError, csv.Error, pd.errors.ParserError):
            continue
    raise ValueError

@st.cache_data(show_spinner=False)
def read_any(u):
    n = u.name.lower()
    if n.endswith(".csv"):
        return read_csv(u)
    if n.endswith((".xlsx", ".xls")):
        return pd.read_excel(u, engine="openpyxl")
    return None

@st.cache_data(show_spinner=False)
def concat_unique(lst):
    return pd.concat(lst, ignore_index=True).drop_duplicates().reset_index(drop=True) if lst else pd.DataFrame()

def to_m2_series(s: pd.Series) -> pd.Series:
    return s.astype(str).str.zfill(6)

def add_cols(df: pd.DataFrame, ref_idx: int, m2_idx: int, label: str) -> pd.DataFrame:
    out = df.copy()
    out["RéférenceProduit"] = out.iloc[:, ref_idx - 1].astype(str)
    out[label] = to_m2_series(out.iloc[:, m2_idx - 1])
    return out

def safe_merge(l: pd.DataFrame, r: pd.DataFrame) -> pd.DataFrame:
    dup = {c: f"{c}_nouveau" for c in r.columns if c in l.columns and c != "RéférenceProduit"}
    return l.merge(r.rename(columns=dup), on="RéférenceProduit", how="outer")

def build_dfrx(df: pd.DataFrame, ent: str) -> pd.DataFrame:
    return pd.DataFrame({"M2": df["M2_nouveau"], "Entreprise": ent, "Code_famille_Client": df["Code_famille_Client"]}).drop_duplicates()

lots = {"cat": ("Catalogue interne", "idx Réf. produit", "idx M2 actuelle"),
        "hist": ("Historique",       "idx Réf. produit", "idx M2 dernière"),
        "cli":  ("Fichier client",   "idx M2",           "idx Code famille")}

for k in lots:
    st.session_state.setdefault(f"{k}_dfs", [])
    st.session_state.setdefault(f"{k}_names", [])

cols = st.columns(3)
for (k, (lab, lab1, lab2)), c in zip(lots.items(), cols):
    with c:
        st.markdown(f"##### {lab}")
        up = st.file_uploader("Drag & drop", accept_multiple_files=True,
                              type=("csv", "xlsx", "xls"), key=f"up_{k}")
        if up:
            for f in up:
                if f.name not in st.session_state[f"{k}_names"]:
                    df_read = read_any(f)
                    if df_read is not None:
                        st.session_state[f"{k}_dfs"].append(df_read)
                        st.session_state[f"{k}_names"].append(f.name)
            st.success(f"{len(up)} ajouté(s)")
        st.number_input(lab1, 1, 50, 1, key=f"{k}_ref", label_visibility="collapsed")
        st.number_input(lab2, 1, 50, 2, key=f"{k}_val", label_visibility="collapsed")
        st.caption(f"{len(st.session_state[f'{k}_dfs'])} fichier(s)")

entreprise = st.text_input("Entreprise (MAJUSCULES)").strip().upper()

def idx_ok(df: pd.DataFrame, idx: int) -> bool:
    return 1 <= idx <= df.shape[1]

if st.button("Fusionner Étape 1"):
    if not all(st.session_state[f"{k}_dfs"] for k in lots) or not entreprise:
        st.warning("Charger les trois lots et renseigner l’entreprise"); st.stop()

    cat_raw  = concat_unique(st.session_state["cat_dfs"])
    hist_raw = concat_unique(st.session_state["hist_dfs"])
    cli_raw  = concat_unique(st.session_state["cli_dfs"])

    for df_raw, key in ((cat_raw, "cat"), (hist_raw, "hist"), (cli_raw, "cli")):
        if not idx_ok(df_raw, st.session_state[f"{key}_ref"]) or not idx_ok(df_raw, st.session_state[f"{key}_val"]):
            st.error(f"Index hors limite pour le lot {key.upper()}"); st.stop()

    cat  = add_cols(cat_raw,  st.session_state["cat_ref"],  st.session_state["cat_val"],  "M2_nouveau")
    hist = add_cols(hist_raw, st.session_state["hist_ref"], st.session_state["hist_val"], "M2_ancien")

    cli_m2 = cli_raw.copy()
    cli_m2["M2"] = to_m2_series(cli_m2.iloc[:, st.session_state["cli_ref"] - 1])
    cli_m2["Code_famille_Client"] = cli_m2.iloc[:, st.session_state["cli_val"] - 1].astype(str)
    cli_m2 = cli_m2[["M2", "Code_famille_Client"]]

    # ---------- MERGE n°1 (catalogue + historique) ----------
    merged = safe_merge(cat, hist[["RéférenceProduit", "M2_ancien"]])

    # ---------- MERGE n°2 (historique + fichier client) ----------
    merged = merged.merge(
        cli_m2,
        left_on="M2_ancien",
        right_on="M2",
        how="left",
        suffixes=("_cat", "")   # <‑ on gère le doublon "M2"
    )

    # On retire la colonne M2 du catalogue (devenue "M2_cat")
    if "M2_cat" in merged.columns:
        merged.drop(columns=["M2_cat"], inplace=True)

    pre_assigned = merged["Code_famille_Client"].notna().sum()

    freq = (merged.dropna(subset=["Code_famille_Client"])
            .groupby("M2_nouveau")["Code_famille_Client"]
            .agg(lambda s: s.value_counts().idxmax()))
    merged["Code_famille_Client"] = merged.apply(
        lambda r: freq.get(r["M2_nouveau"], pd.NA) if pd.isna(r["Code_famille_Client"]) else r["Code_famille_Client"],
        axis=1)
    completed = merged["Code_famille_Client"].notna().sum() - pre_assigned

    maj_list = [f"{m2} -> {code}" for m2, code in freq.items()]
    missing_final = merged[merged["Code_famille_Client"].isna()]["M2_nouveau"].unique()

    summary_txt = "\n".join([
        f"M2 avec code initial : {pre_assigned}",
        f"M2 complétés par majorité : {completed}",
        "\nListe des M2 complétés :",
        *maj_list,
        "\nM2 restants sans code :",
        *missing_final.astype(str)
    ])

    final_df = build_dfrx(merged.drop_duplicates("M2_nouveau"), entreprise)
    dstr = datetime.today().strftime("%y%m%d")

    st.dataframe(final_df.head())
    st.download_button("⬇️ Résultat CSV", final_df.to_csv(index=False, sep=";"),
                       file_name=f"CODES_FINAUX_{dstr}.csv", mime="text/csv")
    st.download_button("⬇️ Suivi TXT", summary_txt,
                       file_name=f"SUIVI_{dstr}.txt", mime="text/plain")
