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
def read_csv_smart(buf: io.BytesIO) -> pd.DataFrame:
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
        return read_csv_smart(u)
    if n.endswith((".xlsx", ".xls")):
        return pd.read_excel(u, engine="openpyxl")
    return None

@st.cache_data(show_spinner=False)
def concat_unique(lst):
    return pd.concat(lst, ignore_index=True).drop_duplicates().reset_index(drop=True) if lst else pd.DataFrame()

def add_cols(df: pd.DataFrame, ref_idx: int, m2_idx: int, label: str) -> pd.DataFrame:
    ref_col = df.columns[ref_idx - 1]
    m2_col  = df.columns[m2_idx - 1]
    out = df.copy()
    if "RéférenceProduit" not in out.columns:
        out["RéférenceProduit"] = out[ref_col]
    if label not in out.columns:
        out[label] = out[m2_col]
    return out

def safe_merge(l: pd.DataFrame, r: pd.DataFrame) -> pd.DataFrame:
    dup = {c: f"{c}_nouveau" for c in r.columns if c in l.columns and c != "RéférenceProduit"}
    return l.merge(r.rename(columns=dup), on="RéférenceProduit", how="outer")

def build_dfrx(df: pd.DataFrame, ent: str) -> pd.DataFrame:
    return pd.DataFrame({"M2": df["M2_nouveau"],"Entreprise": ent,"Code_famille_Client": df["Code_famille_Client"]}).drop_duplicates()

lots = {"cat": ("Catalogue interne", "idx Réf. produit", "idx M2 actuelle"),
        "hist": ("Historique",       "idx Réf. produit", "idx M2 dernière"),
        "cli":  ("Fichier client",   "idx M2", "idx Code famille")}
for k in lots:
    st.session_state.setdefault(f"{k}_dfs", [])
    st.session_state.setdefault(f"{k}_names", [])

cols = st.columns(3)
for (k, (label, lab1, lab2)), c in zip(lots.items(), cols):
    with c:
        st.markdown(f"##### {label}")
        up = st.file_uploader("Drag & drop", accept_multiple_files=True, type=("csv","xlsx","xls"), key=f"up_{k}")
        if up:
            for f in up:
                if f.name not in st.session_state[f"{k}_names"]:
                    d = read_any(f)
                    if d is not None:
                        st.session_state[f"{k}_dfs"].append(d)
                        st.session_state[f"{k}_names"].append(f.name)
            st.success(f"{len(up)} ajouté(s)")
        st.number_input(lab1, 1, 50, 1, key=f"{k}_ref", label_visibility="collapsed")
        st.number_input(lab2, 1, 50, 2, key=f"{k}_val", label_visibility="collapsed")
        st.caption(f"{len(st.session_state[f'{k}_dfs'])} fichier(s)")

entreprise = st.text_input("Entreprise (MAJUSCULES)").strip().upper()

if st.button("Fusionner Étape 1"):
    if not all(st.session_state[f"{k}_dfs"] for k in ("cat","hist","cli")) or not entreprise:
        st.warning("Charger 3 lots + Entreprise"); st.stop()

    cat  = concat_unique(st.session_state["cat_dfs"])
    hist = concat_unique(st.session_state["hist_dfs"])
    cli  = concat_unique(st.session_state["cli_dfs"])

    cat  = add_cols(cat,  st.session_state["cat_ref"],  st.session_state["cat_val"],  "M2_nouveau")
    hist = add_cols(hist, st.session_state["hist_ref"], st.session_state["hist_val"], "M2_ancien")

    cli_m2 = cli.copy()
    cli_m2["M2"] = cli_m2.iloc[:, st.session_state["cli_ref"] - 1]
    cli_m2["Code_famille_Client"] = cli_m2.iloc[:, st.session_state["cli_val"] - 1]
    cli_m2 = cli_m2[["M2", "Code_famille_Client"]]

    merged = safe_merge(cat, hist[["RéférenceProduit","M2_ancien"]])
    merged = merged.merge(cli_m2, left_on="M2_ancien", right_on="M2", how="left")
    merged.drop(columns=["M2"], inplace=True)

    pre_assigned = merged["Code_famille_Client"].notna().sum()

    freq = (merged.dropna(subset=["Code_famille_Client"]).groupby("M2_nouveau")
            ["Code_famille_Client"].agg(lambda s: s.value_counts().idxmax()))
    merged["Code_famille_Client"] = merged.apply(
        lambda r: freq.get(r["M2_nouveau"], pd.NA) if pd.isna(r["Code_famille_Client"]) else r["Code_famille_Client"], axis=1)

    after_assigned = merged["Code_famille_Client"].notna().sum()
    completed = after_assigned - pre_assigned

    maj_applied = merged.groupby("M2_nouveau")["Code_famille_Client"].first()
    maj_list = [f"{m2} -> {code}" for m2, code in freq.items() if m2 in maj_applied.index]

    missing_final = merged[merged["Code_famille_Client"].isna()]["M2_nouveau"].unique()

    summary = [
        f"M2 avec code initial : {pre_assigned}",
        f"M2 complétés par majorité : {completed}",
        "\nListe des M2 complétés :",
        *maj_list,
        "\nM2 restants sans code :",
        *missing_final.astype(str)
    ]
    summary_txt = "\n".join(summary)

    final_df = build_dfrx(merged.drop_duplicates("M2_nouveau"), entreprise)
    dstr = datetime.today().strftime("%y%m%d")
    st.dataframe(final_df.head())
    st.download_button("⬇️ Résultat CSV", final_df.to_csv(index=False, sep=";"),
                       file_name=f"CODES_FINAUX_{dstr}.csv", mime="text/csv")
    st.download_button("⬇️ Suivi TXT", summary_txt, file_name=f"SUIVI_{dstr}.txt", mime="text/plain")
