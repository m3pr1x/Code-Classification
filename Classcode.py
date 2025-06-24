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
    return (pd.concat(lst, ignore_index=True).drop_duplicates().reset_index(drop=True)
            if lst else pd.DataFrame())

def rename_keep(df, i_ref, i_val, new_val):
    m = {df.columns[i_ref-1]: "RéférenceProduit", df.columns[i_val-1]: new_val}
    return df.rename(columns=m)[list(m.values())]

def build_dfrx(df, ent):
    return pd.DataFrame({"Code famille Client": df["Code_famille_Client"],
                         "onsenfou": None,
                         "Entreprises": ent,
                         "M2": "M2_" + df["RéférenceProduit"].astype(str)}).drop_duplicates()

lots = {"cat": ("Catalogue interne", "idx Réf.", "idx M2 actuelle"),
        "hist": ("Historique",       "idx Réf.", "idx M2 dernière"),
        "cli": ("Fichier client",    "idx Réf.", "idx Code famille")}

for k in lots:
    st.session_state.setdefault(f"{k}_dfs", [])
    st.session_state.setdefault(f"{k}_names", [])

cols = st.columns(3)
for (k, (lab, rlab, vlab)), c in zip(lots.items(), cols):
    with c:
        st.markdown(f"##### {lab}")
        files = st.file_uploader("Drag & drop", accept_multiple_files=True,
                                 type=("csv", "xlsx", "xls"), key=f"up_{k}")
        if files:
            for f in files:
                if f.name not in st.session_state[f"{k}_names"]:
                    d = read_any(f)
                    if d is not None:
                        st.session_state[f"{k}_dfs"].append(d)
                        st.session_state[f"{k}_names"].append(f.name)
            st.success(f"{len(files)} ajouté")
        st.number_input(rlab, 1, 50, 1, key=f"{k}_ref", label_visibility="collapsed")
        st.number_input(vlab, 1, 50, 2, key=f"{k}_val", label_visibility="collapsed")
        st.caption(f"{len(st.session_state[f'{k}_dfs'])} fichier(s)")

entreprise = st.text_input("Entreprise (MAJUSCULES)").strip().upper()

if st.button("Fusionner Étape 1"):
    if not all(st.session_state[f"{k}_dfs"] for k in lots) or not entreprise:
        st.stop()
    raw_cat  = concat_unique(st.session_state["cat_dfs"])
    raw_hist = concat_unique(st.session_state["hist_dfs"])
    raw_cli  = concat_unique(st.session_state["cli_dfs"])
    d1 = rename_keep(raw_cat,  st.session_state["cat_ref"],  st.session_state["cat_val"],  "M2_annee_actuelle")
    d2 = rename_keep(raw_hist, st.session_state["hist_ref"], st.session_state["hist_val"], "M2_annee_derniere")
    d3 = rename_keep(raw_cli,  st.session_state["cli_ref"],  st.session_state["cli_val"],  "Code_famille_Client")
    dff = reduce(lambda l, r: l.merge(r, on="RéférenceProduit", how="outer"), (d1, d2, d3))
    dff["Entreprise"] = entreprise
    missing = dff[dff["Code_famille_Client"].isna()]
    dstr = datetime.today().strftime("%y%m%d")
    st.session_state.update(dff_df=dff,
                            missing_df=missing,
                            dff_csv=dff.to_csv(index=False, sep=";").encode(),
                            dstr=dstr,
                            ent=entreprise,
                            missing_file=None)
    st.success("Fusion OK")

if (mis := st.session_state.get("missing_df")) is not None and not mis.empty:
    avail = [c for c in mis.columns if c not in ("Code_famille_Client", "Entreprise")]
    sel = st.multiselect("Colonnes pour le client", avail,
                         default=[c for c in ("M2_annee_actuelle","MACH2_FAM","FAMI_LIBELLE",
                                              "MACH2_SFAM","SFAMI_LIBELLE","MACH2_FONC","FONC_LIBELLE") if c in avail])
    if st.button("Excel client"):
        out = mis[["RéférenceProduit"]+sel].drop_duplicates()
        out.insert(1,"Code_famille_Client","")
        b = io.BytesIO(); out.to_excel(b,index=False); b.seek(0)
        st.session_state["missing_file"] = b
        st.success("Fichier prêt")

if (dff := st.session_state.get("dff_df")) is not None:
    st.dataframe(dff.head(100), use_container_width=True)
    st.download_button("⬇️ DFF", st.session_state["dff_csv"],
                       file_name=f"DFF_{st.session_state.ent}_{st.session_state.dstr}.csv",
                       mime="text/csv")
    if st.session_state.get("missing_file"):
        st.download_button("⬇️ Excel client",
                           st.session_state["missing_file"],
                           file_name=f"CODES_CLIENT_{st.session_state.ent}_{st.session_state.dstr}.xlsx",
                           mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

st.header("Étape 2 : retour client")

dff_file = st.file_uploader("DFF initial", type="csv")
maj_files = st.file_uploader("Retour client", accept_multiple_files=True,
                             type=("csv","xlsx","xls"))

if st.button("Fusionner Étape 2"):
    if not (dff_file and maj_files): st.stop()
    dff_init = pd.read_csv(dff_file, sep=";")
    lst = [read_any(f) for f in maj_files if read_any(f) is not None]
    if not lst: st.stop()
    maj = pd.concat(lst).iloc[:, :2]
    if maj.columns[1] != "Code_famille_Client":
        maj.columns = ["RéférenceProduit", "Code_famille_Client"]
    maj = maj.drop_duplicates()
    dff_fin = dff_init.merge(maj, on="RéférenceProduit", how="left", suffixes=("","_m"))
    dff_fin["Code_famille_Client"] = dff_fin["Code_famille_Client"].fillna(dff_fin["Code_famille_Client_m"])
    dff_fin = dff_fin.drop(columns=["Code_famille_Client_m"])
    ent_out = dff_fin["Entreprise"].dropna().unique()[0] if "Entreprise" in dff_fin.columns else ""
    dfrx = build_dfrx(dff_fin[dff_fin["Code_famille_Client"].notna()], ent_out)
    dstr = datetime.today().strftime("%y%m%d")
    st.dataframe(dfrx.head())
    st.download_button("⬇️ DFRX", dfrx.to_csv(sep="\t", index=False, header=False),
                       file_name=f"DFRXHYBRCMR{dstr}0000", mime="text/plain")
    ack = (f"DFRXHYBRCMR{dstr}000068230116IT"
           f"DFRXHYBRCMR{dstr}RCMRHYBFRX                    OK000000")
    st.download_button("⬇️ TXT", ack,
                       file_name=f"AFRXHYBRCMR{dstr}0000.txt", mime="text/plain")
    miss = dff_fin[dff_fin["Code_famille_Client"].isna()]
    if not miss.empty:
        st.dataframe(miss)
        st.download_button("⬇️ Missing",
                           miss.to_csv(index=False, sep=";").encode(),
                           file_name=f"CODES_MANQUANTS_{dstr}.csv",
                           mime="text/csv")
    else:
        st.success("Tous les codes client sont renseignés.")
