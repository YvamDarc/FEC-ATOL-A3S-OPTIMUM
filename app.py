import re
from datetime import datetime
from io import BytesIO

import numpy as np
import pandas as pd
import streamlit as st


# ============================
# FEC columns (format "classique")
# ============================
FEC_COLUMNS = [
    "JournalCode", "JournalLib",
    "EcritureNum", "EcritureDate",
    "CompteNum", "CompteLib",
    "CompAuxNum", "CompAuxLib",
    "PieceRef", "PieceDate",
    "EcritureLib",
    "Debit", "Credit",
    "EcritureLet", "DateLet",
    "ValidDate",
    "Montantdevise", "Idevise"
]


# ============================
# Helpers
# ============================
def parse_eur(val) -> float:
    """Parse '1 420,00€', '13,00€', 13, 13.0 -> float."""
    if val is None:
        return 0.0
    if isinstance(val, (int, float, np.integer, np.floating)):
        if pd.isna(val):
            return 0.0
        return float(val)

    s = str(val).strip()
    if s == "" or s.lower() == "nan":
        return 0.0

    s = s.replace("€", "").replace("\u00a0", " ").strip()
    s = s.replace(" ", "")
    if "," in s and "." in s:
        s = s.replace(".", "")
    s = s.replace(",", ".")
    s = re.sub(r"[^0-9\.\-]", "", s)

    if s in ("", ".", "-", "-."):
        return 0.0
    try:
        return float(s)
    except ValueError:
        return 0.0


def normalize_text(x) -> str:
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return ""
    s = str(x).strip().lower().replace("\u00a0", " ")
    s = re.sub(r"\s+", " ", s)
    # mini normalisation accents pour matching "encaissés", etc.
    s = (s.replace("é", "e").replace("è", "e").replace("ê", "e").replace("ë", "e")
           .replace("à", "a").replace("â", "a")
           .replace("î", "i").replace("ï", "i")
           .replace("ô", "o")
           .replace("ù", "u").replace("û", "u").replace("ü", "u"))
    return s


def list_sheets(file_bytes: bytes) -> list[str]:
    bio = BytesIO(file_bytes)
    xls = pd.ExcelFile(bio, engine="openpyxl")
    return xls.sheet_names


def read_sheet_raw(file_bytes: bytes, sheet_name_or_index=0) -> pd.DataFrame:
    bio = BytesIO(file_bytes)
    return pd.read_excel(bio, sheet_name=sheet_name_or_index, header=None, engine="openpyxl")


def _norm_cell(s: str) -> str:
    return normalize_text(s)


def find_header_row(raw: pd.DataFrame, start_row: int, end_row: int, required_labels: list[str]) -> tuple[int | None, dict]:
    req = [_norm_cell(x) for x in required_labels]
    for r in range(start_row, min(end_row, len(raw))):
        row_vals = [_norm_cell(str(x) if str(x).lower() != "nan" else "") for x in raw.iloc[r].tolist()]
        col_map = {}
        for label in req:
            found = None
            for c, cell in enumerate(row_vals):
                if label and label in cell:
                    found = c
                    break
            if found is None:
                col_map = {}
                break
            col_map[label] = found
        if col_map:
            return r, col_map
    return None, {}


def parse_date_any(x):
    """Tente de parser une date provenant d'Excel (datetime, string, etc.) en date()."""
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return None
    if isinstance(x, (datetime, pd.Timestamp)):
        return x.date()
    s = str(x).strip()
    if not s or s.lower() == "nan":
        return None
    try:
        return pd.to_datetime(s, dayfirst=True, errors="raise").date()
    except Exception:
        return None


def to_fec_txt_bytes(df: pd.DataFrame) -> bytes:
    """Export FEC en .txt séparateur TAB, décimales point, UTF-8 BOM."""
    if df is None or df.empty:
        return "".encode("utf-8-sig")

    out = df.copy()

    for col in ["Debit", "Credit"]:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce").fillna(0.0).map(lambda x: f"{x:.2f}")

    for c in out.columns:
        out[c] = out[c].astype(str).replace({"nan": "", "None": ""})

    txt = out.to_csv(index=False, sep="\t", encoding="utf-8-sig", lineterminator="\n")
    return txt.encode("utf-8-sig")


def check_balance(fec: pd.DataFrame) -> pd.DataFrame:
    if fec is None or fec.empty:
        return pd.DataFrame()
    chk = fec.groupby(["JournalCode", "EcritureNum"])[["Debit", "Credit"]].sum()
    chk["Delta"] = (chk["Debit"] - chk["Credit"]).round(2)
    return chk


# ============================
# Extraction TIERS PAYANT encaissé (robuste multi-blocs)
# ============================
TP_ENCAISSES_TITLE = "tiers-payant encaisses"
TP_STOP_TITLES = [
    "tiers-payants annules",   # "Tiers-payants Annulés (Par un avoir)"
    "tiers-payants en attente" # "Tiers-payants en attente ..."
]

TP_HEADER = [
    "date pointage",
    "date encaissement",
    "mode de reglement",
    "montant",
    "n° facture"
]


def find_row_contains(raw: pd.DataFrame, needle_norm: str) -> int | None:
    for i in range(len(raw)):
        joined = " ".join([normalize_text(x) for x in raw.iloc[i].tolist() if str(x).lower() != "nan"])
        if needle_norm in joined:
            return i
    return None


def extract_tiers_payant_encaisse(raw: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    """
    Extrait les lignes des tableaux sous la section "Tiers-payant encaissés".
    Gère plusieurs blocs d'en-têtes répétées + saute les lignes 'Total ...'.
    """
    debug = {
        "section_start_row": None,
        "section_end_row": None,
        "header_rows_found": [],
        "rows_kept": 0
    }

    start = find_row_contains(raw, TP_ENCAISSES_TITLE)
    if start is None:
        # fallback: si le titre est absent, on tente quand même un scan global sur les headers
        start = 0
    debug["section_start_row"] = start

    # Définir une fin de section (avant Annulés / En attente)
    end = len(raw)
    for stop in TP_STOP_TITLES:
        r_stop = find_row_contains(raw, stop)
        if r_stop is not None and r_stop > start:
            end = min(end, r_stop)
    debug["section_end_row"] = end

    # On cherche toutes les lignes d'en-tête "Date pointage ... N° facture" dans [start, end)
    header_rows = []
    scan_r = start
    while scan_r < end:
        hr, cols = find_header_row(raw, scan_r, end, TP_HEADER)
        if hr is None:
            break
        header_rows.append((hr, cols))
        scan_r = hr + 1  # continuer après cet en-tête

    debug["header_rows_found"] = [hr for hr, _ in header_rows]

    if not header_rows:
        return pd.DataFrame(columns=["invoice", "date_encaissement", "amount", "mode", "source_row"]), debug

    rows = []
    # Pour chaque bloc, on lit jusqu'au prochain en-tête ou fin de section
    for i, (hr, cols) in enumerate(header_rows):
        block_end = end
        if i < len(header_rows) - 1:
            block_end = header_rows[i + 1][0]

        c_date_enc = cols[_norm_cell("date encaissement")]
        c_amt = cols[_norm_cell("montant")]
        c_inv = cols[_norm_cell("n° facture")]
        c_mode = cols[_norm_cell("mode de reglement")]

        for r in range(hr + 1, block_end):
            # ligne "Total ..." => stop ou skip
            joined = " ".join([normalize_text(x) for x in raw.iloc[r].tolist() if str(x).lower() != "nan"])
            if joined.startswith("total "):
                continue
            if "total tiers-payant encaisses" in joined:
                continue

            inv = raw.iat[r, c_inv]
            inv = "" if inv is None else str(inv).strip()
            if not inv or inv.lower() == "nan":
                continue

            amt = parse_eur(raw.iat[r, c_amt])
            if abs(amt) < 0.01:
                continue

            dt = parse_date_any(raw.iat[r, c_date_enc])
            if dt is None:
                continue

            mode = normalize_text(raw.iat[r, c_mode])

            rows.append({
                "invoice": inv,
                "date_encaissement": dt,
                "amount": round(float(amt), 2),
                "mode": mode,
                "source_row": r
            })

    debug["rows_kept"] = len(rows)
    return pd.DataFrame(rows, columns=["invoice", "date_encaissement", "amount", "mode", "source_row"]), debug


# ============================
# Build FEC (467 -> 584)
# ============================
def build_fec_tiers_payant(tp_df: pd.DataFrame,
                           journal_code: str,
                           journal_lib: str,
                           compte_467: str,
                           lib_467: str,
                           compte_584: str,
                           lib_584: str) -> pd.DataFrame:
    """
    Pour chaque ligne encaissée :
      Débit 584 / Crédit 467
    PieceRef = N° facture (ex: FAC-1000178)
    EcritureNum = <facture>-TP
    EcritureDate = Date encaissement
    """
    if tp_df is None or tp_df.empty:
        return pd.DataFrame(columns=FEC_COLUMNS)

    fec_rows = []
    for _, r in tp_df.iterrows():
        inv = str(r["invoice"]).strip()
        dt = r["date_encaissement"]
        amt = round(float(r["amount"]), 2)

        ecriture_num = f"{inv}-TP"
        lib = f"Encaissement tiers payant {inv}"

        # Débit 584
        fec_rows.append({
            "JournalCode": journal_code, "JournalLib": journal_lib,
            "EcritureNum": ecriture_num, "EcritureDate": dt.strftime("%Y%m%d"),
            "CompteNum": compte_584, "CompteLib": lib_584,
            "CompAuxNum": "", "CompAuxLib": "",
            "PieceRef": inv, "PieceDate": dt.strftime("%Y%m%d"),
            "EcritureLib": lib,
            "Debit": amt, "Credit": 0.0,
            "EcritureLet": "", "DateLet": "",
            "ValidDate": dt.strftime("%Y%m%d"),
            "Montantdevise": "", "Idevise": ""
        })

        # Crédit 467
        fec_rows.append({
            "JournalCode": journal_code, "JournalLib": journal_lib,
            "EcritureNum": ecriture_num, "EcritureDate": dt.strftime("%Y%m%d"),
            "CompteNum": compte_467, "CompteLib": lib_467,
            "CompAuxNum": "", "CompAuxLib": "",
            "PieceRef": inv, "PieceDate": dt.strftime("%Y%m%d"),
            "EcritureLib": lib,
            "Debit": 0.0, "Credit": amt,
            "EcritureLet": "", "DateLet": "",
            "ValidDate": dt.strftime("%Y%m%d"),
            "Montantdevise": "", "Idevise": ""
        })

    fec = pd.DataFrame(fec_rows, columns=FEC_COLUMNS)
    for col in ["Debit", "Credit"]:
        fec[col] = pd.to_numeric(fec[col], errors="coerce").fillna(0.0).round(2)
    return fec


# ============================
# Streamlit UI
# ============================
st.set_page_config(page_title="Tiers payant → FEC (467→584)", layout="wide")
st.title("Tiers payant — Encaissements (467 → 584)")
st.caption("Charge l'export Optimum « Détail des Tiers Payants ». La page génère un FEC pour les lignes « Tiers-payant encaissés » uniquement.")

uploaded_tp = st.file_uploader("Fichier TIERS PAYANT (.xlsx)", type=["xlsx", "xls"])

with st.sidebar:
    st.header("Paramètres")

    st.subheader("Journal")
    journal_code = st.text_input("JournalCode", value="TP")
    journal_lib = st.text_input("JournalLib", value="Tiers payant")

    st.subheader("Comptes")
    compte_467 = st.text_input("Compte 467 (Crédit)", value="467000")
    lib_467 = st.text_input("Libellé 467", value="Tiers payant à recevoir")

    compte_584 = st.text_input("Compte 584 (Débit)", value="584000")
    lib_584 = st.text_input("Libellé 584", value="Tiers payant encaissé")

if not uploaded_tp:
    st.info("Charge le fichier TIERS PAYANT.")
    st.stop()

file_bytes = uploaded_tp.read()
sheets = list_sheets(file_bytes)
sheet_choice = st.selectbox("Onglet à lire", sheets, index=0)
raw_tp = read_sheet_raw(file_bytes, sheet_choice)

tp_enc, dbg = extract_tiers_payant_encaisse(raw_tp)

st.subheader("Debug lecture (utile si ça ne détecte rien)")
st.write(dbg)

st.subheader("Aperçu — Tiers payant encaissé détecté")
st.dataframe(tp_enc, use_container_width=True)

fec_tp = build_fec_tiers_payant(
    tp_df=tp_enc,
    journal_code=journal_code,
    journal_lib=journal_lib,
    compte_467=compte_467,
    lib_467=lib_467,
    compte_584=compte_584,
    lib_584=lib_584,
)

st.subheader("Aperçu FEC — Tiers payant (467 → 584)")
st.dataframe(fec_tp.head(300), use_container_width=True)

st.subheader("Contrôle d'équilibre")
chk = check_balance(fec_tp)
if chk.empty:
    st.info("Aucune écriture générée (si ton fichier a 0 TP encaissé, c'est normal).")
else:
    bad = chk[chk["Delta"].abs() > 0.01]
    if bad.empty:
        st.success("Toutes les écritures sont équilibrées ✅")
    else:
        st.error("Certaines écritures ne sont pas équilibrées ❌")
        st.dataframe(bad, use_container_width=True)

st.subheader("Téléchargement")
st.download_button(
    "FEC Tiers payant.txt [TAB]",
    data=to_fec_txt_bytes(fec_tp),
    file_name="fec_tiers_payant.txt",
    mime="text/plain"
)
