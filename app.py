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

FACTURE_RE = re.compile(r"Facture numéro\s+(\d+)\s+émise le\s+(\d{2}/\d{2}/\d{4})", re.IGNORECASE)
BORDEREAU_RE = re.compile(
    r"Bordereau\s*N°\s*:\s*([A-Za-z0-9\-]+).*?Remis\s+le\s+(\d{2}/\d{2}/\d{4})",
    re.IGNORECASE
)

MODE_NORMALIZE = {
    "carte bancaire": "carte bancaire",
    "cb": "carte bancaire",
    "carte": "carte bancaire",
    "cheque": "chèque",
    "chèque": "chèque",
    "especes": "espèces",
    "espèces": "espèces",
    "virement": "virement",
    "tiers payant": "tiers-payant",
    "tiers-payant": "tiers-payant",
    "tierspayant": "tiers-payant",
}


# ============================
# Helpers
# ============================
def normalize_mode(x) -> str:
    if x is None or (isinstance(x, float) and np.isnan(x)):
        return ""
    s = str(x).strip().lower()
    s = re.sub(r"\s+", " ", s).replace("\u00a0", " ")
    s = s.replace("’", "-").replace("'", "-")
    s = s.replace("tiers payant", "tiers-payant").replace("tierspayant", "tiers-payant")
    return MODE_NORMALIZE.get(s, s)


def parse_eur(val) -> float:
    """Parse '156,85€', '13,00€', 13, 13.0."""
    if val is None:
        return 0.0
    if isinstance(val, (int, float, np.integer, np.floating)):
        if pd.isna(val):
            return 0.0
        return float(val)

    s = str(val).strip()
    if s == "" or s.lower() == "nan":
        return 0.0

    s = s.replace("€", "").replace("\u00a0", " ").strip().replace(" ", "")
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


def parse_tva_rate(val) -> float:
    """Parse '20,00%' -> 0.20 ; 20 -> 0.20 ; '0' -> 0.0"""
    if val is None:
        return 0.0
    if isinstance(val, (int, float, np.integer, np.floating)):
        if pd.isna(val):
            return 0.0
        v = float(val)
        return v / 100.0 if v > 1.0 else v

    s = str(val).strip().lower().replace("\u00a0", " ")
    s = s.replace("%", "").strip()
    if s == "" or s == "nan":
        return 0.0
    s = s.replace(" ", "")
    if "," in s and "." in s:
        s = s.replace(".", "")
    s = s.replace(",", ".")
    s = re.sub(r"[^0-9\.\-]", "", s)

    if s in ("", ".", "-", "-."):
        return 0.0
    v = float(s)
    return v / 100.0 if v > 1.0 else v


def to_csv_bytes(df: pd.DataFrame, sep: str = ";") -> bytes:
    return df.to_csv(index=False, sep=sep, encoding="utf-8-sig").encode("utf-8-sig")


def check_balance(fec: pd.DataFrame) -> pd.DataFrame:
    if fec.empty:
        return pd.DataFrame()
    chk = fec.groupby(["JournalCode", "EcritureNum"])[["Debit", "Credit"]].sum()
    chk["Delta"] = (chk["Debit"] - chk["Credit"]).round(2)
    return chk


# ============================
# Excel helpers
# ============================
def list_sheets(file_bytes: bytes) -> list[str]:
    bio = BytesIO(file_bytes)
    xls = pd.ExcelFile(bio, engine="openpyxl")
    return xls.sheet_names


def read_sheet_raw(file_bytes: bytes, sheet_name: str) -> pd.DataFrame:
    bio = BytesIO(file_bytes)
    return pd.read_excel(bio, sheet_name=sheet_name, header=None, engine="openpyxl")


def _norm_cell(s: str) -> str:
    s = (s or "").strip().lower()
    s = s.replace("\u00a0", " ")
    # rough accent normalization for matching
    s = s.replace("é", "e").replace("è", "e").replace("ê", "e").replace("ë", "e")
    s = s.replace("à", "a").replace("â", "a")
    s = s.replace("î", "i").replace("ï", "i")
    s = s.replace("ô", "o")
    s = s.replace("ù", "u").replace("û", "u").replace("ü", "u")
    s = re.sub(r"\s+", " ", s)
    return s


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


# ============================
# CAISSE sheet parsing
# ============================
def find_facture_rows(raw: pd.DataFrame) -> list[tuple[int, str, str]]:
    res = []
    for i in range(len(raw)):
        row = raw.iloc[i].astype(str).tolist()
        joined = " | ".join([x for x in row if x and x.lower() != "nan"])
        m = FACTURE_RE.search(joined)
        if m:
            res.append((i, m.group(1), m.group(2)))
    return res


def extract_sales_lines(raw: pd.DataFrame) -> pd.DataFrame:
    factures = find_facture_rows(raw)
    if not factures:
        return pd.DataFrame(columns=["invoice_number", "invoice_date", "tva_rate", "ttc_net", "source_row"])

    factures_with_end = factures + [(len(raw), "", "")]
    rows = []

    for idx in range(len(factures)):
        r0, inv, date_str = factures[idx]
        r1 = factures_with_end[idx + 1][0]

        header_row, cols = find_header_row(raw, r0, r1, ["Produits", "TVA", "Montant du"])
        if header_row is None:
            continue

        c_prod = cols[_norm_cell("Produits")]
        c_tva = cols[_norm_cell("TVA")]
        c_mdu = cols[_norm_cell("Montant du")]

        inv_date = datetime.strptime(date_str, "%d/%m/%Y").date()

        for r in range(header_row + 1, r1):
            prod = raw.iat[r, c_prod]
            prod_s = "" if prod is None else str(prod).strip()
            if prod_s == "" or prod_s.lower() == "nan":
                continue

            rate = parse_tva_rate(raw.iat[r, c_tva])
            ttc_net = parse_eur(raw.iat[r, c_mdu])

            if abs(ttc_net) < 1e-9:
                continue

            rows.append({
                "invoice_number": str(inv),
                "invoice_date": inv_date,
                "tva_rate": round(float(rate), 6),
                "ttc_net": round(float(ttc_net), 2),
                "source_row": r
            })

    return pd.DataFrame(rows)


def extract_encaissements(raw: pd.DataFrame) -> pd.DataFrame:
    factures = find_facture_rows(raw)
    if not factures:
        return pd.DataFrame(columns=["invoice_number", "invoice_date", "amount", "mode", "source_row"])

    factures_with_end = factures + [(len(raw), "", "")]
    rows = []

    for idx in range(len(factures)):
        r0, inv, date_str = factures[idx]
        r1 = factures_with_end[idx + 1][0]

        header_row, cols = find_header_row(raw, r0, r1, ["Montant encaissé", "Mode de règlement"])
        if header_row is None:
            continue

        c_amt = cols[_norm_cell("Montant encaissé")]
        c_mode = cols[_norm_cell("Mode de règlement")]

        inv_date = datetime.strptime(date_str, "%d/%m/%Y").date()

        for r in range(header_row + 1, r1):
            amt = parse_eur(raw.iat[r, c_amt])
            md = normalize_mode(raw.iat[r, c_mode])
            if md and abs(amt) > 1e-9:
                rows.append({
                    "invoice_number": str(inv),
                    "invoice_date": inv_date,
                    "amount": round(float(amt), 2),
                    "mode": md,
                    "source_row": r
                })

    return pd.DataFrame(rows)


# ============================
# REMISES file parsing (optional)
# ============================
def extract_remises_cheques(raw: pd.DataFrame) -> pd.DataFrame:
    starts = []
    for i in range(len(raw)):
        row = raw.iloc[i].astype(str).tolist()
        joined = " | ".join([x for x in row if x and x.lower() != "nan"])
        m = BORDEREAU_RE.search(joined)
        if m:
            starts.append((i, m.group(1), m.group(2)))

    if not starts:
        return pd.DataFrame(columns=["bordereau_id", "remise_date", "total_montant"])

    starts_with_end = starts + [(len(raw), "", "")]
    rows = []

    for k in range(len(starts)):
        r0, bid, dstr = starts[k]
        r1 = starts_with_end[k + 1][0]
        remise_date = datetime.strptime(dstr, "%d/%m/%Y").date()

        header_row, cols = find_header_row(raw, r0, r1, ["Date", "Montant"])
        if header_row is None:
            continue
        c_montant = cols[_norm_cell("Montant")]

        total = 0.0
        for r in range(header_row + 1, r1):
            line = " ".join([str(x) for x in raw.iloc[r].tolist() if str(x).lower() != "nan"]).lower()
            if "nombre de cheque" in line or "nombre de ch" in line:
                break
            amt = parse_eur(raw.iat[r, c_montant])
            if abs(amt) > 1e-9:
                total += amt

        total = round(total, 2)
        if abs(total) > 0.009:
            rows.append({"bordereau_id": str(bid), "remise_date": remise_date, "total_montant": total})

    return pd.DataFrame(rows)


def extract_remises_especes(raw: pd.DataFrame) -> pd.DataFrame:
    header_row, cols = find_header_row(raw, 0, len(raw), ["N° bordereau", "Statut", "Montant"])
    if header_row is None:
        return pd.DataFrame(columns=["bordereau_id", "remise_date", "total_montant"])

    c_bord = cols[_norm_cell("N° bordereau")]
    c_stat = cols[_norm_cell("Statut")]
    c_mont = cols[_norm_cell("Montant")]

    rows = []
    for r in range(header_row + 1, len(raw)):
        bord = raw.iat[r, c_bord]
        if bord is None or str(bord).strip() == "" or str(bord).lower() == "nan":
            continue

        if "total" in str(bord).strip().lower():
            break

        statut = str(raw.iat[r, c_stat])
        m = re.search(r"(\d{2}/\d{2}/\d{4})", statut)
        if not m:
            continue
        dt = datetime.strptime(m.group(1), "%d/%m/%Y").date()

        amt = round(parse_eur(raw.iat[r, c_mont]), 2)
        if abs(amt) > 0.009:
            rows.append({"bordereau_id": str(bord).strip(), "remise_date": dt, "total_montant": amt})

    return pd.DataFrame(rows)


# ============================
# Build mappings from CSV text
# ============================
def build_vat_map_from_csv(text: str) -> dict:
    text = (text or "").strip()
    if not text:
        return {}

    df = pd.read_csv(BytesIO(text.encode("utf-8")), sep=";")
    vat_map = {}
    for _, r in df.iterrows():
        try:
            rate = round(float(r["TauxTVA"]), 6)
        except Exception:
            continue
        vat_map[rate] = {
            "rev_acc": str(r.get("Compte70", "")).strip(),
            "rev_lib": str(r.get("Lib70", "")).strip(),
            "vat_acc": str(r.get("CompteTVA", "")).strip(),
            "vat_lib": str(r.get("LibTVA", "")).strip(),
        }
    return vat_map


def build_mode_map_from_csv(text: str) -> tuple[dict, dict]:
    text = (text or "").strip()
    if not text:
        return {}, {}

    df = pd.read_csv(BytesIO(text.encode("utf-8")), sep=";")
    acc = {}
    lib = {}
    for _, r in df.iterrows():
        md = normalize_mode(r.get("Mode", ""))
        if not md:
            continue
        acc[md] = str(r.get("CompteNum", "")).strip()
        lib[md] = str(r.get("CompteLib", "")).strip()
    return acc, lib


# ============================
# Build FEC
# ============================
def build_fec_sales(sales_lines: pd.DataFrame,
                    journal_code: str,
                    journal_lib: str,
                    compte_53: str,
                    lib_53: str,
                    vat_map: dict,
                    group_per_invoice_and_rate: bool = True) -> pd.DataFrame:
    if sales_lines.empty:
        return pd.DataFrame(columns=FEC_COLUMNS)

    df = sales_lines.copy()
    if group_per_invoice_and_rate:
        df = df.groupby(["invoice_number", "invoice_date", "tva_rate"], as_index=False)["ttc_net"].sum()

    fec_rows = []
    for _, row in df.iterrows():
        inv = str(row["invoice_number"])
        dt = row["invoice_date"]
        rate = round(float(row["tva_rate"]), 6)
        ttc = round(float(row["ttc_net"]), 2)

        if rate not in vat_map:
            continue

        ht = ttc / (1.0 + rate) if (1.0 + rate) != 0 else ttc
        tva = ttc - ht
        ht = round(ht, 2)
        tva = round(tva, 2)

        rev_acc = vat_map[rate]["rev_acc"]
        rev_lib = vat_map[rate]["rev_lib"]
        vat_acc = vat_map[rate]["vat_acc"]
        vat_lib = vat_map[rate]["vat_lib"]

        ecriture_num = f"{inv}-VT"
        lib = f"Vente facture {inv} TVA {rate*100:.2f}%"

        # Debit 53 TTC
        fec_rows.append({
            "JournalCode": journal_code, "JournalLib": journal_lib,
            "EcritureNum": ecriture_num, "EcritureDate": dt.strftime("%Y%m%d"),
            "CompteNum": compte_53, "CompteLib": lib_53,
            "CompAuxNum": "", "CompAuxLib": "",
            "PieceRef": inv, "PieceDate": dt.strftime("%Y%m%d"),
            "EcritureLib": lib,
            "Debit": ttc, "Credit": 0.0,
            "EcritureLet": "", "DateLet": "",
            "ValidDate": dt.strftime("%Y%m%d"),
            "Montantdevise": "", "Idevise": ""
        })

        # Credit 70 HT
        fec_rows.append({
            "JournalCode": journal_code, "JournalLib": journal_lib,
            "EcritureNum": ecriture_num, "EcritureDate": dt.strftime("%Y%m%d"),
            "CompteNum": rev_acc, "CompteLib": rev_lib,
            "CompAuxNum": "", "CompAuxLib": "",
            "PieceRef": inv, "PieceDate": dt.strftime("%Y%m%d"),
            "EcritureLib": lib,
            "Debit": 0.0, "Credit": ht,
            "EcritureLet": "", "DateLet": "",
            "ValidDate": dt.strftime("%Y%m%d"),
            "Montantdevise": "", "Idevise": ""
        })

        # Credit TVA
        if abs(tva) > 0.009:
            fec_rows.append({
                "JournalCode": journal_code, "JournalLib": journal_lib,
                "EcritureNum": ecriture_num, "EcritureDate": dt.strftime("%Y%m%d"),
                "CompteNum": vat_acc, "CompteLib": vat_lib,
                "CompAuxNum": "", "CompAuxLib": "",
                "PieceRef": inv, "PieceDate": dt.strftime("%Y%m%d"),
                "EcritureLib": lib,
                "Debit": 0.0, "Credit": tva,
                "EcritureLet": "", "DateLet": "",
                "ValidDate": dt.strftime("%Y%m%d"),
                "Montantdevise": "", "Idevise": ""
            })

    fec = pd.DataFrame(fec_rows, columns=FEC_COLUMNS)
    for col in ["Debit", "Credit"]:
        fec[col] = pd.to_numeric(fec[col], errors="coerce").fillna(0.0).round(2)
    return fec


def build_fec_settlements(enc_df: pd.DataFrame,
                          journal_code: str,
                          journal_lib: str,
                          compte_53: str,
                          lib_53: str,
                          mode_to_debit_account: dict,
                          mode_to_debit_lib: dict,
                          group_same_mode_per_invoice: bool = True) -> pd.DataFrame:
    if enc_df.empty:
        return pd.DataFrame(columns=FEC_COLUMNS)

    df = enc_df.copy()
    if group_same_mode_per_invoice:
        df = df.groupby(["invoice_number", "invoice_date", "mode"], as_index=False)["amount"].sum()

    fec_rows = []
    for _, row in df.iterrows():
        inv = str(row["invoice_number"])
        dt = row["invoice_date"]
        mode = row["mode"]
        amt = round(float(row["amount"]), 2)

        debit_acc = mode_to_debit_account.get(mode, "")
        debit_lib = mode_to_debit_lib.get(mode, f"Règlement {mode}".strip())
        if not debit_acc:
            continue

        ecriture_num = f"{inv}-ENC"
        lib = f"Encaissement facture {inv} ({mode})"

        # Debit payment account
        fec_rows.append({
            "JournalCode": journal_code, "JournalLib": journal_lib,
            "EcritureNum": ecriture_num, "EcritureDate": dt.strftime("%Y%m%d"),
            "CompteNum": debit_acc, "CompteLib": debit_lib,
            "CompAuxNum": "", "CompAuxLib": "",
            "PieceRef": inv, "PieceDate": dt.strftime("%Y%m%d"),
            "EcritureLib": lib,
            "Debit": amt, "Credit": 0.0,
            "EcritureLet": "", "DateLet": "",
            "ValidDate": dt.strftime("%Y%m%d"),
            "Montantdevise": "", "Idevise": ""
        })

        # Credit 53
        fec_rows.append({
            "JournalCode": journal_code, "JournalLib": journal_lib,
            "EcritureNum": ecriture_num, "EcritureDate": dt.strftime("%Y%m%d"),
            "CompteNum": compte_53, "CompteLib": lib_53,
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


def build_fec_remises(remises_df: pd.DataFrame,
                      journal_code: str,
                      journal_lib: str,
                      compte_debit: str,
                      lib_debit: str,
                      compte_credit: str,
                      lib_credit: str,
                      prefix_num: str,
                      lib_prefix: str) -> pd.DataFrame:
    if remises_df.empty:
        return pd.DataFrame(columns=FEC_COLUMNS)

    fec_rows = []
    for _, row in remises_df.iterrows():
        bid = str(row["bordereau_id"])
        dt = row["remise_date"]
        amt = round(float(row["total_montant"]), 2)

        ecriture_num = f"{prefix_num}-{bid}"
        lib = f"{lib_prefix} {bid}"

        # Debit (512)
        fec_rows.append({
            "JournalCode": journal_code, "JournalLib": journal_lib,
            "EcritureNum": ecriture_num, "EcritureDate": dt.strftime("%Y%m%d"),
            "CompteNum": compte_debit, "CompteLib": lib_debit,
            "CompAuxNum": "", "CompAuxLib": "",
            "PieceRef": bid, "PieceDate": dt.strftime("%Y%m%d"),
            "EcritureLib": lib,
            "Debit": amt, "Credit": 0.0,
            "EcritureLet": "", "DateLet": "",
            "ValidDate": dt.strftime("%Y%m%d"),
            "Montantdevise": "", "Idevise": ""
        })

        # Credit (5112 or 531)
        fec_rows.append({
            "JournalCode": journal_code, "JournalLib": journal_lib,
            "EcritureNum": ecriture_num, "EcritureDate": dt.strftime("%Y%m%d"),
            "CompteNum": compte_credit, "CompteLib": lib_credit,
            "CompAuxNum": "", "CompAuxLib": "",
            "PieceRef": bid, "PieceDate": dt.strftime("%Y%m%d"),
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
st.set_page_config(page_title="Optimum → FEC (ventes + encaissements + remises optionnelles)", layout="wide")
st.title("Export Optimum/AS3 → FEC (Ventes + Encaissements + Remises en banque optionnelles)")

st.caption("1) Charge l'export CAISSE (obligatoire). 2) Charge éventuellement un export REMISES (chèques+espèces) séparé.")

uploaded_caisse = st.file_uploader("1) Fichier CAISSE (.xlsx)", type=["xlsx", "xls"], key="caisse")

uploaded_remises = st.file_uploader("2) Fichier REMISES (.xlsx) — optionnel", type=["xlsx", "xls"], key="remises")

with st.sidebar:
    st.header("Paramètres")

    st.subheader("Compte 53 (caisse à ventiler)")
    compte_53 = st.text_input("Compte 53", value="530000")
    lib_53 = st.text_input("Libellé 53", value="Caisse à ventiler")

    st.subheader("Journal VENTES (CA)")
    jv_code = st.text_input("JournalCode ventes", value="VT")
    jv_lib = st.text_input("JournalLib ventes", value="Ventes caisse")

    st.subheader("Journal ENCAISSEMENTS")
    je_code = st.text_input("JournalCode encaissements", value="BQ")
    je_lib = st.text_input("JournalLib encaissements", value="Règlements")

    st.subheader("Options")
    group_sales = st.checkbox("Regrouper ventes par facture + taux TVA", value=True)
    group_payments = st.checkbox("Regrouper encaissements par facture + mode", value=True)

    st.subheader("Séparateur export")
    csv_sep = st.selectbox("Séparateur CSV", options=[";", ",", "\t"], index=0)

    st.subheader("Grille TVA → comptes 70 + TVA")
    st.caption("Format CSV (;) : TauxTVA;Compte70;Lib70;CompteTVA;LibTVA")
    vat_default_text = """TauxTVA;Compte70;Lib70;CompteTVA;LibTVA
0.20;707000;Ventes;445710;TVA collectée 20%
0.10;707010;Ventes 10%;445712;TVA collectée 10%
0.055;707005;Ventes 5,5%;445713;TVA collectée 5,5%
0.00;707000;Ventes exonérées;445700;TVA collectée 0%
"""
    vat_text = st.text_area("Grille TVA", value=vat_default_text, height=170)

    st.subheader("Grille modes de règlement → compte Débit")
    st.caption("Format CSV (;) : Mode;CompteNum;CompteLib")
    mode_default_text = """Mode;CompteNum;CompteLib
carte bancaire;511000;CB à encaisser
chèque;511200;Chèques à encaisser
espèces;531000;Caisse
virement;512000;Banque
tiers-payant;467000;Tiers payant à recevoir
"""
    mode_text = st.text_area("Grille modes", value=mode_default_text, height=170)

    st.subheader("Paramètres REMISES en banque (si fichier remises fourni)")
    jr_code = st.text_input("JournalCode remises", value="BQ")
    jr_lib = st.text_input("JournalLib remises", value="Remises en banque")

    compte_512 = st.text_input("Compte 512 (Banque) - Débit", value="512000")
    lib_512 = st.text_input("Lib 512", value="Banque")

    compte_5112 = st.text_input("Compte 5112 (Chèques) - Crédit", value="511200")
    lib_5112 = st.text_input("Lib 5112", value="Chèques à encaisser")

    compte_531 = st.text_input("Compte 531 (Espèces) - Crédit", value="531000")
    lib_531 = st.text_input("Lib 531", value="Caisse espèces")

if not uploaded_caisse:
    st.info("Charge au moins le fichier CAISSE.")
    st.stop()

# ============================
# Read CAISSE
# ============================
file_bytes_caisse = uploaded_caisse.read()
sheets_caisse = list_sheets(file_bytes_caisse)

sheet_caisse = st.selectbox("Onglet CAISSE à utiliser", sheets_caisse, index=0)
raw_caisse = read_sheet_raw(file_bytes_caisse, sheet_caisse)

# ============================
# Parse mappings
# ============================
try:
    vat_map = build_vat_map_from_csv(vat_text)
except Exception as e:
    st.error(f"Erreur lecture grille TVA : {e}")
    st.stop()

try:
    mode_acc, mode_lib = build_mode_map_from_csv(mode_text)
except Exception as e:
    st.error(f"Erreur lecture grille modes : {e}")
    st.stop()

# ============================
# Extract CAISSE data
# ============================
sales_lines = extract_sales_lines(raw_caisse)
enc = extract_encaissements(raw_caisse)

# ============================
# Build CAISSE FEC
# ============================
fec_sales = build_fec_sales(
    sales_lines=sales_lines,
    journal_code=jv_code,
    journal_lib=jv_lib,
    compte_53=compte_53,
    lib_53=lib_53,
    vat_map=vat_map,
    group_per_invoice_and_rate=group_sales,
)

fec_sett = build_fec_settlements(
    enc_df=enc,
    journal_code=je_code,
    journal_lib=je_lib,
    compte_53=compte_53,
    lib_53=lib_53,
    mode_to_debit_account=mode_acc,
    mode_to_debit_lib=mode_lib,
    group_same_mode_per_invoice=group_payments,
)

fec_all_parts = [fec_sales, fec_sett]

# ============================
# Optional REMISES file
# ============================
rem_cheques = pd.DataFrame(columns=["bordereau_id", "remise_date", "total_montant"])
rem_especes = pd.DataFrame(columns=["bordereau_id", "remise_date", "total_montant"])
fec_rem_cheques = pd.DataFrame(columns=FEC_COLUMNS)
fec_rem_especes = pd.DataFrame(columns=FEC_COLUMNS)

if uploaded_remises is not None:
    file_bytes_remises = uploaded_remises.read()
    sheets_remises = list_sheets(file_bytes_remises)

    st.subheader("Paramétrage fichier REMISES (optionnel)")
    sheet_cheques = st.selectbox("Onglet REMISES CHÈQUES", sheets_remises, index=0, key="sheet_cheques")
    sheet_especes = st.selectbox("Onglet REMISES ESPÈCES", sheets_remises, index=min(1, len(sheets_remises)-1), key="sheet_especes")

    raw_cheques = read_sheet_raw(file_bytes_remises, sheet_cheques)
    raw_especes = read_sheet_raw(file_bytes_remises, sheet_especes)

    rem_cheques = extract_remises_cheques(raw_cheques)
    rem_especes = extract_remises_especes(raw_especes)

    fec_rem_cheques = build_fec_remises(
        remises_df=rem_cheques,
        journal_code=jr_code,
        journal_lib=jr_lib,
        compte_debit=compte_512,
        lib_debit=lib_512,
        compte_credit=compte_5112,
        lib_credit=lib_5112,
        prefix_num="REMCHQ",
        lib_prefix="Remise chèques bordereau",
    )

    fec_rem_especes = build_fec_remises(
        remises_df=rem_especes,
        journal_code=jr_code,
        journal_lib=jr_lib,
        compte_debit=compte_512,
        lib_debit=lib_512,
        compte_credit=compte_531,
        lib_credit=lib_531,
        prefix_num="REMESP",
        lib_prefix="Remise espèces bordereau",
    )

    fec_all_parts.extend([fec_rem_cheques, fec_rem_especes])

fec_all = pd.concat(fec_all_parts, ignore_index=True)

# ============================
# Display
# ============================
st.subheader("Synthèse CAISSE")
c1, c2, c3, c4 = st.columns(4)
with c1:
    st.metric("Lignes ventes", int(len(sales_lines)))
with c2:
    st.metric("Factures", int(sales_lines["invoice_number"].nunique()) if not sales_lines.empty else 0)
with c3:
    st.metric("Lignes encaissements", int(len(enc)))
with c4:
    st.metric("Total encaissé", f"{enc['amount'].sum():,.2f} €".replace(",", " ") if not enc.empty else "0,00 €")

if uploaded_remises is not None:
    st.subheader("Synthèse REMISES")
    r1, r2, r3 = st.columns(3)
    with r1:
        st.metric("Bordereaux chèques", int(len(rem_cheques)))
    with r2:
        st.metric("Bordereaux espèces", int(len(rem_especes)))
    with r3:
        total_rem = 0.0
        if not rem_cheques.empty:
            total_rem += rem_cheques["total_montant"].sum()
        if not rem_especes.empty:
            total_rem += rem_especes["total_montant"].sum()
        st.metric("Total remises", f"{total_rem:,.2f} €".replace(",", " "))

st.subheader("Aperçu - Ventes (articles)")
st.dataframe(sales_lines.head(200), use_container_width=True)

st.subheader("Aperçu - Encaissements")
st.dataframe(enc.head(200), use_container_width=True)

if uploaded_remises is not None:
    st.subheader("Aperçu - Bordereaux chèques")
    st.dataframe(rem_cheques, use_container_width=True)

    st.subheader("Aperçu - Bordereaux espèces")
    st.dataframe(rem_especes, use_container_width=True)

st.subheader("Aperçu FEC - Global")
st.dataframe(fec_all.head(300), use_container_width=True)

st.subheader("Contrôles d'équilibre")
chk_all = check_balance(fec_all)
if chk_all.empty:
    st.info("Aucune écriture générée.")
else:
    bad = chk_all[chk_all["Delta"].abs() > 0.01]
    if bad.empty:
        st.success("Toutes les écritures sont équilibrées ✅")
    else:
        st.error("Certaines écritures ne sont pas équilibrées ❌")
        st.dataframe(bad, use_container_width=True)

st.subheader("Téléchargements")
col1, col2, col3, col4 = st.columns(4)

with col1:
    st.download_button("CSV FEC - Ventes", data=to_csv_bytes(fec_sales, sep=csv_sep),
                       file_name="fec_ventes.csv", mime="text/csv")
with col2:
    st.download_button("CSV FEC - Encaissements", data=to_csv_bytes(fec_sett, sep=csv_sep),
                       file_name="fec_encaissements.csv", mime="text/csv")
with col3:
    # only meaningful if remises file provided; still downloads empty CSV otherwise
    fec_rem_all = pd.concat([fec_rem_cheques, fec_rem_especes], ignore_index=True)
    st.download_button("CSV FEC - Remises (optionnel)", data=to_csv_bytes(fec_rem_all, sep=csv_sep),
                       file_name="fec_remises.csv", mime="text/csv")
with col4:
    st.download_button("CSV FEC - Global", data=to_csv_bytes(fec_all, sep=csv_sep),
                       file_name="fec_global.csv", mime="text/csv")
