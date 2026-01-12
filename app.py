import re
from datetime import datetime
from io import BytesIO

import numpy as np
import pandas as pd
import streamlit as st


# ============================
# Constantes FEC
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
# Helpers parsing
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


def parse_tva_rate(val) -> float:
    """'20,00%'->0.20 ; 20->0.20 ; '0'->0.0"""
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
# Extraction Factures + Encaissements
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


def extract_invoice_sales_bundle(raw: pd.DataFrame) -> pd.DataFrame:
    """
    1 ligne par facture.
    TTC = valeur 'Montant du' sur la ligne TOTAL (produit vide).
    """
    factures = find_facture_rows(raw)
    if not factures:
        return pd.DataFrame(columns=[
            "invoice_number", "invoice_date",
            "total_ttc", "total_vat", "vat_rate",
            "source_row_total"
        ])

    factures_with_end = factures + [(len(raw), "", "")]
    out = []

    for idx in range(len(factures)):
        r0, inv, date_str = factures[idx]
        r1 = factures_with_end[idx + 1][0]
        inv_date = datetime.strptime(date_str, "%d/%m/%Y").date()

        header_row, cols = find_header_row(
            raw, r0, r1,
            ["Produits", "TVA", "Total opération", "Montant TVA", "Montant du"]
        )
        if header_row is None:
            continue

        c_prod = cols[_norm_cell("Produits")]
        c_rate = cols[_norm_cell("TVA")]
        c_total_op = cols[_norm_cell("Total opération")]
        c_vat = cols[_norm_cell("Montant TVA")]
        c_ttc = cols[_norm_cell("Montant du")]

        best_total = None

        for r in range(header_row + 1, r1):
            prod = raw.iat[r, c_prod]
            prod_s = "" if prod is None else str(prod).strip()
            if prod_s and prod_s.lower() != "nan":
                continue  # ligne détaillée

            ttc = parse_eur(raw.iat[r, c_ttc])
            if abs(ttc) < 1e-9:
                continue

            total_op = raw.iat[r, c_total_op]
            vat_amt = raw.iat[r, c_vat]
            rate = raw.iat[r, c_rate]

            total_op_num = parse_eur(total_op)
            vat_amt_num = parse_eur(vat_amt)
            rate_num = parse_tva_rate(rate)

            # ligne total = produit vide + TTC + (col total_op ou col TVA renseignée, même 0)
            total_op_present = str(total_op).strip().lower() not in ("", "nan")
            vat_present = str(vat_amt).strip().lower() not in ("", "nan")

            if total_op_present or vat_present or abs(total_op_num) > 1e-9 or abs(vat_amt_num) > 1e-9:
                best_total = {
                    "invoice_number": str(inv),
                    "invoice_date": inv_date,
                    "total_ttc": round(float(ttc), 2),
                    "total_vat": round(float(vat_amt_num), 2),
                    "vat_rate": round(float(rate_num), 6) if abs(rate_num) > 1e-9 else None,
                    "source_row_total": r
                }

        if best_total is None:
            out.append({
                "invoice_number": str(inv),
                "invoice_date": inv_date,
                "total_ttc": None,
                "total_vat": 0.0,
                "vat_rate": None,
                "source_row_total": None
            })
        else:
            out.append(best_total)

    return pd.DataFrame(out)


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
# Mappings TVA / modes
# ============================
def build_vat_map_from_csv(text: str) -> dict:
    """
    CSV (;) : TauxTVA;Compte70;Lib70;CompteTVA;LibTVA
    """
    text = (text or "").strip()
    if not text:
        return {}

    df = pd.read_csv(BytesIO(text.encode("utf-8")), sep=";")
    needed = {"TauxTVA", "Compte70", "Lib70", "CompteTVA", "LibTVA"}
    if not needed.issubset(set(df.columns)):
        raise ValueError(f"Colonnes attendues: {sorted(needed)}")

    vat_map = {}
    for _, r in df.iterrows():
        try:
            rate = round(float(r["TauxTVA"]), 6)
        except Exception:
            continue
        vat_map[rate] = {
            "rev_acc": str(r["Compte70"]).strip(),
            "rev_lib": str(r["Lib70"]).strip(),
            "vat_acc": str(r["CompteTVA"]).strip(),
            "vat_lib": str(r["LibTVA"]).strip(),
        }
    return vat_map


def build_mode_map_from_csv(text: str) -> tuple[dict, dict]:
    """
    CSV (;) : Mode;CompteNum;CompteLib
    """
    text = (text or "").strip()
    if not text:
        return {}, {}

    df = pd.read_csv(BytesIO(text.encode("utf-8")), sep=";")
    needed = {"Mode", "CompteNum", "CompteLib"}
    if not needed.issubset(set(df.columns)):
        raise ValueError(f"Colonnes attendues: {sorted(needed)}")

    acc = {}
    lib = {}
    for _, r in df.iterrows():
        md = normalize_mode(r["Mode"])
        if not md:
            continue
        acc[md] = str(r["CompteNum"]).strip()
        lib[md] = str(r["CompteLib"]).strip()
    return acc, lib


# ============================
# Ventes : 1 écriture / facture
# ============================
def _infer_rate_from_ttc_vat(ttc: float, vat: float) -> float | None:
    ht = ttc - vat
    if abs(ht) < 1e-9:
        return None
    rate = vat / ht
    if rate < 0 or rate > 1.0:
        return None
    return round(float(rate), 6)


def _closest_rate_in_map(rate: float, vat_map: dict, tol: float = 0.002) -> float | None:
    if not vat_map:
        return None
    best = min(vat_map.keys(), key=lambda x: abs(x - rate))
    if abs(best - rate) <= tol:
        return best
    return None


def build_fec_sales_per_invoice(
    invoices: pd.DataFrame,
    journal_code: str,
    journal_lib: str,
    compte_53: str,
    lib_53: str,
    vat_map: dict,
    compte_70_controle: str,
    lib_70_controle: str,
    compte_tva_fallback: str,
    lib_tva_fallback: str,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    if invoices.empty:
        return pd.DataFrame(columns=FEC_COLUMNS), pd.DataFrame(columns=["invoice_number", "invoice_date", "reason"])

    fec_rows = []
    warnings = []

    for _, invr in invoices.iterrows():
        inv = str(invr["invoice_number"])
        dt = invr["invoice_date"]

        if invr["total_ttc"] is None or pd.isna(invr["total_ttc"]):
            warnings.append({"invoice_number": inv, "invoice_date": dt, "reason": "Ligne TOTAL (Montant du) non détectée -> vente ignorée"})
            continue

        ttc = round(float(invr["total_ttc"]), 2)
        vat_amt_line = round(float(invr["total_vat"] or 0.0), 2)

        # taux : priorité au taux lu, sinon déduction via TTC/TVA
        rate = invr["vat_rate"]
        if rate is not None and not pd.isna(rate):
            rate = round(float(rate), 6)
        else:
            rate = _infer_rate_from_ttc_vat(ttc, vat_amt_line) if abs(vat_amt_line) > 0.009 else None

        # TVA recalculée si taux connu (règle souhaitée)
        if rate is not None:
            tva = round((ttc / (1.0 + rate)) * rate, 2)
        else:
            tva = vat_amt_line

        ht = round(ttc - tva, 2)

        mapped_rate = _closest_rate_in_map(rate, vat_map) if rate is not None else None

        if mapped_rate is not None:
            acc_70 = vat_map[mapped_rate]["rev_acc"]
            lib_70 = vat_map[mapped_rate]["rev_lib"]
            acc_tva = vat_map[mapped_rate]["vat_acc"]
            lib_tva = vat_map[mapped_rate]["vat_lib"]
        else:
            acc_70 = compte_70_controle
            lib_70 = lib_70_controle
            acc_tva = compte_tva_fallback
            lib_tva = lib_tva_fallback
            warnings.append({"invoice_number": inv, "invoice_date": dt, "reason": f"Taux non mappé (rate={rate}) -> HT en contrôle + TVA fallback"})

        # EcritureNum et PieceRef = NUMERO FACTURE
        ecriture_num = inv
        lib_ecr = f"Vente facture {inv}"

        # Débit 53 TTC
        fec_rows.append({
            "JournalCode": journal_code, "JournalLib": journal_lib,
            "EcritureNum": ecriture_num, "EcritureDate": dt.strftime("%Y%m%d"),
            "CompteNum": compte_53, "CompteLib": lib_53,
            "CompAuxNum": "", "CompAuxLib": "",
            "PieceRef": inv, "PieceDate": dt.strftime("%Y%m%d"),
            "EcritureLib": lib_ecr,
            "Debit": ttc, "Credit": 0.0,
            "EcritureLet": "", "DateLet": "",
            "ValidDate": dt.strftime("%Y%m%d"),
            "Montantdevise": "", "Idevise": ""
        })

        # Crédit TVA
        if abs(tva) > 0.009:
            fec_rows.append({
                "JournalCode": journal_code, "JournalLib": journal_lib,
                "EcritureNum": ecriture_num, "EcritureDate": dt.strftime("%Y%m%d"),
                "CompteNum": acc_tva, "CompteLib": lib_tva,
                "CompAuxNum": "", "CompAuxLib": "",
                "PieceRef": inv, "PieceDate": dt.strftime("%Y%m%d"),
                "EcritureLib": lib_ecr,
                "Debit": 0.0, "Credit": tva,
                "EcritureLet": "", "DateLet": "",
                "ValidDate": dt.strftime("%Y%m%d"),
                "Montantdevise": "", "Idevise": ""
            })

        # Crédit HT
        if abs(ht) > 0.009:
            fec_rows.append({
                "JournalCode": journal_code, "JournalLib": journal_lib,
                "EcritureNum": ecriture_num, "EcritureDate": dt.strftime("%Y%m%d"),
                "CompteNum": acc_70, "CompteLib": lib_70,
                "CompAuxNum": "", "CompAuxLib": "",
                "PieceRef": inv, "PieceDate": dt.strftime("%Y%m%d"),
                "EcritureLib": lib_ecr,
                "Debit": 0.0, "Credit": ht,
                "EcritureLet": "", "DateLet": "",
                "ValidDate": dt.strftime("%Y%m%d"),
                "Montantdevise": "", "Idevise": ""
            })

    fec = pd.DataFrame(fec_rows, columns=FEC_COLUMNS)
    for col in ["Debit", "Credit"]:
        fec[col] = pd.to_numeric(fec[col], errors="coerce").fillna(0.0).round(2)

    warn_df = pd.DataFrame(warnings, columns=["invoice_number", "invoice_date", "reason"])
    return fec, warn_df


# ============================
# Encaissements
# ============================
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

        # Débit compte règlement
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

        # Crédit 53
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


# ============================
# Streamlit APP
# ============================
st.set_page_config(page_title="Optimum → FEC", layout="wide")
st.title("Optimum / AS3 export caisse → FEC (Ventes par facture + Encaissements)")

uploaded_caisse = st.file_uploader("Fichier CAISSE (.xlsx)", type=["xlsx", "xls"], key="caisse")

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

    st.subheader("Compte HT de contrôle (si taux non mappé)")
    compte_70_controle = st.text_input("Compte 70 contrôle", value="708000")
    lib_70_controle = st.text_input("Lib 70 contrôle", value="Ventes – contrôle Optimum")

    st.subheader("Compte TVA fallback (si taux non mappé)")
    compte_tva_fallback = st.text_input("Compte TVA fallback", value="445799")
    lib_tva_fallback = st.text_input("Lib TVA fallback", value="TVA collectée – contrôle")

    st.subheader("Options")
    group_payments = st.checkbox("Regrouper encaissements par facture + mode", value=True)

    st.subheader("Séparateur export")
    csv_sep = st.selectbox("Séparateur CSV", options=[";", ",", "\t"], index=0)

    st.subheader("Grille TVA → comptes 70 + TVA")
    st.caption("Format CSV (;) : TauxTVA;Compte70;Lib70;CompteTVA;LibTVA")
    vat_default_text = """TauxTVA;Compte70;Lib70;CompteTVA;LibTVA
0.20;708000;Ventes;445710;TVA collectée 20%
0.055;708005;Ventes 5,5%;445713;TVA collectée 5,5%
0.10;708010;Ventes 10%;445712;TVA collectée 10%
0.00;708000;Ventes exonérées;445700;TVA collectée 0%
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

if not uploaded_caisse:
    st.info("Charge le fichier CAISSE.")
    st.stop()

# Lecture Excel
file_bytes = uploaded_caisse.read()
sheets = list_sheets(file_bytes)
sheet = st.selectbox("Onglet CAISSE à utiliser", sheets, index=0)
raw = read_sheet_raw(file_bytes, sheet)

# Mappings
vat_map = build_vat_map_from_csv(vat_text)
mode_acc, mode_lib = build_mode_map_from_csv(mode_text)

# Extraction
invoices_sales = extract_invoice_sales_bundle(raw)
enc = extract_encaissements(raw)

# Construction FEC
fec_sales, sales_warnings = build_fec_sales_per_invoice(
    invoices=invoices_sales,
    journal_code=jv_code, journal_lib=jv_lib,
    compte_53=compte_53, lib_53=lib_53,
    vat_map=vat_map,
    compte_70_controle=compte_70_controle, lib_70_controle=lib_70_controle,
    compte_tva_fallback=compte_tva_fallback, lib_tva_fallback=lib_tva_fallback
)

fec_enc = build_fec_settlements(
    enc_df=enc,
    journal_code=je_code, journal_lib=je_lib,
    compte_53=compte_53, lib_53=lib_53,
    mode_to_debit_account=mode_acc,
    mode_to_debit_lib=mode_lib,
    group_same_mode_per_invoice=group_payments
)

fec_all = pd.concat([fec_sales, fec_enc], ignore_index=True)

# Affichage
st.subheader("Factures (base ventes)")
st.dataframe(invoices_sales.head(300), use_container_width=True)

if not sales_warnings.empty:
    st.subheader("Avertissements ventes")
    st.dataframe(sales_warnings, use_container_width=True)

st.subheader("Encaissements")
st.dataframe(enc.head(300), use_container_width=True)

st.subheader("FEC (global) - aperçu")
st.dataframe(fec_all.head(400), use_container_width=True)

st.subheader("Contrôle équilibre par écriture")
chk = check_balance(fec_all)
if chk.empty:
    st.info("Aucune écriture générée.")
else:
    bad = chk[chk["Delta"].abs() > 0.01]
    if bad.empty:
        st.success("Toutes les écritures sont équilibrées ✅")
    else:
        st.error("Certaines écritures ne sont pas équilibrées ❌")
        st.dataframe(bad, use_container_width=True)

# Download
st.subheader("Téléchargements")
c1, c2, c3 = st.columns(3)
with c1:
    st.download_button("CSV FEC - Ventes (VT)", data=to_csv_bytes(fec_sales, sep=csv_sep), file_name="fec_ventes.csv", mime="text/csv")
with c2:
    st.download_button("CSV FEC - Encaissements", data=to_csv_bytes(fec_enc, sep=csv_sep), file_name="fec_encaissements.csv", mime="text/csv")
with c3:
    st.download_button("CSV FEC - Global", data=to_csv_bytes(fec_all, sep=csv_sep), file_name="fec_global.csv", mime="text/csv")
