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

# ⚠️ on ne touche pas aux remises (comme demandé)
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
# CAISSE parsing
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
    Une ligne par facture avec :
    - invoice_number, invoice_date
    - total_ttc (Montant du total facture)
    - vat_amount (Montant TVA du total facture si dispo sinon recalcul)
    - vat_rate (si identifiable sur la ligne total, sinon None)
    - sum_detail_ttc (somme des Montant du lignes détaillées)
    - has_detail (bool)
    - has_total (bool)
    """
    factures = find_facture_rows(raw)
    if not factures:
        return pd.DataFrame(columns=[
            "invoice_number", "invoice_date", "total_ttc", "vat_amount", "vat_rate",
            "sum_detail_ttc", "has_detail", "has_total", "source_row_total"
        ])

    factures_with_end = factures + [(len(raw), "", "")]
    out = []

    for idx in range(len(factures)):
        r0, inv, date_str = factures[idx]
        r1 = factures_with_end[idx + 1][0]
        inv_date = datetime.strptime(date_str, "%d/%m/%Y").date()

        header_row, cols = find_header_row(raw, r0, r1, ["Produits", "TVA", "Montant TVA", "Montant du"])
        if header_row is None:
            continue

        c_prod = cols[_norm_cell("Produits")]
        c_rate = cols[_norm_cell("TVA")]
        c_vat = cols[_norm_cell("Montant TVA")]
        c_ttc = cols[_norm_cell("Montant du")]

        sum_detail = 0.0
        has_detail = False

        total_ttc = None
        total_vat = None
        total_rate = None
        source_row_total = None

        for r in range(header_row + 1, r1):
            prod = raw.iat[r, c_prod]
            prod_s = "" if prod is None else str(prod).strip()
            prod_is_empty = (prod_s == "" or prod_s.lower() == "nan")

            ttc = parse_eur(raw.iat[r, c_ttc])
            vat = parse_eur(raw.iat[r, c_vat])
            rate = parse_tva_rate(raw.iat[r, c_rate])

            if not prod_is_empty:
                if abs(ttc) > 1e-9:
                    sum_detail += ttc
                    has_detail = True
                continue

            # ligne "total" probable : produit vide + TTC non nul
            if abs(ttc) > 1e-9:
                total_ttc = round(float(ttc), 2)
                total_vat = round(float(vat), 2) if abs(vat) > 1e-9 else None
                total_rate = round(float(rate), 6) if abs(rate) > 1e-9 else None
                source_row_total = r

        sum_detail = round(float(sum_detail), 2)

        out.append({
            "invoice_number": str(inv),
            "invoice_date": inv_date,
            "total_ttc": total_ttc,
            "vat_amount": total_vat,
            "vat_rate": total_rate,
            "sum_detail_ttc": sum_detail,
            "has_detail": bool(has_detail),
            "has_total": total_ttc is not None,
            "source_row_total": source_row_total
        })

    return pd.DataFrame(out)


def extract_invoice_sales_bundle(raw: pd.DataFrame) -> pd.DataFrame:
    """
    Une ligne par facture.
    IMPORTANT : on récupère le TTC (= Montant du) sur la VRAIE ligne total :
    - Produits vide
    - Montant du non nul
    - et au moins 1 des colonnes (Total opération / Montant TVA) est renseignée (même 0)
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

        # On exige ces colonnes pour fiabiliser la détection de la ligne total
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

        best_total = None  # on garde la DERNIERE ligne total valide rencontrée

        for r in range(header_row + 1, r1):
            prod = raw.iat[r, c_prod]
            prod_s = "" if prod is None else str(prod).strip()
            prod_is_empty = (prod_s == "" or prod_s.lower() == "nan")

            if not prod_is_empty:
                continue

            ttc = parse_eur(raw.iat[r, c_ttc])
            if abs(ttc) < 1e-9:
                continue

            total_op = parse_eur(raw.iat[r, c_total_op])
            vat_amt = parse_eur(raw.iat[r, c_vat])
            rate = parse_tva_rate(raw.iat[r, c_rate])

            # Condition "ligne total" robuste :
            # produit vide + TTC non nul + (total_op renseigné OU TVA renseignée)
            if abs(total_op) > 1e-9 or abs(vat_amt) > 1e-9 or str(raw.iat[r, c_total_op]).strip() != "" or str(raw.iat[r, c_vat]).strip() != "":
                best_total = {
                    "invoice_number": str(inv),
                    "invoice_date": inv_date,
                    "total_ttc": round(float(ttc), 2),
                    "total_vat": round(float(vat_amt), 2) if abs(vat_amt) > 1e-9 else 0.0,
                    "vat_rate": round(float(rate), 6) if abs(rate) > 1e-9 else None,
                    "source_row_total": r
                }

        if best_total is not None:
            out.append(best_total)
        else:
            # pas de total détecté => on ignore en ventes (car tu veux 1 écriture / facture basée sur Montant du)
            out.append({
                "invoice_number": str(inv),
                "invoice_date": inv_date,
                "total_ttc": None,
                "total_vat": 0.0,
                "vat_rate": None,
                "source_row_total": None
            })

    return pd.DataFrame(out)


def _infer_rate_from_ttc_vat(ttc: float, vat: float) -> float | None:
    """
    Si on a TTC et TVA, on peut déduire le taux :
      vat = ht * rate, ht = ttc - vat => rate = vat / (ttc - vat)
    """
    ht = ttc - vat
    if abs(ht) < 1e-9:
        return None
    rate = vat / ht
    if rate < 0 or rate > 1.0:
        return None
    return round(float(rate), 6)


def _closest_rate_in_map(rate: float, vat_map: dict, tol: float = 0.002) -> float | None:
    """Matche le taux au plus proche dans la grille TVA (tolérance ~0,2 point)."""
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
    """
    1 écriture par facture (EcritureNum = numéro facture, PieceRef = numéro facture).
    TTC = Montant du (ligne total).
    TVA = recalcul via taux si dispo / sinon taux déduit / sinon TVA ligne total.
    """
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

        # 1) déterminer le taux : priorité au taux lu, sinon déduction via TTC/TVA
        rate = invr["vat_rate"]
        if rate is not None and not pd.isna(rate):
            rate = round(float(rate), 6)
        else:
            rate = _infer_rate_from_ttc_vat(ttc, vat_amt_line) if abs(vat_amt_line) > 0.009 else None

        # 2) si on a un taux, on RECALCULE la TVA selon ta règle
        if rate is not None:
            tva = round((ttc / (1.0 + rate)) * rate, 2)
        else:
            tva = vat_amt_line

        ht = round(ttc - tva, 2)

        # 3) mapping comptes selon le taux (en rapprochant au plus proche)
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
            warnings.append({"invoice_number": inv, "invoice_date": dt, "reason": f"Taux non trouvé/mappé (rate={rate}) -> HT en {compte_70_controle} + TVA fallback"})

        # ✅ Tu voulais : EcritureNum = numéro facture (pas de -VT)
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
# Encaissements (inchangé)
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
# Streamlit UI
# ============================
st.set_page_config(page_title="Optimum → FEC (ventes par facture)", layout="wide")
st.title("Export Optimum/AS3 → FEC (Ventes = 1 écriture par facture)")

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

if not uploaded_caisse:
    st.info("Charge le fichier CAISSE.")
    st.stop()

file_bytes_caisse = uploaded_caisse.read()
sheets_caisse = list_sheets(file_bytes_caisse)
sheet_caisse = st.selectbox("Onglet CAISSE à utiliser", sheets_caisse, index=0)
raw_caisse = read_sheet_raw(file_bytes_caisse, sheet_caisse)

vat_map = build_vat_map_from_csv(vat_text)
mode_acc, mode_lib = build_mode_map_from_csv(mode_text)

# ==== Extraction factures (ventes) + encaissements
invoices_sales = extract_invoice_sales_bundle(raw_caisse)
enc = extract_encaissements(raw_caisse)

# ==== VENTES : 1 écriture par facture, TVA recalculée via taux si dispo
fec_sales, sales_warnings = build_fec_sales_per_invoice(
    invoices=invoices_sales,
    journal_code=jv_code,
    journal_lib=jv_lib,
    compte_53=compte_53,
    lib_53=lib_53,
    vat_map=vat_map,
    compte_70_controle=compte_70_controle,
    lib_70_controle=lib_70_controle,
    compte_tva_fallback=compte_tva_fallback,
    lib_tva_fallback=lib_tva_fallback,
)

# ==== ENCAISSEMENTS inchangés
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

fec_all = pd.concat([fec_sales, fec_sett], ignore_index=True)

# ==== Affichages
st.subheader("Aperçu factures (base ventes)")
st.dataframe(invoices_sales.head(200), use_container_width=True)

if sales_warnings is not None and not sales_warnings.empty:
    st.subheader("Avertissements ventes")
    st.dataframe(sales_warnings, use_container_width=True)

st.subheader("Aperçu encaissements")
st.dataframe(enc.head(200), use_container_width=True)

st.subheader("Aperçu FEC (global)")
st.dataframe(fec_all.head(300), use_container_width=True)

st.subheader("Contrôles d'équilibre")
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

st.subheader("Téléchargements")
c1, c2, c3 = st.columns(3)
with c1:
    st.download_button("CSV FEC - Ventes (VT)", data=to_csv_bytes(fec_sales, sep=csv_sep), file_name="fec_ventes.csv", mime="text/csv")
with c2:
    st.download_button("CSV FEC - Encaissements", data=to_csv_bytes(fec_sett, sep=csv_sep), file_name="fec_encaissements.csv", mime="text/csv")
with c3:
    st.download_button("CSV FEC - Global", data=to_csv_bytes(fec_all, sep=csv_sep), file_name="fec_global.csv", mime="text/csv")
