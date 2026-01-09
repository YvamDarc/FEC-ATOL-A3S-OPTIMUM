import re
from datetime import datetime
from io import BytesIO

import numpy as np
import pandas as pd
import streamlit as st


# -----------------------------
# Constantes / colonnes FEC
# -----------------------------
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


# -----------------------------
# Helpers parsing
# -----------------------------
def normalize_mode(x: str) -> str:
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


def read_export_to_df(file_bytes: bytes) -> pd.DataFrame:
    bio = BytesIO(file_bytes)
    return pd.read_excel(bio, sheet_name=0, header=None, engine="openpyxl")


def find_facture_rows(raw: pd.DataFrame) -> list[tuple[int, str, str]]:
    res = []
    for i in range(len(raw)):
        row = raw.iloc[i].astype(str).tolist()
        joined = " | ".join([x for x in row if x and x.lower() != "nan"])
        m = FACTURE_RE.search(joined)
        if m:
            res.append((i, m.group(1), m.group(2)))
    return res


def find_header_row(raw: pd.DataFrame, start_row: int, end_row: int, required_labels: list[str]) -> tuple[int | None, dict]:
    """
    Find the header row containing all required labels (case-insensitive),
    return (row_index, {label: col_index})
    """
    req = [x.strip().lower() for x in required_labels]
    for r in range(start_row, min(end_row, len(raw))):
        row = raw.iloc[r].astype(str).str.strip().str.lower().tolist()
        row = [x.replace("\u00a0", " ") for x in row]
        col_map = {}
        for label in req:
            found = None
            for c, cell in enumerate(row):
                if label in cell:
                    found = c
                    break
            if found is None:
                col_map = {}
                break
            col_map[label] = found
        if col_map:
            return r, col_map
    return None, {}


# -----------------------------
# Extraction ventes (articles)
# -----------------------------
def extract_sales_lines(raw: pd.DataFrame) -> pd.DataFrame:
    """
    Return normalized sales lines:
    invoice_number, invoice_date, tva_rate, ttc_net (Montant du)
    """
    factures = find_facture_rows(raw)
    if not factures:
        return pd.DataFrame(columns=["invoice_number", "invoice_date", "tva_rate", "ttc_net", "source_row"])

    factures_with_end = factures + [(len(raw), "", "")]
    rows = []

    for idx in range(len(factures)):
        r0, inv, date_str = factures[idx]
        r1 = factures_with_end[idx + 1][0]

        # Header articles: needs at least Produits, TVA, Montant du
        header_row, cols = find_header_row(
            raw, r0, r1,
            required_labels=["produits", "tva", "montant du"]
        )
        if header_row is None:
            continue

        c_prod = cols["produits"]
        c_tva = cols["tva"]
        c_montant_du = cols["montant du"]

        inv_date = datetime.strptime(date_str, "%d/%m/%Y").date()

        for r in range(header_row + 1, r1):
            prod = raw.iat[r, c_prod]
            tva_cell = raw.iat[r, c_tva]
            mdu = raw.iat[r, c_montant_du]

            prod_s = "" if prod is None else str(prod).strip()
            if prod_s == "" or prod_s.lower() == "nan":
                # ignore subtotal / blank lines
                continue

            tva_rate = parse_tva_rate(tva_cell)
            ttc_net = parse_eur(mdu)

            # keep meaningful sale lines (including small lines but exclude zeros)
            if abs(ttc_net) < 1e-9:
                continue

            rows.append({
                "invoice_number": str(inv),
                "invoice_date": inv_date,
                "tva_rate": float(tva_rate),
                "ttc_net": round(float(ttc_net), 2),
                "source_row": r
            })

    return pd.DataFrame(rows)


# -----------------------------
# Extraction encaissements
# -----------------------------
def extract_encaissements(raw: pd.DataFrame) -> pd.DataFrame:
    """
    Return normalized payments:
    invoice_number, invoice_date, amount, mode
    """
    factures = find_facture_rows(raw)
    if not factures:
        return pd.DataFrame(columns=["invoice_number", "invoice_date", "amount", "mode", "source_row"])

    factures_with_end = factures + [(len(raw), "", "")]
    rows = []

    for idx in range(len(factures)):
        r0, inv, date_str = factures[idx]
        r1 = factures_with_end[idx + 1][0]

        header_row, cols = find_header_row(
            raw, r0, r1,
            required_labels=["montant encaissé", "mode de règlement"]
        )
        if header_row is None:
            continue

        c_amt = cols["montant encaissé"]
        c_mode = cols["mode de règlement"]

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


# -----------------------------
# Génération écritures ventes (Débit 53 / Crédit 70+TVA)
# -----------------------------
def build_fec_sales(sales_lines: pd.DataFrame,
                    journal_code: str,
                    journal_lib: str,
                    compte_53: str,
                    lib_53: str,
                    vat_map: dict,
                    group_per_invoice_and_rate: bool = True) -> pd.DataFrame:
    """
    vat_map: {rate: {"rev_acc":..., "rev_lib":..., "vat_acc":..., "vat_lib":...}}
    """
    if sales_lines.empty:
        return pd.DataFrame(columns=FEC_COLUMNS)

    df = sales_lines.copy()

    if group_per_invoice_and_rate:
        df = df.groupby(["invoice_number", "invoice_date", "tva_rate"], as_index=False)["ttc_net"].sum()

    fec_rows = []

    for _, row in df.iterrows():
        inv = str(row["invoice_number"])
        dt = row["invoice_date"]
        rate = float(row["tva_rate"])
        ttc = float(row["ttc_net"])

        # compute HT + TVA from TTC and rate
        ht = ttc / (1.0 + rate) if (1.0 + rate) != 0 else ttc
        tva = ttc - ht

        ht = round(ht, 2)
        tva = round(tva, 2)
        ttc = round(ttc, 2)

        key = rate
        if key not in vat_map:
            # if not found exact, try rounding (common)
            key2 = round(rate, 4)
            if key2 in vat_map:
                key = key2
            else:
                # skip unmapped rates
                continue

        rev_acc = vat_map[key]["rev_acc"]
        rev_lib = vat_map[key]["rev_lib"]
        vat_acc = vat_map[key]["vat_acc"]
        vat_lib = vat_map[key]["vat_lib"]

        ecriture_num = f"{inv}-VT"
        piece_ref = inv
        lib = f"Vente facture {inv} TVA {rate*100:.2f}%"

        # Debit 53 (TTC)
        fec_rows.append({
            "JournalCode": journal_code,
            "JournalLib": journal_lib,
            "EcritureNum": ecriture_num,
            "EcritureDate": dt.strftime("%Y%m%d"),
            "CompteNum": compte_53,
            "CompteLib": lib_53,
            "CompAuxNum": "",
            "CompAuxLib": "",
            "PieceRef": piece_ref,
            "PieceDate": dt.strftime("%Y%m%d"),
            "EcritureLib": lib,
            "Debit": ttc,
            "Credit": 0.0,
            "EcritureLet": "",
            "DateLet": "",
            "ValidDate": dt.strftime("%Y%m%d"),
            "Montantdevise": "",
            "Idevise": ""
        })

        # Credit 70 (HT)
        fec_rows.append({
            "JournalCode": journal_code,
            "JournalLib": journal_lib,
            "EcritureNum": ecriture_num,
            "EcritureDate": dt.strftime("%Y%m%d"),
            "CompteNum": rev_acc,
            "CompteLib": rev_lib,
            "CompAuxNum": "",
            "CompAuxLib": "",
            "PieceRef": piece_ref,
            "PieceDate": dt.strftime("%Y%m%d"),
            "EcritureLib": lib,
            "Debit": 0.0,
            "Credit": ht,
            "EcritureLet": "",
            "DateLet": "",
            "ValidDate": dt.strftime("%Y%m%d"),
            "Montantdevise": "",
            "Idevise": ""
        })

        # Credit TVA
        if abs(tva) > 0.009:
            fec_rows.append({
                "JournalCode": journal_code,
                "JournalLib": journal_lib,
                "EcritureNum": ecriture_num,
                "EcritureDate": dt.strftime("%Y%m%d"),
                "CompteNum": vat_acc,
                "CompteLib": vat_lib,
                "CompAuxNum": "",
                "CompAuxLib": "",
                "PieceRef": piece_ref,
                "PieceDate": dt.strftime("%Y%m%d"),
                "EcritureLib": lib,
                "Debit": 0.0,
                "Credit": tva,
                "EcritureLet": "",
                "DateLet": "",
                "ValidDate": dt.strftime("%Y%m%d"),
                "Montantdevise": "",
                "Idevise": ""
            })

    fec = pd.DataFrame(fec_rows, columns=FEC_COLUMNS)
    for col in ["Debit", "Credit"]:
        fec[col] = pd.to_numeric(fec[col], errors="coerce").fillna(0.0).round(2)

    return fec


# -----------------------------
# Génération écritures encaissements (Crédit 53 / Débit comptes règlements)
# -----------------------------
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
        piece_ref = inv
        lib = f"Encaissement facture {inv} ({mode})"

        # Debit payment account
        fec_rows.append({
            "JournalCode": journal_code,
            "JournalLib": journal_lib,
            "EcritureNum": ecriture_num,
            "EcritureDate": dt.strftime("%Y%m%d"),
            "CompteNum": debit_acc,
            "CompteLib": debit_lib,
            "CompAuxNum": "",
            "CompAuxLib": "",
            "PieceRef": piece_ref,
            "PieceDate": dt.strftime("%Y%m%d"),
            "EcritureLib": lib,
            "Debit": amt,
            "Credit": 0.0,
            "EcritureLet": "",
            "DateLet": "",
            "ValidDate": dt.strftime("%Y%m%d"),
            "Montantdevise": "",
            "Idevise": ""
        })

        # Credit 53
        fec_rows.append({
            "JournalCode": journal_code,
            "JournalLib": journal_lib,
            "EcritureNum": ecriture_num,
            "EcritureDate": dt.strftime("%Y%m%d"),
            "CompteNum": compte_53,
            "CompteLib": lib_53,
            "CompAuxNum": "",
            "CompAuxLib": "",
            "PieceRef": piece_ref,
            "PieceDate": dt.strftime("%Y%m%d"),
            "EcritureLib": lib,
            "Debit": 0.0,
            "Credit": amt,
            "EcritureLet": "",
            "DateLet": "",
            "ValidDate": dt.strftime("%Y%m%d"),
            "Montantdevise": "",
            "Idevise": ""
        })

    fec = pd.DataFrame(fec_rows, columns=FEC_COLUMNS)
    for col in ["Debit", "Credit"]:
        fec[col] = pd.to_numeric(fec[col], errors="coerce").fillna(0.0).round(2)

    return fec


def to_csv_bytes(df: pd.DataFrame, sep: str = ";") -> bytes:
    return df.to_csv(index=False, sep=sep, encoding="utf-8-sig").encode("utf-8-sig")


def check_balance(fec: pd.DataFrame) -> pd.DataFrame:
    if fec.empty:
        return pd.DataFrame()
    chk = fec.groupby(["JournalCode", "EcritureNum"])[["Debit", "Credit"]].sum()
    chk["Delta"] = (chk["Debit"] - chk["Credit"]).round(2)
    return chk


# -----------------------------
# UI Streamlit
# -----------------------------
st.set_page_config(page_title="Caisse Optimum → Ventes + Encaissements (FEC)", layout="wide")
st.title("Export caisse (Optimum/AS3) → Écritures ventes + encaissements (format FEC)")

uploaded = st.file_uploader("Importer le fichier (.xlsx export caisse)", type=["xlsx", "xls"])

with st.sidebar:
    st.header("Paramètres")

    st.subheader("Compte 53 (caisse à ventiler)")
    compte_53 = st.text_input("Compte 53", value="530000")
    lib_53 = st.text_input("Libellé 53", value="Caisse à ventiler")

    st.subheader("Journal VENTES (constatation CA)")
    jv_code = st.text_input("JournalCode ventes", value="VT")
    jv_lib = st.text_input("JournalLib ventes", value="Ventes caisse")

    st.subheader("Journal ENCAISSEMENTS (règlements)")
    je_code = st.text_input("JournalCode encaissements", value="BQ")
    je_lib = st.text_input("JournalLib encaissements", value="Banque / règlements")

    st.subheader("Mapping TVA → comptes 70 + TVA")
    st.caption("Une ligne par taux (ex: 0.20, 0.10, 0.055). Tu peux n’avoir que 2 comptes TVA si tu veux.")
    default_vat_map = pd.DataFrame([
        {"TauxTVA": 0.20, "Compte70": "707000", "Lib70": "Ventes de marchandises", "CompteTVA": "445710", "LibTVA": "TVA collectée 20%"},
        {"TauxTVA": 0.10, "Compte70": "707010", "Lib70": "Ventes taux 10%", "CompteTVA": "445712", "LibTVA": "TVA collectée 10%"},
        {"TauxTVA": 0.055, "Compte70": "707005", "Lib70": "Ventes taux 5,5%", "CompteTVA": "445713", "LibTVA": "TVA collectée 5,5%"},
        {"TauxTVA": 0.00, "Compte70": "707000", "Lib70": "Ventes exonérées", "CompteTVA": "445700", "LibTVA": "TVA collectée 0%"},
    ])
    vat_editor = st.data_editor(default_vat_map, use_container_width=True, num_rows="dynamic", key="vat_editor")

    st.subheader("Mapping modes → comptes (Débit)")
    default_mode_map = pd.DataFrame([
        {"Mode": "carte bancaire", "CompteNum": "511000", "CompteLib": "CB à encaisser"},
        {"Mode": "chèque", "CompteNum": "511200", "CompteLib": "Chèques à encaisser"},
        {"Mode": "espèces", "CompteNum": "531000", "CompteLib": "Caisse"},
        {"Mode": "virement", "CompteNum": "512000", "CompteLib": "Banque"},
        {"Mode": "tiers-payant", "CompteNum": "467000", "CompteLib": "Tiers payant à recevoir"},
    ])
    mode_editor = st.data_editor(default_mode_map, use_container_width=True, num_rows="dynamic", key="mode_editor")

    st.subheader("Options")
    group_sales = st.checkbox("Regrouper ventes par facture + taux TVA", value=True)
    group_payments = st.checkbox("Regrouper encaissements par facture + mode", value=True)

    st.subheader("Exports")
    csv_sep = st.selectbox("Séparateur CSV", options=[";", ",", "\t"], index=0)

if uploaded:
    raw = read_export_to_df(uploaded.read())

    sales_lines = extract_sales_lines(raw)
    enc = extract_encaissements(raw)

    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.metric("Lignes ventes", int(len(sales_lines)))
    with c2:
        st.metric("Factures (ventes)", int(sales_lines["invoice_number"].nunique()) if not sales_lines.empty else 0)
    with c3:
        st.metric("Lignes encaissements", int(len(enc)))
    with c4:
        st.metric("Total encaissé", f"{enc['amount'].sum():,.2f} €".replace(",", " ") if not enc.empty else "0,00 €")

    st.subheader("Aperçu ventes (lignes articles)")
    st.dataframe(sales_lines.head(200), use_container_width=True)

    st.subheader("Aperçu encaissements")
    st.dataframe(enc.head(200), use_container_width=True)

    # Build vat_map dict
    vat_map = {}
    for _, r in vat_editor.iterrows():
        try:
            rate = float(r.get("TauxTVA"))
        except Exception:
            continue
        vat_map[round(rate, 6)] = {
            "rev_acc": str(r.get("Compte70", "")).strip(),
            "rev_lib": str(r.get("Lib70", "")).strip(),
            "vat_acc": str(r.get("CompteTVA", "")).strip(),
            "vat_lib": str(r.get("LibTVA", "")).strip(),
        }

    # Build mode maps
    mode_to_debit_account = {}
    mode_to_debit_lib = {}
    for _, r in mode_editor.iterrows():
        md = normalize_mode(r.get("Mode"))
        if not md:
            continue
        mode_to_debit_account[md] = str(r.get("CompteNum", "")).strip()
        mode_to_debit_lib[md] = str(r.get("CompteLib", "")).strip()

    # Warn unmapped VAT rates
    if not sales_lines.empty:
        rates = sorted(set([round(float(x), 6) for x in sales_lines["tva_rate"].unique().tolist()]))
        unmapped_rates = [x for x in rates if x not in vat_map]
        if unmapped_rates:
            st.warning("Taux TVA détectés sans mapping (ils seront ignorés en génération ventes) : "
                       + ", ".join([str(x) for x in unmapped_rates]))

    # Warn unmapped payment modes
    if not enc.empty:
        modes = sorted(enc["mode"].unique().tolist())
        unmapped_modes = [m for m in modes if not mode_to_debit_account.get(m)]
        if unmapped_modes:
            st.warning("Modes de règlement détectés sans compte (ils seront ignorés en génération encaissements) : "
                       + ", ".join(unmapped_modes))

    # Generate FEC
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
        mode_to_debit_account=mode_to_debit_account,
        mode_to_debit_lib=mode_to_debit_lib,
        group_same_mode_per_invoice=group_payments,
    )

    fec_all = pd.concat([fec_sales, fec_sett], ignore_index=True)

    st.subheader("Contrôles d’équilibre")
    chk_sales = check_balance(fec_sales)
    chk_sett = check_balance(fec_sett)
    chk_all = check_balance(fec_all)

    colA, colB, colC = st.columns(3)
    with colA:
        st.write("Ventes")
        if chk_sales.empty:
            st.info("Aucune écriture ventes.")
        else:
            bad = chk_sales[chk_sales["Delta"].abs() > 0.01]
            st.dataframe(bad if not bad.empty else chk_sales.head(20), use_container_width=True)
            st.success("OK ✅" if bad.empty else "KO ❌ (voir lignes)")
    with colB:
        st.write("Encaissements")
        if chk_sett.empty:
            st.info("Aucune écriture encaissement.")
        else:
            bad = chk_sett[chk_sett["Delta"].abs() > 0.01]
            st.dataframe(bad if not bad.empty else chk_sett.head(20), use_container_width=True)
            st.success("OK ✅" if bad.empty else "KO ❌ (voir lignes)")
    with colC:
        st.write("Global")
        if chk_all.empty:
            st.info("Aucune écriture.")
        else:
            bad = chk_all[chk_all["Delta"].abs() > 0.01]
            st.dataframe(bad if not bad.empty else chk_all.head(20), use_container_width=True)
            st.success("OK ✅" if bad.empty else "KO ❌ (voir lignes)")

    st.subheader("Aperçu FEC Ventes")
    st.dataframe(fec_sales.head(200), use_container_width=True)

    st.subheader("Aperçu FEC Encaissements")
    st.dataframe(fec_sett.head(200), use_container_width=True)

    st.subheader("Téléchargements")
    d1, d2, d3 = st.columns(3)
    with d1:
        st.download_button("CSV FEC - Ventes",
                           data=to_csv_bytes(fec_sales, sep=csv_sep),
                           file_name="fec_ventes.csv",
                           mime="text/csv")
    with d2:
        st.download_button("CSV FEC - Encaissements",
                           data=to_csv_bytes(fec_sett, sep=csv_sep),
                           file_name="fec_encaissements.csv",
                           mime="text/csv")
    with d3:
        st.download_button("CSV FEC - Global",
                           data=to_csv_bytes(fec_all, sep=csv_sep),
                           file_name="fec_global.csv",
                           mime="text/csv")

else:
    st.write("👉 Importe l’export caisse en .xlsx")
    st.caption("Cette version génère : ventes (Débit 53 / Crédit 70+TVA) + encaissements (Débit règlement / Crédit 53).")
