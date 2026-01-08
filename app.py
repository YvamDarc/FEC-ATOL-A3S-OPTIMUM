import re
from datetime import datetime
from io import BytesIO

import numpy as np
import pandas as pd
import streamlit as st


# -----------------------------
# Helpers
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

MODE_NORMALIZE = {
    "carte bancaire": "carte bancaire",
    "cb": "carte bancaire",
    "carte": "carte bancaire",
    "chè que": "chèque",
    "cheque": "chèque",
    "chèque": "chèque",
    "especes": "espèces",
    "espèces": "espèces",
    "virement": "virement",
    "tiers payant": "tiers-payant",
    "tiers-payant": "tiers-payant",
    "tierspayant": "tiers-payant",
}


def normalize_mode(x: str) -> str:
    if x is None or (isinstance(x, float) and np.isnan(x)):
        return ""
    s = str(x).strip().lower()
    s = re.sub(r"\s+", " ", s)
    s = s.replace("’", "-").replace("'", "-")
    s = s.replace("tiers payant", "tiers-payant")
    s = s.replace("tierspayant", "tiers-payant")
    return MODE_NORMALIZE.get(s, s)


def parse_eur(val) -> float:
    """Parse values like '156,85€', '13,00€', 13, 13.0, '0,00€'."""
    if val is None:
        return 0.0
    if isinstance(val, (int, float, np.integer, np.floating)):
        if pd.isna(val):
            return 0.0
        return float(val)

    s = str(val).strip()
    if s == "" or s.lower() == "nan":
        return 0.0

    # remove currency and spaces
    s = s.replace("€", "").replace("\u00a0", " ").strip()
    s = s.replace(" ", "")

    # handle european decimals
    # if both comma and dot exist, assume dot is thousand sep -> remove dots then comma->dot
    if "," in s and "." in s:
        s = s.replace(".", "")
    s = s.replace(",", ".")

    # keep only valid characters
    s = re.sub(r"[^0-9\.\-]", "", s)
    if s in ("", ".", "-", "-."):
        return 0.0

    try:
        return float(s)
    except ValueError:
        return 0.0


def excel_to_df(file_bytes: bytes) -> pd.DataFrame:
    # Read raw to preserve weird headers/merged cells
    bio = BytesIO(file_bytes)
    df = pd.read_excel(bio, sheet_name=0, header=None, engine="openpyxl")
    # normalize cells to strings or keep numeric, but easier to scan with strings
    return df


FACTURE_RE = re.compile(r"Facture numéro\s+(\d+)\s+émise le\s+(\d{2}/\d{2}/\d{4})", re.IGNORECASE)


def find_facture_rows(raw: pd.DataFrame) -> list[tuple[int, str, str]]:
    """Return list of (row_index, invoice_number, invoice_date_str)."""
    res = []
    for i in range(len(raw)):
        row = raw.iloc[i].astype(str).tolist()
        joined = " | ".join([x for x in row if x and x.lower() != "nan"])
        m = FACTURE_RE.search(joined)
        if m:
            res.append((i, m.group(1), m.group(2)))
    return res


def find_header_row_and_cols(raw: pd.DataFrame, start_row: int, end_row: int) -> tuple[int | None, dict]:
    """
    Within [start_row, end_row), find the row containing headers including:
    - 'Montant encaissé'
    - 'Mode de règlement'
    Return (header_row_index, col_map)
    col_map = {'montant': idx, 'mode': idx}
    """
    target_a = "montant encaissé"
    target_b = "mode de règlement"
    for r in range(start_row, min(end_row, len(raw))):
        row = raw.iloc[r].astype(str).str.strip().str.lower().tolist()
        # normalize
        row_norm = [x.replace("\u00a0", " ") for x in row]
        montant_idx = None
        mode_idx = None
        for c, cell in enumerate(row_norm):
            if target_a in cell:
                montant_idx = c
            if target_b in cell:
                mode_idx = c
        if montant_idx is not None and mode_idx is not None:
            return r, {"montant": montant_idx, "mode": mode_idx}
    return None, {}


def extract_encaissements(raw: pd.DataFrame) -> pd.DataFrame:
    """
    Parse the export into a normalized table:
    invoice_number, invoice_date, amount, mode
    """
    factures = find_facture_rows(raw)
    if not factures:
        return pd.DataFrame(columns=["invoice_number", "invoice_date", "amount", "mode", "source_row"])

    # Add sentinel end
    factures_with_end = factures + [(len(raw), "", "")]
    rows = []

    for idx in range(len(factures)):
        r0, inv, date_str = factures[idx]
        r1 = factures_with_end[idx + 1][0]

        header_row, cols = find_header_row_and_cols(raw, r0, r1)
        if header_row is None:
            continue

        # data starts after header_row
        for r in range(header_row + 1, r1):
            montant = raw.iat[r, cols["montant"]] if cols.get("montant") is not None else None
            mode = raw.iat[r, cols["mode"]] if cols.get("mode") is not None else None

            amt = parse_eur(montant)
            md = normalize_mode(mode)

            # Keep only meaningful payment rows
            if md and abs(amt) > 1e-9:
                rows.append({
                    "invoice_number": inv,
                    "invoice_date": datetime.strptime(date_str, "%d/%m/%Y").date(),
                    "amount": round(float(amt), 2),
                    "mode": md,
                    "source_row": r
                })

    return pd.DataFrame(rows)


def build_fec_settlement(enc_df: pd.DataFrame,
                         journal_code: str,
                         journal_lib: str,
                         credit_account_411: str,
                         credit_account_lib: str,
                         mode_to_debit_account: dict,
                         mode_to_debit_lib: dict,
                         piece_ref_prefix: str = "",
                         ecriture_lib_prefix: str = "Encaissement facture",
                         group_same_mode_per_invoice: bool = True) -> pd.DataFrame:
    """
    Build settlement entries:
    Debit: payment account (by mode)
    Credit: 411 (client collectif)
    One entry per invoice+mode (or per line if not grouped).
    """
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
        amt = float(row["amount"])

        debit_acc = mode_to_debit_account.get(mode, "")
        debit_lib = mode_to_debit_lib.get(mode, f"Règlement {mode}".strip())

        if not debit_acc:
            # skip if unmapped
            continue

        ecriture_num = f"{inv}-ENC"
        piece_ref = f"{piece_ref_prefix}{inv}".strip()
        ecr_lib = f"{ecriture_lib_prefix} {inv} ({mode})"

        # Debit line
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
            "EcritureLib": ecr_lib,
            "Debit": round(amt, 2),
            "Credit": 0.0,
            "EcritureLet": "",
            "DateLet": "",
            "ValidDate": dt.strftime("%Y%m%d"),
            "Montantdevise": "",
            "Idevise": ""
        })

        # Credit line
        fec_rows.append({
            "JournalCode": journal_code,
            "JournalLib": journal_lib,
            "EcritureNum": ecriture_num,
            "EcritureDate": dt.strftime("%Y%m%d"),
            "CompteNum": credit_account_411,
            "CompteLib": credit_account_lib,
            "CompAuxNum": "",
            "CompAuxLib": "",
            "PieceRef": piece_ref,
            "PieceDate": dt.strftime("%Y%m%d"),
            "EcritureLib": ecr_lib,
            "Debit": 0.0,
            "Credit": round(amt, 2),
            "EcritureLet": "",
            "DateLet": "",
            "ValidDate": dt.strftime("%Y%m%d"),
            "Montantdevise": "",
            "Idevise": ""
        })

    fec = pd.DataFrame(fec_rows, columns=FEC_COLUMNS)

    # Ensure numeric formatting
    for col in ["Debit", "Credit"]:
        fec[col] = pd.to_numeric(fec[col], errors="coerce").fillna(0.0).round(2)

    return fec


def to_csv_bytes(df: pd.DataFrame, sep: str = ";") -> bytes:
    return df.to_csv(index=False, sep=sep, encoding="utf-8-sig").encode("utf-8-sig")


# -----------------------------
# Streamlit UI
# -----------------------------
st.set_page_config(page_title="Excel caisse → écritures (format FEC)", layout="wide")
st.title("Excel caisse (AS3 / Optimum) → Écritures d'encaissement (CSV format FEC)")

st.info(
    "Cette app lit un export Excel de relevé de caisse (factures + encaissements) et génère des écritures "
    "d'encaissement : Débit compte de règlement / Crédit 411. "
    "Tu peux mapper les modes de règlement vers tes comptes."
)

uploaded = st.file_uploader("Importer l'Excel (export caisse)", type=["xlsx", "xls"])

with st.sidebar:
    st.header("Paramètres comptables")

    journal_code = st.text_input("JournalCode", value="BQ")
    journal_lib = st.text_input("JournalLib", value="Banque / Encaissements")

    st.subheader("Compte crédit (collectif clients)")
    credit_411 = st.text_input("CompteNum crédit", value="411000")
    credit_411_lib = st.text_input("CompteLib crédit", value="Clients")

    st.subheader("Mapping modes → comptes (Débit)")
    default_map = {
        "carte bancaire": ("511000", "CB à encaisser"),
        "chèque": ("511200", "Chèques à encaisser"),
        "espèces": ("531000", "Caisse"),
        "virement": ("512000", "Banque"),
        "tiers-payant": ("467000", "Tiers payant à recevoir"),
    }

    # editable mapping table
    map_df = pd.DataFrame(
        [{"Mode": k, "CompteNum": v[0], "CompteLib": v[1]} for k, v in default_map.items()]
    )
    map_df = st.data_editor(
        map_df,
        use_container_width=True,
        num_rows="dynamic",
        key="map_editor"
    )

    group_same_mode_per_invoice = st.checkbox(
        "Regrouper par facture + mode (recommandé)",
        value=True
    )

    st.subheader("Exports CSV")
    csv_sep = st.selectbox("Séparateur", options=[";", ",", "\t"], index=0)
    st.caption("En France, la plupart des imports attendent ';'.")

if uploaded:
    file_bytes = uploaded.read()

    try:
        raw = excel_to_df(file_bytes)
    except Exception as e:
        st.error(f"Impossible de lire l'Excel : {e}")
        st.stop()

    enc = extract_encaissements(raw)

    if enc.empty:
        st.warning("Aucun encaissement détecté. Vérifie que les colonnes 'Montant encaissé' et 'Mode de règlement' existent dans ton export.")
        st.dataframe(raw.head(50), use_container_width=True)
        st.stop()

    # Build mapping dicts
    mode_to_debit_account = {}
    mode_to_debit_lib = {}
    for _, r in map_df.iterrows():
        md = normalize_mode(r.get("Mode"))
        acc = str(r.get("CompteNum", "")).strip()
        lib = str(r.get("CompteLib", "")).strip()
        if md:
            mode_to_debit_account[md] = acc
            mode_to_debit_lib[md] = lib

    # Summary
    st.subheader("Encaissements détectés")
    c1, c2, c3 = st.columns(3)
    with c1:
        st.metric("Nb lignes encaissement", int(len(enc)))
    with c2:
        st.metric("Nb factures", int(enc["invoice_number"].nunique()))
    with c3:
        st.metric("Total encaissé", f"{enc['amount'].sum():,.2f} €".replace(",", " "))

    # Show pivot by mode
    pivot = enc.pivot_table(index="mode", values="amount", aggfunc="sum").sort_values("amount", ascending=False)
    st.write("Répartition par mode :")
    st.dataframe(pivot, use_container_width=True)

    st.write("Détail encaissements (extrait) :")
    st.dataframe(enc.head(200), use_container_width=True)

    # Generate FEC entries
    fec = build_fec_settlement(
        enc_df=enc,
        journal_code=journal_code,
        journal_lib=journal_lib,
        credit_account_411=credit_411,
        credit_account_lib=credit_411_lib,
        mode_to_debit_account=mode_to_debit_account,
        mode_to_debit_lib=mode_to_debit_lib,
        group_same_mode_per_invoice=group_same_mode_per_invoice,
    )

    # detect unmapped modes
    detected_modes = sorted(enc["mode"].unique().tolist())
    unmapped = [m for m in detected_modes if not mode_to_debit_account.get(m)]
    if unmapped:
        st.warning("Modes détectés sans compte associé (ils seront ignorés dans la génération) : " + ", ".join(unmapped))

    if fec.empty:
        st.error("Aucune écriture générée (mapping incomplet ?).")
        st.stop()

    # Basic balance check per EcritureNum
    check = fec.groupby("EcritureNum")[["Debit", "Credit"]].sum()
    check["Delta"] = (check["Debit"] - check["Credit"]).round(2)
    bad = check[check["Delta"].abs() > 0.01]
    if not bad.empty:
        st.error("Certaines écritures ne sont pas équilibrées (à investiguer) :")
        st.dataframe(bad, use_container_width=True)
    else:
        st.success("Écritures équilibrées ✅")

    st.subheader("Écritures (format FEC) - aperçu")
    st.dataframe(fec.head(200), use_container_width=True)

    # Downloads
    colA, colB = st.columns(2)
    with colA:
        st.download_button(
            "Télécharger CSV ÉCRITURES (détail)",
            data=to_csv_bytes(enc, sep=csv_sep),
            file_name="encaissements_detectes.csv",
            mime="text/csv"
        )
    with colB:
        st.download_button(
            "Télécharger CSV format FEC (encaissements)",
            data=to_csv_bytes(fec, sep=csv_sep),
            file_name="fec_encaissements.csv",
            mime="text/csv"
        )

else:
    st.write("👉 Importe un export Excel de caisse pour générer les écritures d'encaissement.")
    st.caption("Astuce : si ton export a plusieurs onglets, on peut ajouter un sélecteur d'onglet facilement.")
