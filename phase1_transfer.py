"""
phase1_transfer.py
------------------
Phase 1 core logic (no Streamlit UI):
- Read "Transaction cum Holding" report-style CSV
- Extract transaction table that starts with '"POSTED DATE"'
- Keep only Debit rows (DCFlag == "D")
- Extract Demat (NSDL IN..., else CDSL last 16 digits)
- Map Demat -> Client Name using Demat master (Col A Name, Col B CDSL16, Col C NSDL IN...)
Returns a normalized DataFrame used by Phase 2.
"""

import io
import re
from typing import Optional, Tuple, Union

import pandas as pd


def _normalize_cell(x) -> str:
    if x is None or (isinstance(x, float) and pd.isna(x)) or pd.isna(x):
        return ""
    return str(x).strip()


def _extract_nsdl_in(raw: str) -> Optional[str]:
    """Extract NSDL IN... token (spaces removed) from the end-side 'IN' occurrence."""
    s = _normalize_cell(raw).upper()
    if not s:
        return None

    s_nospace = s.replace(" ", "")
    matches = list(re.finditer(r"IN", s_nospace))
    if not matches:
        return None

    start = matches[-1].start()
    tail = s_nospace[start:]
    m = re.match(r"(IN[0-9A-Z]+)", tail)
    return m.group(1) if m else None


def _extract_last_16_digits(raw: str) -> Optional[str]:
    """Extract last 16 digits (CDSL) ignoring non-digits."""
    s = _normalize_cell(raw)
    if not s:
        return None
    digits = re.sub(r"\D", "", s)
    return digits[-16:] if len(digits) >= 16 else None


def _extract_demat(raw: str) -> Tuple[Optional[str], Optional[str]]:
    nsdl = _extract_nsdl_in(raw)
    if nsdl:
        return "NSDL", nsdl

    cdsl = _extract_last_16_digits(raw)
    if cdsl:
        return "CDSL", cdsl

    return None, None


def _read_csv_block(csv_bytes: bytes) -> pd.DataFrame:
    """Reads report-style CSV text and extracts only the transaction table."""
    text = csv_bytes.decode("utf-8", errors="replace")
    lines = text.splitlines()

    header_idx = None
    for i, line in enumerate(lines):
        if line.strip().startswith('"POSTED DATE"'):
            header_idx = i
            break
    if header_idx is None:
        raise ValueError('Could not find the transaction table header starting with `"POSTED DATE"`.')

    table_lines = []
    for j in range(header_idx, len(lines)):
        if lines[j].strip() == "":
            break
        table_lines.append(lines[j])

    if len(table_lines) < 2:
        raise ValueError("Transaction table header found, but no transaction rows detected under it.")

    df = pd.read_csv(io.StringIO("\n".join(table_lines)), dtype=str)

    rename_map = {
        "POSTED DATE": "PostedDate",
        "ISIN": "ISIN",
        "TRANSACTION DESCRIPTION": "DematRaw",
        "TRANSACTION UNITS": "Units",
        "TRANSACTION DEBIT/CREDIT FLAG (D/C)": "DCFlag",
    }
    df = df.rename(columns=rename_map)

    needed = ["PostedDate", "ISIN", "DematRaw", "Units", "DCFlag"]
    missing = [c for c in needed if c not in df.columns]
    if missing:
        raise ValueError(f"Missing expected columns in the transaction table: {missing}")

    return df[needed].copy()


def _load_demat_master_xlsx(excel_bytes: bytes) -> pd.DataFrame:
    m = pd.read_excel(io.BytesIO(excel_bytes), dtype=str, engine="openpyxl")
    if m.shape[1] < 3:
        raise ValueError("Demat master must have at least 3 columns: Name, CDSL, NSDL.")

    m = m.iloc[:, :3].copy()
    m.columns = ["Name", "CDSL_16", "NSDL_IN"]
    m["Name"] = m["Name"].apply(_normalize_cell)

    def norm_cdsl(x) -> Optional[str]:
        digits = re.sub(r"\D", "", _normalize_cell(x))
        return digits[-16:] if len(digits) >= 16 else None

    def norm_nsdl(x) -> Optional[str]:
        s = _normalize_cell(x).upper().replace(" ", "")
        return s if s else None

    m["CDSL_16"] = m["CDSL_16"].apply(norm_cdsl)
    m["NSDL_IN"] = m["NSDL_IN"].apply(norm_nsdl)

    return m


def _map_clients(txn_df: pd.DataFrame, master: pd.DataFrame) -> pd.DataFrame:
    df = txn_df.copy()
    df["DCFlag"] = df["DCFlag"].astype(str).str.strip().str.upper()
    df = df[df["DCFlag"] == "D"].copy()

    df["PostedDate"] = pd.to_datetime(df["PostedDate"], errors="coerce", dayfirst=True)
    df["ISIN"] = df["ISIN"].astype(str).str.strip().str.upper()

    df["Units"] = df["Units"].astype(str).str.replace(",", "", regex=False).str.strip()
    df["Units_num"] = pd.to_numeric(df["Units"], errors="coerce")

    extracted = df["DematRaw"].apply(lambda x: pd.Series(_extract_demat(x)))
    extracted.columns = ["MatchType", "ExtractedDemat"]
    df = pd.concat([df, extracted], axis=1)

    cdsl_map = (
        master.dropna(subset=["CDSL_16"])
        .drop_duplicates(subset=["CDSL_16"])
        .set_index("CDSL_16")["Name"]
        .to_dict()
    )
    nsdl_map = (
        master.dropna(subset=["NSDL_IN"])
        .drop_duplicates(subset=["NSDL_IN"])
        .set_index("NSDL_IN")["Name"]
        .to_dict()
    )

    def resolve(row):
        mt = row.get("MatchType")
        dem = row.get("ExtractedDemat")
        if not mt or not dem:
            return "", "NO_DEMAT_FOUND"

        if mt == "NSDL":
            key = str(dem).upper().replace(" ", "")
            name = nsdl_map.get(key, "")
            return name, ("OK" if name else "NSDL_NOT_IN_MASTER")

        if mt == "CDSL":
            key = re.sub(r"\D", "", str(dem))
            key = key[-16:] if len(key) >= 16 else key
            name = cdsl_map.get(key, "")
            return name, ("OK" if name else "CDSL_NOT_IN_MASTER")

        return "", "UNKNOWN_TYPE"

    resolved = df.apply(lambda r: pd.Series(resolve(r)), axis=1)
    resolved.columns = ["ClientName", "MatchQuality"]
    df = pd.concat([df, resolved], axis=1)

    out_cols = [
        "PostedDate",
        "ISIN",
        "Units_num",
        "ClientName",
        "MatchType",
        "ExtractedDemat",
        "MatchQuality",
        "DematRaw",
    ]
    return df[out_cols].sort_values(["PostedDate", "ISIN"], na_position="last")


def run_phase1(csv_file: Union[bytes, object], demat_master_file: Union[bytes, object]) -> pd.DataFrame:
    """Run Phase-1. Accept bytes or Streamlit UploadedFile-like objects (with .getvalue())."""
    csv_bytes = csv_file if isinstance(csv_file, (bytes, bytearray)) else csv_file.getvalue()
    master_bytes = demat_master_file if isinstance(demat_master_file, (bytes, bytearray)) else demat_master_file.getvalue()

    txns = _read_csv_block(csv_bytes)
    master = _load_demat_master_xlsx(master_bytes)
    return _map_clients(txns, master)


def phase1_to_excel_bytes(df: pd.DataFrame) -> bytes:
    """Export Phase-1 dataframe to an xlsx bytes payload."""
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Phase1_Debits")
    buf.seek(0)
    return buf.getvalue()
