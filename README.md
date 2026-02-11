# Bond Transfer Reconciliation System
_Phase‑1 & Phase‑2 Orchestrated Streamlit Application_

---

## 📌 Purpose

This application automates **bond transfer reconciliation** by validating depository transactions against internal records.

It checks whether bond transfers:
- Went to the **correct client**
- Were done on the **correct / acceptable date**
- Had the **correct number of units**
- Are correctly marked as **Transferred** internally

The final output is a **color‑coded Excel reconciliation report** with a focused **Exceptions** sheet for operations review.

---

## 🧠 Architecture Overview

The system runs in **two phases**, controlled by a **master orchestrator**.

### Phase‑1: Transaction Normalization
Inputs:
- Transaction‑cum‑Holding CSV (report‑style)
- Demat Master Excel

What it does:
- Extracts the real transaction table from the CSV
- Filters **Debit (D)** transactions only
- Extracts **CDSL (16‑digit)** or **NSDL (IN…)** demat numbers from narration
- Maps demat numbers to **Client Names**

Output:
- Clean, standardized transaction dataset

---

### Phase‑2: Reconciliation Against Internal Records
Inputs:
- Phase‑1 output (in‑memory)
- Macro‑enabled Excel (.xlsm)

Sheets used:
- **Bond Info**
- **KB HUF**

Matching Logic:
- ISIN → strict
- Units → strict
- Client Name → **fuzzy matching (default 95%)**
- Date logic:
  - Exact match → OK
  - PostedDate ≥ KB date → Review
  - PostedDate < KB date → Mismatch
- Status must be **Transferred**

Outputs:
- Reconciliation sheet (all transactions)
- Exceptions sheet (only non‑OK rows)

---

## 🎨 Color Coding (Excel Output)

- 🟢 Green → Correct / OK
- 🟡 Yellow → Review required (date tolerance)
- 🔴 Red → Mismatch / action needed
- ⚪ Grey → Missing / incomplete data

---

## 📁 Project Structure

```
bond-transfer-recon/
│
├── app.py                  # Master Orchestrator (Streamlit UI)
├── phase1_transfer.py      # Phase‑1 logic (no UI)
├── phase2_recon.py         # Phase‑2 logic + fuzzy matching + date filter
├── requirements.txt
└── README.md
```

---

## ▶ How to Run

### 1️⃣ Create virtual environment (recommended)
```bash
python -m venv venv
venv\Scripts\activate      # Windows
# source venv/bin/activate  # Mac / Linux
```

### 2️⃣ Install dependencies
```bash
pip install -r requirements.txt
```

### 3️⃣ Run the application
```bash
streamlit run app.py
```

Your browser will open automatically.

---

## 📥 Files Required at Runtime

- Transaction CSV report
- Demat Master Excel
- Macro‑enabled Excel (.xlsm) containing:
  - Sheet: **Bond Info**
  - Sheet: **KB HUF**

All files are uploaded via UI — no hard‑coded paths.

---

## 🔐 Git Safety

Safe to push to Git because:
- No client data stored
- No credentials or secrets
- No absolute file paths

Recommended `.gitignore`:
```
venv/
__pycache__/
*.xlsx
*.xlsm
*.csv
```

---

## 👤 Intended Users

- Operations teams
- Compliance / audit
- Portfolio / bond operations

---

## 🏁 Final Note

This system is designed to be:
- Deterministic
- Auditable
- Ops‑friendly
- Easily extensible

