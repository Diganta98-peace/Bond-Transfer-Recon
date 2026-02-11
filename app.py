import streamlit as st
import pandas as pd

from phase1_transfer import run_phase1
from phase2_recon import load_macro_sheets, run_phase2, to_colored_excel_bytes


st.set_page_config(page_title="Bond Transfer Master Orchestrator", layout="wide")
st.title("Bond Transfer Matching — Master Orchestrator (Phase 1 + Phase 2)")

# ===================== Uploads =====================
col1, col2, col3 = st.columns(3)
with col1:
    csv_file = st.file_uploader("1) Transaction CSV Report", type=["csv"])
with col2:
    demat_file = st.file_uploader("2) Demat Master", type=["xlsx", "xls"])
with col3:
    macro_file = st.file_uploader("3) Macro (.xlsm)", type=["xlsm", "xlsx"])

name_threshold = st.slider("Fuzzy match threshold (Client Name)", 80, 100, 95, 1)

# ===================== KB Date Filter =====================
kb_date_from = None
kb_date_to = None

if macro_file is not None:
    try:
        with st.spinner("Reading KB HUF dates for filter..."):
            _, kbhuf = load_macro_sheets(macro_file)
            kb_dates = kbhuf["KB_Date_dt"].dropna()

        if len(kb_dates) > 0:
            min_d = kb_dates.min().date()
            max_d = kb_dates.max().date()

            st.subheader("KB HUF Date Filter (optional)")
            c4, c5 = st.columns(2)
            with c4:
                kb_date_from = st.date_input(
                    "From date (KB HUF Col H)",
                    value=min_d,
                    min_value=min_d,
                    max_value=max_d,
                )
            with c5:
                kb_date_to = st.date_input(
                    "To date (KB HUF Col H)",
                    value=max_d,
                    min_value=min_d,
                    max_value=max_d,
                )

            # Convert to timestamps for the phase2 filter
            kb_date_from = pd.Timestamp(kb_date_from)
            kb_date_to = pd.Timestamp(kb_date_to)
        else:
            st.info("KB HUF dates are empty / not readable; date filter disabled.")
    except Exception as e:
        st.warning(f"Could not read macro for date filter: {e}")

# ===================== Run =====================
run_btn = st.button(
    "Run Phase 1 + Phase 2",
    type="primary",
    disabled=not (csv_file and demat_file and macro_file),
)

if run_btn:
    # ---------- Phase 1 ----------
    with st.spinner("Phase 1 running..."):
        phase1_df = run_phase1(csv_file, demat_file)

    st.success(f"Phase 1 complete: {len(phase1_df)} debit rows.")
    st.dataframe(phase1_df.head(50), use_container_width=True)

    # ---------- Phase 2 ----------
    with st.spinner("Phase 2 running..."):
        recon, exceptions, kb_unmatched = run_phase2(
            phase1_df=phase1_df,
            macro_file=macro_file,
            name_threshold=name_threshold,
            kb_date_from=kb_date_from,
            kb_date_to=kb_date_to,
        )

    ok_count = int((recon["ReconStatus"] == "OK").sum())

    st.subheader("Phase 2 Summary")
    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Total (DP/Phase1)", len(recon))
    m2.metric("OK", ok_count)
    m3.metric("Exceptions (DP → KB)", len(exceptions))
    m4.metric("KB Unmatched (KB → DP)", len(kb_unmatched))

    tab1, tab2, tab3 = st.tabs(
        ["Reconciliation (All)", "Exceptions (DP → KB)", "KB Unmatched (KB → DP)"]
    )
    with tab1:
        st.dataframe(recon, use_container_width=True)
    with tab2:
        st.dataframe(exceptions, use_container_width=True)
    with tab3:
        st.dataframe(kb_unmatched, use_container_width=True)

    # ---------- Export ----------
    excel_bytes = to_colored_excel_bytes(
        phase1_df=phase1_df,
        recon=recon,
        exceptions=exceptions,
        kb_unmatched=kb_unmatched,
        include_phase1_sheet=True,
        include_kb_unmatched_sheet=True,
    )

    st.download_button(
        "Download Combined Excel (Color-coded)",
        data=excel_bytes,
        file_name="bond_transfer_reconciliation.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
else:
    st.info("Upload all 3 files to enable the Run button.")
