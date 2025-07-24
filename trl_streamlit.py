import streamlit as st
import pandas as pd
from sqlalchemy import create_engine

# --- DB Connection ---

@st.cache_resource
def get_engine():
    creds = st.secrets["db"]
    conn_str = f"mssql+pymssql://{creds.user}:{creds.password}@{creds.host}:{creds.port}/{creds.db}"
    return create_engine(conn_str)

engine = get_engine()

# --- TRL Checks ---
checks = {
    "Invalid Non-Extrapolated Null Rationale": {
        "description": "Rows with NULL rationale but not marked as extrapolated.",
        "query": """
            SELECT * FROM dbo.co_trl
            WHERE is_extrapolated = 0 AND rationale IS NULL;
        """
    },
    "Invalid Backward Extrapolation": {
        "description": "First-year extrapolated entries not using TRL ID 9.",
        "query": """
            WITH first_year AS (
                SELECT client_id, MIN(reference_year) AS min_year
                FROM dbo.co_trl
                GROUP BY client_id
            )
            SELECT t.*
            FROM dbo.co_trl t
            JOIN first_year f ON t.client_id = f.client_id AND t.reference_year = f.min_year
            WHERE t.is_extrapolated = 1 AND t.trl_id != 9;
        """
    },
    "Duplicate TRL Entries": {
        "description": "Duplicate rows for the same client_id, reference_year, and trl_id.",
        "query": """
            WITH duplicates AS (
                SELECT client_id, reference_year, trl_id
                FROM dbo.co_trl
                GROUP BY client_id, reference_year, trl_id
                HAVING COUNT(*) > 1
            )
            SELECT t.*
            FROM dbo.co_trl t
            JOIN duplicates d 
              ON t.client_id = d.client_id 
             AND t.reference_year = d.reference_year 
             AND t.trl_id = d.trl_id;
        """
    },
    "Priority Violation": {
        "description": "Lower-priority TRL entries for same client/year.",
        "query": """
            WITH ranked_trl AS (
              SELECT *,
                ROW_NUMBER() OVER (
                  PARTITION BY client_id, reference_year
                  ORDER BY 
                    CASE 
                      WHEN manual_review_quality_id = 1 THEN 1
                      WHEN manual_review_quality_id = 2 THEN 2
                      WHEN source = 'Cloud A' THEN 3
                      WHEN source = 'Perplexity (Sonar)' THEN 4
                      ELSE 5
                    END,
                    acq_date DESC
                ) AS rnk
              FROM dbo.co_trl
            )
            SELECT *
            FROM ranked_trl
            WHERE rnk > 1;
        """
    },
    "Null Rationale not Extrapolated (≠ 2025)": {
        "description": "NULL rationales without extrapolation flag (except 2025).",
        "query": """
            SELECT * FROM dbo.co_trl
            WHERE rationale IS NULL 
              AND (is_extrapolated != 1 OR is_extrapolated IS NULL)
              AND reference_year != 2025;
        """
    },
    "Extrapolated Rows with Rationale": {
        "description": "Extrapolated rows that unexpectedly have rationale.",
        "query": """
            SELECT * FROM dbo.co_trl
            WHERE is_extrapolated = 1 AND rationale IS NOT NULL;
        """
    },
}

# --- UI ---
st.title("🔍 TRL Validation Dashboard")

st.sidebar.header("🎯 Validation Rules")
selected_checks = st.sidebar.multiselect(
    "Select which TRL validations to run:",
    list(checks.keys())
)

run_button = st.sidebar.button("🚀 Run Checks")

# --- Run selected validations ---
if run_button:
    if not selected_checks:
        st.warning("Please select at least one check to run.")
    else:
        for name in selected_checks:
            st.subheader(f"✅ {name}")
            st.markdown(f"**Logic:** {checks[name]['description']}")
            df = pd.read_sql(checks[name]["query"], engine)
            st.info(f"🔍 {len(df)} records found.")
            st.dataframe(df)
            if not df.empty:
                st.download_button(
                    label="📥 Download CSV",
                    data=df.to_csv(index=False),
                    file_name=f"{name.replace(' ', '_')}.csv"
                )
