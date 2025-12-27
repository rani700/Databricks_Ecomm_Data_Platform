import streamlit as st
import pandas as pd
from databricks.sdk import WorkspaceClient
import time
from databricks.sdk.service import sql
import re

# Initialize Databricks Client
w = WorkspaceClient()

# Configuration
WAREHOUSE_ID = "4d5d11d2b0dfb4fc" 
CATALOG = "ecomm_data_project"
SCHEMA = "gold"

def is_safe_query(sql_query):
    """MNC-Level Security: Prevent DDL/DML Injection"""
    forbidden = ["DROP", "DELETE", "TRUNCATE", "INSERT", "UPDATE", "GRANT", "REVOKE"]
    for word in forbidden:
        if re.search(rf"\b{word}\b", sql_query, re.IGNORECASE):
            return False, f"Safety violation: {word} is not allowed."
            
    if not sql_query.strip().upper().startswith("SELECT") and not sql_query.strip().upper().startswith("WITH"):
        return False, f"Guardrail Blocked: Query must start with SELECT. AI sent: {sql_query[:50]}..."
        
    return True, "Safe"

def run_ai_query(user_question):
    # 1. DEFINE SCHEMA CONTEXT (Updated with ISO Code Knowledge)
    schema_context = (
        f"Table: {CATALOG}.{SCHEMA}.ecom_one_big_table.\n"
        "Columns:\n"
        "- Country (String): ISO 2-letter country code (e.g., 'US', 'GB', 'DE', 'IN', 'FR').\n" # <--- TAUGHT AI HERE
        "- Country_TotalSellers (Long): Count of sellers.\n"
        "- Country_TotalBuyers (Long): Count of buyers.\n"
        "- User_ProductsSold (Integer): Products sold by a user.\n"
        "- User_ID (String): Unique user ID.\n"
    )
    
    # 2. BUILD PROMPT (With Translation Rule)
    raw_prompt = (
        f"Task: Generate Spark SQL for: {user_question}.\n"
        f"Context: {schema_context}\n"
        "Rules:\n"
        "1. Use ONLY the columns listed in the Context.\n"
        "2. To find 'most sellers', use 'Country_TotalSellers'.\n"
        "3. **CRITICAL:** The 'Country' column uses 2-letter ISO codes. You MUST translate country names to codes (e.g., 'United Kingdom' -> 'GB', 'Germany' -> 'DE').\n" # <--- NEW RULE
        "4. For other string filters, use ILIKE.\n"
        "5. Return ONLY the SQL string. No markdown."
    )
    
    safe_prompt = raw_prompt.replace("'", "''")
    
    model_name = "databricks-meta-llama-3-3-70b-instruct"
    sql_gen_cmd = f"SELECT ai_query('{model_name}', '{safe_prompt}')"
    
    try:
        # 3. GENERATE SQL
        gen_stmt = w.statement_execution.execute_statement(
            warehouse_id=WAREHOUSE_ID, catalog=CATALOG, schema=SCHEMA, statement=sql_gen_cmd
        )
        
        while gen_stmt.status.state in [sql.StatementState.PENDING, sql.StatementState.RUNNING]:
            time.sleep(1)
            gen_stmt = w.statement_execution.get_statement(gen_stmt.statement_id)
            
        if gen_stmt.status.state != sql.StatementState.SUCCEEDED:
            st.error(f"AI Generation Failed: {gen_stmt.status.error.message}")
            return None

        # 4. CLEAN AI OUTPUT
        raw_output = gen_stmt.result.data_array[0][0].strip()
        clean_sql = raw_output.replace("```sql", "").replace("```", "").strip()
        
        if "SELECT" in clean_sql.upper():
            start_idx = clean_sql.upper().find("SELECT")
            clean_sql = clean_sql[start_idx:]
            
        if ";" in clean_sql:
            clean_sql = clean_sql.split(";")[0]

        # Auto-Limit for safety
        if "LIMIT" not in clean_sql.upper() and "COUNT(" not in clean_sql.upper():
            clean_sql += " LIMIT 100"

        # 5. SECURITY CHECK
        is_safe, message = is_safe_query(clean_sql)
        if not is_safe:
            st.error(message)
            return None

        # 6. EXECUTE DATA QUERY
        st.code(clean_sql, language="sql")
        data_stmt = w.statement_execution.execute_statement(
            warehouse_id=WAREHOUSE_ID, catalog=CATALOG, schema=SCHEMA, statement=clean_sql
        )
        
        while data_stmt.status.state in [sql.StatementState.PENDING, sql.StatementState.RUNNING]:
            time.sleep(1)
            data_stmt = w.statement_execution.get_statement(data_stmt.statement_id)
            
        if data_stmt.status.state == sql.StatementState.SUCCEEDED:
            if data_stmt.result and data_stmt.result.data_array:
                cols = [col.name for col in data_stmt.manifest.schema.columns]
                return pd.DataFrame(data_stmt.result.data_array, columns=cols)
            else:
                return pd.DataFrame(columns=["Status"], data=[[f"Success! But found 0 results for: {clean_sql}"]])
        else:
            st.error(f"Data Query Failed: {data_stmt.status.error.message}")
            return None

    except Exception as e:
        st.error(f"Execution Error: {str(e)}")
        return None

# Streamlit UI
st.set_page_config(page_title="E-Comm AI Agent", page_icon="🛒")
st.title("🤖 E-Commerce Data Genius")
st.markdown(f"Querying: `{CATALOG}.{SCHEMA}.ecom_one_big_table`")

query = st.text_input("Ask a question about your customers:", placeholder="e.g., Which country has the most sellers?")

if query:
    with st.spinner("AI is analyzing and querying..."):
        data = run_ai_query(query)
        if data is not None:
            st.subheader("Results")
            if "Status" in data.columns and len(data) == 1:
                 status_msg = str(data.iloc[0,0])
                 if "0 results" in status_msg:
                     st.warning(f"⚠️ Query ran successfully, but found 0 records.\n\nTip: The database uses 2-letter codes (e.g., 'GB' for UK). The AI tries to translate, but sometimes exact codes work best.")
                 else:
                     st.info(status_msg)
            else:
                st.dataframe(data, use_container_width=True)
                st.caption(f"Showing {len(data)} rows.")
            
            csv = data.to_csv(index=False).encode('utf-8')
            st.download_button("Download CSV", csv, "results.csv", "text/csv")