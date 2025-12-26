import streamlit as st
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import sql

# Initialize Databricks Client
w = WorkspaceClient()

def run_ai_query(user_question):
    # This calls the AI Function Llama 3 directly through the SQL warehouse
    prompt = f"Write Spark SQL for: {user_question}. Table: ecomm_data_project.gold.ecom_one_big_table"
    
    # We execute the ai_query() directly via the SQL execution API
    query_gen = f"SELECT ai_query('databricks-meta-llama-3-1-70b-instruct', '{prompt}')"
    
    # In a real app, you would use w.statement_execution.execute_statement()
    # For now, let's assume the SQL is generated and then executed.
    # ... logic to execute generated SQL ...
    return results

# Streamlit UI Logic
st.title("🛒 E-Commerce Data Genius")
query = st.text_input("What would you like to know about our customers?")

if query:
    data = run_ai_query(query)
    st.table(data)