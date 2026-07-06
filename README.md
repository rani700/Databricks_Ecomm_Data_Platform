# 🛒 E-Commerce GenAI Data Agent — Natural Language → Spark SQL on Databricks

Ask questions about an e-commerce dataset in plain English — a **Llama 3.3 (70B)** agent turns them into optimized Spark SQL, runs them against a **Medallion lakehouse** (Bronze → Silver → Gold), and streams results back to a **Streamlit** UI. No SQL knowledge required.

**Stack:** Databricks (SQL Warehouse, `ai_query`, Unity Catalog) · Llama 3.3 70B · PySpark · Streamlit · Databricks SDK · GitHub Actions

## Architecture

![Architecture diagram](<Untitled Diagram.drawio (2).png>)

### How a question becomes an answer

1. **NL → SQL.** The Streamlit app wraps the user's question in a schema-aware prompt — column descriptions plus domain rules (e.g., country names must be translated to ISO-2 codes) — and invokes `ai_query()` on `databricks-meta-llama-3-3-70b-instruct` through the SQL Warehouse.
2. **Guardrails.** The generated SQL is cleaned (markdown stripped, statement isolated) and validated before execution: DDL/DML keywords (`DROP`, `DELETE`, `TRUNCATE`, `INSERT`, `UPDATE`, `GRANT`, `REVOKE`) are blocked, and only `SELECT`/`WITH` statements are allowed through.
3. **Execution.** The sanitized query runs against the Gold layer (`ecom_one_big_table`) via the Databricks Statement Execution API, with asynchronous polling on both the generation and execution calls so the UI never freezes.
4. **Results.** Rendered as an interactive table with a CSV download for offline analysis.

![Sequential flow diagram](<sequential flow diag.png>)

## Data pipeline (Medallion architecture)

| Layer | Notebook | Purpose |
|---|---|---|
| Setup | `00_Setup_Infrastructure.ipynb` | Catalog, schemas and infrastructure bootstrap |
| Bronze | `Bronze_Layer.ipynb` | Raw data ingestion |
| Silver | `Silver_Layer.ipynb` | Cleaning and conforming |
| Gold | `Gold_Layer.ipynb` | Business-level aggregates → `ecom_one_big_table`, the table the agent queries |
| Monitoring | `00_Pipeline_Monitoring.ipynb` | Pipeline health checks |
| Agent | `05_GenAI_Data_Agent.ipynb` | GenAI agent development notebook |

Shared logic lives in `Shared_Functions.ipynb`.

## Project structure

```
├── .github/workflows/deploy.yml   # CI/CD: sync notebooks to Databricks on push
├── 00_Setup_Infrastructure.ipynb
├── Bronze_Layer.ipynb / Silver_Layer.ipynb / Gold_Layer.ipynb
├── 00_Pipeline_Monitoring.ipynb
├── 05_GenAI_Data_Agent.ipynb
└── ecomm-app/                     # Streamlit front end
    ├── app.py                     # NL→SQL agent, guardrails, async polling
    ├── app.yaml
    └── requirements.txt
```

## Run the app

```bash
cd ecomm-app
pip install -r requirements.txt
# authenticate the Databricks SDK (e.g. DATABRICKS_HOST + DATABRICKS_TOKEN)
streamlit run app.py
```

Update `WAREHOUSE_ID`, `CATALOG` and `SCHEMA` at the top of `app.py` to point at your workspace.

## CI/CD

Every push to `main` triggers a GitHub Actions workflow that installs the Databricks CLI and syncs the linked Databricks Repo to the latest commit — notebooks deploy automatically, no manual imports.
