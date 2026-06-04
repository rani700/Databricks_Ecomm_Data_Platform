# 🛒 E-Commerce GenAI Data Agent (End-to-End Data Engineering Project)
This project demonstrates an **End-to-End Generative AI Data Platform** that enables business users to query massive e-commerce datasets using natural language. 

Built on the **Databricks Data Intelligence Platform**, it leverages a Medallion Architecture (Bronze/Silver/Gold) for data processing and a custom **AI Agent (Llama 3.3)** hosted in a Streamlit App. The agent translates English questions into optimized Spark SQL queries, allowing for instant data retrieval without requiring SQL knowledge.

Here is a detailed breakdown of the workflow:

Phase 1: Intent Translation (Natural Language to SQL)
User Input: The flow begins when the user submits a natural language question (e.g., "Which country has most sellers?") through the Streamlit App interface.
Prompt Transmission: The Streamlit App packages this question and sends it to the Databricks SDK.
LLM Execution: The Databricks SDK forwards an EXECUTE command containing an ai_query(...) function to the SQL Warehouse. This specifically invokes the Llama 3 model to translate the natural language into an optimized SQL statement.
Asynchronous Polling (Loop 1): Because LLM inference can take time, the architecture uses a polling mechanism. The Streamlit app repeatedly asks the Databricks SDK "Is it done yet?" while the query is running, preventing the user interface from freezing.
SQL Retrieval: Once the LLM finishes processing, the Databricks SDK returns the generated SQL string (e.g., "SELECT * FROM...") back to the Streamlit App.

Phase 2: Security & Validation
Guardrails: Before executing the AI-generated code, the Streamlit App performs a crucial internal loop labeled Clean & Check Safety. This step scrubs the returned SQL of any markdown formatting and ensures it only contains safe, read-only statements (mitigating the risk of SQL injection or destructive DROP/DELETE commands).

Phase 3: Data Retrieval & Presentation
Query Execution: The Streamlit App sends the sanitized SQL query back to the Databricks SDK.
Warehouse Processing: The SDK issues the EXECUTE command to the SQL Warehouse to run against the actual e-commerce datasets.
Asynchronous Polling (Loop 2): Similar to the first phase, a second polling loop is initiated. The app checks the status of the data retrieval, which is highly efficient for querying massive, multi-layered datasets (like a Medallion architecture) without timing out the connection.
Result Delivery: The SQL Warehouse completes the query and returns the raw data (rows and columns) to the Streamlit App.
User Interface: Finally, the Streamlit App renders this data for the user, displaying it as an interactive table alongside a download button for further offline analysis.

