Create virtual environment
```bash
python3 -m venv .venv
source .venv/bin/activate
```
=== Generated APP ===

Instructions to Run the Application:

Install Required Packages:

```bash
pip install -r requirements.txt
```
Set Up Pinecone:
Replace 'YOUR_PINECONE_API_KEY' and 'YOUR_PINECONE_ENVIRONMENT' with your actual Pinecone API key and environment.

Set Up Ollama Endpoint:

Replace 'http://localhost:port/ollama_endpoint' with the actual endpoint where your local Ollama LLM is running.

Run the Streamlit App:

```bash
streamlit run your_script_name.py
```

Notes:

Data Sources:
The scan_data_sources function simulates scanning data sources and creates a sample data catalog. In a real application, you would connect to your databases and retrieve actual schemas.

Pinecone Usage:
The application stores embeddings of table schemas in Pinecone to retrieve relevant tables based on the user's question.

Insight Generation:
The generate_insight function assumes that you have a local API endpoint for Ollama. It sends the user's question and the filtered data to Ollama to generate insights.

Filtering and Grouping:
Users can filter data based on selected column values and choose a column to group by. Charts are regenerated when filter or group options are changed.

Bookmarking Insights:
Users can bookmark insights, which are then accessible from the sidebar. Bookmarked insights include the question, the generated insight, and the corresponding chart.

Settings Menu:
The sidebar includes a settings menu where users can input data source connection strings and LLM endpoint information.

Please ensure all placeholder values are replaced with actual configurations suitable for your environment.