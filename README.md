⚡ Building a Data-Driven Future
Analysing EV Infrastructure through an ETL Pipeline
An end-to-end ETL and analytics pipeline for U.S. electric vehicle infrastructure — from live API scraping to interactive forecasting.

Python | Dagster | MongoDB | PostgreSQL | Prophet | Dash

MSc in Data Analytics — Analytics Programming & Data Visualisation, National College of Ireland

🔎 Overview
This project demonstrates how a modern orchestration tool (Dagster) can manage a multi-source ETL pipeline that:

Extracts real-time EV charging station data (JSON) from the OpenChargeMap API, plus local CSV datasets.
Loads the semi-structured web-scraped data into MongoDB.
Transforms all datasets — cleaning missing values, flattening JSON, and normalizing formats with Pandas.
Loads the cleaned, structured data into PostgreSQL for downstream analysis.
Analyses & visualizes the data in a Jupyter Notebook using Matplotlib, Seaborn, Plotly, Folium, and Dash, including a Prophet-based 10-year growth forecast.
📑 Contents
Architecture
Repository Contents
Datasets
Tech Stack
Prerequisites
Setup
Running the Pipeline
Running the Analysis
Key Findings
Security Notes
Limitations & Future Work
🏗️ Architecture
 Washington EV Population.csv ┐
 EV Fuel Stations.csv          ├──► Local CSV ingestion ──┐
 All Fuel Stations.csv        ┘                            │
                                                             ▼
 OpenChargeMap API ──► Web Scraping (JSON) ──► MongoDB ──► Clean & Normalize ──► PostgreSQL ──► Jupyter Notebook
                                                                                  (Analysis, Viz, Forecasting, Dash)
Data flow stages, mirroring the Dagster asset graph in assets.py:

Stage	Dagster Asset	Description
Extract	web_scraped_data	Pulls up to 50,000 charging-station records from the OpenChargeMap API (paginated, deduplicated, with retry logic).
Load (raw)	mongodb_data	Upserts the scraped records into a MongoDB collection, keyed on UUID.
Transform	cleaned_data	Cleans the MongoDB data and three local CSVs (drops empty/high-missing columns) and flattens JSON to tabular form.
Load (final)	store_to_postgres	Writes all four cleaned datasets into PostgreSQL tables, avoiding duplicate inserts.
📂 Repository Contents
File	Description
assets.py	Dagster asset definitions for the ETL pipeline (extract, MongoDB load, clean/transform, PostgreSQL load).
__init__.py	Dagster Definitions object wiring the four assets together for the Dagster UI/CLI.
Analytical_Programming_Code.ipynb	Jupyter Notebook that reads the processed data back from PostgreSQL and performs analysis, visualization, forecasting, and the Dash dashboard.
26510_..._APDV_....pdf	Full project report/paper describing methodology, related work, results, and references.
🗂️ Datasets
Dataset	Source	Format	Purpose
Charging station POIs	OpenChargeMap API	JSON (web-scraped)	Real-time charging infrastructure across the US.
Washington EV Population	data.gov	CSV	EV registrations in Washington State (adoption trends, models, types).
EV Fuel Stations	AFDC	CSV	US charging-station details (connector types, networks, access).
All Fuel Stations	AFDC	CSV	All fuel station types, used for EV-vs-traditional comparison.
🛠️ Tech Stack
Orchestration: Dagster
Databases: MongoDB (semi-structured staging) · PostgreSQL (structured storage)
Language/Libraries: Python, Pandas, SQLAlchemy, PyMongo, Requests
Analysis & Visualization: Matplotlib, Seaborn, Plotly, Folium, Dash
Forecasting: Prophet (Facebook/Meta)
✅ Prerequisites
Python 3.9+
A running MongoDB instance (default: mongodb://localhost:27017/)
A running PostgreSQL instance with a database named Analytics
An OpenChargeMap API key
⚙️ Setup
Clone the repository

git clone <repo-url>
cd <repo-name>
Install dependencies

pip install dagster dagster-webserver pandas pymongo sqlalchemy psycopg2-binary \
            matplotlib seaborn plotly folium dash scikit-learn prophet jupyter
Configure credentials Set your OpenChargeMap API key, MongoDB URI, and PostgreSQL connection string as environment variables rather than hardcoding them (see Notes below), then update assets.py to read from the environment, e.g.:

import os
api_key = os.environ["OPENCHARGEMAP_API_KEY"]
mongo_uri = os.environ.get("MONGODB_URI", "mongodb://localhost:27017/")
pg_conn = os.environ["POSTGRES_URL"]
Provide local CSV files Place the three CSV datasets (Washington EV population, EV fuel stations, all fuel stations) in a local directory and update the base_dir path in the cleaned_data asset in assets.py to point to it.

🚀 Running the Pipeline
Launch the Dagster UI to materialize the assets:

dagster dev -f assets.py
Then open http://localhost:3000, select all four assets (web_scraped_data → mongodb_data → cleaned_data → store_to_postgres), and click Materialize.

Alternatively, materialize from the CLI:

dagster asset materialize --select "*" -f assets.py
📊 Running the Analysis
Once the pipeline has populated PostgreSQL, open the notebook:

jupyter notebook Analytical_Programming_Code.ipynb
The notebook connects to PostgreSQL, retrieves the four processed tables, and generates:

Top 10 states by number of EV charging stations
Distribution of EV types and top EV models in Washington
Interactive charging-station map (Folium)
EV adoption trend over time
EV connector type distribution
EV vs. all fuel stations comparison
Charging speed by network (box plot)
Accessibility analysis of charging stations
Cumulative growth and a 10-year Prophet forecast
An interactive Dash dashboard combining the above visuals
💡 Key Findings
California leads all states in number of EV charging stations, followed by Texas and Florida.
Tesla vehicles dominate the EV market in Washington State.
J1772 is the most common EV connector type, followed by Tesla Superchargers.
EV-specific charging stations still significantly lag behind traditional fuel stations in total count.
The Prophet forecasting model projects continued, substantial growth in EV charging stations over the next decade.
🔐 Notes on Security/Configuration
The original coursework code contains a hardcoded OpenChargeMap API key and local database credentials for demonstration purposes. Before publishing or reusing this code, replace all hardcoded secrets and local file paths (e.g. C:\Users\...) with environment variables or a .env file (excluded via .gitignore), and rotate/revoke any exposed API keys or credentials.

🔭 Limitations & Future Work
No real-time/streaming data integration — the pipeline runs as a batch process.
Dataset coverage does not include private charging stations.
Future work includes expanding geographic coverage (e.g. UAE/Dubai) and applying LSTM/neural network models for higher-accuracy forecasting.
👤 Author
Hrushikesh Nitin Kumthekar — MSc Data Analytics (x23313731), National College of Ireland

📄 License
This project was developed for academic purposes as part of the Analytics Programming & Data Visualisation module. Add a license of your choice (e.g. MIT) if you intend to share or reuse this code publicly.

