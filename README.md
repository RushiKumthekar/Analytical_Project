# ⚡ Building a Data-Driven Future: EV Infrastructure Analytics

> **Analysing U.S. Electric Vehicle Infrastructure through an End-to-End ETL & Analytics Pipeline**

An end-to-end **ETL, analytics, visualization, and forecasting pipeline** for U.S. electric vehicle (EV) charging infrastructure. The project combines real-time API data with public datasets and uses **Dagster, MongoDB, PostgreSQL, Pandas, Prophet, Plotly, Folium, and Dash** to build a complete data workflow.

**MSc Data Analytics — Analytics Programming & Data Visualisation**
**National College of Ireland**

---

## 🔎 Overview

This project demonstrates how a modern data orchestration platform can manage a multi-source ETL pipeline for EV infrastructure analytics.

The pipeline:

1. **Extracts** real-time EV charging station data from the OpenChargeMap API and loads local CSV datasets.
2. **Stages** semi-structured JSON data in MongoDB.
3. **Transforms** the datasets using Pandas by cleaning missing values, flattening JSON, and standardizing formats.
4. **Loads** cleaned and structured datasets into PostgreSQL.
5. **Analyses** EV infrastructure and adoption trends using Python.
6. **Visualizes** insights through Matplotlib, Seaborn, Plotly, Folium, and Dash.
7. **Forecasts** future charging infrastructure growth using Prophet.

### 🚗 Project Flow

```text
                         ┌──────────────────────────────┐
                         │   Washington EV Population   │
                         │           CSV                │
                         └──────────────┬───────────────┘
                                        │
                         ┌──────────────▼───────────────┐
                         │      EV Fuel Stations        │
                         │           CSV                │
                         └──────────────┬───────────────┘
                                        │
                         ┌──────────────▼───────────────┐
                         │      All Fuel Stations       │
                         │           CSV                │
                         └──────────────┬───────────────┘
                                        │
                                        ▼
                              ┌─────────────────┐
                              │  Local Dataset  │
                              │    Ingestion    │
                              └────────┬────────┘
                                       │
                                       │
OpenChargeMap API ──► JSON Extraction ──► MongoDB
                                       │
                                       ▼
                              ┌─────────────────┐
                              │ Clean & Normalize│
                              │     Pandas      │
                              └────────┬────────┘
                                       │
                                       ▼
                              ┌─────────────────┐
                              │   PostgreSQL    │
                              │ Structured Data │
                              └────────┬────────┘
                                       │
                                       ▼
                         ┌──────────────────────────┐
                         │ Jupyter Notebook         │
                         │                          │
                         │ • Analysis               │
                         │ • Visualization          │
                         │ • Forecasting             │
                         │ • Interactive Dashboard  │
                         └──────────────────────────┘
```

---

## 🏗️ Architecture

The ETL workflow is orchestrated using **Dagster** and consists of four primary assets.

| Stage            | Dagster Asset       | Description                                                                                                                  |
| ---------------- | ------------------- | ---------------------------------------------------------------------------------------------------------------------------- |
| **Extract**      | `web_scraped_data`  | Retrieves up to 50,000 charging-station records from the OpenChargeMap API using pagination, deduplication, and retry logic. |
| **Load (Raw)**   | `mongodb_data`      | Upserts scraped JSON records into MongoDB using UUID as the unique key.                                                      |
| **Transform**    | `cleaned_data`      | Cleans MongoDB data and three local CSV datasets, removes high-missing-value columns, flattens JSON, and normalizes data.    |
| **Load (Final)** | `store_to_postgres` | Writes cleaned datasets to PostgreSQL while avoiding duplicate records.                                                      |

### Dagster Asset Graph

```text
web_scraped_data
        │
        ▼
mongodb_data
        │
        ▼
cleaned_data
        │
        ▼
store_to_postgres
```

---

## 📂 Repository Contents

| File                                | Description                                                                                  |
| ----------------------------------- | -------------------------------------------------------------------------------------------- |
| `assets.py`                         | Dagster asset definitions implementing the ETL pipeline.                                     |
| `__init__.py`                       | Dagster `Definitions` object wiring the assets together.                                     |
| `Analytical_Programming_Code.ipynb` | Jupyter Notebook containing analysis, visualization, forecasting, and dashboard development. |
| `26510_..._APDV_....pdf`            | Full academic project report describing methodology, related work, results, and references.  |
| `README.md`                         | Project documentation and setup instructions.                                                |

---

## 🗂️ Datasets

| Dataset                      | Source                               | Format | Purpose                                                                           |
| ---------------------------- | ------------------------------------ | ------ | --------------------------------------------------------------------------------- |
| **Charging Station POIs**    | OpenChargeMap API                    | JSON   | Real-time U.S. EV charging infrastructure data.                                   |
| **Washington EV Population** | Data.gov                             | CSV    | EV registrations, adoption trends, vehicle models, and types in Washington State. |
| **EV Fuel Stations**         | Alternative Fuels Data Center (AFDC) | CSV    | EV charging station information including networks, connectors, and access.       |
| **All Fuel Stations**        | AFDC                                 | CSV    | Comparison between EV charging infrastructure and traditional fuel stations.      |

---

## 🛠️ Tech Stack

### Data Engineering

* **Python**
* **Dagster**
* **Pandas**
* **Requests**
* **PyMongo**
* **SQLAlchemy**

### Databases

* **MongoDB** — semi-structured/raw data staging
* **PostgreSQL** — cleaned and structured analytical data

### Analytics & Visualization

* **Jupyter Notebook**
* **Matplotlib**
* **Seaborn**
* **Plotly**
* **Folium**
* **Dash**
* **Scikit-learn**

### Forecasting

* **Prophet**

---

## ⚙️ Prerequisites

Before running the project, install:

* Python **3.9+**
* MongoDB
* PostgreSQL
* OpenChargeMap API key
* Jupyter Notebook

You will also need:

```text
MongoDB:
mongodb://localhost:27017/

PostgreSQL:
A database named Analytics
```

---

## 🚀 Installation

### 1. Clone the repository

```bash
git clone <repo-url>
cd <repo-name>
```

### 2. Create a virtual environment

```bash
python -m venv venv
```

Activate it:

**Windows**

```bash
venv\Scripts\activate
```

**macOS / Linux**

```bash
source venv/bin/activate
```

### 3. Install dependencies

```bash
pip install dagster dagster-webserver pandas pymongo sqlalchemy psycopg2-binary \
matplotlib seaborn plotly folium dash scikit-learn prophet jupyter
```

---

## 🔐 Configuration

For security, credentials should **not** be hardcoded in the source code.

Set the required environment variables.

### Windows PowerShell

```powershell
$env:OPENCHARGEMAP_API_KEY="your_api_key"
$env:MONGODB_URI="mongodb://localhost:27017/"
$env:POSTGRES_URL="postgresql://username:password@localhost:5432/Analytics"
```

### macOS / Linux

```bash
export OPENCHARGEMAP_API_KEY="your_api_key"
export MONGODB_URI="mongodb://localhost:27017/"
export POSTGRES_URL="postgresql://username:password@localhost:5432/Analytics"
```

The Python application can then access them using:

```python
import os

api_key = os.environ["OPENCHARGEMAP_API_KEY"]

mongo_uri = os.environ.get(
    "MONGODB_URI",
    "mongodb://localhost:27017/"
)

pg_conn = os.environ["POSTGRES_URL"]
```

### Recommended `.env` approach

Create a `.env` file locally:

```env
OPENCHARGEMAP_API_KEY=your_api_key
MONGODB_URI=mongodb://localhost:27017/
POSTGRES_URL=postgresql://username:password@localhost:5432/Analytics
```

Add it to `.gitignore`:

```gitignore
.env
__pycache__/
*.pyc
.ipynb_checkpoints/
venv/
```

> **Important:** If an API key or database credential has previously been committed to a public repository, revoke/rotate it immediately and replace it with environment-based configuration.

---

## 📁 Dataset Configuration

Place the three local CSV datasets in a local directory:

```text
data/
├── Washington EV Population.csv
├── EV Fuel Stations.csv
└── All Fuel Stations.csv
```

Update the `base_dir` path in the `cleaned_data` asset in `assets.py` to point to the dataset directory.

For example:

```python
base_dir = "data/"
```

---

# 🚀 Running the Pipeline

## Option 1 — Dagster UI

Start Dagster:

```bash
dagster dev -f assets.py
```

Then open:

```text
http://localhost:3000
```

In the Dagster UI, materialize the assets in the following order:

```text
web_scraped_data
       ↓
mongodb_data
       ↓
cleaned_data
       ↓
store_to_postgres
```

---

## Option 2 — Dagster CLI

You can also materialize the complete pipeline from the command line:

```bash
dagster asset materialize --select "*" -f assets.py
```

---

# 📊 Running the Analysis

Once the ETL pipeline has populated PostgreSQL, launch the Jupyter Notebook:

```bash
jupyter notebook Analytical_Programming_Code.ipynb
```

The notebook connects to PostgreSQL and performs the downstream analysis.

### Analysis includes

* 🔌 Top 10 U.S. states by number of EV charging stations
* 🚗 EV types and top EV models in Washington State
* 🗺️ Interactive charging-station map using Folium
* 📈 EV adoption trends over time
* 🔋 EV connector type distribution
* ⛽ EV charging stations vs. traditional fuel stations
* ⚡ Charging speed analysis by network
* 🔓 Charging-station accessibility analysis
* 📊 Cumulative charging infrastructure growth
* 🔮 10-year Prophet forecasting
* 📱 Interactive Dash dashboard

---

# 📈 Key Findings

The analysis identified several notable patterns in the datasets:

### 🇺🇸 Geographic Distribution

California has the largest number of EV charging stations in the analysed U.S. dataset, followed by Texas and Florida.

### 🚗 Washington EV Market

Tesla vehicles represent a substantial share of the Washington EV registration dataset, with Tesla models among the most frequently recorded vehicles.

### 🔌 Connector Types

J1772 appears as the most common connector type in the analysed charging-station data, followed by Tesla-related charging infrastructure.

### ⛽ EV vs. Traditional Fuel Infrastructure

The analysed data shows that EV-specific charging infrastructure remains considerably smaller in total station count than traditional fuel infrastructure.

### 🔮 Forecasting

The Prophet model projects continued growth in EV charging infrastructure over the following decade based on historical trends.

> Forecast results represent model-based projections and should not be interpreted as guaranteed future outcomes.

---

# 📊 Visualizations

The project uses several visualization technologies:

| Tool           | Usage                                       |
| -------------- | ------------------------------------------- |
| **Matplotlib** | Statistical and analytical charts           |
| **Seaborn**    | Distribution and comparative visualizations |
| **Plotly**     | Interactive charts                          |
| **Folium**     | Geographic charging-station maps            |
| **Dash**       | Interactive analytics dashboard             |

---

# 🗄️ Data Storage Strategy

The project uses two database technologies for different stages of the pipeline.

### MongoDB

MongoDB acts as the **raw/semi-structured staging layer**.

```text
OpenChargeMap JSON
       ↓
    MongoDB
       ↓
Raw charging-station records
```

This allows the original nested API structure to be retained before transformation.

### PostgreSQL

PostgreSQL acts as the **structured analytical database**.

```text
MongoDB + CSV datasets
          ↓
     Pandas ETL
          ↓
      PostgreSQL
          ↓
     SQL Analysis
```

This separation demonstrates a common data-engineering pattern where raw and analytical data are stored using technologies appropriate to their respective structures.

---

# 🔄 ETL Process

## 1. Extract

Data is extracted from:

* OpenChargeMap API
* Data.gov
* Alternative Fuels Data Center datasets

The API extraction includes:

* Pagination
* Deduplication
* Retry handling
* JSON processing
* Large-volume data retrieval

## 2. Load Raw Data

The API response is stored in MongoDB.

Records are upserted using the station UUID to reduce duplication.

## 3. Transform

Pandas is used to:

* Handle missing values
* Remove highly incomplete columns
* Flatten nested JSON
* Normalize column formats
* Standardize datasets
* Prepare data for relational storage

## 4. Load Structured Data

The cleaned datasets are written into PostgreSQL tables.

The database becomes the primary source for downstream analytics.

---

# 🔐 Security Notes

The original academic coursework implementation may contain demonstration credentials or local file paths.

Before publishing or reusing this project:

* Remove hardcoded API keys.
* Remove database passwords.
* Use environment variables.
* Add `.env` to `.gitignore`.
* Replace local Windows paths such as `C:\Users\...`.
* Rotate/revoke any credentials that were previously exposed.
* Never commit secrets to GitHub.

Example:

```python
# ❌ Avoid
api_key = "my-secret-api-key"

# ✅ Recommended
api_key = os.environ["OPENCHARGEMAP_API_KEY"]
```

---

# ⚠️ Limitations

The current implementation has several limitations:

* Batch processing rather than real-time streaming.
* Charging-station coverage depends on the available source datasets.
* Private charging stations may not be fully represented.
* Historical data availability varies by source.
* Forecast accuracy depends on the quality and historical coverage of the input data.
* Prophet provides statistical forecasts rather than causal predictions.
* The current geographic focus is primarily the United States.

---

# 🔭 Future Work

Potential improvements include:

### 🌍 Geographic Expansion

Extend the pipeline to additional regions such as:

* UAE
* Dubai
* Europe
* Asia-Pacific

### ⚡ Real-Time Streaming

Introduce streaming technologies to support continuously updated infrastructure data.

Potential technologies:

```text
Kafka
        ↓
Streaming ingestion
        ↓
MongoDB / Data Lake
        ↓
PostgreSQL / Warehouse
        ↓
Real-time Dashboard
```

### 🤖 Advanced Forecasting

Compare Prophet with machine-learning and deep-learning approaches such as:

* LSTM
* GRU
* XGBoost
* Random Forest
* Temporal Fusion Transformer

### ☁️ Cloud Deployment

The pipeline could be migrated to cloud infrastructure using services such as:

* AWS
* Microsoft Azure
* Google Cloud

### 📊 Advanced Analytics

Future versions could incorporate:

* EV-to-charger ratios
* Charging demand forecasting
* Geographic accessibility scores
* Charging-station utilization
* Infrastructure gap analysis
* Population-normalized charger density
* Socioeconomic and demographic factors

---

# 🎓 Academic Context

This project was developed as part of the:

**MSc Data Analytics — Analytics Programming & Data Visualisation**

**National College of Ireland**

The project demonstrates practical application of:

* ETL pipeline design
* Data orchestration
* API integration
* NoSQL and relational databases
* Data cleaning and transformation
* Exploratory data analysis
* Data visualization
* Geospatial analysis
* Time-series forecasting
* Interactive dashboard development

---

# 👤 Author

**Hrushikesh Nitin Kumthekar**

MSc Data Analytics
National College of Ireland

---

# 📄 License

This project was developed for academic purposes as part of the Analytics Programming & Data Visualisation module.

If you intend to distribute or reuse the project publicly, consider adding an appropriate open-source license such as the **MIT License**.

---

## ⭐ Project Highlights

```text
⚡ Real-time EV infrastructure data
🔄 End-to-end ETL pipeline
🛠️ Dagster orchestration
🍃 MongoDB raw-data staging
🐘 PostgreSQL analytical storage
🐼 Pandas data transformation
📊 Interactive data visualization
🗺️ Geospatial analysis
🔮 Prophet forecasting
📱 Dash dashboard
🔐 Environment-based configuration
```

> **From raw API data to actionable EV infrastructure insights — an end-to-end data engineering and analytics project.**
