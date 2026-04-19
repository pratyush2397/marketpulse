# 📈 MarketPulse — End-to-End Stock Market Analytics Platform

> A production-grade Data Engineering portfolio project covering the full modern data stack:
> Python · OOP · DSA · PySpark · Delta Lake · Azure · Data Modelling · Data Warehousing · AI/RAG

---

## 🏗️ Architecture Overview

```
Data Sources (yfinance · Mock Stream · News API)
        ↓
Ingestion Layer (Python OOP · DSA · Rate Limiting)
        ↓
Bronze Layer (Delta Lake · Auto Loader · Azure Blob · Watermark)
        ↓
Silver Layer (PySpark · Window Functions · OOP Transforms · DQ)
        ↓
Gold Layer (Star Schema · SCD Type 2 · Aggregates · DWH)
        ↓
Orchestration (Databricks Workflows · ADF · Azure Key Vault)
        ↓
Serving (Streamlit Dashboard · RAG Chatbot · Prediction API)
```

---

## 🧱 Tech Stack

| Category | Technologies |
|---|---|
| Language | Python 3.10+ |
| Big Data | PySpark, Databricks |
| Storage | Azure Blob Storage, Delta Lake |
| Orchestration | Databricks Workflows, Azure Data Factory |
| Cloud | Azure (Blob, Key Vault, ADLS Gen2) |
| Data Quality | Delta Constraints, custom DQValidator |
| Warehousing | Star Schema, SCD Type 2, Fact/Dim design |
| Dashboard | Streamlit |
| AI Layer | LangChain, OpenAI/Ollama, FAISS (RAG) |
| CI/CD | GitHub Actions |
| Infra (local) | Docker, Apache Spark (local mode) |

---

## 📁 Project Structure

```
MarketPulse/
│
├── ingestion/                    # Layer 1 — Python ingestion classes
│   ├── __init__.py
│   ├── stock_ingester.py         # StockIngester OOP class (yfinance wrapper)
│   ├── stream_consumer.py        # StreamConsumer — mock real-time generator
│   ├── event_queue.py            # EventQueue — heapq priority queue (DSA)
│   ├── news_scraper.py           # NewsScraper — headlines for AI layer
│   └── tests/
│       └── test_ingestion.py
│
├── bronze/                       # Layer 2 — Raw Delta Lake ingestion
│   ├── batch_ingest.py           # yfinance → Delta bronze table
│   ├── stream_ingest.py          # Auto Loader / Structured Streaming
│   ├── watermark_tracker.py      # Watermark state management
│   └── notebooks/
│       └── bronze_pipeline.ipynb
│
├── silver/                       # Layer 3 — Cleaned + enriched data
│   ├── transform_engine.py       # TransformEngine OOP class
│   ├── feature_engineering.py    # Moving averages, RSI, VWAP, Bollinger
│   ├── dq_validator.py           # DQValidator — schema + null + range checks
│   └── notebooks/
│       └── silver_pipeline.ipynb
│
├── gold/                         # Layer 4 — Star schema / Data Warehouse
│   ├── schema/
│   │   ├── fact_trades.sql       # Fact table DDL
│   │   ├── dim_stock.sql         # SCD Type 2 dimension
│   │   ├── dim_date.sql
│   │   └── dim_sector.sql
│   ├── scd_handler.py            # SCD Type 2 MERGE logic
│   ├── aggregations.py           # Daily/sector aggregates
│   └── notebooks/
│       └── gold_pipeline.ipynb
│
├── orchestration/                # Layer 5 — Pipeline scheduling
│   ├── workflows/
│   │   └── marketpulse_job.json  # Databricks Workflow config
│   ├── adf/
│   │   └── pipeline_config.json  # ADF pipeline definition
│   └── alerts/
│       └── notification_config.py
│
├── dashboard/                    # Layer 6 — Streamlit UI
│   ├── app.py                    # Main Streamlit app
│   ├── pages/
│   │   ├── overview.py           # Market overview page
│   │   ├── stock_detail.py       # Individual stock analysis
│   │   └── sector_analysis.py    # Sector breakdown
│   └── components/
│       └── charts.py             # Reusable chart components
│
├── ai_layer/                     # Layer 7 — RAG Chatbot + Prediction
│   ├── rag/
│   │   ├── chatbot.py            # LangChain RAG pipeline
│   │   ├── embeddings.py         # FAISS vector store
│   │   └── prompts.py            # Prompt templates
│   ├── prediction/
│   │   ├── model.py              # sklearn price prediction
│   │   └── api.py                # FastAPI serving endpoint
│   └── notebooks/
│       └── ai_exploration.ipynb
│
├── config/                       # Configuration
│   ├── settings.py               # Environment config class
│   ├── constants.py              # Tickers, paths, schema names
│   └── secrets_template.env      # Template — never commit real secrets
│
├── docs/                         # Documentation
│   ├── architecture.md           # Detailed architecture doc
│   ├── data_model.md             # Star schema explanation
│   ├── setup_guide.md            # Local + Databricks setup
│   └── diagrams/
│       └── architecture.png
│
├── scripts/
│   ├── setup_repo.sh             # One-click repo setup
│   └── seed_data.py              # Generate sample data
│
├── tests/                        # Integration tests
│   ├── test_bronze.py
│   ├── test_silver.py
│   └── test_gold.py
│
├── .github/
│   └── workflows/
│       └── ci.yml                # GitHub Actions CI pipeline
│
├── docker/
│   └── docker-compose.yml        # Local Spark + Delta setup
│
├── requirements.txt
├── .gitignore
└── README.md
```

---

## 🚀 Getting Started

### Prerequisites

```bash
Python 3.10+
Java 11+ (for PySpark local mode)
Docker (optional, for local Spark)
Databricks account (Community Edition is free)
Azure account (free tier sufficient)
```

### Local Setup

```bash
# 1. Clone the repo
git clone https://github.com/yourusername/MarketPulse.git
cd MarketPulse

# 2. Create virtual environment
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate

# 3. Install dependencies
pip install -r requirements.txt

# 4. Set environment variables
cp config/secrets_template.env .env
# Edit .env with your API keys

# 5. Run setup script
bash scripts/setup_repo.sh

# 6. Seed initial data
python scripts/seed_data.py
```

### Databricks Setup

```bash
# 1. Upload notebooks from bronze/, silver/, gold/ to Databricks workspace
# 2. Create a cluster with Databricks Runtime 13.x+ (includes Delta Lake)
# 3. Mount Azure Blob Storage using secret scope
# 4. Run notebooks in order: bronze → silver → gold
# 5. Create Workflow using orchestration/workflows/marketpulse_job.json
```

---

## 📊 Data Model

### Star Schema (Gold Layer)

```
                    ┌─────────────┐
                    │  dim_date   │
                    └──────┬──────┘
                           │
┌────────────┐    ┌────────┴────────┐    ┌─────────────┐
│  dim_stock │────│   fact_trades   │────│  dim_sector │
└────────────┘    └────────┬────────┘    └─────────────┘
                           │
                    ┌──────┴──────┐
                    │  agg_daily  │
                    └─────────────┘
```

**fact_trades** — grain: one row per ticker per day
- `trade_id`, `stock_sk`, `date_sk`, `sector_sk`
- `open`, `high`, `low`, `close`, `volume`
- `daily_return_pct`, `vwap`, `ma_7`, `ma_30`, `rsi_14`

**dim_stock** — SCD Type 2
- `stock_sk` (surrogate key), `ticker`, `company_name`, `exchange`
- `valid_from`, `valid_to`, `is_current`

---

## 🧠 Concepts Covered

| Concept | Where in Project |
|---|---|
| Python OOP | `StockIngester`, `TransformEngine`, `DQValidator` classes |
| DSA | `EventQueue` (heapq), sliding window, hash maps for dedup |
| SQL | All Gold layer DDL, window functions in Silver |
| PySpark | Silver transforms, window functions, joins, AQE |
| Data Modelling | Star schema design, fact/dim, grain definition |
| Data Warehousing | SCD Type 2, slowly changing dimensions, conformed dims |
| Delta Lake | ACID, time travel, MERGE, Auto Loader, `_delta_log` |
| Cloud (Azure) | Blob Storage, Key Vault, ADLS Gen2, ADF |
| Streaming | Structured Streaming, watermark, exactly-once |
| AI/RAG | LangChain, embeddings, FAISS, LLM Q&A on stock data |
| CI/CD | GitHub Actions, automated tests on push |

---

## 📈 Key Features

- **Real-time simulation** — mock stream generates tick-level stock events
- **Incremental ingestion** — watermark-based, idempotent, restartable
- **Financial indicators** — MA(7/30/50), RSI(14), VWAP, Bollinger Bands
- **SCD Type 2** — full history of company metadata changes
- **Ask your data** — RAG chatbot answers questions like *"Which sector performed best last quarter?"*
- **Price prediction** — simple ML model served via FastAPI

---

## 🗓️ Build Timeline

| Phase | Weeks | Deliverable |
|---|---|---|
| Foundation | 1–2 | Repo setup, ingestion classes, Bronze layer |
| Core pipeline | 3–6 | Silver transforms, feature engineering, DQ |
| Warehouse | 7–9 | Gold star schema, SCD Type 2, aggregates |
| Orchestration | 10 | Databricks Workflows, ADF, alerts |
| Serving | 11 | Streamlit dashboard |
| AI Layer | 12 | RAG chatbot, prediction API |

---

## 👤 Author

**Pratyush** — Senior Data Engineer  
Stack: Python · PySpark · Databricks · Azure · Delta Lake  
[LinkedIn](#) · [GitHub](#)

---

## 📄 License

MIT License — free to use, fork, and build on.
