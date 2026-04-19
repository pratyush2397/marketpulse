#!/bin/bash
# MarketPulse — One-click repo folder structure setup
# Run: bash scripts/setup_repo.sh

echo "🚀 Setting up MarketPulse project structure..."

# Root directories
dirs=(
  "ingestion/tests"
  "bronze/notebooks"
  "silver/notebooks"
  "gold/schema"
  "gold/notebooks"
  "orchestration/workflows"
  "orchestration/adf"
  "orchestration/alerts"
  "dashboard/pages"
  "dashboard/components"
  "ai_layer/rag"
  "ai_layer/prediction"
  "ai_layer/notebooks"
  "config"
  "docs/diagrams"
  "scripts"
  "tests"
  ".github/workflows"
  "docker"
)

for dir in "${dirs[@]}"; do
  mkdir -p "$dir"
  touch "$dir/.gitkeep"
  echo "  ✅ Created $dir"
done

# Create __init__.py files for Python packages
packages=(
  "ingestion"
  "bronze"
  "silver"
  "gold"
  "dashboard"
  "ai_layer"
  "config"
)

for pkg in "${packages[@]}"; do
  touch "$pkg/__init__.py"
done

# Create placeholder Python files
touch ingestion/stock_ingester.py
touch ingestion/stream_consumer.py
touch ingestion/event_queue.py
touch ingestion/news_scraper.py
touch ingestion/tests/test_ingestion.py

touch bronze/batch_ingest.py
touch bronze/stream_ingest.py
touch bronze/watermark_tracker.py

touch silver/transform_engine.py
touch silver/feature_engineering.py
touch silver/dq_validator.py

touch gold/scd_handler.py
touch gold/aggregations.py
touch gold/schema/fact_trades.sql
touch gold/schema/dim_stock.sql
touch gold/schema/dim_date.sql
touch gold/schema/dim_sector.sql

touch orchestration/alerts/notification_config.py
touch dashboard/app.py
touch dashboard/pages/overview.py
touch dashboard/pages/stock_detail.py
touch dashboard/pages/sector_analysis.py
touch dashboard/components/charts.py

touch ai_layer/rag/chatbot.py
touch ai_layer/rag/embeddings.py
touch ai_layer/rag/prompts.py
touch ai_layer/prediction/model.py
touch ai_layer/prediction/api.py

touch config/settings.py
touch config/constants.py

touch tests/test_bronze.py
touch tests/test_silver.py
touch tests/test_gold.py

touch scripts/seed_data.py

# Create .gitignore
cat > .gitignore << 'EOF'
# Python
__pycache__/
*.py[cod]
*.egg-info/
dist/
build/
venv/
.env
*.env

# Notebooks
.ipynb_checkpoints/

# Delta Lake
_delta_log/
*.parquet

# IDE
.vscode/
.idea/

# Secrets — NEVER commit
config/secrets.env
*.pem
*.key

# OS
.DS_Store
Thumbs.db
EOF

# Create secrets template
cat > config/secrets_template.env << 'EOF'
# Copy this to .env and fill in your values
# NEVER commit .env to GitHub

ALPHA_VANTAGE_API_KEY=your_key_here
AZURE_STORAGE_ACCOUNT_NAME=your_account
AZURE_STORAGE_ACCOUNT_KEY=your_key
AZURE_KEY_VAULT_URL=https://your-vault.vault.azure.net/
DATABRICKS_HOST=https://your-workspace.azuredatabricks.net
DATABRICKS_TOKEN=your_token
OPENAI_API_KEY=your_key_if_using_openai
EOF

# Create requirements.txt
cat > requirements.txt << 'EOF'
# Data ingestion
yfinance==0.2.36
requests==2.31.0
beautifulsoup4==4.12.2

# PySpark
pyspark==3.5.0
delta-spark==3.0.0

# Azure
azure-storage-blob==12.19.0
azure-keyvault-secrets==4.7.0
azure-identity==1.15.0

# Data processing
pandas==2.1.4
numpy==1.26.2

# Dashboard
streamlit==1.29.0
plotly==5.18.0

# AI layer
langchain==0.1.0
openai==1.6.1
faiss-cpu==1.7.4
sentence-transformers==2.2.2

# API
fastapi==0.108.0
uvicorn==0.25.0

# ML
scikit-learn==1.3.2

# Testing
pytest==7.4.3

# Utils
python-dotenv==1.0.0
loguru==0.7.2
EOF

# Create GitHub Actions CI
cat > .github/workflows/ci.yml << 'EOF'
name: MarketPulse CI

on:
  push:
    branches: [ main, develop ]
  pull_request:
    branches: [ main ]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
    - uses: actions/checkout@v3
    - name: Set up Python
      uses: actions/setup-python@v4
      with:
        python-version: '3.10'
    - name: Install dependencies
      run: pip install -r requirements.txt
    - name: Run tests
      run: pytest tests/ -v
EOF

echo ""
echo "✅ MarketPulse project structure created successfully!"
echo ""
echo "Next steps:"
echo "  1. cd into your project root"
echo "  2. git init"
echo "  3. cp config/secrets_template.env .env"
echo "  4. pip install -r requirements.txt"
echo "  5. Start building ingestion/stock_ingester.py"
