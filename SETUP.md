# Local Development Setup

## Prerequisites

**Python 3.12** is required for this project.

## Quick Start

### 1. Clone the Repository

```bash
git clone https://github.com/Vedant321/product-analytics-platform.git
cd product-analytics-platform
```

### 2. Create Virtual Environment

**Option A: Using venv (Recommended)**

```bash
# Create virtual environment with Python 3.12
python3.12 -m venv venv

# Activate virtual environment
# macOS/Linux:
source venv/bin/activate

# Windows:
venv\Scripts\activate
```

**Option B: Using conda**

```bash
conda create -n product-analytics python=3.12
conda activate product-analytics
```

### 3. Upgrade pip

```bash
pip install --upgrade pip
```

### 4. Install Dependencies

```bash
pip install -r requirements.txt
```

### 5. Verify Installation

```bash
# Check Python version
python --version  # Should show Python 3.12.x

# Check PySpark
python -c "import pyspark; print(f'PySpark: {pyspark.__version__}')"

# Check Delta Lake
python -c "import delta; print('Delta Lake: Installed')"

# Check Kafka
python -c "import kafka; print('Kafka: Installed')"

# Check pandas
python -c "import pandas; print(f'Pandas: {pandas.__version__}')"
```

## Environment Configuration

Create a `.env` file in the project root for local configuration:

```bash
# .env
DATABRICKS_HOST=https://dbc-eedc1a2c-39d4.cloud.databricks.com
DATABRICKS_TOKEN=your_personal_access_token
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
DATABASE_URL=postgresql://user:password@localhost:5432/dbname
```

## Running Jupyter Locally

```bash
# Start JupyterLab
jupyter lab

# Or start classic Notebook
jupyter notebook
```

## Running Tests

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=.

# Run specific test file
pytest tests/test_ingestion.py
```

## Code Formatting

```bash
# Format code with black
black .

# Sort imports with isort
isort .

# Check linting with flake8
flake8 .

# Type checking with mypy
mypy .
```

## Common Issues

### Issue: Python 3.12 not found

**Solution:**

```bash
# macOS (using Homebrew)
brew install python@3.12

# Ubuntu/Debian
sudo apt update
sudo apt install python3.12 python3.12-venv

# Windows
# Download from https://www.python.org/downloads/
```

### Issue: PySpark Java dependency error

**Solution:** Install Java 11 or 17

```bash
# macOS
brew install openjdk@17

# Ubuntu/Debian
sudo apt install openjdk-17-jdk

# Set JAVA_HOME
export JAVA_HOME=$(/usr/libexec/java_home -v 17)  # macOS
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64  # Linux
```

### Issue: Kafka connection errors locally

**Solution:** Install and run Kafka locally

```bash
# Using Docker (recommended)
docker run -d \
  --name kafka \
  -p 9092:9092 \
  apache/kafka:latest
```

## Deactivating Virtual Environment

```bash
deactivate
```

## Package Versions

All packages in `requirements.txt` are verified to work with **Python 3.12**.

Key dependencies:
* PySpark 3.5.3
* Delta Lake 3.2.0
* Pandas 2.2.2
* Kafka-Python 2.0.2
* dbt-core 1.8.7
* Databricks SDK 0.35.0

## Next Steps

Once setup is complete:
1. Review the project structure in README.md
2. Start with Phase 1: Event Replay System
3. Configure Databricks workspace connection
4. Download the eCommerce dataset from Kaggle
