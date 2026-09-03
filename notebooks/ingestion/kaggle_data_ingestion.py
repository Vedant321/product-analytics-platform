# Databricks notebook source
# DBTITLE 1,Load Configuration
# Load platform configuration
%run ../../config/platform_config

# COMMAND ----------

# DBTITLE 1,Install Kaggle Library
# MAGIC %pip install kaggle --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Set Kaggle Credentials
"""
Set up Kaggle API credentials from environment variables

Credentials are loaded from:
- Environment variables (set via .env file or Databricks cluster environment)
- Databricks Secrets (production)

To get your Kaggle credentials:
1. Go to https://www.kaggle.com/
2. Click your profile picture → Settings
3. Scroll to API section → Click "Create New Token"
4. Downloads kaggle.json with your username and key
5. Add them to your .env file locally (see .env.example)
"""

import logging

# Configure logging
logger = logging.getLogger(__name__)


import os

# Try to load from Databricks Secrets first (production)
try:
    KAGGLE_USERNAME = dbutils.secrets.get(scope="kaggle", key="username")
    KAGGLE_KEY = dbutils.secrets.get(scope="kaggle", key="key")
    logger.info(" Loaded credentials from Databricks Secrets")
except:
    # Fall back to environment variables (from .env or cluster config)
    KAGGLE_USERNAME = os.getenv('KAGGLE_USERNAME')
    KAGGLE_KEY = os.getenv('KAGGLE_KEY')
    
    if not KAGGLE_USERNAME or not KAGGLE_KEY:
        raise ValueError(
            "Kaggle credentials not found!\n"
            "\nFor local development: Create a .env file in project root\n"
            "For Databricks: Set cluster environment variables or use Secrets"
        )
    logger.info(" Loaded credentials from environment variables")

# Set environment variables for Kaggle API
os.environ['KAGGLE_USERNAME'] = KAGGLE_USERNAME
os.environ['KAGGLE_KEY'] = KAGGLE_KEY

logger.info("Username: {KAGGLE_USERNAME}")
logger.info("\n Kaggle API ready!")

# COMMAND ----------

# DBTITLE 1,Download Dataset from Kaggle
"""
Download eCommerce Behavior Data from Kaggle

Dataset: ecommerce-behavior-data-from-multi-category-store
Size: ~32GB compressed, contains 7 CSV files (Oct 2019 - Apr 2020)
"""

import kaggle
from kaggle.api.kaggle_api_extended import KaggleApi
import zipfile
import os

# Initialize Kaggle API
api = KaggleApi()
api.authenticate()

logger.info(" Kaggle API authenticated successfully")

# Dataset identifier
dataset = "mkechinov/ecommerce-behavior-data-from-multi-category-store"

# Download to temporary location first
temp_download_path = "/tmp/kaggle_download"
os.makedirs(temp_download_path, exist_ok=True)

logger.info("\nDownloading dataset: {dataset}")
logger.info("Destination: {temp_download_path}")
logger.info("\n⚠  This is a large dataset (~32GB). Download may take 10-20 minutes...\n")

# Download dataset
api.dataset_download_files(
    dataset,
    path=temp_download_path,
    unzip=True  # Auto-extract after download
)

logger.info("\n Dataset downloaded and extracted successfully!")

# List downloaded files
downloaded_files = os.listdir(temp_download_path)
logger.info("\nDownloaded files ({len(downloaded_files)}):")
for file in sorted(downloaded_files):
    file_path = os.path.join(temp_download_path, file)
    file_size_mb = os.path.getsize(file_path) / (1024 * 1024)
    logger.info("  • {file} ({file_size_mb:.2f} MB)")

# COMMAND ----------

# DBTITLE 1,Move Files to Unity Catalog Volume
"""
Move downloaded CSV files to Unity Catalog Volume

Volume path: /Volumes/product_analytics/ecommerce/raw_data
"""

import shutil

# Get volume path from config
volume_path = config.catalog.volume_path

logger.info("📍 Target Volume: {volume_path}")
logger.info("\n Moving files to volume...\n")

# Create volume directory if it doesn't exist (though it should from SQL creation)
os.makedirs(volume_path, exist_ok=True)

# Move each CSV file to the volume
moved_files = []
for file in downloaded_files:
    if file.endswith('.csv'):
        source_path = os.path.join(temp_download_path, file)
        dest_path = os.path.join(volume_path, file)
        
        # Copy file to volume
        shutil.copy2(source_path, dest_path)
        
        file_size_mb = os.path.getsize(dest_path) / (1024 * 1024)
        logger.info("   {file} → {volume_path} ({file_size_mb:.2f} MB)")
        moved_files.append(file)

logger.info("\n Successfully moved {len(moved_files)} CSV files to volume!")

# Cleanup temp directory
shutil.rmtree(temp_download_path)
logger.info("\n🧹 Cleaned up temporary download directory")

# COMMAND ----------

# DBTITLE 1,Validate Data and Preview
"""
Validate the dataset and preview the data structure
"""

import pandas as pd
import os
import subprocess

# Get volume path from config
volume_path = config.catalog.volume_path

print("="*80)
logger.info("DATA VALIDATION SUMMARY")
print("="*80)

# List all CSV files in the volume
csv_files = [f for f in os.listdir(volume_path) if f.endswith('.csv')]
logger.info("\n Total CSV files in volume: {len(csv_files)}\n")

# Show file details
total_size_gb = 0
for file in sorted(csv_files):
    file_path = os.path.join(volume_path, file)
    file_size_gb = os.path.getsize(file_path) / (1024 * 1024 * 1024)
    total_size_gb += file_size_gb
    logger.info("  • {file:<50} {file_size_gb:>8.2f} GB")

logger.info("\nTotal dataset size: {total_size_gb:.2f} GB")

# Preview first file using pandas (small sample)
if csv_files:
    sample_file = sorted(csv_files)[0]
    sample_path = os.path.join(volume_path, sample_file)
    
    logger.info("\n Previewing schema from: {sample_file}")
    print("="*80)
    
    # Read first 5 rows
    df_preview = pd.read_csv(sample_path, nrows=5)
    
    logger.info("\nColumns ({len(df_preview.columns)}):")
    for col in df_preview.columns:
        logger.info("  • {col} ({df_preview[col].dtype})")
    
    logger.info("\nSample data (first 5 rows):")
    display(df_preview)
    
    # Count total rows using wc -l (faster, no memory overhead)
    logger.info("\n📄 Counting rows in {sample_file}...")
    import subprocess
    result = subprocess.run(['wc', '-l', sample_path], capture_output=True, text=True)
    row_count = int(result.stdout.split()[0]) - 1  # Subtract 1 for header
    logger.info("   Rows: {row_count:,}")
    
print("\n" + "="*80)
logger.info(" Data validation complete! Ready for ingestion pipeline.")
print("="*80)