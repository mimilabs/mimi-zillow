# Databricks notebook source

import requests
from pathlib import Path
import datetime

# COMMAND ----------

t = datetime.datetime.now().strftime('%Y%m%d')

# COMMAND ----------

volumepath = "/Volumes/mimi_ws_1/zillow/src"

# COMMAND ----------

def download_file(url, filename, folder):
    # NOTE the stream=True parameter below
    with requests.get(f"{url}", stream=True) as r:
        r.raise_for_status()
        with open(f"{folder}/{filename}", 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192): 
                # If you have chunk encoded response uncomment if
                # and set chunk_size parameter to None.
                #if chunk: 
                f.write(chunk)

# COMMAND ----------

url = "https://files.zillowstatic.com/research/public_csvs/zhvi/Zip_zhvi_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv"
download_file(url, f"homevalue_zipcode_{t}.csv", "/Volumes/mimi_ws_1/zillow/src/")

# COMMAND ----------

url = "https://files.zillowstatic.com/research/public_csvs/zori/Zip_zori_uc_sfrcondomfr_sm_month.csv"
download_file(url, f"rentals_zipcode_{t}.csv", "/Volumes/mimi_ws_1/zillow/src/")

# COMMAND ----------


