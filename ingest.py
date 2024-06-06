# Databricks notebook source
!pip install tqdm

# COMMAND ----------

import pandas as pd
from pathlib import Path
import datetime
from dateutil.parser import parse
from tqdm import tqdm
import re

# COMMAND ----------

def camel_to_underscore(text):
    return re.sub(r'(?<!^)(?=[A-Z])', '_', text).lower()

def change_header(header_org):
    return [camel_to_underscore(re.sub(r'\W+', '', column))
            for column in header_org]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Home Value

# COMMAND ----------

file_lst = []
for file in Path("/Volumes/mimi_ws_1/zillow/src").glob("homevalue_zipcode*"):
    file_lst.append(file)
# sort
file_lst = sorted(file_lst)

# COMMAND ----------

pdf = pd.read_csv(file_lst[-1], dtype={"RegionID": str,
                                       "RegionName": str})

# COMMAND ----------

col_set1 = pdf.columns[:9]
col_set2 = pdf.columns[9:]

# COMMAND ----------

pdf_lst = []
for colname in tqdm(col_set2):
    pdf_t = pdf.loc[:,col_set1]
    pdf_t["date"] = parse(colname).date()
    pdf_t["value"] = pdf[colname]
    pdf_t.columns = change_header(pdf_t.columns)
    pdf_t = pdf_t.dropna(subset=["value"])
    pdf_lst.append(pdf_t)
pdf_full = pd.concat(pdf_lst)
ifd = parse(file_lst[-1].stem[-8]).date()
pdf_full["_input_file_date"] = ifd
pdf_full = pdf_full.rename(columns={"region_i_d": "region_id",
                         "region_name": "zcta"})
pdf_full = pdf_full.drop(columns=["region_type"])    

# COMMAND ----------

df = spark.createDataFrame(pdf_full)
(df.write
    .format('delta')
    .mode("overwrite")
    .saveAsTable("mimi_ws_1.zillow.homevalue_zcta"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Rent

# COMMAND ----------

file_lst = []
for file in Path("/Volumes/mimi_ws_1/zillow/src").glob("rentals_zipcode*"):
    file_lst.append(file)
# sort
file_lst = sorted(file_lst)

# COMMAND ----------

pdf = pd.read_csv(file_lst[-1], dtype={"RegionID": str,
                                       "RegionName": str})

# COMMAND ----------

col_set1 = pdf.columns[:9]
col_set2 = pdf.columns[9:]

# COMMAND ----------

pdf_lst = []
for colname in tqdm(col_set2):
    pdf_t = pdf.loc[:,col_set1]
    pdf_t["date"] = parse(colname).date()
    pdf_t["value"] = pdf[colname]
    pdf_t.columns = change_header(pdf_t.columns)
    pdf_t = pdf_t.dropna(subset=["value"])
    pdf_lst.append(pdf_t)
pdf_full = pd.concat(pdf_lst)
ifd = parse(file_lst[-1].stem[-8]).date()
pdf_full["_input_file_date"] = ifd
pdf_full = pdf_full.rename(columns={"region_i_d": "region_id",
                         "region_name": "zcta"})
pdf_full = pdf_full.drop(columns=["region_type"])

# COMMAND ----------

df = spark.createDataFrame(pdf_full)
(df.write
    .format('delta')
    .mode("overwrite")
    .saveAsTable("mimi_ws_1.zillow.rent_zcta"))

# COMMAND ----------


