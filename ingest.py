# Databricks notebook source
# MAGIC %run /Workspace/Repos/yubin.park@mimilabs.ai/mimi-common-utils/ingestion_utils

# COMMAND ----------

# MAGIC %md
# MAGIC ## Home Value

# COMMAND ----------

file_lst = []
for file in Path("/Volumes/mimi_ws_1/zillow/src").glob("homevalue_zipcode*"):
    file_lst.append(file)
# sort
file_lst = sorted(file_lst)
file = file_lst[-1]
pdf = pd.read_csv(file, dtype={"RegionID": str, "RegionName": str})
col_set1 = pdf.columns[:9]
col_set2 = pdf.columns[9:]
pdf_lst = []
for colname in col_set2:
    pdf_t = pdf.loc[:,col_set1]
    pdf_t["date"] = parse(colname).date()
    pdf_t["value"] = pdf[colname]
    pdf_t.columns = change_header(pdf_t.columns)
    pdf_t = pdf_t.dropna(subset=["value"])
    pdf_lst.append(pdf_t)
pdf_full = pd.concat(pdf_lst)
pdf_full["mimi_src_file_date"] = parse(file.stem[-8:]).date()
pdf_full["mimi_src_file_name"] = file.name
pdf_full["mimi_dlt_load_date"] = datetime.today().date()
pdf_full = pdf_full.rename(columns={"region_i_d": "region_id", "region_name": "zip"})
pdf_full = pdf_full.drop(columns=["region_type"])
df = spark.createDataFrame(pdf_full)
(df.write
    .format('delta')
    .mode("overwrite")
    .saveAsTable("mimi_ws_1.zillow.homevalue_zip"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Rent

# COMMAND ----------

file_lst = []
for file in Path("/Volumes/mimi_ws_1/zillow/src").glob("rentals_zipcode*"):
    file_lst.append(file)
# sort
file_lst = sorted(file_lst)
file = file_lst[-1]
pdf = pd.read_csv(file, dtype={"RegionID": str, "RegionName": str})
col_set1 = pdf.columns[:9]
col_set2 = pdf.columns[9:]
pdf_lst = []
for colname in col_set2:
    pdf_t = pdf.loc[:,col_set1]
    pdf_t["date"] = parse(colname).date()
    pdf_t["value"] = pdf[colname]
    pdf_t.columns = change_header(pdf_t.columns)
    pdf_t = pdf_t.dropna(subset=["value"])
    pdf_lst.append(pdf_t)
pdf_full = pd.concat(pdf_lst)
pdf_full["mimi_src_file_date"] = parse(file.stem[-8:]).date()
pdf_full["mimi_src_file_name"] = file.name
pdf_full["mimi_dlt_load_date"] = datetime.today().date()
pdf_full = pdf_full.rename(columns={"region_i_d": "region_id",
                         "region_name": "zip"})
pdf_full = pdf_full.drop(columns=["region_type"])
df = spark.createDataFrame(pdf_full)
(df.write
    .format('delta')
    .mode("overwrite")
    .saveAsTable("mimi_ws_1.zillow.rent_zip"))
