#This is a code file to extract and display the compressed data.

import pandas as pd

df_data = pd.read_csv('sample_sales_data.csv')

#Reading the raw segmentation of data into its cold, warm and hot rows
df_cold_raw = pd.read_csv('output/cold_raw.csv')
print(df_cold_raw)
df_warm_raw = pd.read_csv('output/warm_raw.csv')
print(df_warm_raw)
df_hot_raw = pd.read_csv('output/hot_raw.csv')
print(df_hot_raw)

# Read the parquet files
df_hot = pd.read_parquet("output/hot.parquet", engine="pyarrow")
df_warm = pd.read_parquet("output/warm.parquet", engine="pyarrow")
df_cold = pd.read_parquet("output/cold.parquet", engine="pyarrow")

