import pandas as pd

df_data = pd.read_csv('sample_sales_data.csv')

# Read the parquet file
df_hot = pd.read_parquet("output/hot.parquet", engine="pyarrow")
df_warm = pd.read_parquet("output/warm.parquet", engine="pyarrow")
df_cold = pd.read_parquet("output/cold.parquet", engine="pyarrow")

# Inspect the data
print(df_data)
print(df_hot)
print(df_warm)
print(df_cold)

