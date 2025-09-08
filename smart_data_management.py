import os
import pandas as pd
from datetime import datetime

def deduplicate(df):
    return df.drop_duplicates(subset=["Order ID"])

def transform(df):
    df = df.copy()
    df["Order Date"] = pd.to_datetime(df["Order Date"], errors="coerce")
    return df

def partition_by_date(df, asof_date):
    df = df.copy()
    hot_cutoff = asof_date - pd.DateOffset(years=3)
    warm_cutoff = asof_date - pd.DateOffset(years=6)

    hot = df[df["Order Date"] >= hot_cutoff]
    warm = df[(df["Order Date"] < hot_cutoff) & (df["Order Date"] >= warm_cutoff)]
    cold = df[df["Order Date"] < warm_cutoff]

    return hot, warm, cold

def retain(df, tier):
    if tier == "Hot":
        kept_cols = df.columns.tolist()
        desc = "Full fidelity preservation"
        df_ret = df[kept_cols].copy()
        compression = "snappy"

    elif tier == "Warm":
        kept_cols = [c for c in df.columns if c not in ["Unit Cost", "Total Cost"]]
        desc = "Pruned low-priority columns"
        df_ret = df[kept_cols].copy()
        compression = "gzip"

    elif tier == "Cold":
        kept_cols = ["Region", "Country", "Item Type", "Order Date", "Total Revenue", "Total Profit"]
        desc = "High-level summary only"
        df_ret = df[kept_cols].copy()
        df_ret = df_ret.groupby(["Region", "Country", "Item Type"]).agg({
            "Total Revenue": "sum",
            "Total Profit": "sum"
        }).reset_index()
        compression = "brotli"

    else:
        raise ValueError(f"Unknown tier: {tier}")

    return df_ret, desc, compression

def run_pipeline(input_file, output_dir, asof_date):
    print(f"No CLI arguments detected.\nAuto-running on: {input_file}")
    print(f"Results will be saved in: {output_dir}")
    os.makedirs(output_dir, exist_ok=True)

    total_input_size = os.path.getsize(input_file) / (1024 * 1024)
    print(f"Found 1 input file(s). Total input size: {total_input_size:.2f} MB")

    df = pd.read_csv(input_file, parse_dates=["Order Date"])
    hot, warm, cold = partition_by_date(df, asof_date)
    print(f"Finished partitioning {len(df):,} rows.\n")

    tiers = {"Hot": hot, "Warm": warm, "Cold": cold}
    before_sizes, after_sizes = {}, {}

    for tier, data in tiers.items():
        # Save raw tier as CSV → measure disk size
        temp_file = os.path.join(output_dir, f"{tier.lower()}_raw.csv")
        data.to_csv(temp_file, index=False)
        before_size = os.path.getsize(temp_file) / (1024 * 1024)
        before_sizes[tier] = before_size
        
        print(f"[{tier}] dedup + transform...")
        data = deduplicate(data)
        data = transform(data)

        # Apply retention + save as parquet with compression
        retained, desc, compression = retain(data, tier)
        retained_file = os.path.join(output_dir, f"{tier.lower()}.parquet")
        retained.to_parquet(retained_file, compression=compression, engine="pyarrow")
        after_size = os.path.getsize(retained_file) / (1024 * 1024)
        after_sizes[tier] = after_size

        # Delete temporary CSV
        #os.remove(temp_file)

        print(f"[{tier}] {desc}")
        print(f"Before: {before_size:.2f} MB → After: {after_size:.2f} MB "
              f"(saved {100*(1-after_size/before_size):.1f}%)\n")

    print("\n--- GRAND TOTAL ---")
    total_before = sum(before_sizes.values())
    total_after = sum(after_sizes.values())
    print(f"saved {100*(1-total_after/total_before):.1f}% "
          f"({total_before:.2f} MB → {total_after:.2f} MB)")

def main():
    input_file = os.path.join(os.getcwd(), "sample_sales_data.csv")
    output_dir = os.path.join(os.getcwd(), "output")
    asof_date = datetime.today()
    run_pipeline(input_file, output_dir, asof_date)

if __name__ == "__main__":
    main()
