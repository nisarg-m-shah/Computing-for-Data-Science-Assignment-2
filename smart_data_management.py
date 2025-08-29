# smart_data_management.py

import os
import sys
import argparse
import pandas as pd
from datetime import datetime
from pathlib import Path
import warnings
warnings.simplefilter("ignore", category=pd.errors.SettingWithCopyWarning)

# ---------------------------
# Deduplication
# ---------------------------
def deduplicate(df):
    """Remove duplicate rows based on Order ID (unique per order)."""
    if "Order ID" in df.columns:
        return df.drop_duplicates(subset=["Order ID"])
    else:
        return df.drop_duplicates()


# ---------------------------
# Transform
# ---------------------------
def transform(df):
    """Basic transformations if needed (ensure datetime parsing)."""
    if "Order Date" in df.columns:
        df["Order Date"] = pd.to_datetime(df["Order Date"], errors="coerce")
    return df


# ---------------------------
# Partition data into tiers
# ---------------------------
def partition_by_date(df, asof):
    hot_cutoff = asof.replace(year=asof.year - 3)
    warm_cutoff = asof.replace(year=asof.year - 6)

    hot = df[df["Order Date"] >= hot_cutoff]
    warm = df[(df["Order Date"] < hot_cutoff) & (df["Order Date"] >= warm_cutoff)]
    cold = df[df["Order Date"] < warm_cutoff]

    return hot, warm, cold


# ---------------------------
# Retention (compress / keep subset)
# ---------------------------
def retain(df, tier):
    """Simulate retention policies (compression/column drops)."""
    if tier == "Hot":
        return df  # keep everything
    elif tier == "Warm":
        return df.drop(columns=["Region", "Country"], errors="ignore")
    else:  # Cold
        return df[["Order Date", "Order ID", "Item Type", "Units Sold", "Total Profit"]]
    

# ---------------------------
# Pipeline
# ---------------------------
def run_pipeline(input_file, output_dir, asof_date, chunksize=100000):
    print(f"No CLI arguments detected.\nAuto-running on: {input_file}")
    print(f"Results will be saved in: {output_dir}")

    os.makedirs(output_dir, exist_ok=True)

    total_size = os.path.getsize(input_file) / (1024 * 1024)
    print(f"Found 1 input file(s). Total input size: {total_size:.2f} MB")

    print(f"Tiers by Order Date with as-of {asof_date.date()}:")
    print(f"  Hot  : >= {asof_date.year-3}-{asof_date.month:02}-{asof_date.day:02} (last 3 years)")
    print(f"  Warm : {asof_date.year-6}-{asof_date.month:02}-{asof_date.day:02} .. "
          f"{asof_date.year-3}-{asof_date.month:02}-{asof_date.day:02} (3–6 years)")
    print(f"  Cold : < {asof_date.year-6}-{asof_date.month:02}-{asof_date.day:02} (>6 years)")

    # Load CSV
    print("Streaming and partitioning...")
    df = pd.read_csv(input_file, parse_dates=["Order Date"])
    hot, warm, cold = partition_by_date(df, asof_date)
    print(f"Finished partitioning {len(df):,} rows.\n")

    tiers = {"Hot": hot, "Warm": warm, "Cold": cold}

    before_sizes = {}
    after_sizes = {}

    for tier, data in tiers.items():
        print(f"[{tier}] loading raw -> dedup -> transform...")
        data = deduplicate(data)
        data = transform(data)

        before_size = data.memory_usage(deep=True).sum() / (1024 * 1024)
        before_sizes[tier] = before_size

        retained = retain(data, tier)
        retained_file = os.path.join(output_dir, f"{tier.lower()}.parquet")
        retained.to_parquet(retained_file, compression="zstd", engine="pyarrow")
        after_size = os.path.getsize(retained_file) / (1024 * 1024)
        after_sizes[tier] = after_size

    print("\n--- Storage BEFORE retention ---")
    for tier in tiers:
        print(f"{tier:<5}: {before_sizes[tier]:.2f} MB")

    print("\n--- Storage AFTER retention ---")
    for tier in tiers:
        print(f"{tier:<5}: saved {100*(1-after_sizes[tier]/before_sizes[tier]):.1f}% "
              f"({before_sizes[tier]:.2f} MB → {after_sizes[tier]:.2f} MB)")

    total_before = sum(before_sizes.values())
    total_after = sum(after_sizes.values())

    print("\n--- GRAND TOTAL ---")
    print(f"saved {100*(1-total_after/total_before):.1f}% "
          f"({total_before:.2f} MB → {total_after:.2f} MB)")

    # Combine all retained
    retained_df = pd.concat([
        pd.read_parquet(os.path.join(output_dir, "hot.parquet")),
        pd.read_parquet(os.path.join(output_dir, "warm.parquet")),
        pd.read_parquet(os.path.join(output_dir, "cold.parquet")),
    ])
    retained_file = os.path.join(output_dir, "retained.parquet")
    retained_df.to_parquet(retained_file, compression="zstd", engine="pyarrow")
    final_size = os.path.getsize(retained_file) / (1024 * 1024)
    print(f"\nWrote final retained dataset → {retained_file} ({final_size:.2f} MB)\n")

    # === Per-year breakdown ===
    print("--- Per-year breakdown (exact file sizes) ---")
    year_dir = os.path.join(output_dir, "per_year")
    os.makedirs(year_dir, exist_ok=True)

    rows = []
    for year, group in retained_df.groupby(retained_df["Order Date"].dt.year):
        year_file = os.path.join(year_dir, f"{year}.parquet")
        group.to_parquet(year_file, compression="zstd", engine="pyarrow")

        before_bytes = group.memory_usage(deep=True).sum()
        after_bytes = os.path.getsize(year_file)

        rows.append({
            "Year": year,
            "Before": before_bytes / (1024 * 1024),
            "After": after_bytes / (1024 * 1024),
            "Pct": 100 * (1 - after_bytes / before_bytes) if before_bytes > 0 else 0
        })

    df_years = pd.DataFrame(rows).sort_values("Year")
    print(f"{'Year':<6}{'Before':>14}{'After':>14}{'% Saved':>10}")
    for r in df_years.itertuples():
        print(f"{r.Year:<6}{r.Before:10.2f} MB{r.After:12.2f} MB{r.Pct:9.1f}%")

    print(f"\n(Per-year parquet files saved in: {year_dir})")


# ---------------------------
# Main
# ---------------------------
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", help="Path to input CSV file")
    parser.add_argument("--outdir", help="Output directory")
    parser.add_argument("--asof", help="As-of date (YYYY-MM-DD)", default=datetime.today().strftime("%Y-%m-%d"))
    args = parser.parse_args()

    if not args.input or not args.outdir:
        # Auto mode
        base_dir = Path(__file__).resolve().parent
        input_file = str(base_dir / "sample_sales_data.csv")
        output_dir = str(base_dir / "output")
    else:
        input_file = args.input
        output_dir = args.outdir

    asof_date = datetime.strptime(args.asof, "%Y-%m-%d")
    run_pipeline(input_file, output_dir, asof_date, chunksize=100000)


if __name__ == "__main__":
    main()
