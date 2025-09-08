# CIA 2 Assignment: Controlling Unbounded Data Growth

This repository contains the implementation and research documentation for a temporal data tiering system that achieves 97.5% storage reduction through progressive optimization strategies.

## Repository Structure

### Core Implementation
- **`smart_data_management.py`** - Main Python implementation of the tiered data lifecycle management system. Processes 5M records through Hot/Warm/Cold tiers with progressive optimization (deduplication, column pruning, aggregation, and adaptive compression).

### Data Files  
- **`sample_sales_data.csv`** - 5 million record enterprise sales dataset from Kaggle (596.22 MB). Contains 14 columns spanning temporal, categorical, and numerical data types used for system evaluation.

### Output
- **`output/`** - Directory containing processed results from the tiered optimization pipeline, including tier-specific Parquet files and performance metrics.

### Validation & Analysis
- **`extracting_data.py`** - Utility script for extracting and validating data from the generated Parquet files to verify proper tier compartmentalization and optimization according to the defined tiering strategy.

### Research Documentation
- **`Final Research Paper.pdf`** - Comprehensive research paper documenting the methodology, implementation, experimental results (97.5% storage reduction), trade-off analysis, and findings on the dominance of format conversion over compression algorithms.

## Key Results
- **Storage Reduction**: 97.5% overall (596.22 MB → 20.59 MB)
- **Processing Scale**: 5,000,000 transaction records
- **Tier Performance**: Hot (93.7%), Warm (95.5%), Cold (100.0% reduction)

## Usage
```bash
python smart_data_management.py
```
The system automatically processes `sample_sales_data.csv` and saves optimized results to the `output/` directory.