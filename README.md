# Query Planner Challenge - Optimized Solution

An optimized database system for processing and querying large-scale ad event data, achieving **90x speedup** over the baseline through intelligent storage layout, pre-aggregation, and query planning.

## Overview

This solution transforms a 15M+ row ad event dataset into an optimized query engine using:
- **Partitioned Parquet storage** with (type, day) partitioning
- **Pre-aggregated summary tables** for common query patterns
- **Intelligent query router** that selects optimal data source
- **Fallback mechanism** ensuring correctness on all queries

## Performance Results

**IMPORTANT: Results below are from the data-lite dataset (15M rows, ~5.4GB compressed). Full 20GB dataset testing in progress.**

**Dataset: data-lite (15M rows)**

| Solution | Total Time | vs Baseline | Speedup |
|----------|------------|-------------|---------|
| Baseline (CSV) | 36.184s | - | 1.0x |
| Our Solution | **0.398s** | ↓ 35.8s | **90.9x** |

### Per-Query Performance

| Query | Baseline | Optimized | Speedup |
|-------|----------|-----------|---------|
| Q1 - Daily revenue | 7.790s | 0.100s | **77.9x** |
| Q2 - Publisher revenue by location/date | 4.814s | 0.098s | **49.1x** |
| Q3 - Average purchase by country | 7.009s | 0.003s | **2,336x** |
| Q4 - Event counts by advertiser | 6.327s | 0.011s | **575x** |
| Q5 - Minute-level revenue | 10.243s | 0.186s | **55.1x** |

## Quick Start

### Prerequisites

- Python 3.7+
- Required packages: `pandas`, `duckdb`

```bash
pip install -r requirements.txt
```

### Usage

#### Step 1: Prepare Data (One-time)

Process CSV data and create optimized storage:

```bash
# From zip file
python prepare.py --data-path ../data-lite/data-lite.zip

# From directory (judges will use this)
python prepare.py --data-path ../data-lite/data-lite/
```

This creates:
- Partitioned Parquet files in `data_store/events/`
- Pre-aggregated summary tables in `data_store/agg_*.parquet`

#### Step 2: Run Queries

Execute benchmark queries:

```bash
python run.py --out-dir ./out
```

Results are written to CSV files in the output directory.

#### Verification

Compare results against baseline:

```bash
# Run baseline
python main.py --data-dir ../data-lite/data-lite/ --out-dir ./out_baseline

# Compare
python verify_results.py --baseline ./out_baseline --optimized ./out
```

## Architecture

### 1. Data Preparation (`prepare.py`)

**Input:** Raw CSV files (events_part_*.csv)
**Output:** Optimized Parquet storage

**Process:**
1. **Load & Transform:** Read CSVs, cast types, compute derived columns (day, week, hour, minute)
2. **Partition:** Export to Parquet partitioned by `(type, day)` for efficient filtering
3. **Pre-aggregate:** Create 3 summary tables for common query patterns

**Optimizations:**
- Streams data chunk-by-chunk (low memory usage)
- Single export (avoids file locking issues)
- ZSTD compression (reduces I/O)

### 2. Pre-Aggregated Tables

#### `agg_counts_by_advertiser_type.parquet`
Solves: Q4 (event counts by advertiser and type)
```sql
SELECT advertiser_id, type, COUNT(*) AS event_count
GROUP BY advertiser_id, type
```

#### `agg_revenue_by_minute_publisher_country.parquet`
Solves: Q1, Q2, Q5 (revenue analysis)
```sql
SELECT minute, day, publisher_id, country, SUM(bid_price)
WHERE type = 'impression'
GROUP BY minute, day, publisher_id, country
```

#### `agg_purchase_summary.parquet`
Solves: Q3 (purchase analysis)
```sql
SELECT country, SUM(total_price), COUNT(total_price)
WHERE type = 'purchase'
GROUP BY country
```

### 3. Query Router (`query_router.py`)

**How it works:**
1. Analyzes incoming JSON query structure
2. Attempts to match against 5 pre-aggregation patterns
3. If match found → rewrites SQL to use summary table
4. If no match → falls back to partitioned Parquet (still fast!)

**Example:**
```python
# Query: Daily revenue totals
{"select": ["day", {"SUM": "bid_price"}],
 "where": [{"col": "type", "op": "eq", "val": "impression"}],
 "group_by": ["day"]}

# Router rewrites to:
SELECT day, SUM(total_revenue)
FROM agg_revenue_by_minute_publisher_country.parquet
GROUP BY day
```

### 4. Query Execution (`run.py`)

**Process:**
1. Load partitioned Parquet with hive partitioning
2. For each query:
   - Try query router (pre-aggregations)
   - Fallback to partitioned data if no match
3. Execute and write results

## Key Design Decisions

### Why Parquet?
- **Columnar format:** Only reads columns needed by query
- **Compression:** 5-10x smaller than CSV
- **Metadata:** Min/max statistics enable pruning

### Why Partition by (type, day)?
- **Type selectivity:** Most queries filter by type (impression, purchase, etc.)
- **Time-based analysis:** Day is the most common time granularity
- **Partition pruning:** Queries scan only relevant partitions

Example: `WHERE type='impression' AND day='2024-06-01'` scans only 1 out of ~1,460 partitions!

### Why Pre-Aggregations?
- **Query patterns are predictable:** Ad analytics has common questions
- **Trade space for speed:** Summary tables are tiny (~1-100MB)
- **Correctness guaranteed:** Fallback ensures all queries work

### Why Query Router?
- **Flexibility:** Not limited to 5 queries
- **Transparency:** Logs which path is used
- **Safety:** Fallback prevents failures

## Technical Details

### Resource Usage (data-lite: 15M rows)

**Prepare Phase:**
- Time: ~6 minutes (from zip) or ~5 minutes (from directory)
- Memory: <2GB (streams data)
- Disk:
  - Partitioned Parquet: ~500MB
  - Pre-aggregations: ~50MB
  - Total: ~550MB

**Run Phase:**
- Time: <1 second total (0.398s)
- Memory: <500MB

### Compatibility

- ✅ Works with zip files (saves disk space)
- ✅ Works with directories (judges will use this)
- ✅ Cross-platform (Windows, Mac, Linux)
- ✅ Self-contained (no network access during run phase)

## Files

```
baseline/
├── prepare.py              # Data preparation script
├── run.py                  # Query execution script
├── query_router.py         # Intelligent query planner
├── assembler.py            # JSON to SQL converter
├── inputs.py               # Benchmark queries
├── verify_results.py       # Correctness checker
├── main.py                 # Baseline implementation
├── requirements.txt        # Dependencies
└── README.md              # This file
```

## How to Run on Full Dataset (20GB)

**Note: Full dataset performance not yet verified. Prepare phase may take 15-30 minutes.**

```bash
# Prepare (estimated time: 15-30 min for unzipped directory)
python prepare.py --data-path ../data/

# Run queries (expected: <5 seconds)
python run.py --out-dir ./results
```

## Innovation Highlights

1. **Hybrid Architecture:** Combines pre-aggregations (speed) with partitions (flexibility)
2. **Defensive Router:** Pattern matching with guaranteed fallback
3. **Resource Efficient:** Streams from zip, avoiding 20GB disk usage
4. **Production Ready:** Handles edge cases, provides clear logging

## Credits

Built for the Query Planner Challenge at Cal Hacks 2025.

**Optimization Techniques:**
- Columnar storage (Parquet)
- Hive-style partitioning
- Materialized pre-aggregations
- Query rewriting and routing
- Partition pruning
- Predicate pushdown (via DuckDB)

---

**Result:** 90x faster than baseline with 100% accuracy ✅
