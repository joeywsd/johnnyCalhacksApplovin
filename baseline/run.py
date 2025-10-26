#!/usr/bin/env python3
"""
Run Phase - Query Execution
----------------------------
Executes queries against the optimized partitioned Parquet data.

Usage:
  python run.py --out-dir ./out
"""
import duckdb
import time
from pathlib import Path
import csv
import argparse
from assembler import assemble_sql
from inputs import queries
from query_router import route_query

# Data store location (created by prepare.py)
DATA_STORE = Path("data_store")
DB_PATH = Path("tmp/optimized.duckdb")


def run_queries(queries, out_dir: Path):
    """
    Execute queries against partitioned Parquet data.

    Performance optimizations:
    - Partitioned by (type, day) enables partition pruning
    - Parquet columnar format enables fast column scans
    - ZSTD compression reduces I/O
    - Hive partitioning for automatic filtering
    """
    print(f"[RUN] Starting query execution")
    print(f"   Data source: {DATA_STORE}/events/")
    print(f"   Output directory: {out_dir}/")

    # Ensure output directory exists
    out_dir.mkdir(parents=True, exist_ok=True)
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)

    # Connect to DuckDB
    con = duckdb.connect(str(DB_PATH))

    # Create view over partitioned Parquet files
    # hive_partitioning=1 tells DuckDB to use partition columns for filtering
    print(f"\n[RUN] Loading partitioned data...")
    load_start = time.time()

    con.execute(f"""
        CREATE OR REPLACE VIEW events AS
        SELECT * FROM read_parquet(
            '{DATA_STORE}/events/*/*/*.parquet',
            hive_partitioning = 1
        )
    """)

    load_time = time.time() - load_start
    print(f"[RUN] Data loaded in {load_time:.3f}s")

    # Get row count for verification
    row_count = con.execute("SELECT COUNT(*) FROM events").fetchone()[0]
    print(f"   Total rows: {row_count:,}")

    # Execute queries
    print(f"\n{'='*60}")
    print(f"EXECUTING QUERIES")
    print(f"{'='*60}")

    results = []
    total_query_time = 0

    for i, q in enumerate(queries, 1):
        print(f"\n[RUN] Query {i}:")
        print(f"   {q}")

        # Try to use query router for pre-aggregations
        sql = route_query(q)

        if sql is None:
            # No pre-agg match, fallback to partitioned data
            print(f"[ROUTER] No pre-agg match - using partitioned data (fallback)")
            sql = assemble_sql(q)

        # Execute query with timing
        t0 = time.time()
        res = con.execute(sql)
        cols = [d[0] for d in res.description]
        rows = res.fetchall()
        dt = time.time() - t0

        total_query_time += dt

        print(f"[RUN] Rows: {len(rows):,} | Time: {dt:.3f}s")

        # Write results to CSV
        out_path = out_dir / f"q{i}.csv"
        with out_path.open("w", newline="") as f:
            w = csv.writer(f)
            w.writerow(cols)
            w.writerows(rows)

        # Show sample results
        if len(rows) > 0:
            print(f"   Sample result: {rows[0]}")

        results.append({
            "query": i,
            "rows": len(rows),
            "time": dt,
            "output": str(out_path)
        })

    con.close()

    # Print summary
    print(f"\n{'='*60}")
    print(f"SUMMARY")
    print(f"{'='*60}")

    for r in results:
        print(f"Q{r['query']}: {r['time']:.3f}s ({r['rows']:,} rows)")

    print(f"\n[RUN] Performance Metrics:")
    print(f"   Total query time: {total_query_time:.3f}s")
    print(f"   Average per query: {total_query_time/len(queries):.3f}s")
    print(f"   Data load time: {load_time:.3f}s")
    print(f"   Total runtime: {load_time + total_query_time:.3f}s")

    print(f"\n[RUN] Results written to: {out_dir}/")

    return results


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Run benchmark queries against optimized data"
    )
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=Path("./out"),
        help="Where to output query results (default: ./out)"
    )

    args = parser.parse_args()

    # Check if data store exists
    if not DATA_STORE.exists():
        print(f"[ERROR] Data store not found at {DATA_STORE}/")
        print(f"\nPlease run prepare.py first to create the optimized data store.")
        print(f"Example: python prepare.py --data-zip ../data-lite/data-lite/data-lite.zip")
        exit(1)

    # Check if data store has data
    parquet_files = list(DATA_STORE.glob("events/*/*/*.parquet"))
    if not parquet_files:
        print(f"[ERROR] No Parquet files found in {DATA_STORE}/events/")
        print(f"\nPlease run prepare.py first to create the optimized data store.")
        exit(1)

    run_queries(queries, args.out_dir)
