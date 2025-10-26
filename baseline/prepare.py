#!/usr/bin/env python3
"""
Prepare Phase - Data Optimization
----------------------------------
Processes data from either a zip file or directory, transforms it,
and writes partitioned Parquet files for optimal query performance.

Usage:
  python prepare.py --data-path ./data-lite/data-lite.zip  # From zip
  python prepare.py --data-path ./data-lite/data-lite/data-lite       # From directory
  python prepare.py --data-path ./data/data.zip                        # Full dataset
"""
import zipfile
import pandas as pd
import duckdb
import argparse
from pathlib import Path
import time

# Output directory for optimized data
DATA_STORE = Path("data_store")


def prepare_from_directory(data_dir: Path):
    """
    Process CSV files from an unzipped directory.
    This is the method judges will use.
    """
    print(f"[PREPARE] Reading from unzipped directory: {data_dir}")
    start_time = time.time()

    # Clean up old outputs
    import shutil
    if DATA_STORE.exists():
        print(f"[PREPARE] Removing old data store...")
        shutil.rmtree(DATA_STORE)
    DATA_STORE.mkdir(parents=True, exist_ok=True)

    # Create temporary database file
    temp_db = Path("tmp/prepare_temp.duckdb")
    temp_db.parent.mkdir(parents=True, exist_ok=True)
    if temp_db.exists():
        temp_db.unlink()

    # Connect to persistent DuckDB database
    con = duckdb.connect(str(temp_db))

    # Create table with final schema
    print(f"[PREPARE] Creating events table...")
    con.execute("""
        CREATE TABLE events (
            ts TIMESTAMP,
            week TIMESTAMP,
            day DATE,
            hour TIMESTAMP,
            minute VARCHAR,
            type VARCHAR,
            auction_id VARCHAR,
            advertiser_id INTEGER,
            publisher_id INTEGER,
            bid_price DOUBLE,
            user_id BIGINT,
            total_price DOUBLE,
            country VARCHAR
        )
    """)

    # Find all CSV files
    csv_files = sorted(data_dir.glob("events_part_*.csv"))

    if not csv_files:
        raise FileNotFoundError(f"No events_part_*.csv files found in {data_dir}")

    print(f"[PREPARE] Found {len(csv_files)} CSV files")
    print(f"[PREPARE] Loading data into table...")

    total_rows = 0
    for i, csv_file in enumerate(csv_files, 1):
        print(f"[PREPARE] Processing {i}/{len(csv_files)}: {csv_file.name}")

        # Read CSV into pandas
        chunk_df = pd.read_csv(csv_file, dtype=str)

        if chunk_df.empty:
            print(f"[PREPARE] WARNING: Skipping empty file: {csv_file}")
            continue

        # Register DataFrame with DuckDB
        con.register('chunk_df', chunk_df)

        # Transform and INSERT into events table
        con.execute("""
            INSERT INTO events
            WITH raw AS (
                SELECT * FROM chunk_df
            ),
            casted AS (
                SELECT
                    to_timestamp(TRY_CAST(ts AS DOUBLE) / 1000.0)    AS ts,
                    type,
                    auction_id,
                    TRY_CAST(advertiser_id AS INTEGER)        AS advertiser_id,
                    TRY_CAST(publisher_id  AS INTEGER)        AS publisher_id,
                    NULLIF(bid_price, '')::DOUBLE             AS bid_price,
                    TRY_CAST(user_id AS BIGINT)               AS user_id,
                    NULLIF(total_price, '')::DOUBLE           AS total_price,
                    country
                FROM raw
            )
            SELECT
                ts,
                DATE_TRUNC('week', ts)              AS week,
                DATE(ts)                            AS day,
                DATE_TRUNC('hour', ts)              AS hour,
                STRFTIME(ts, '%Y-%m-%d %H:%M')      AS minute,
                type,
                auction_id,
                advertiser_id,
                publisher_id,
                bid_price,
                user_id,
                total_price,
                country
            FROM casted
        """)

        total_rows += len(chunk_df)
        print(f"[PREPARE] Loaded {len(chunk_df):,} rows (total: {total_rows:,})")

    # Now export to partitioned Parquet ONCE (avoids file locking)
    print(f"\n[PREPARE] Exporting to partitioned Parquet...")
    export_start = time.time()

    con.execute(f"""
        COPY events
        TO '{DATA_STORE}/events' (
            FORMAT PARQUET,
            PARTITION_BY (type, day),
            OVERWRITE_OR_IGNORE true,
            COMPRESSION 'ZSTD'
        )
    """)

    export_time = time.time() - export_start
    print(f"[PREPARE] Export complete in {export_time:.2f}s")

    # Create pre-aggregated summary tables for faster queries
    print(f"\n[PREPARE] Creating pre-aggregated summary tables...")
    preagg_start = time.time()

    # Priority #1: Advertiser event counts (for Q4)
    print(f"[PREPARE] Creating advertiser event counts table...")
    con.execute(f"""
        COPY (
            SELECT
                advertiser_id,
                type,
                COUNT(*) AS event_count
            FROM read_parquet('{DATA_STORE}/events/*/*/*.parquet', hive_partitioning=1)
            GROUP BY advertiser_id, type
        ) TO '{DATA_STORE}/agg_counts_by_advertiser_type.parquet' (FORMAT PARQUET)
    """)

    # Priority #2: Revenue by minute/day/publisher/country (for Q1, Q2, Q5)
    print(f"[PREPARE] Creating revenue summary table...")
    con.execute(f"""
        COPY (
            SELECT
                minute,
                day,
                publisher_id,
                country,
                SUM(bid_price) AS total_revenue
            FROM read_parquet('{DATA_STORE}/events/*/*/*.parquet', hive_partitioning=1)
            WHERE type = 'impression'
            GROUP BY minute, day, publisher_id, country
        ) TO '{DATA_STORE}/agg_revenue_by_minute_publisher_country.parquet' (FORMAT PARQUET)
    """)

    # Priority #3: Purchase summary by country (for Q3)
    print(f"[PREPARE] Creating purchase summary table...")
    con.execute(f"""
        COPY (
            SELECT
                country,
                SUM(total_price) AS sum_of_price,
                COUNT(total_price) AS count_of_purchases
            FROM read_parquet('{DATA_STORE}/events/*/*/*.parquet', hive_partitioning=1)
            WHERE type = 'purchase'
            GROUP BY country
        ) TO '{DATA_STORE}/agg_purchase_summary.parquet' (FORMAT PARQUET)
    """)

    preagg_time = time.time() - preagg_start
    print(f"[PREPARE] Pre-aggregation complete in {preagg_time:.2f}s")

    total_rows = con.execute("SELECT COUNT(*) FROM events").fetchone()[0]

    elapsed = time.time() - start_time
    print(f"\n[PREPARE] Preparation complete!")
    print(f"   Total rows processed: {total_rows:,}")
    print(f"   Time taken: {elapsed:.2f}s")
    print(f"   Output directory: {DATA_STORE}/")

    # Show partition structure
    print(f"\n[PREPARE] Partition structure:")
    partitions = sorted(DATA_STORE.glob("events/*/*/*.parquet"))
    if partitions:
        print(f"   Total partition files: {len(partitions)}")
        print(f"   Sample partitions:")
        for p in partitions[:5]:
            rel_path = p.relative_to(DATA_STORE)
            print(f"      {rel_path}")
        if len(partitions) > 5:
            print(f"      ... and {len(partitions) - 5} more")

    # Show pre-agg files
    print(f"\n[PREPARE] Pre-aggregated tables:")
    preagg_files = sorted(DATA_STORE.glob("agg_*.parquet"))
    for p in preagg_files:
        print(f"   {p.name}")

    con.close()

    # Clean up temp database
    if temp_db.exists():
        temp_db.unlink()


def prepare_from_zip(zip_path: Path):
    """
    Stream CSV files from zip, transform, and write partitioned Parquet.

    Strategy:
    - Process one CSV at a time (low memory usage)
    - Load all data into DuckDB table first
    - Export to Parquet once (avoids file locking issues)
    """
    print(f"[PREPARE] Reading from zip file: {zip_path}")
    start_time = time.time()

    # Clean up old outputs
    import shutil
    if DATA_STORE.exists():
        print(f"[PREPARE] Removing old data store...")
        shutil.rmtree(DATA_STORE)
    DATA_STORE.mkdir(parents=True, exist_ok=True)

    # Create temporary database file
    temp_db = Path("tmp/prepare_temp.duckdb")
    temp_db.parent.mkdir(parents=True, exist_ok=True)
    if temp_db.exists():
        temp_db.unlink()

    # Connect to persistent DuckDB database
    con = duckdb.connect(str(temp_db))

    # Create table with final schema
    print(f"[PREPARE] Creating events table...")
    con.execute("""
        CREATE TABLE events (
            ts TIMESTAMP,
            week TIMESTAMP,
            day DATE,
            hour TIMESTAMP,
            minute VARCHAR,
            type VARCHAR,
            auction_id VARCHAR,
            advertiser_id INTEGER,
            publisher_id INTEGER,
            bid_price DOUBLE,
            user_id BIGINT,
            total_price DOUBLE,
            country VARCHAR
        )
    """)

    # Open the zip file
    with zipfile.ZipFile(zip_path, 'r') as zf:
        # Find all CSV files in the zip (skip __MACOSX and other metadata files)
        csv_files = [f for f in zf.namelist()
                     if f.endswith('.csv') and 'events_part_' in f and '__MACOSX' not in f]

        if not csv_files:
            raise FileNotFoundError(f"No events_part_*.csv files found in {zip_path}")

        print(f"[PREPARE] Found {len(csv_files)} CSV files in zip")
        print(f"[PREPARE] Loading data into table...")

        total_rows = 0
        for i, csv_file in enumerate(csv_files, 1):
            print(f"[PREPARE] Processing {i}/{len(csv_files)}: {csv_file}")

            # Read CSV from zip into pandas DataFrame
            with zf.open(csv_file) as f:
                # Read as strings (matches baseline behavior)
                chunk_df = pd.read_csv(f, dtype=str)

                if chunk_df.empty:
                    print(f"[PREPARE] WARNING: Skipping empty file: {csv_file}")
                    continue

                # Register DataFrame with DuckDB
                con.register('chunk_df', chunk_df)

                # Transform and INSERT into events table
                con.execute("""
                    INSERT INTO events
                    WITH raw AS (
                        SELECT * FROM chunk_df
                    ),
                    casted AS (
                        SELECT
                            to_timestamp(TRY_CAST(ts AS DOUBLE) / 1000.0)    AS ts,
                            type,
                            auction_id,
                            TRY_CAST(advertiser_id AS INTEGER)        AS advertiser_id,
                            TRY_CAST(publisher_id  AS INTEGER)        AS publisher_id,
                            NULLIF(bid_price, '')::DOUBLE             AS bid_price,
                            TRY_CAST(user_id AS BIGINT)               AS user_id,
                            NULLIF(total_price, '')::DOUBLE           AS total_price,
                            country
                        FROM raw
                    )
                    SELECT
                        ts,
                        DATE_TRUNC('week', ts)              AS week,
                        DATE(ts)                            AS day,
                        DATE_TRUNC('hour', ts)              AS hour,
                        STRFTIME(ts, '%Y-%m-%d %H:%M')      AS minute,
                        type,
                        auction_id,
                        advertiser_id,
                        publisher_id,
                        bid_price,
                        user_id,
                        total_price,
                        country
                    FROM casted
                """)

                total_rows += len(chunk_df)
                print(f"[PREPARE] Loaded {len(chunk_df):,} rows (total: {total_rows:,})")

    # Now export to partitioned Parquet ONCE (avoids file locking)
    print(f"\n[PREPARE] Exporting to partitioned Parquet...")
    export_start = time.time()

    con.execute(f"""
        COPY events
        TO '{DATA_STORE}/events' (
            FORMAT PARQUET,
            PARTITION_BY (type, day),
            OVERWRITE_OR_IGNORE true,
            COMPRESSION 'ZSTD'
        )
    """)

    export_time = time.time() - export_start
    print(f"[PREPARE] Export complete in {export_time:.2f}s")

    # Create pre-aggregated summary tables for faster queries
    print(f"\n[PREPARE] Creating pre-aggregated summary tables...")
    preagg_start = time.time()

    # Priority #1: Advertiser event counts (for Q4)
    print(f"[PREPARE] Creating advertiser event counts table...")
    con.execute(f"""
        COPY (
            SELECT
                advertiser_id,
                type,
                COUNT(*) AS event_count
            FROM read_parquet('{DATA_STORE}/events/*/*/*.parquet', hive_partitioning=1)
            GROUP BY advertiser_id, type
        ) TO '{DATA_STORE}/agg_counts_by_advertiser_type.parquet' (FORMAT PARQUET)
    """)

    # Priority #2: Revenue by minute/day/publisher/country (for Q1, Q2, Q5)
    print(f"[PREPARE] Creating revenue summary table...")
    con.execute(f"""
        COPY (
            SELECT
                minute,
                day,
                publisher_id,
                country,
                SUM(bid_price) AS total_revenue
            FROM read_parquet('{DATA_STORE}/events/*/*/*.parquet', hive_partitioning=1)
            WHERE type = 'impression'
            GROUP BY minute, day, publisher_id, country
        ) TO '{DATA_STORE}/agg_revenue_by_minute_publisher_country.parquet' (FORMAT PARQUET)
    """)

    # Priority #3: Purchase summary by country (for Q3)
    print(f"[PREPARE] Creating purchase summary table...")
    con.execute(f"""
        COPY (
            SELECT
                country,
                SUM(total_price) AS sum_of_price,
                COUNT(total_price) AS count_of_purchases
            FROM read_parquet('{DATA_STORE}/events/*/*/*.parquet', hive_partitioning=1)
            WHERE type = 'purchase'
            GROUP BY country
        ) TO '{DATA_STORE}/agg_purchase_summary.parquet' (FORMAT PARQUET)
    """)

    preagg_time = time.time() - preagg_start
    print(f"[PREPARE] Pre-aggregation complete in {preagg_time:.2f}s")

    total_rows = con.execute("SELECT COUNT(*) FROM events").fetchone()[0]

    elapsed = time.time() - start_time
    print(f"\n[PREPARE] Preparation complete!")
    print(f"   Total rows processed: {total_rows:,}")
    print(f"   Time taken: {elapsed:.2f}s")
    print(f"   Output directory: {DATA_STORE}/")

    # Show partition structure
    print(f"\n[PREPARE] Partition structure:")
    partitions = sorted(DATA_STORE.glob("events/*/*/*.parquet"))
    if partitions:
        print(f"   Total partition files: {len(partitions)}")
        print(f"   Sample partitions:")
        for p in partitions[:5]:
            rel_path = p.relative_to(DATA_STORE)
            print(f"      {rel_path}")
        if len(partitions) > 5:
            print(f"      ... and {len(partitions) - 5} more")

    # Show pre-agg files
    print(f"\n[PREPARE] Pre-aggregated tables:")
    preagg_files = sorted(DATA_STORE.glob("agg_*.parquet"))
    for p in preagg_files:
        print(f"   {p.name}")

    con.close()

    # Clean up temp database
    if temp_db.exists():
        temp_db.unlink()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Prepare data from zip file or directory, creating optimized Parquet partitions"
    )
    parser.add_argument(
        "--data-path",
        type=Path,
        default=Path("./data-lite/data-lite.zip"),
        help="Path to zip file or directory containing CSV data (default: ./data-lite/data-lite.zip)"
    )

    args = parser.parse_args()
    data_path = args.data_path
go
    if not data_path.exists():
        print(f"[ERROR] Path not found: {data_path}")
        print(f"\nPlease provide a valid zip file or directory path.")
        print(f"Examples:")
        print(f"  python prepare.py --data-path ./data-lite/data-lite.zip     # From zip")
        print(f"  python prepare.py --data-path ./data-lite/data-lite/data-lite         # From directory")
        exit(1)

    # Detect input type and call appropriate function
    if data_path.is_dir():
        # Judges' method: Read from unzipped directory
        prepare_from_directory(data_path)
    elif data_path.is_file() and data_path.suffix == '.zip':
        # Your local method: Read from zip file (saves disk space)
        prepare_from_zip(data_path)
    else:
        print(f"[ERROR] Data path must be a directory or .zip file: {data_path}")
        print(f"\nCurrent path type: {data_path}")
        exit(1)