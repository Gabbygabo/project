import os
import time
import statistics
import pandas as pd
from sqlalchemy import create_engine, text
from src.config import DB   # ✅ Import DB settings from config.py


def get_latest_curated_parquet(base_dir="data/curated", filename="orders_curated.parquet"):
    """Find the latest run_id folder and return the path to its curated Parquet file."""
    run_dirs = [
        os.path.join(base_dir, d)
        for d in os.listdir(base_dir)
        if d.startswith("run_id=")
    ]
    if not run_dirs:
        raise FileNotFoundError("No run_id folders found in data/curated/")
    latest_run = max(run_dirs, key=os.path.getmtime)
    print(f"Using curated dataset from: {latest_run}")
    return os.path.join(latest_run, filename)


def run_benchmark(curated_path, output_dir, repeats: int = 5):
    """Compare the same logical dataset in CSV, JSON Lines, Parquet, and PostgreSQL."""
    df = pd.read_parquet(curated_path)
    results = []

    # --- CSV ---
    csv_path = os.path.join(output_dir, "curated.csv")
    for _ in range(repeats):
        t0 = time.perf_counter()
        df.to_csv(csv_path, index=False)
        write_time = time.perf_counter() - t0

        size = os.path.getsize(csv_path)

        t0 = time.perf_counter()
        df2 = pd.read_csv(csv_path, low_memory=False)
        full_read = time.perf_counter() - t0

        t0 = time.perf_counter()
        _ = df2[df2["status"] == "COMPLETE"]
        filtered_read = time.perf_counter() - t0

        results.append(("CSV", size, write_time, full_read, filtered_read, len(df2)))

    # --- JSON Lines ---
    jsonl_path = os.path.join(output_dir, "curated.jsonl")
    for _ in range(repeats):
        t0 = time.perf_counter()
        df.to_json(jsonl_path, orient="records", lines=True)
        write_time = time.perf_counter() - t0

        size = os.path.getsize(jsonl_path)

        t0 = time.perf_counter()
        df2 = pd.read_json(jsonl_path, lines=True)
        full_read = time.perf_counter() - t0

        t0 = time.perf_counter()
        _ = df2[df2["status"] == "COMPLETE"]
        filtered_read = time.perf_counter() - t0

        results.append(("JSONL", size, write_time, full_read, filtered_read, len(df2)))

    # --- Parquet ---
    parquet_path = os.path.join(output_dir, "curated.parquet")
    for _ in range(repeats):
        t0 = time.perf_counter()
        df.to_parquet(parquet_path, compression="snappy")
        write_time = time.perf_counter() - t0

        size = os.path.getsize(parquet_path)

        t0 = time.perf_counter()
        df2 = pd.read_parquet(parquet_path)
        full_read = time.perf_counter() - t0

        t0 = time.perf_counter()
        _ = df2[df2["status"] == "COMPLETE"]
        filtered_read = time.perf_counter() - t0

        results.append(("Parquet", size, write_time, full_read, filtered_read, len(df2)))

    # --- PostgreSQL via SQLAlchemy ---
    engine = create_engine(
        f"postgresql+psycopg2://{DB['user']}:{DB['password']}@{DB['host']}:{DB['port']}/{DB['dbname']}"
    )

    for _ in range(repeats):
        # Full read
        t0 = time.perf_counter()
        df2 = pd.read_sql("SELECT * FROM curated.sales_order_lines", engine)
        full_read = time.perf_counter() - t0

        # Filtered read
        t0 = time.perf_counter()
        df3 = pd.read_sql("SELECT * FROM curated.sales_order_lines WHERE status='COMPLETE'", engine)
        filtered_read = time.perf_counter() - t0

        # Sizes (✅ fixed with text())
        with engine.connect() as conn:
            total_size = conn.execute(
                text("SELECT pg_total_relation_size('curated.sales_order_lines');")
            ).scalar()

        results.append(("Postgres", total_size, None, full_read, filtered_read, len(df2)))

    engine.dispose()

    # --- Aggregate medians ---
    summary = {}
    for fmt in set(r[0] for r in results):
        fmt_results = [r for r in results if r[0] == fmt]
        write_times = [r[2] for r in fmt_results if r[2] is not None]
        summary[fmt] = {
            "size": fmt_results[0][1],
            "write_time_median": statistics.median(write_times) if write_times else None,
            "full_read_median": statistics.median(r[3] for r in fmt_results),
            "filtered_read_median": statistics.median(r[4] for r in fmt_results),
            "rows": fmt_results[0][5],
        }
    return summary


def write_partitioned_parquet(df, output_dir):
    """Write Parquet partitioned by order_year/order_month."""
    if "order_timestamp" in df.columns:
        df["order_year"] = pd.to_datetime(df["order_timestamp"]).dt.year
        df["order_month"] = pd.to_datetime(df["order_timestamp"]).dt.month
    elif "created_at" in df.columns:
        df["order_year"] = pd.to_datetime(df["created_at"]).dt.year
        df["order_month"] = pd.to_datetime(df["created_at"]).dt.month
    else:
        raise KeyError("No suitable date column found in dataframe")

    df.to_parquet(
        output_dir,
        compression="snappy",
        partition_cols=["order_year", "order_month"]
    )


if __name__ == "__main__":
    curated_path = get_latest_curated_parquet()
    output_dir = "data/curated"

    summary = run_benchmark(curated_path, output_dir, repeats=5)

    print("\n=== Storage Benchmark Summary ===")
    for fmt, stats in summary.items():
        print(f"\nFormat: {fmt}")
        for k, v in stats.items():
            print(f"  {k}: {v}")

    df = pd.read_parquet(curated_path)
    write_partitioned_parquet(df, "data/partitioned")
    print("\nPartitioned parquet written to data/partitioned/")
