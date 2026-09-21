import psycopg
import pandas as pd
from pathlib import Path
from datetime import datetime, timezone
from src.config import DB

def upsert_curated(df, run_id: str) -> int:
    """Load curated.sales_order_lines using rerun-safe UPSERT semantics.

    Requirement: order_id is the conflict key. A rerun with unchanged records
    must not create duplicate business keys.
    """
    upsert_sql = """
    INSERT INTO curated.sales_order_lines (
        order_id, customer_id, product_id, order_timestamp,
        customer_city, customer_tier,
        product_name, category, brand,
        quantity, unit_price, discount_pct,
        gross_amount, discount_amount, net_amount,
        status, source_updated_at, pipeline_run_id,
        processed_at_utc, record_hash
    )
    VALUES (
        %(order_id)s, %(customer_id)s, %(product_id)s, %(order_timestamp)s,
        %(customer_city)s, %(customer_tier)s,
        %(product_name)s, %(category)s, %(brand)s,
        %(quantity)s, %(unit_price)s, %(discount_pct)s,
        %(gross_amount)s, %(discount_amount)s, %(net_amount)s,
        %(status)s, %(source_updated_at)s, %(pipeline_run_id)s,
        %(processed_at_utc)s, %(record_hash)s
    )
    ON CONFLICT (order_id)
    DO UPDATE SET
        customer_id = EXCLUDED.customer_id,
        product_id = EXCLUDED.product_id,
        order_timestamp = EXCLUDED.order_timestamp,
        customer_city = EXCLUDED.customer_city,
        customer_tier = EXCLUDED.customer_tier,
        product_name = EXCLUDED.product_name,
        category = EXCLUDED.category,
        brand = EXCLUDED.brand,
        quantity = EXCLUDED.quantity,
        unit_price = EXCLUDED.unit_price,
        discount_pct = EXCLUDED.discount_pct,
        gross_amount = EXCLUDED.gross_amount,
        discount_amount = EXCLUDED.discount_amount,
        net_amount = EXCLUDED.net_amount,
        status = EXCLUDED.status,
        source_updated_at = EXCLUDED.source_updated_at,
        pipeline_run_id = EXCLUDED.pipeline_run_id,
        processed_at_utc = EXCLUDED.processed_at_utc,
        record_hash = EXCLUDED.record_hash
    WHERE sales_order_lines.record_hash <> EXCLUDED.record_hash;
    """

    count = 0
    with psycopg.connect(
        host=DB["host"],
        dbname=DB["dbname"],
        user=DB["user"],
        password=DB["password"]
    ) as conn:
        with conn.cursor() as cur:
            for _, row in df.iterrows():
                cur.execute(upsert_sql, row.to_dict())
                count += 1
        conn.commit()
    return count


def load_partition(year: int, month: int, run_id: str) -> int:
    """Load only a selected year/month partition and record audit.partition_loads."""
    # Directly use data/partitioned path (no config change)
    partition_path = Path("data/partitioned") / f"order_year={year}" / f"order_month={month}"
    df = pd.read_parquet(partition_path)

    # UPSERT into curated.sales_order_lines
    count = upsert_curated(df, run_id)

    # Record the load in audit.partition_loads
    partition_key = f"{year}-{month:02d}"   # e.g. "2026-01"
    loaded_at_utc = datetime.now(timezone.utc)

    with psycopg.connect(
        host=DB["host"],
        dbname=DB["dbname"],
        user=DB["user"],
        password=DB["password"]
    ) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO audit.partition_loads (partition_key, loaded_at_utc, row_count, pipeline_run_id) VALUES (%s, %s, %s, %s)",
                (partition_key, loaded_at_utc, count, run_id)
            )
        conn.commit()

    return count
