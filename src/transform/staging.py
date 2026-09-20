import pandas as pd
from pathlib import Path
from datetime import datetime, timezone
from src.config import path_for

def build_staging(raw_dir: Path, run_id: str):
    """Create cleaned, typed staging datasets.

    Rules:
    - Deduplicate by business key, keeping greatest updated_at.
    - Parse timestamps as UTC.
    - Normalize emails/cities and flatten product.category.
    - Validate order quantity/status and product price.
    - Add pipeline_run_id and staged_at_utc audit columns.
    - Write invalid records to data/quarantine/ with a reason.

    Return a dict of staging DataFrames and a quarantine DataFrame.
    """
    staging_dir = path_for("staging") / f"run_id={run_id}"
    staging_dir.mkdir(parents=True, exist_ok=True)

    quarantine_dir = path_for("quarantine") / f"run_id={run_id}"
    quarantine_dir.mkdir(parents=True, exist_ok=True)

    staged = {}
    quarantine_records = []

    staged_at = datetime.now(timezone.utc)

    # --- Customers ---
    customers = pd.read_csv(raw_dir / "customers.csv")
    customers["email"] = customers["email"].str.strip().str.lower()
    customers["city"] = customers["city"].str.strip().str.title()
    customers["updated_at"] = pd.to_datetime(customers["updated_at"], utc=True)
    customers = customers.sort_values("updated_at").drop_duplicates("customer_id", keep="last")
    customers["pipeline_run_id"] = run_id
    customers["staged_at_utc"] = staged_at
    customers.to_parquet(staging_dir / "customers.parquet", index=False)
    staged["customers"] = customers

    # --- Products ---
    products = pd.read_json(raw_dir / "products.json")
    products["category_name"] = products["category"].apply(lambda c: c.get("name") if isinstance(c, dict) else None)
    products["category_department"] = products["category"].apply(lambda c: c.get("department") if isinstance(c, dict) else None)
    products["unit_price"] = pd.to_numeric(products["unit_price"], errors="coerce")
    products["updated_at"] = pd.to_datetime(products["updated_at"], utc=True)

    invalid_products = products[products["unit_price"] < 0].copy()
    if not invalid_products.empty:
        invalid_products["reason"] = "Negative price"
        quarantine_records.append(invalid_products)

    products = products[products["unit_price"] >= 0]
    products = products.sort_values("updated_at").drop_duplicates("product_id", keep="last")
    products["pipeline_run_id"] = run_id
    products["staged_at_utc"] = staged_at
    products.to_parquet(staging_dir / "products.parquet", index=False)
    staged["products"] = products

    # --- Orders (CSV, not JSON!) ---
    orders = pd.read_csv(raw_dir / "orders.csv")
    orders["order_timestamp"] = pd.to_datetime(orders["order_timestamp"], utc=True)
    orders["updated_at"] = pd.to_datetime(orders["updated_at"], utc=True)
    orders["quantity"] = pd.to_numeric(orders["quantity"], errors="coerce")

    allowed_statuses = ["PENDING", "PAID", "PACKED", "SHIPPED", "DELIVERED", "CANCELLED"]
    invalid_orders = orders[
        (orders["quantity"] < 1) | (orders["quantity"] > 20) | (~orders["status"].isin(allowed_statuses))
    ].copy()
    if not invalid_orders.empty:
        invalid_orders["reason"] = "Invalid quantity/status"
        quarantine_records.append(invalid_orders)

    orders = orders[
        (orders["quantity"].between(1, 20)) & (orders["status"].isin(allowed_statuses))
    ]
    orders = orders.sort_values("updated_at").drop_duplicates("order_id", keep="last")
    orders["pipeline_run_id"] = run_id
    orders["staged_at_utc"] = staged_at
    orders.to_parquet(staging_dir / "orders.parquet", index=False)
    staged["orders"] = orders

    # --- Quarantine ---
    if quarantine_records:
        quarantine_df = pd.concat(quarantine_records, ignore_index=True)
        quarantine_df.to_parquet(quarantine_dir / "quarantine.parquet", index=False)
    else:
        quarantine_df = pd.DataFrame()

    return staged, quarantine_df 