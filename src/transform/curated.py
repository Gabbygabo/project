import pandas as pd
import hashlib
from datetime import datetime, timezone
from src.config import path_for

def build_curated(staging: dict, run_id: str):
    # Create output directories
    curated_dir = path_for("curated") / f"run_id={run_id}"
    curated_dir.mkdir(parents=True, exist_ok=True)

    quarantine_dir = path_for("quarantine") / f"run_id={run_id}"
    quarantine_dir.mkdir(parents=True, exist_ok=True)

    # Load staged DataFrames
    customers = staging["customers"]
    products = staging["products"]
    orders = staging["orders"]

    # Join orders → customers → products
    merged = orders.merge(customers, on="customer_id", how="left", suffixes=("", "_cust"))
    merged = merged.merge(products, on="product_id", how="left", suffixes=("", "_prod"))

    # Identify orphan references (missing customer or product)
    orphans = merged[merged["email"].isna() | merged["unit_price"].isna()].copy()
    if not orphans.empty:
        orphans["reason"] = "Orphan reference"
        orphans.to_parquet(quarantine_dir / "curated_orphans.parquet", index=False)

    # Keep only valid joined rows
    curated = merged.dropna(subset=["email", "unit_price"]).copy()

    # Flatten required fields (fill with None if missing)
    curated["customer_city"] = curated["city"] if "city" in curated.columns else None
    curated["customer_tier"] = curated["tier"] if "tier" in curated.columns else None
    curated["product_name"] = curated["name"] if "name" in curated.columns else None
    curated["category"] = curated["category"] if "category" in curated.columns else None
    curated["brand"] = curated["brand"] if "brand" in curated.columns else None

    # Calculations
    curated["gross_amount"] = curated["quantity"] * curated["unit_price"]
    curated["discount_amount"] = curated["gross_amount"] * curated["discount_pct"]
    curated["net_amount"] = curated["gross_amount"] - curated["discount_amount"]

    # Audit columns
    curated["source_updated_at"] = curated["updated_at"]
    curated["pipeline_run_id"] = run_id
    curated["processed_at_utc"] = datetime.now(timezone.utc)
    curated["status"] = "active"

    # Deterministic record hash
    def make_hash(row):
        content = f"{row['order_id']}-{row['customer_id']}-{row['product_id']}-{row['quantity']}-{row['unit_price']}-{row['discount_pct']}"
        return hashlib.sha256(content.encode()).hexdigest()
    curated["record_hash"] = curated.apply(make_hash, axis=1)

    # Sanitize: convert dicts to strings
    curated = curated.applymap(lambda x: str(x) if isinstance(x, dict) else x)

    # Cast numeric fields
    for col in ["quantity", "unit_price", "discount_pct", "gross_amount", "discount_amount", "net_amount"]:
        curated[col] = pd.to_numeric(curated[col], errors="coerce")

    # ✅ Save curated dataset (critical for load step)
    curated.to_parquet(curated_dir / "orders_curated.parquet", index=False)

    return curated, orphans
