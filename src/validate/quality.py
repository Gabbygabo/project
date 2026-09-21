# src/validate/quality.py

import pandas as pd

def validate_curated(df: pd.DataFrame) -> list[str]:
    """Return a list of human-readable validation errors.

    Minimum checks: order_id uniqueness/non-null, quantity range,
    nonnegative amounts, allowed statuses, required audit fields.
    """
    errors = []

    # --- order_id checks ---
    if df["order_id"].isnull().any():
        errors.append("Some rows have null order_id.")
    if df["order_id"].duplicated().any():
        errors.append("Duplicate order_id values found.")

    # --- quantity range ---
    if (df["quantity"] <= 0).any():
        errors.append("Quantity must be positive.")

    # --- nonnegative amounts ---
    for col in ["gross_amount", "discount_amount", "net_amount"]:
        if (df[col] < 0).any():
            errors.append(f"Negative values found in {col}.")

    # --- allowed statuses ---
    allowed_statuses = {"NEW", "PROCESSING", "COMPLETE", "CANCELLED"}
    invalid_statuses = set(df["status"].unique()) - allowed_statuses
    if invalid_statuses:
        errors.append(f"Invalid status values: {invalid_statuses}")

    # --- required audit fields ---
    required_fields = ["source_updated_at", "pipeline_run_id", "processed_at_utc", "record_hash"]
    for field in required_fields:
        if field not in df.columns:
            errors.append(f"Missing required audit field: {field}")
        elif df[field].isnull().any():
            errors.append(f"Null values found in audit field: {field}")

    return errors
