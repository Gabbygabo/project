from .postgres import upsert_curated, load_partition

# Alias for compatibility with cli.py
load_curated = upsert_curated

__all__ = ["upsert_curated", "load_partition", "load_curated"]
