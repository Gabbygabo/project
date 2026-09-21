import argparse
import logging
from src.config import PROJECT_ROOT, DB, SETTINGS, path_for
from src.common.audit import new_run_id
from src.extract import extract_sources
from src.transform.staging import build_staging
from src.transform.curated import build_curated
from src.load import load_curated
from src.load.postgres import load_partition   

# Configure logging
logging.basicConfig(
    filename="pipeline.log",
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

def main():
    parser = argparse.ArgumentParser(description='DSS150P modular pipeline')
    sub = parser.add_subparsers(dest='command', required=True)

    # --- extract ---
    e = sub.add_parser('extract')
    e.add_argument('--run-id', type=str)
    e.add_argument('--mode', choices=['full', 'partition'], required=True)
    e.add_argument('--year', type=int, required=True)
    e.add_argument('--month', type=int, required=True)

    # --- transform ---
    t = sub.add_parser('transform')
    t.add_argument('--run-id', type=str, required=True)

    # --- load ---
    l = sub.add_parser('load')
    l.add_argument('--run-id', type=str, required=True)

    # --- validate ---
    v = sub.add_parser('validate')
    v.add_argument('--run-id', type=str, required=True)

    # --- run-all ---
    sub.add_parser('run-all')

    # --- load-partition (Task 9.4) ---
    lp = sub.add_parser('load-partition')
    lp.add_argument('--year', type=int, required=True)
    lp.add_argument('--month', type=int, required=True)

    args = parser.parse_args()

    try:
        # --- extract ---
        if args.command == 'extract':
            run_id = args.run_id or new_run_id()
            logging.info(f"Starting extraction for run_id={run_id}, mode={args.mode}, year={args.year}, month={args.month}")
            raw_path = extract_sources(run_id)
            print(f"Extraction complete. Snapshot at: {raw_path}")
            return

        # --- transform ---
        if args.command == 'transform':
            logging.info(f"Starting staging+curation for run_id={args.run_id}")
            raw_dir = path_for("raw") / f"run_id={args.run_id}"
            staged, quarantine = build_staging(raw_dir, args.run_id)
            print(f"Staging complete. Outputs at: {path_for('staging') / f'run_id={args.run_id}'}")
            if not quarantine.empty:
                print(f"Quarantined records written to: {path_for('quarantine') / f'run_id={args.run_id}'}")

            # ✅ Call curate here so ETL works without separate curate step
            curated, orphans = build_curated(staged, args.run_id)
            print(f"Curated complete. Outputs at: {path_for('curated') / f'run_id={args.run_id}'}")
            if not orphans.empty:
                print(f"Orphan records quarantined at: {path_for('quarantine') / f'run_id={args.run_id}'}")
            return

        # --- load ---
        if args.command == 'load':
            logging.info(f"Starting load into PostgreSQL for run_id={args.run_id}")
            curated_dir = path_for("curated") / f"run_id={args.run_id}"
            curated_df = None
            try:
                import pandas as pd
                curated_df = pd.read_parquet(curated_dir / "orders_curated.parquet")
            except Exception as e:
                logging.error(f"Load failed: curated parquet not found for run_id={args.run_id}")
                raise

            load_curated(curated_df, args.run_id)
            print(f"Load complete. Data inserted/updated in PostgreSQL.")
            return

        # --- validate ---
        if args.command == 'validate':
            logging.info(f"Starting validation for run_id={args.run_id}")
            curated_dir = path_for("curated") / f"run_id={args.run_id}"
            try:
                import pandas as pd
                curated_df = pd.read_parquet(curated_dir / "orders_curated.parquet")
            except Exception as e:
                logging.error(f"Validate failed: curated parquet not found for run_id={args.run_id}")
                raise

            # Simple validation rule: dataset must not be empty
            if curated_df.empty:
                raise ValueError("Validation failed: curated dataset is empty")
            else:
                print(f"Validation passed: {len(curated_df)} records found for run_id={args.run_id}")
            return

        # --- run-all ---
        if args.command == 'run-all':
            run_id = new_run_id()
            logging.info(f"Starting full pipeline for run_id={run_id}")
            print(f"Running full pipeline for run_id={run_id}")

            # Extraction
            raw_path = extract_sources(run_id)
            print(f"Extraction complete. Snapshot at: {raw_path}")

            # Staging + Curated
            raw_dir = path_for("raw") / f"run_id={run_id}"
            staged, quarantine = build_staging(raw_dir, run_id)
            print(f"Staging complete. Outputs at: {path_for('staging') / f'run_id={run_id}'}")
            if not quarantine.empty:
                print(f"Quarantined records written to: {path_for('quarantine') / f'run_id={run_id}'}")

            curated, orphans = build_curated(staged, run_id)
            print(f"Curated complete. Outputs at: {path_for('curated') / f'run_id={run_id}'}")
            if not orphans.empty:
                print(f"Orphan records quarantined at: {path_for('quarantine') / f'run_id={run_id}'}")

            # Load
            load_curated(curated, run_id)
            print(f"Load complete. Data inserted/updated in PostgreSQL.")

            # Validate
            if curated.empty:
                raise ValueError("Validation failed: curated dataset is empty")
            else:
                print(f"Validation passed: {len(curated)} records found for run_id={run_id}")
            return

        # --- load-partition (Task 9.4) ---
        if args.command == 'load-partition':
            run_id = new_run_id()
            logging.info(f"Starting partition load for year={args.year}, month={args.month}, run_id={run_id}")
            row_count = load_partition(args.year, args.month, run_id)
            print(f"Partition {args.year}-{args.month} loaded with {row_count} rows (run_id={run_id})")
            return

    except FileNotFoundError as e:
        logging.error(f"{args.command.capitalize()} failed for run_id={getattr(args, 'run_id', None)}: Missing file {e}")
        raise
    except Exception as e:
        logging.exception(f"{args.command.capitalize()} failed for run_id={getattr(args, 'run_id', None)}")
        raise

if __name__ == '__main__':
    main()
