# DSS150P Laboratory 3 Starter Repository

This repository supports Module 2: Pipeline Construction, Storage, and Orchestration.
It is intentionally incomplete. Students must implement the marked TODOs and document their decisions.

## Main progression
- Goal 1: reproducible environment, modularization, Git, Docker, configuration
- Goal 2: raw -> staging -> curated transformations; audit/error handling; rerun-safe loading
- Goal 3: CSV/JSON/Parquet/PostgreSQL comparison; partitioning; selected-partition load
- Goal 4: Apache Airflow DAG for extract -> transform -> load -> validate

Start with `DSS150P_Laboratory_Activity_3.pdf`.

## Recommended commands
```bash
cp .env.example .env
python -m venv .venv
# activate .venv then:
pip install -r requirements.txt
python -m src.cli validate-env
```
The provided `.env.example` uses `POSTGRES_HOST=localhost` for host-side commands. Docker Compose overrides the application containers to use the service hostname `postgres`.

Docker/PostgreSQL:
```bash
docker compose up -d postgres
docker compose run --rm pipeline python -m src.cli validate-env
```

Airflow in Goal 4:
```bash
docker compose -f docker-compose.yml -f docker-compose.airflow.yml up airflow-init
docker compose -f docker-compose.yml -f docker-compose.airflow.yml up -d airflow-webserver airflow-scheduler
```
Airflow UI: http://localhost:8080 (training credentials: admin/admin; change if reused outside the lab).
------------------------------------------------

**All documentations and screenshots/copy pasted evidence output are inside the folder of documentations**

6-INITIAL SETUPS
-created own env.example as it is not in the repository, then copied the environment in docker-compose.yml.

TASKS 7
7.1 The virtual environment should not be committed to Git because it is machine-specific, large in size, and unnecessary for reproducibility.
The requirements.txt will handle it instead as it is more lightweight and without needing my own .venv.

7.2 inside the extract folder, there are __init__.py and files.py
inside the transform folder, there are __init__.py and curated.py and stating.py
inside the load folder, there are __init__.py and postgres.py
inside the validate folder, there are __init__.py and quality.py
inside the benchmark folder, there are __init__py. and storage.py
lastly, there is a cli.py in src folder

**All of these are inside the src folder, after inspecting, each module have one primary responsibility.

7.5 EXPLANATION: The config.py module acts as the single translator, loading both sources and exposing them as usable settings, ensuring reproducibility and keeping secrets out of committed code.

TASKS 8
8.2 settings.yml and config.py have different keys.
config.py has "raw", "source", etc.
settings.yml has "{key}_dir" at the end.

8.6 The POSTGRES_DB in .env was named as postgres, instead of dss150p. I changed it to dss150p and it worked okay now.

TASKS 9
9.1 I used SQLAlchemy because the system recommended it, and that I was worrying there might be some errors to encounter.

Below is a quick snapshot of the output.

(.venv) C:\Users\Lenovo Thinkpad T460\project>python -m src.benchmark.storage
Using curated dataset from: data/curated\run_id=run_20260920T132227Z_1ff5112e
C:\Users\Lenovo Thinkpad T460\project\src\benchmark\storage.py:96: UserWarning: pandas only supports SQLAlchemy connectable (engine/connection) or database string URI or sqlite3 DBAPI2 connection. Other DBAPI2 objects are not tested. Please consider using SQLAlchemy.
  df2 = pd.read_sql("SELECT * FROM curated.sales_order_lines", conn)
C:\Users\Lenovo Thinkpad T460\project\src\benchmark\storage.py:101: UserWarning: pandas only supports SQLAlchemy connectable (engine/connection) or database string URI or sqlite3 DBAPI2 connection. Other DBAPI2 objects are not tested. Please consider using SQLAlchemy.
  df3 = pd.read_sql("SELECT * FROM curated.sales_order_lines WHERE status='COMPLETE'", conn)

9.2 Based on the result, in terms of write_time_median, Parquet has the lowest write_time_median. Whereas, csx got the highest.
On the other hand, in terms of full_read_median, Parquet still has the lowest, and JSONL got the highest.
Lastly, in terms of filtered_read_median, CSV got the lowest, and postgres was the highest.

For context, I am using a Laptop Lenovo Thinkpad T460 and has a following specs:
CPU: Intel Core i5‑6300U @ 2.40 GHz (dual‑core, 4 threads)
RAM: 8 GB DDR4
Storage: 256 GB SSD
Operating System: Windows 11 Pro 64‑bit

9.3 Partitioning makes the system to touch the files it only needs, instead of scanning the whole dataset, especially if it is large. Making the query much faster and lesser memory usage.

9.4 After reruuning the same partition load, it failed. This means that is is not duplicated and that the deduplication logic is working

Below is the output after running it again:

(.venv) C:\Users\Lenovo Thinkpad T460\project>python -m src.cli load-partition --year 2026 --month 1
Traceback (most recent call last):
  File "<frozen runpy>", line 198, in _run_module_as_main
  File "<frozen runpy>", line 88, in _run_code
  File "C:\Users\Lenovo Thinkpad T460\project\src\cli.py", line 126, in <module>
    main()
    ~~~~^^
  File "C:\Users\Lenovo Thinkpad T460\project\src\cli.py", line 114, in main
    row_count = load_partition(args.year, args.month, run_id)
  File "C:\Users\Lenovo Thinkpad T460\project\src\load\postgres.py", line 91, in load_partition
    cur.execute(
    ~~~~~~~~~~~^
        "INSERT INTO audit.partition_loads (partition_key, loaded_at_utc, row_count, pipeline_run_id) VALUES (%s, %s, %s, %s)",
        ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
        (partition_key, loaded_at_utc, count, run_id)
        ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    )
    ^
  File "C:\Users\Lenovo Thinkpad T460\AppData\Local\Programs\Python\Python313\Lib\site-packages\psycopg\cursor.py", line 97, in execute
    raise ex.with_traceback(None)
psycopg.errors.UniqueViolation: duplicate key value violates unique constraint "partition_loads_pkey"
DETAIL:  Key (partition_key)=(2026-01) already exists.

and below is the output after running it once/the first time:
(.venv) C:\Users\Lenovo Thinkpad T460\project>python -m src.cli load-partition --year 2026 --month 1
Partition 2026-1 loaded with 2511 rows (run_id=run_20260921T110516Z_b83a96d4)

9.5 answers
1. Which file format was smallest on your machine, and what encoding/compression characteristics help explain the result? 
Parquet was the smallest because it stores data in columns and applies compression, which reduces repeated values and saves space.

2. Which representation was fastest for a full dataset read? Does that imply it is best for every workload?
Parquet was fastest for reading the whole dataset, but that doesn’t mean it’s best for every workload. Databases like PostgreSQL are better when you only need specific rows or queries.

3. How did filtered retrieval differ between Parquet and PostgreSQL? What additional PostgreSQL design (such as an index) could change the result?
Parquet quickly skips irrelevant chunks because of columnar storage, while PostgreSQL scans indexes and rows but adding a proper index in PostgreSQL could make filtered queries faster.

4. Why is JSON Lines generally more pipeline-friendly than one giant JSON array for append/stream-oriented processing?
JSON Lines is pipeline‑friendly because you can process one record at a time and easily append new lines, while a giant array forces you to load everything at once.

5. What happens if a partition key has extremely high cardinality or poor query locality?  
If a partition key has too many unique values or poor locality, queries won’t benefit from partition pruning and the system may end up scanning almost everything, which can slow the performance.

TASKS 10
10.1 When I searched the link in the search bar, at first it was not responding. saying that it did not send data. But later it finally worked.

10.2 For schedule, the daily cron sched is at 2am because this is outside the business hours. Meaning, it avoids the busy hours on daytime and also ensure that the data is refreshed and validated before the next business day begins.

For catch-up behavior, it's explicitly disable catch-up so Airflow will not try to backfill all past dates since the start date. Thus, avoiding the overwhelming of the system with unnecessary historical runs.

10.3 I triggered the manual run of the DAG and successfully executed the dour tasks. However, I have many attempts because I still have some backjobs that need to achieve.

10.4 I triggered a DAG run with run_mode=partition and year=2026, month=9. The load task executed the load-partition command. The audit table shows that a new entry for partition_key=2026-09 with row_count 949 and correct pipeline_run_id.

10.5 I renamed orders.csv to have a missing source. The DAG failed and retried, with logs showing the error and failure callback as shown in the evidences in Task 10. After restoring the file, I cleared the failed task and re‑ran the DAG, which completed successfully. This was safe to rerun because the source data was never corrupted but sonly temporarily unavailable.

10.6 If I needed to backfill a historical month while the DAG normally runs daily, I would trigger the DAG with parameters for that specific year and month. The data interval would be set to cover that month only, so the pipeline processes just that slice of data. Because each step in the pipeline is idempotent (same input → same output), rerunning won’t corrupt or duplicate results. The load step uses UPSERT logic, so even if records already exist, they’ll be updated instead of double‑loaded. This way, I can safely backfill past data without interfering with the daily runs.


INTEGRATED TECHNICAL ACCEPTANCE TEST:
Goal 1: Environment
- Command: python -m src.cli validate-env 
* Checks that your Python environment and configs are correct.
- Command: docker compose up -d postgres
* Starts the PostgreSQL database container in the background.

Goal 2: Full Pipeline
- Command: python -m src.cli run-all
* Runs the entire pipeline (extract → transform → curate → load).
- Command: python -m src.cli load
* Explicitly loads curated data into PostgreSQL.
- Command: python -m src.cli validate
* Validates that the curated data exists and is non‑empty.

Goal 3: Benchmark and Partition
- Command: python -m src.cli benchmark --repeats 5
* Runs performance benchmarking (like timing pipeline runs).
- Command: python -m src.cli load-partition --year 2026 --month 1
* Loads only the January 2026 partition into PostgreSQL.

Goal 4: Airflow
- Command: docker compose -f docker-compose.yml -f docker-compose.airflow.yml up -d airflow-webserver airflow-scheduler
* Starts Airflow’s webserver and scheduler so you can run the pipeline as a DAG with tasks and dependencies.


This sequence demonstrates that the repository can be run from a clean state by another student or engineer. 
Each goal shows environment setup, full pipeline execution, benchmarking and partition loading, and orchestration 
with Airflow. Evidence includes logs, screenshots, and database entries confirming successful runs.

TECHNICAL QUESTIONS:
1. Why is `record_hash` useful for rerun-safe loading, and which columns should not be included in it?
record_hash is useful because it makes each row unique and lets reruns avoid duplicate loads. Do not include columns like timestamps or run IDs since they change every run.

2. Why should raw data usually be preserved even when staging/curated outputs are sufficient for analytics?
Raw data should be preserved because it is the original source of truth. If staging or curated data is wrong, you can rebuild from raw.

3.  What is the difference between a data-quality rejection and a system exception?
A data-quality rejection means the data itself is invalid (like missing values). A system exception means the pipeline or system broke (like a crash or missing file).

4. Why might Parquet outperform CSV for selected analytical workloads even if both contain the same rows?
Parquet can be faster than CSV because it stores data in a compressed, column-based format, which makes certain queries more efficient.

5.  Why is a DAG that contains all transformation logic directly considered harder to maintain?
A DAG with all transformations directly inside is harder to maintain because the logic is tangled in one place, making changes or debugging more complex.

6.  How do retries interact with idempotency? Give an example where retries without idempotency cause damage.
Retries are safe only if tasks are idempotent. Without idempotency, retries can cause damage — for example, charging a customer twice if the same payment step is retried.

7. What trade-off is introduced by partitioning too aggressively?
Partitioning too aggressively creates too many small files or tables, which can slow queries and add overhead in managing them.

8.  How would you adapt the pipeline if the source became an API or database instead of local files?
If the source became an API or database, the pipeline would need connectors to fetch data directly, handle authentication, and adjust extraction logic to work with queries instead of local files.



AI DISCLOSURE: Artificial Intelligence (AI) was used throughout the completion of this activity to provide guidance, explanations, and coding syntax. All code syntax applied in the project was generated with the help of AI, including commands, structured examples, and technical workflows. AI assisted in clarifying the purpose and correct usage of coding steps, explained technical concepts in detail, and ensured that the commands were properly aligned with best practices. Beyond providing syntax, AI also helped deepen the understanding of certain technical aspects, such as why specific environments should not be committed, the role of virtual environments, and the importance of reproducibility in coding projects. AI further supported the drafting of documentation, including this disclosure, to maintain transparency about its contributions. In summary, all code syntax was done with the help of AI, and AI also aided in strengthening comprehension of technical processes, while the learner executed the commands and carried out the project work independently.