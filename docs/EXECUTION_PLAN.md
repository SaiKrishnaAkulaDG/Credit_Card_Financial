# EXECUTION_PLAN.md

**Credit Card Financial Transactions Lake**
*Version 1.0 · PBVI Phase 3 Output · Classification: Training Demo System*

---

## Open Questions Check

ARCHITECTURE.md Section 11 states: "None. All missing information items from Interrogate are resolved. All Explore-surfaced gaps are traced."

**All open questions resolved. Plan proceeds.**

---

## Resolved Decisions Table

| ID | Question | Resolution |
|---|---|---|
| MI1 | Missing transactions file behaviour | Clean exit, no watermark change, no run log entry (DD3) |
| MI4 | Gold idempotency mechanism | Full truncate + rewrite on every run (DD6) |
| MI5 | Error sanitisation ownership | pipeline.py exclusively owns the sanitise-then-write path (DD7) |
| MI6 | transactions_by_type STRUCT sparsity | Fixed-schema STRUCT always emits all five types with zero-fill (DD10) |
| MI7 | Run log partial-row risk | Write-on-completion only; a gap is the diagnostic signal (DD8) |
| CL2 | Failed sign join outcome | INVALID_TRANSACTION_CODE quarantine — never null _signed_amount (DD5) |
| DD-P1 | Silver filesystem partitioning | dbt-duckdb does not natively produce Hive-style `date=YYYY-MM-DD` partition folders. Silver and quarantine are written as single flat Parquet files filtered by `transaction_date` column. Bronze retains filesystem partitioning (Python + DuckDB, no dbt constraint). The boundary is intentional: Bronze = filesystem-partitioned; Silver/Gold = relational (column-filtered). No invariant changes required — invariants reference data correctness per date, not filesystem structure. |

---

## Session Overview

| Session | Name | Goal | Tasks | Est. Duration |
|---|---|---|---|---|
| 1 | Scaffold + Bronze Loader | Repo structure, Docker, dependencies, bronze_loader.py verified end-to-end | 4 | 2–3 hrs |
| 2 | Silver Layer | All three Silver dbt models + quarantine verified; conservation law holds | 4 | 3–4 hrs |
| 3 | Gold Layer | Both Gold dbt models verified; aggregation correctness confirmed | 2 | 1–2 hrs |
| 4 | Pipeline Coordinator | pipeline.py historical + incremental pipelines verified; watermark and run log correct | 3 | 2–3 hrs |
| 5 | End-to-End + Idempotency | Full pipeline runs, idempotency verified, audit chain traced | 2 | 1–2 hrs |

---

## Session 1 — Scaffold + Bronze Loader

**Session goal:** A running Docker environment with all dependencies installed, the dbt project skeleton in place, and a verified bronze_loader.py that correctly ingests CSV files to Parquet partitions with audit columns. Session ends with a committed, independently runnable Bronze module.

**Integration check:**
```bash
docker compose run --rm pipeline python -c "
import duckdb, pathlib
# Confirm bronze partitions written for transactions and accounts
for entity in ['transactions', 'accounts', 'transaction_codes']:
    files = list(pathlib.Path('/data/bronze').rglob('*.parquet'))
    assert len(files) > 0, f'No bronze files found'
# Confirm audit columns present
conn = duckdb.connect()
row = conn.execute(\"SELECT _source_file, _ingested_at, _pipeline_run_id FROM read_parquet('/data/bronze/transactions/date=*/data.parquet') LIMIT 1\").fetchone()
assert all(v is not None for v in row), 'Null audit column found'
print('Session 1 integration check PASSED')
"
```

---

### Task 1.1 — Repository Scaffold and Docker Environment

**Description:** Initialise the repository directory structure, Docker Compose configuration, Dockerfile, and dbt project skeleton (dbt_project.yml, profiles.yml, empty model stub files). Confirm `docker compose build` succeeds and the container starts.

**CC prompt:**
```
Create the project scaffold for a Medallion data lake pipeline. Do not implement any pipeline logic — only structure and configuration.

Files to create:
- docker-compose.yml: defines one service named `pipeline` using a local Dockerfile. Bind-mounts ./source (read-only) to /source, and ./data (read-write) to /data. Sets PYTHONUNBUFFERED=1.
- Dockerfile: FROM python:3.11-slim. Installs dbt-core==1.7.* and dbt-duckdb==1.7.*. Copies requirements.txt and installs it. Working directory /app.
- requirements.txt: duckdb>=0.10, dbt-core~=1.7, dbt-duckdb~=1.7
- dbt_project/dbt_project.yml: project named credit_card_lake, model-paths: [models], profile: credit_card_lake. Model materialisation defaults: bronze=table, silver=table, gold=table.
- dbt_project/profiles.yml: profile credit_card_lake, target dev, type duckdb, path /data/lake.duckdb, threads 1.
- dbt_project/models/silver/.gitkeep
- dbt_project/models/gold/.gitkeep
- pipeline/__init__.py (empty)
- bronze_loader/__init__.py (empty)
- source/.gitkeep
- data/.gitkeep

Do not create pipeline.py or any SQL model files. Do not implement any logic.
```

**Test cases:**
| Case | Scenario | Expected |
|---|---|---|
| TC-1 | `docker compose build` | Build completes with exit code 0 |
| TC-2 | `docker compose run --rm pipeline python --version` | Outputs Python 3.11.x |
| TC-3 | `docker compose run --rm pipeline dbt --version` | Outputs dbt-core 1.7.x |
| TC-4 | `docker compose run --rm pipeline python -c "import duckdb; print(duckdb.__version__)"` | Prints version without error |

**Verification command:**
```bash
docker compose build && \
docker compose run --rm pipeline python --version && \
docker compose run --rm pipeline dbt --version && \
docker compose run --rm pipeline python -c "import duckdb; print('duckdb ok')"
```

**Invariant flag:** None — scaffolding only.

---

### Task 1.2 — Path Constants Module

**Description:** Create a `constants.py` module that defines all file path patterns as named constants. All components import from this single source of truth — no path string is constructed more than once in the codebase.

**CC prompt:**
```
Create pipeline/constants.py defining all file path constants for the Credit Card Transactions Lake.

Requirements:
- SOURCE_DIR = Path("/source")
- DATA_DIR = Path("/data")
- BRONZE_DIR = DATA_DIR / "bronze"
- SILVER_DIR = DATA_DIR / "silver"
- GOLD_DIR = DATA_DIR / "gold"
- PIPELINE_DIR = DATA_DIR / "pipeline"
- CONTROL_FILE = PIPELINE_DIR / "control.parquet"
- RUN_LOG_FILE = PIPELINE_DIR / "run_log.parquet"

# Silver paths — flat files (not filesystem-partitioned; dbt-duckdb writes single files)
- SILVER_TRANSACTIONS_FILE = SILVER_DIR / "transactions" / "data.parquet"
- SILVER_ACCOUNTS_FILE = SILVER_DIR / "accounts" / "data.parquet"
- SILVER_TRANSACTION_CODES_FILE = SILVER_DIR / "transaction_codes" / "data.parquet"
- SILVER_QUARANTINE_FILE = SILVER_DIR / "quarantine" / "data.parquet"

# Gold paths — flat files
- GOLD_DAILY_SUMMARY_FILE = GOLD_DIR / "daily_summary" / "data.parquet"
- GOLD_WEEKLY_ACCOUNT_SUMMARY_FILE = GOLD_DIR / "weekly_account_summary" / "data.parquet"

Functions (return Path objects):
- bronze_transactions_partition(date: date) -> Path: returns BRONZE_DIR / "transactions" / f"date={date}" / "data.parquet"
- bronze_accounts_partition(date: date) -> Path: returns BRONZE_DIR / "accounts" / f"date={date}" / "data.parquet"
- bronze_transaction_codes_file() -> Path: returns BRONZE_DIR / "transaction_codes" / "data.parquet"
- source_transactions_file(date: date) -> Path: returns SOURCE_DIR / f"transactions_{date}.csv"
- source_accounts_file(date: date) -> Path: returns SOURCE_DIR / f"accounts_{date}.csv"
- source_transaction_codes_file() -> Path: returns SOURCE_DIR / "transaction_codes.csv"

Note: Silver and Gold use flat named constants (not functions) because they are single files, not date-partitioned. Bronze uses functions because each date produces a distinct partition path.

All paths must be constructed from the named constants above — no hardcoded strings anywhere in this file.
Do not create any other files. Do not implement any logic beyond path construction.
```

**Test cases:**
| Case | Scenario | Expected |
|---|---|---|
| TC-1 | Import constants module | No import error |
| TC-2 | `bronze_transactions_partition(date(2024,1,15))` | Returns `Path("/data/bronze/transactions/date=2024-01-15/data.parquet")` |
| TC-3 | `source_accounts_file(date(2024,1,15))` | Returns `Path("/source/accounts_2024-01-15.csv")` |
| TC-4 | `bronze_transaction_codes_file()` | Returns `Path("/data/bronze/transaction_codes/data.parquet")` |

**Verification command:**
```bash
docker compose run --rm pipeline python -c "
from datetime import date
from pipeline.constants import (
    bronze_transactions_partition, source_transactions_file, bronze_transaction_codes_file,
    SILVER_TRANSACTIONS_FILE, SILVER_QUARANTINE_FILE, GOLD_DAILY_SUMMARY_FILE
)
assert str(bronze_transactions_partition(date(2024,1,15))) == '/data/bronze/transactions/date=2024-01-15/data.parquet'
assert str(source_transactions_file(date(2024,1,15))) == '/source/transactions_2024-01-15.csv'
assert str(bronze_transaction_codes_file()) == '/data/bronze/transaction_codes/data.parquet'
assert str(SILVER_TRANSACTIONS_FILE) == '/data/silver/transactions/data.parquet'
assert str(SILVER_QUARANTINE_FILE) == '/data/silver/quarantine/data.parquet'
assert str(GOLD_DAILY_SUMMARY_FILE) == '/data/gold/daily_summary/data.parquet'
print('Task 1.2 PASSED')
"
```

**Invariant flag:** IC13 (Bronze partition path is a fixed contract). Code review: confirm no path strings are constructed outside constants.py.

---

### Task 1.3 — Bronze Loader: Transactions and Accounts

**Description:** Implement `bronze_loader.py` with a function `load_bronze_partition(entity, date, run_id)` that reads a source CSV, adds three audit columns, and writes a Parquet partition. Implements skip-on-exists (INV-B1). Handles the date-partitioned entities: transactions and accounts.

**CC prompt:**
```
Create bronze_loader/loader.py implementing the Bronze ingestion function for date-partitioned entities.

Import from pipeline.constants for all paths.

Implement:

def load_bronze_partition(entity: str, dt: date, run_id: str) -> dict:
    """
    Load a date-partitioned CSV to a Bronze Parquet partition.
    
    entity: 'transactions' or 'accounts'
    dt: the date to load
    run_id: pipeline run identifier
    
    Returns: {"status": "SUCCESS"|"SKIPPED", "records_written": int}
    
    Behaviour:
    - Determine source CSV path and output partition path using constants module functions
    - If the output partition already exists: return {"status": "SKIPPED", "records_written": 0}
      Do NOT overwrite, append to, or modify the existing partition.
    - If source CSV does not exist: raise FileNotFoundError with message "Source file not found: {filename_only}"
      (filename only — no full path in the error message)
    - Read source CSV using duckdb with an explicit schema (do not infer types):
      transactions schema: transaction_id VARCHAR, account_id VARCHAR, transaction_date DATE,
        amount DECIMAL(18,4), transaction_code VARCHAR, merchant_name VARCHAR, channel VARCHAR
      accounts schema: account_id VARCHAR, open_date DATE, credit_limit DECIMAL(18,4),
        current_balance DECIMAL(18,4), billing_cycle_start INTEGER, billing_cycle_end INTEGER,
        account_status VARCHAR
    - Add three audit columns BEFORE writing:
        _source_file: the CSV filename only (not full path)
        _ingested_at: current UTC timestamp (use datetime.utcnow())
        _pipeline_run_id: the run_id parameter
    - Create parent directory if it does not exist
    - Write to the output partition path as Parquet using duckdb COPY TO
    - Return {"status": "SUCCESS", "records_written": <row count>}
    
    Must NOT:
    - Apply any transformations to source field values
    - Infer the schema at runtime
    - Write to any path outside the constants-defined bronze partition paths
    - Raise exceptions for missing accounts files (caller handles this case)
    """
```

**Test cases:**
| Case | Scenario | Expected |
|---|---|---|
| TC-1 | Load transactions for a date with valid CSV | Returns `{"status": "SUCCESS", "records_written": N}`, partition file exists |
| TC-2 | Load same partition again (skip-on-exists) | Returns `{"status": "SKIPPED", "records_written": 0}`, no file modification |
| TC-3 | All three audit columns present and non-null | `_source_file`, `_ingested_at`, `_pipeline_run_id` non-null on every row |
| TC-4 | `_source_file` contains only filename, not full path | Value is `transactions_2024-01-15.csv`, not `/source/transactions_2024-01-15.csv` |
| TC-5 | Source CSV not found | Raises `FileNotFoundError` with message containing filename only |
| TC-6 | Source field values unchanged | `amount` in Bronze matches source CSV — no transformation applied |

**Verification command:**
```bash
docker compose run --rm pipeline python -c "
import duckdb, pathlib
from datetime import date
from bronze_loader.loader import load_bronze_partition

# TC-1: happy path
result = load_bronze_partition('transactions', date(2024,1,15), 'run-test-001')
assert result['status'] == 'SUCCESS'
assert result['records_written'] > 0

# TC-2: skip-on-exists
result2 = load_bronze_partition('transactions', date(2024,1,15), 'run-test-002')
assert result2['status'] == 'SKIPPED'

# TC-3 + TC-4: audit columns
conn = duckdb.connect()
row = conn.execute(\"SELECT _source_file, _ingested_at, _pipeline_run_id FROM read_parquet('/data/bronze/transactions/date=2024-01-15/data.parquet') LIMIT 1\").fetchone()
assert row[0] == 'transactions_2024-01-15.csv', f'Got: {row[0]}'
assert row[1] is not None
assert row[2] == 'run-test-001'

print('Task 1.3 PASSED')
"
```

**Invariant flag:** INV-B1 (Bronze Immutability and Idempotency), INV-B2 (Bronze Audit Columns Completeness), INV-B3 (Raw Data Fidelity). Code review: confirm partition existence check precedes any write; confirm audit columns added before write; confirm no field transformations applied.

---

### Task 1.4 — Bronze Loader: Transaction Codes (Non-Partitioned)

**Description:** Extend bronze_loader.py with `load_bronze_transaction_codes(run_id)` for the single reference file. Same skip-on-exists pattern; non-partitioned path.

**CC prompt:**
```
Add to bronze_loader/loader.py:

def load_bronze_transaction_codes(run_id: str) -> dict:
    """
    Load transaction_codes.csv to bronze/transaction_codes/data.parquet.
    
    Returns: {"status": "SUCCESS"|"SKIPPED", "records_written": int}
    
    Behaviour:
    - Output path: bronze_transaction_codes_file() from constants
    - If output file already exists: return {"status": "SKIPPED", "records_written": 0}
    - If source file does not exist: raise FileNotFoundError with filename only in message
    - Read with explicit schema:
        transaction_code VARCHAR, transaction_type VARCHAR, description VARCHAR,
        debit_credit_indicator VARCHAR, affects_balance BOOLEAN
    - Add audit columns: _source_file (filename only), _ingested_at (UTC), _pipeline_run_id
    - Write to output path as Parquet
    - Return {"status": "SUCCESS", "records_written": <row count>}
    
    Must NOT apply any transformation to source field values.
    """
```

**Test cases:**
| Case | Scenario | Expected |
|---|---|---|
| TC-1 | Load transaction_codes.csv | Returns SUCCESS, file exists at correct path |
| TC-2 | Load again (skip-on-exists) | Returns SKIPPED, existing file unchanged |
| TC-3 | Audit columns non-null | All three audit columns non-null on every row |
| TC-4 | Row count matches source CSV | `SELECT COUNT(*) FROM parquet = wc -l source CSV - 1` |

**Verification command:**
```bash
docker compose run --rm pipeline python -c "
import duckdb
from bronze_loader.loader import load_bronze_transaction_codes

result = load_bronze_transaction_codes('run-test-tc-001')
assert result['status'] == 'SUCCESS'

result2 = load_bronze_transaction_codes('run-test-tc-002')
assert result2['status'] == 'SKIPPED'

conn = duckdb.connect()
count = conn.execute(\"SELECT COUNT(*) FROM read_parquet('/data/bronze/transaction_codes/data.parquet')\").fetchone()[0]
assert count == result['records_written']

nulls = conn.execute(\"SELECT COUNT(*) FROM read_parquet('/data/bronze/transaction_codes/data.parquet') WHERE _pipeline_run_id IS NULL\").fetchone()[0]
assert nulls == 0, f'Null pipeline_run_id: {nulls}'

print('Task 1.4 PASSED')
"
```

**Invariant flag:** INV-B1, INV-B2, INV-B3. Same code review items as Task 1.3.

---

## Session 2 — Silver Layer

**Session goal:** All three Silver dbt models (transactions, accounts, transaction_codes) and the quarantine model are implemented and verified. The conservation law (INV-S1) holds for transactions. Silver deduplication is cross-partition. Sign assignment uses the transaction_codes join exclusively.

**Integration check:**
```bash
docker compose run --rm pipeline python -c "
import duckdb
conn = duckdb.connect()

# Conservation law: bronze = silver + quarantine for each date
# Silver and quarantine are flat files; filter by transaction_date column
dates = conn.execute(\"SELECT DISTINCT strftime(transaction_date, '%Y-%m-%d') FROM read_parquet('/data/bronze/transactions/date=*/data.parquet')\").fetchall()
for (dt,) in dates:
    bronze = conn.execute(f\"SELECT COUNT(*) FROM read_parquet('/data/bronze/transactions/date={dt}/data.parquet')\").fetchone()[0]
    silver = conn.execute(f\"SELECT COUNT(*) FROM read_parquet('/data/silver/transactions/data.parquet') WHERE transaction_date = '{dt}'\").fetchone()[0]
    quar   = conn.execute(f\"SELECT COUNT(*) FROM read_parquet('/data/silver/quarantine/data.parquet') WHERE transaction_date = '{dt}'\").fetchone()[0]
    assert bronze == silver + quar, f'Conservation law broken for {dt}: {bronze} != {silver} + {quar}'

# Global uniqueness across all dates
dup = conn.execute(\"SELECT transaction_id, COUNT(*) c FROM read_parquet('/data/silver/transactions/data.parquet') GROUP BY 1 HAVING c > 1\").fetchone()
assert dup is None, f'Duplicate transaction_id found: {dup}'

# No null _signed_amount
nulls = conn.execute(\"SELECT COUNT(*) FROM read_parquet('/data/silver/transactions/data.parquet') WHERE _signed_amount IS NULL\").fetchone()[0]
assert nulls == 0

print('Session 2 integration check PASSED')
"
```

---

### Task 2.1 — Silver Transaction Codes Model

**Description:** dbt model `silver_transaction_codes.sql` that promotes Bronze transaction_codes to Silver. Loaded once; not re-run on incremental. Carries forward Bronze audit columns and adds Silver audit columns.

**CC prompt:**
```
Create dbt_project/models/silver/silver_transaction_codes.sql.

This model loads transaction codes from Bronze to Silver. It is run once during historical pipeline initialisation.

Requirements:
- Source: read_parquet('/data/bronze/transaction_codes/data.parquet')
- Output path (materialised as table in dbt, but final Parquet at): /data/silver/transaction_codes/data.parquet
- Materialisation: table (dbt config at top of file)
- Select all source columns: transaction_code, transaction_type, description, debit_credit_indicator, affects_balance
- Carry forward from Bronze: _source_file AS _source_file, _ingested_at AS _bronze_ingested_at
- Add Silver audit columns:
    _pipeline_run_id: passed as a dbt variable var('run_id'), default 'UNSET'
    (Note: _promoted_at is a Silver transactions-specific column — do not add it to transaction_codes)

No filtering, no transformation of source values. Do not deduplicate — the source is a reference file with unique transaction_code values by definition.

Do not create any other files.
```

**Test cases:**
| Case | Scenario | Expected |
|---|---|---|
| TC-1 | dbt run for silver_transaction_codes | Exits 0, model materialises |
| TC-2 | Row count matches Bronze | `COUNT(silver) = COUNT(bronze)` |
| TC-3 | All five transaction types present | PURCHASE, PAYMENT, FEE, INTEREST, REFUND all in transaction_type column |
| TC-4 | `_bronze_ingested_at` non-null | Carried from Bronze `_ingested_at` |
| TC-5 | `_pipeline_run_id` non-null | Populated from var('run_id') |

**Verification command:**
```bash
docker compose run --rm pipeline bash -c "
cd dbt_project && dbt run --select silver_transaction_codes --vars '{run_id: test-run-001}' && \
python -c \"
import duckdb
conn = duckdb.connect()
silver = conn.execute(\\\"SELECT COUNT(*) FROM read_parquet('/data/silver/transaction_codes/data.parquet')\\\").fetchone()[0]
bronze = conn.execute(\\\"SELECT COUNT(*) FROM read_parquet('/data/bronze/transaction_codes/data.parquet')\\\").fetchone()[0]
assert silver == bronze, f'{silver} != {bronze}'
types = conn.execute(\\\"SELECT DISTINCT transaction_type FROM read_parquet('/data/silver/transaction_codes/data.parquet')\\\").fetchall()
assert len(types) == 5
print('Task 2.1 PASSED')
\"
"
```

**Invariant flag:** INV-S9 (Transaction Codes Sequencing), INV-S4 (Sign Assignment — reference table must exist). Code review: confirm no filtering or transformation of source values.

---

### Task 2.2 — Silver Accounts Model

**Description:** dbt model `silver_accounts.sql` that upserts account delta records from Bronze. One row per account_id (latest wins). Applies account rejection rules before upserting.

**CC prompt:**
```
Create dbt_project/models/silver/silver_accounts.sql.

This model promotes account delta records from Bronze Accounts to Silver, applying quality rules and upserting.

Requirements:
- Source: read_parquet('/data/bronze/accounts/date=*/data.parquet') — all date partitions
- Output: single non-partitioned file at /data/silver/accounts/data.parquet
- Materialisation: table (full replace — this is fine for SCD Type 1 upsert at dbt level)
- dbt variable: var('processing_date') — process only records from the partition matching this date

Quality rules (reject to quarantine — these go to silver_quarantine, NOT here):
- NULL_REQUIRED_FIELD: any of account_id, open_date, credit_limit, current_balance, billing_cycle_start, billing_cycle_end, account_status is null
- INVALID_ACCOUNT_STATUS: account_status not in ('ACTIVE', 'SUSPENDED', 'CLOSED')

This model selects ONLY records that pass all quality rules for the given processing_date.
Records that fail are handled by silver_quarantine.sql (separate model).

Upsert logic:
- Start from existing silver/accounts records (if file exists — use a conditional read or LEFT JOIN pattern)
- For the processing_date partition, take passing records
- Final output: one row per account_id — if account_id exists in both old silver and new delta, the new delta record wins
- Use ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY _bronze_ingested_at DESC) to resolve conflicts

Silver audit columns to add:
- _source_file: carried from Bronze _source_file
- _bronze_ingested_at: carried from Bronze _ingested_at
- _pipeline_run_id: var('run_id', 'UNSET')
- _record_valid_from: current_timestamp at promotion time

Do not apply business transformations to any account field value.
Do not create any other files.
```

**Test cases:**
| Case | Scenario | Expected |
|---|---|---|
| TC-1 | Load first delta for a new account | Account appears in Silver with _is_resolvable context available |
| TC-2 | Load updated delta for existing account | Silver has one row for the account; latest values present |
| TC-3 | Re-run for same processing_date | Silver row count and values identical (idempotency) |
| TC-4 | NULL account_id in source | Record excluded from Silver (goes to quarantine) |
| TC-5 | Invalid account_status | Record excluded from Silver (goes to quarantine) |
| TC-6 | `COUNT(*) = COUNT(DISTINCT account_id)` | Exactly one row per account_id in Silver |

**Verification command:**
```bash
docker compose run --rm pipeline bash -c "
cd dbt_project && dbt run --select silver_accounts --vars '{run_id: test-002, processing_date: 2024-01-01}' && \
python -c \"
import duckdb
conn = duckdb.connect()
total = conn.execute(\\\"SELECT COUNT(*) FROM read_parquet('/data/silver/accounts/data.parquet')\\\").fetchone()[0]
distinct = conn.execute(\\\"SELECT COUNT(DISTINCT account_id) FROM read_parquet('/data/silver/accounts/data.parquet')\\\").fetchone()[0]
assert total == distinct, f'Duplicate account_id: {total} rows vs {distinct} distinct'
nulls = conn.execute(\\\"SELECT COUNT(*) FROM read_parquet('/data/silver/accounts/data.parquet') WHERE _pipeline_run_id IS NULL\\\").fetchone()[0]
assert nulls == 0
print('Task 2.2 PASSED')
\"
"
```

**Invariant flag:** INV-S8 (Accounts Upsert Correctness), INV-C6 (Audit Chain Completeness). Code review: confirm single-row-per-account guarantee; confirm upsert replaces existing record on conflict.

---

### Task 2.3 — Silver Transactions Model

**Description:** dbt model `silver_transactions.sql` — the most complex Silver model. Applies all transaction quality rules, performs cross-partition deduplication, joins to Silver transaction_codes for sign assignment, sets `_is_resolvable` by joining to Silver accounts.

**CC prompt:**
```
Create dbt_project/models/silver/silver_transactions.sql.

This model promotes transaction records from a single Bronze date partition to Silver, applying all quality rules.

IMPORTANT — materialisation: dbt-duckdb writes a single flat Parquet file, not filesystem-partitioned folders.
Silver transactions is a single file at /data/silver/transactions/data.parquet containing all dates.
Deduplication is enforced via the transaction_date column filter and the transaction_id uniqueness check.
Use: {{ config(materialized='table') }}

dbt variables consumed:
- var('processing_date'): the date to process (string, YYYY-MM-DD)
- var('run_id', 'UNSET'): pipeline run identifier

Step 1 — Source: read from Bronze partition for processing_date only:
  read_parquet('/data/bronze/transactions/date=' || var('processing_date') || '/data.parquet')

Step 2 — Quality rule checks. A record is rejected (goes to quarantine) if ANY of:
  - NULL_REQUIRED_FIELD: transaction_id, account_id, transaction_date, amount, transaction_code, or channel is null or empty string
  - INVALID_AMOUNT: amount <= 0 or amount is not a valid number
  - DUPLICATE_TRANSACTION_ID: transaction_id already exists in the current Silver transactions file
    read_parquet('/data/silver/transactions/data.parquet') — full file scan.
    If the Silver transactions file does not yet exist, treat as zero existing records (first run case).
  - INVALID_TRANSACTION_CODE: transaction_code not found in read_parquet('/data/silver/transaction_codes/data.parquet')
  - INVALID_CHANNEL: channel not in ('ONLINE', 'IN_STORE')

Step 3 — For records that PASS all checks:
  - Join to silver/transaction_codes on transaction_code to get debit_credit_indicator
  - Compute _signed_amount: IF debit_credit_indicator = 'DR' THEN amount ELSE amount * -1
  - Safeguard: if _signed_amount would be null, reject with INVALID_TRANSACTION_CODE

Step 4 — Resolvability check:
  - LEFT JOIN to read_parquet('/data/silver/accounts/data.parquet') on account_id
  - _is_resolvable = (join matched) — COALESCE to false, never null

Step 5 — Output for THIS model: UNION of existing Silver transactions + newly passing records
  The dbt table materialisation replaces the entire file on each run.
  To preserve previously promoted records while adding new ones:
    SELECT * FROM existing_silver  -- all previously promoted records (if file exists)
    UNION ALL
    SELECT <new passing records from processing_date>
  This ensures the full Silver file always contains all dates promoted so far.

Output columns per record:
  transaction_id, account_id, transaction_date, amount, transaction_code, merchant_name, channel,
  _source_file (from Bronze), _bronze_ingested_at (Bronze _ingested_at), _pipeline_run_id (var('run_id')),
  _promoted_at (current_timestamp), _is_resolvable (BOOLEAN, never null), _signed_amount (DECIMAL, never null)

This model outputs ONLY passing records. Rejected records are output by silver_quarantine.sql.
Do not quarantine inside this model — it selects valid records only.

Do not create any other files.
```

**Test cases:**
| Case | Scenario | Expected |
|---|---|---|
| TC-1 | Valid transaction | Appears in Silver with correct _signed_amount |
| TC-2 | DR transaction code | `_signed_amount = +amount` |
| TC-3 | CR transaction code | `_signed_amount = -amount` |
| TC-4 | Null transaction_id | Rejected — not in Silver |
| TC-5 | Amount = 0 | Rejected — not in Silver |
| TC-6 | Unknown transaction_code | Rejected — not in Silver |
| TC-7 | Invalid channel value | Rejected — not in Silver |
| TC-8 | transaction_id already in Silver | Rejected — DUPLICATE_TRANSACTION_ID |
| TC-9 | account_id not in Silver Accounts | In Silver, `_is_resolvable = false` |
| TC-10 | `_is_resolvable` is never null | `COUNT(*) WHERE _is_resolvable IS NULL = 0` |
| TC-11 | `_signed_amount` is never null | `COUNT(*) WHERE _signed_amount IS NULL = 0` |

**Verification command:**
```bash
docker compose run --rm pipeline bash -c "
cd dbt_project && dbt run --select silver_transactions --vars '{run_id: test-003, processing_date: 2024-01-01}' && \
python -c \"
import duckdb
conn = duckdb.connect()
nulls_sign = conn.execute(\\\"SELECT COUNT(*) FROM read_parquet('/data/silver/transactions/data.parquet') WHERE _signed_amount IS NULL\\\").fetchone()[0]
assert nulls_sign == 0, f'Null _signed_amount: {nulls_sign}'
nulls_res = conn.execute(\\\"SELECT COUNT(*) FROM read_parquet('/data/silver/transactions/data.parquet') WHERE _is_resolvable IS NULL\\\").fetchone()[0]
assert nulls_res == 0, f'Null _is_resolvable: {nulls_res}'
dup = conn.execute(\\\"SELECT transaction_id, COUNT(*) c FROM read_parquet('/data/silver/transactions/data.parquet') GROUP BY 1 HAVING c > 1\\\").fetchone()
assert dup is None, f'Duplicate: {dup}'
print('Task 2.3 PASSED')
\"
"
```

**Invariant flag:** INV-S1, INV-S2, INV-S3, INV-S4, INV-S5, INV-S6. Code review: (1) cross-partition deduplication check precedes promotion; (2) `_signed_amount` computed by join, not direct logic; (3) `_is_resolvable` COALESCE ensures non-null; (4) no Silver record is created for any rejected condition.

---

### Task 2.4 — Silver Quarantine Model

**Description:** dbt model `silver_quarantine.sql` that collects rejected records from the Bronze transactions partition for a given date, assigns rejection reason codes, and writes to the quarantine partition.

**CC prompt:**
```
Create dbt_project/models/silver/silver_quarantine.sql.

This model identifies rejected records from a Bronze transactions partition and writes them to quarantine with rejection reason codes.

IMPORTANT — materialisation: quarantine is a single flat Parquet file at /data/silver/quarantine/data.parquet.
It accumulates rejected records from all dates. Use: {{ config(materialized='table') }}
Output is the UNION of existing quarantine records + newly rejected records from processing_date.

dbt variables: var('processing_date'), var('run_id', 'UNSET')

Source: read_parquet('/data/bronze/transactions/date=' || var('processing_date') || '/data.parquet')

For each Bronze record, evaluate rejection rules in priority order (a record gets ONE reason code — the first that applies):
1. NULL_REQUIRED_FIELD: transaction_id IS NULL OR transaction_id = '' OR account_id IS NULL OR account_id = '' OR transaction_date IS NULL OR amount IS NULL OR transaction_code IS NULL OR transaction_code = '' OR channel IS NULL OR channel = ''
2. INVALID_AMOUNT: amount IS NULL OR amount <= 0
3. DUPLICATE_TRANSACTION_ID: transaction_id exists in read_parquet('/data/silver/transactions/data.parquet')
   (treat as empty if file does not yet exist)
4. INVALID_TRANSACTION_CODE: transaction_code NOT IN (SELECT transaction_code FROM read_parquet('/data/silver/transaction_codes/data.parquet'))
5. INVALID_CHANNEL: channel NOT IN ('ONLINE', 'IN_STORE')

A record is quarantined if it matches ANY of the above.
A record that passes ALL checks is NOT quarantined (it goes to silver_transactions).

Output columns:
- All original source columns: transaction_id, account_id, transaction_date, amount, transaction_code, merchant_name, channel
- All Bronze audit columns: _source_file, _ingested_at (as _bronze_ingested_at), _pipeline_run_id (as _bronze_pipeline_run_id)
- Quarantine audit columns:
  _source_file: from Bronze _source_file
  _pipeline_run_id: var('run_id')
  _rejected_at: current_timestamp
  _rejection_reason: the rejection reason code string (from list above)

Final SELECT: UNION ALL of existing quarantine records (if file exists) + newly rejected records from processing_date.

Conservation law must hold per date: this model + silver_transactions.sql together account for all Bronze records for processing_date, checkable by filtering on transaction_date.

Do not create any other files.
```

**Test cases:**
| Case | Scenario | Expected |
|---|---|---|
| TC-1 | Conservation law: bronze = silver + quarantine | Exact row count equality per date |
| TC-2 | Quarantine record has `_rejection_reason` | Never null; value is from pre-defined list |
| TC-3 | Quarantine record has all source fields | No source column is null/missing in quarantine output |
| TC-4 | `_rejected_at` non-null | Present on every quarantine record |
| TC-5 | Same transaction_id not in both Silver and quarantine | No transaction_id appears in both |

**Verification command:**
```bash
docker compose run --rm pipeline bash -c "
cd dbt_project && dbt run --select silver_quarantine --vars '{run_id: test-004, processing_date: 2024-01-01}' && \
python -c \"
import duckdb
conn = duckdb.connect()
bronze = conn.execute(\\\"SELECT COUNT(*) FROM read_parquet('/data/bronze/transactions/date=2024-01-01/data.parquet')\\\").fetchone()[0]
silver = conn.execute(\\\"SELECT COUNT(*) FROM read_parquet('/data/silver/transactions/data.parquet') WHERE transaction_date = '2024-01-01'\\\").fetchone()[0]
quar = conn.execute(\\\"SELECT COUNT(*) FROM read_parquet('/data/silver/quarantine/data.parquet') WHERE transaction_date = '2024-01-01'\\\").fetchone()[0]
assert bronze == silver + quar, f'Conservation: {bronze} != {silver}+{quar}'
null_reason = conn.execute(\\\"SELECT COUNT(*) FROM read_parquet('/data/silver/quarantine/data.parquet') WHERE _rejection_reason IS NULL\\\").fetchone()[0]
assert null_reason == 0
print('Task 2.4 PASSED')
\"
"
```

**Invariant flag:** INV-S1 (Conservation Law), INV-S7 (Quarantine Integrity). Code review: confirm all source columns included in quarantine SELECT; confirm `_rejection_reason` never null; confirm conservation law is structurally enforced by having no other drop path.

---

## Session 3 — Gold Layer

**Session goal:** Both Gold dbt models are implemented and verified. Aggregations use `_signed_amount` exclusively. `transactions_by_type` STRUCT always emits all five types. Gold is built from Silver only.

**Integration check:**
```bash
docker compose run --rm pipeline python -c "
import duckdb
conn = duckdb.connect()

# Gold daily summary: one row per date in Silver (resolvable only)
silver_dates = conn.execute(\"SELECT DISTINCT transaction_date FROM read_parquet('/data/silver/transactions/data.parquet') WHERE _is_resolvable = true\").fetchall()
gold_dates = conn.execute(\"SELECT DISTINCT transaction_date FROM read_parquet('/data/gold/daily_summary/data.parquet')\").fetchall()
assert set(silver_dates) == set(gold_dates), 'Gold daily dates mismatch'

# Gold total_signed_amount matches Silver sum per date
for (dt,) in silver_dates:
    silver_sum = conn.execute(f\"SELECT SUM(_signed_amount) FROM read_parquet('/data/silver/transactions/data.parquet') WHERE transaction_date = '{dt}' AND _is_resolvable = true\").fetchone()[0]
    gold_sum = conn.execute(f\"SELECT total_signed_amount FROM read_parquet('/data/gold/daily_summary/data.parquet') WHERE transaction_date = '{dt}'\").fetchone()[0]
    assert abs(float(silver_sum) - float(gold_sum)) < 0.0001, f'Sum mismatch on {dt}'

print('Session 3 integration check PASSED')
"
```

---

### Task 3.1 — Gold Daily Summary Model

**Description:** dbt model `gold_daily_summary.sql`. Full rebuild (table materialisation). One row per transaction_date from Silver where `_is_resolvable = true`. Fixed `transactions_by_type` STRUCT with zero-fill for absent types.

**CC prompt:**
```
Create dbt_project/models/gold/gold_daily_summary.sql.

Source: read_parquet('/data/silver/transactions/data.parquet')
Filter: WHERE _is_resolvable = true — apply BEFORE any aggregation.
Materialisation: table (full truncate + insert on every run)
Output path: /data/gold/daily_summary/data.parquet

Output one row per transaction_date with these columns:
- transaction_date DATE
- total_transactions INTEGER: COUNT(*) of qualifying records
- total_signed_amount DECIMAL: SUM(_signed_amount)
- transactions_by_type STRUCT: always contains exactly five keys: PURCHASE, PAYMENT, FEE, INTEREST, REFUND. For each key, include {count: INTEGER, total: DECIMAL}. Zero-fill absent types (count=0, total=0.00). Do NOT use a GROUP BY pivot — construct the STRUCT explicitly using conditional aggregation:
    STRUCT_PACK(
      PURCHASE := STRUCT_PACK(count := COUNT(*) FILTER (WHERE transaction_type='PURCHASE'), total := COALESCE(SUM(_signed_amount) FILTER (WHERE transaction_type='PURCHASE'), 0.00)),
      PAYMENT := STRUCT_PACK(count := COUNT(*) FILTER (WHERE transaction_type='PAYMENT'), total := COALESCE(SUM(_signed_amount) FILTER (WHERE transaction_type='PAYMENT'), 0.00)),
      FEE := STRUCT_PACK(count := COUNT(*) FILTER (WHERE transaction_type='FEE'), total := COALESCE(SUM(_signed_amount) FILTER (WHERE transaction_type='FEE'), 0.00)),
      INTEREST := STRUCT_PACK(count := COUNT(*) FILTER (WHERE transaction_type='INTEREST'), total := COALESCE(SUM(_signed_amount) FILTER (WHERE transaction_type='INTEREST'), 0.00)),
      REFUND := STRUCT_PACK(count := COUNT(*) FILTER (WHERE transaction_type='REFUND'), total := COALESCE(SUM(_signed_amount) FILTER (WHERE transaction_type='REFUND'), 0.00))
    )
  Join to silver_transaction_codes on transaction_code to get transaction_type for the STRUCT grouping.
- online_transactions INTEGER: COUNT(*) WHERE channel = 'ONLINE'
- instore_transactions INTEGER: COUNT(*) WHERE channel = 'IN_STORE'
- _computed_at TIMESTAMP: current_timestamp
- _pipeline_run_id VARCHAR: var('run_id', 'UNSET')
- _source_period_start DATE: MIN(transaction_date) across all qualifying Silver records
- _source_period_end DATE: MAX(transaction_date) across all qualifying Silver records

Must NOT:
- Reference any Bronze source directly
- Use raw `amount` column — only `_signed_amount`
- Apply sign logic — read `_signed_amount` as-is

Do not create any other files.
```

**Test cases:**
| Case | Scenario | Expected |
|---|---|---|
| TC-1 | One row per Silver resolvable date | `COUNT(gold) = COUNT(DISTINCT transaction_date WHERE _is_resolvable=true)` |
| TC-2 | `total_signed_amount` matches Silver sum | Within 0.0001 tolerance per date |
| TC-3 | `transactions_by_type` has all five keys | `PURCHASE`, `PAYMENT`, `FEE`, `INTEREST`, `REFUND` always present |
| TC-4 | Day with no REFUND transactions | `transactions_by_type.REFUND.count = 0`, `total = 0.00` |
| TC-5 | `_is_resolvable = false` records excluded | Gold total < Silver total where unresolvable records exist |
| TC-6 | `_pipeline_run_id` non-null | No null run IDs in Gold |
| TC-7 | No Bronze column referenced | `grep -i 'bronze' gold_daily_summary.sql` returns nothing |

**Verification command:**
```bash
docker compose run --rm pipeline bash -c "
cd dbt_project && dbt run --select gold_daily_summary --vars '{run_id: test-g01}' && \
python -c \"
import duckdb
conn = duckdb.connect()
silver_dates = conn.execute(\\\"SELECT COUNT(DISTINCT transaction_date) FROM read_parquet('/data/silver/transactions/data.parquet') WHERE _is_resolvable = true\\\").fetchone()[0]
gold_rows = conn.execute(\\\"SELECT COUNT(*) FROM read_parquet('/data/gold/daily_summary/data.parquet')\\\").fetchone()[0]
assert silver_dates == gold_rows, f'{silver_dates} != {gold_rows}'
null_run = conn.execute(\\\"SELECT COUNT(*) FROM read_parquet('/data/gold/daily_summary/data.parquet') WHERE _pipeline_run_id IS NULL\\\").fetchone()[0]
assert null_run == 0
print('Task 3.1 PASSED')
\"
"
```

**Invariant flag:** INV-G1 (Resolvability Filter), INV-G2 (No Bronze Dependency), INV-G3 (Full Rebuild), INV-G4 (Aggregation on `_signed_amount`), INV-G5 (STRUCT Completeness). Code review: (1) `WHERE _is_resolvable = true` in base query before GROUP BY; (2) no `amount` without underscore; (3) no `/data/bronze/` in FROM clauses; (4) all five STRUCT keys constructed explicitly.

---

### Task 3.2 — Gold Weekly Account Summary Model

**Description:** dbt model `gold_weekly_account_summary.sql`. One row per (week_start_date, account_id) from Silver resolvable transactions. ISO week Monday–Sunday. Joins to Silver accounts for `closing_balance`.

**CC prompt:**
```
Create dbt_project/models/gold/gold_weekly_account_summary.sql.

Source: read_parquet('/data/silver/transactions/data.parquet')
Filter: WHERE _is_resolvable = true — apply BEFORE aggregation.
Materialisation: table (full rebuild on every run)
Output path: /data/gold/weekly_account_summary/data.parquet

Week definition: ISO week, Monday start.
- week_start_date = DATE_TRUNC('week', transaction_date)  -- Monday of the ISO week
- week_end_date = DATE_TRUNC('week', transaction_date) + INTERVAL 6 DAYS  -- Sunday

Output one row per (week_start_date, account_id). Only include (week, account) pairs with at least one resolvable transaction.

Output columns:
- week_start_date DATE
- week_end_date DATE
- account_id VARCHAR
- total_purchases INTEGER: COUNT(*) WHERE transaction_type = 'PURCHASE'
- avg_purchase_amount DECIMAL: AVG(_signed_amount) WHERE transaction_type = 'PURCHASE', NULL if no purchases
- total_payments DECIMAL: SUM(_signed_amount) WHERE transaction_type = 'PAYMENT', 0.00 if none
- total_fees DECIMAL: SUM(_signed_amount) WHERE transaction_type = 'FEE', 0.00 if none
- total_interest DECIMAL: SUM(_signed_amount) WHERE transaction_type = 'INTEREST', 0.00 if none
- closing_balance DECIMAL: current_balance from read_parquet('/data/silver/accounts/data.parquet') for this account_id. LEFT JOIN — if account not found, closing_balance = NULL.
- _computed_at TIMESTAMP: current_timestamp
- _pipeline_run_id VARCHAR: var('run_id', 'UNSET')

Join to silver_transaction_codes on transaction_code to get transaction_type.

Must NOT:
- Reference any Bronze source directly
- Use raw `amount` — only `_signed_amount`
- Apply sign logic

Do not create any other files.
```

**Test cases:**
| Case | Scenario | Expected |
|---|---|---|
| TC-1 | One row per (week, account) with ≥1 resolvable transaction | No duplicates on `(week_start_date, account_id)` |
| TC-2 | `total_purchases` matches Silver PURCHASE count | Per-account, per-week count matches filtered Silver |
| TC-3 | Account with no purchases that week | `total_purchases = 0`, `avg_purchase_amount = NULL` |
| TC-4 | `closing_balance` from Silver accounts | Matches `current_balance` in silver/accounts for same account_id |
| TC-5 | `week_end_date = week_start_date + 6 days` | Sunday date calculation correct |
| TC-6 | No `_is_resolvable = false` records in aggregation | Run query against Silver with flag=false; none appear in Gold |

**Verification command:**
```bash
docker compose run --rm pipeline bash -c "
cd dbt_project && dbt run --select gold_weekly_account_summary --vars '{run_id: test-g02}' && \
python -c \"
import duckdb
conn = duckdb.connect()
dup = conn.execute(\\\"SELECT week_start_date, account_id, COUNT(*) c FROM read_parquet('/data/gold/weekly_account_summary/data.parquet') GROUP BY 1,2 HAVING c > 1\\\").fetchone()
assert dup is None, f'Duplicate key: {dup}'
null_run = conn.execute(\\\"SELECT COUNT(*) FROM read_parquet('/data/gold/weekly_account_summary/data.parquet') WHERE _pipeline_run_id IS NULL\\\").fetchone()[0]
assert null_run == 0
print('Task 3.2 PASSED')
\"
"
```

**Invariant flag:** INV-G1, INV-G2, INV-G3, INV-G4, INV-T1. Code review: same as 3.1 plus confirm `closing_balance` joins Silver accounts not Bronze.

---

## Session 4 — Pipeline Coordinator

**Session goal:** `pipeline.py` is implemented with both historical and incremental pipelines. Watermark, run log, error sanitisation, and file existence pre-check all verified. The coordinator is the only component that writes to control.parquet and run_log.parquet.

**Integration check:**
```bash
docker compose run --rm pipeline python pipeline.py historical --start-date 2024-01-01 --end-date 2024-01-07 && \
docker compose run --rm pipeline python -c "
import duckdb
conn = duckdb.connect()
watermark = conn.execute(\"SELECT last_processed_date FROM read_parquet('/data/pipeline/control.parquet')\").fetchone()[0]
assert str(watermark) == '2024-01-07', f'Watermark wrong: {watermark}'
log_count = conn.execute(\"SELECT COUNT(*) FROM read_parquet('/data/pipeline/run_log.parquet')\").fetchone()[0]
assert log_count > 0
null_run_ids = conn.execute(\"SELECT COUNT(*) FROM read_parquet('/data/pipeline/run_log.parquet') WHERE run_id IS NULL\").fetchone()[0]
assert null_run_ids == 0
print('Session 4 integration check PASSED')
"
```

---

### Task 4.1 — Control Table and Run Log Utilities

**Description:** Implement `pipeline/control.py` with functions for reading/writing the control table (single-row overwrite) and appending to the run log. These are the only functions in the codebase that touch these two files.

**CC prompt:**
```
Create pipeline/control.py implementing control table and run log operations.

Import from pipeline.constants for all paths.

Implement these functions:

def read_watermark() -> date | None:
    """
    Read last_processed_date from CONTROL_FILE.
    Returns None if the control file does not exist.
    Raises ValueError if the file has more than one row.
    """

def write_watermark(last_processed_date: date, run_id: str) -> None:
    """
    Write a single-row Parquet file to CONTROL_FILE.
    Columns: last_processed_date (DATE), updated_at (TIMESTAMP, UTC now), updated_by_run_id (VARCHAR)
    OVERWRITES the existing file — never appends.
    Creates parent directory if needed.
    Raises ValueError if last_processed_date is None.
    """

def append_run_log_entry(
    run_id: str,
    pipeline_type: str,          # HISTORICAL or INCREMENTAL
    model_name: str,
    layer: str,                  # BRONZE, SILVER, or GOLD
    started_at: datetime,
    completed_at: datetime,
    status: str,                 # SUCCESS, FAILED, or SKIPPED
    records_processed: int | None,
    records_written: int | None,
    records_rejected: int | None,
    error_message: str | None
) -> None:
    """
    Append ONE row to RUN_LOG_FILE. Never overwrites existing rows.
    Creates the file if it does not exist.
    Creates parent directory if needed.
    error_message must already be sanitised before this function is called.
    """

def sanitise_error(raw: str) -> str:
    """
    Remove file paths, IP addresses, and credential-like strings from an error message.
    Returns a cleaned string safe to store in the run log.
    Rules:
    - Replace any substring matching r'/[^\s]+' (path-like) with '[path redacted]'
    - Replace any substring matching r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}' with '[ip redacted]'
    - Truncate to 500 characters
    """

Do not implement pipeline.py here. Do not create any other files.
```

**Test cases:**
| Case | Scenario | Expected |
|---|---|---|
| TC-1 | `read_watermark()` on missing file | Returns None |
| TC-2 | `write_watermark(date(2024,1,15), 'run-1')` | File has exactly one row; `last_processed_date = 2024-01-15` |
| TC-3 | `write_watermark` called twice | File still has exactly one row (overwrite) |
| TC-4 | `append_run_log_entry` called three times | File has three rows |
| TC-5 | `sanitise_error('/data/some/path crashed')` | Returns string with `[path redacted]` |
| TC-6 | Control file with two rows raises ValueError | `ValueError` raised on `read_watermark()` |

**Verification command:**
```bash
docker compose run --rm pipeline python -c "
from datetime import date, datetime
from pipeline.control import read_watermark, write_watermark, append_run_log_entry, sanitise_error

assert read_watermark() is None

write_watermark(date(2024,1,15), 'run-1')
assert read_watermark() == date(2024,1,15)

write_watermark(date(2024,1,16), 'run-2')
assert read_watermark() == date(2024,1,16)

import duckdb
count = duckdb.connect().execute(\"SELECT COUNT(*) FROM read_parquet('/data/pipeline/control.parquet')\").fetchone()[0]
assert count == 1, f'Expected 1 row, got {count}'

now = datetime.utcnow()
for i in range(3):
    append_run_log_entry('run-x', 'HISTORICAL', f'model_{i}', 'SILVER', now, now, 'SUCCESS', 10, 10, 0, None)

count2 = duckdb.connect().execute(\"SELECT COUNT(*) FROM read_parquet('/data/pipeline/run_log.parquet')\").fetchone()[0]
assert count2 == 3

cleaned = sanitise_error('/data/bronze/file crashed due to /etc/config error')
assert '/data' not in cleaned and '/etc' not in cleaned

print('Task 4.1 PASSED')
"
```

**Invariant flag:** INV-C3 (Run Log Append-Only), INV-C5 (Run Log Entry Uniqueness), INV-C7 (Control Table Single-Row), INV-E1 (Error Sanitisation). Code review: confirm `write_watermark` overwrites (not appends); confirm `append_run_log_entry` only appends; confirm sanitisation removes paths.

---

### Task 4.2 — Historical Pipeline Implementation

**Description:** Implement `pipeline.py historical` subcommand. Sequences transaction_codes Bronze → Silver, then for each date: accounts Bronze → transactions Bronze → silver_accounts → silver_transactions → silver_quarantine. Then Gold models. Then watermark advancement. Run log written on completion.

**CC prompt:**
```
Create pipeline.py implementing the historical pipeline subcommand.

CLI interface:
  python pipeline.py historical --start-date YYYY-MM-DD --end-date YYYY-MM-DD

Imports:
- from pipeline.control import read_watermark, write_watermark, append_run_log_entry, sanitise_error
- from bronze_loader.loader import load_bronze_partition, load_bronze_transaction_codes
- from pipeline.constants import source_transactions_file, source_accounts_file
- Standard: datetime, uuid, subprocess, sys, argparse, pathlib

Execution sequence (STRICT ORDER — do not deviate):
1. Generate run_id = str(uuid.uuid4())
2. Load Bronze transaction_codes (call load_bronze_transaction_codes(run_id))
3. Run dbt model: silver_transaction_codes (pass --vars run_id)
4. For each date from start_date to end_date inclusive (date order):
   a. If source_accounts_file(date) exists: load_bronze_partition('accounts', date, run_id); else skip accounts (not an error)
   b. load_bronze_partition('transactions', date, run_id)
   c. dbt run silver_accounts --vars processing_date + run_id
   d. dbt run silver_transactions --vars processing_date + run_id
   e. dbt run silver_quarantine --vars processing_date + run_id
5. dbt run gold_daily_summary --vars run_id
6. dbt run gold_weekly_account_summary --vars run_id
7. write_watermark(end_date, run_id)
8. Write all run log entries (append_run_log_entry for each model) — write-on-completion

dbt invocation: use subprocess.run(['dbt', 'run', '--select', model_name, '--vars', f'{{run_id: {run_id}, processing_date: {date}}}'], cwd='dbt_project', capture_output=True)

Error handling:
- Wrap the ENTIRE sequence in a single try/except
- If any step raises or returns non-zero exit code: call sanitise_error on the exception/stderr, write a FAILED run log entry, and re-raise (do NOT advance watermark)
- No inner try/except blocks that swallow exceptions
- Watermark advancement (step 7) ONLY if all prior steps succeeded

Missing accounts file behaviour: log a SKIPPED run log entry for silver_accounts for that date. Do not treat as a failure.

Do not implement the incremental pipeline in this task.
```

**Test cases:**
| Case | Scenario | Expected |
|---|---|---|
| TC-1 | Full historical run over 7-day range | Watermark = end_date; run log has entries for all models |
| TC-2 | Re-run same range (idempotency) | Bronze SKIPs; Silver row counts identical; watermark unchanged if already = end_date |
| TC-3 | dbt model fails mid-sequence | Watermark not advanced; FAILED run log entry written |
| TC-4 | Watermark = end_date after success | `read_watermark() == end_date` |
| TC-5 | Run log entry for every model | `COUNT(DISTINCT model_name)` matches expected model list |

**Verification command:**
```bash
docker compose run --rm pipeline python pipeline.py historical --start-date 2024-01-01 --end-date 2024-01-07 && \
docker compose run --rm pipeline python -c "
import duckdb
from pipeline.control import read_watermark
from datetime import date as d
# Watermark should equal 2024-01-07 — replace 2024-01-07 with the actual last seed date before running
wm = read_watermark()
assert wm is not None, 'No watermark written'
assert str(wm) == '2024-01-07', f'Watermark: {wm}'
conn = duckdb.connect()
statuses = conn.execute(\"SELECT DISTINCT status FROM read_parquet('/data/pipeline/run_log.parquet')\").fetchall()
assert ('FAILED',) not in statuses, f'FAILED entries in run log'
print('Task 4.2 PASSED')
"
```

**Invariant flag:** INV-C1 (Watermark Gate), INV-C3, INV-C4, INV-C6, INV-S9 (transaction_codes loaded first), INV-E1, INV-E2. Code review: (1) watermark write is the last operation before run log writes; (2) no inner catch blocks; (3) sanitise_error called before every run log write with error_message.

---

### Task 4.3 — Incremental Pipeline Implementation

**Description:** Implement `pipeline.py incremental` subcommand. Reads watermark, determines next_date, checks transactions file existence, processes one date, advances watermark.

**CC prompt:**
```
Add the incremental subcommand to pipeline.py.

CLI interface:
  python pipeline.py incremental

Execution sequence:
1. Generate run_id = str(uuid.uuid4())
2. watermark = read_watermark()
   If watermark is None: print "No watermark found — run historical pipeline first" and exit(1)
3. next_date = watermark + timedelta(days=1)
4. Check if source_transactions_file(next_date) exists
   If NOT: print f"No transactions file for {next_date} — nothing to process" and exit(0) cleanly.
   Do NOT write a run log entry. Do NOT change the watermark.
5. If accounts file exists: load_bronze_partition('accounts', next_date, run_id); else skip
6. load_bronze_partition('transactions', next_date, run_id)
7. dbt run silver_accounts --vars processing_date=next_date, run_id
8. dbt run silver_transactions --vars processing_date=next_date, run_id
9. dbt run silver_quarantine --vars processing_date=next_date, run_id
10. dbt run gold_daily_summary --vars run_id
11. dbt run gold_weekly_account_summary --vars run_id
12. write_watermark(next_date, run_id)
13. Write all run log entries

Same error handling pattern as historical: single top-level try/except, no inner catches, sanitise before log write, do not advance watermark on failure.

Note: silver_transaction_codes is NOT run in incremental — reference data is already loaded.
```

**Test cases:**
| Case | Scenario | Expected |
|---|---|---|
| TC-1 | Run incremental after historical | Processes watermark+1; watermark advances by 1 day |
| TC-2 | Run incremental when no file for next_date | Clean exit 0; watermark unchanged; no run log entry |
| TC-3 | Run incremental twice same day (idempotency) | Bronze SKIPPED; Silver unchanged; watermark unchanged (already = next_date) |
| TC-4 | No watermark (no historical run) | Exits with error message; no crash |

**Verification command:**
```bash
docker compose run --rm pipeline python pipeline.py incremental && \
docker compose run --rm pipeline python -c "
from datetime import date
from pipeline.control import read_watermark
wm = read_watermark()
print(f'Watermark after incremental: {wm}')
assert wm is not None
print('Task 4.3 PASSED')
"
```

**Invariant flag:** INV-C1, INV-C2 (Sequential Progression, Clean Exit). Code review: (1) clean exit with no log write when file absent; (2) exactly one date processed per invocation; (3) watermark write is last act before log writes.

---

## Session 5 — End-to-End Verification + Idempotency

**Session goal:** Full pipeline runs end-to-end against the 7-day seed data. All invariants from INVARIANTS.md verified by DuckDB query. Idempotency confirmed by double-run. Audit chain traced end-to-end.

**Integration check:**
```bash
docker compose run --rm pipeline python -c "
import duckdb
conn = duckdb.connect()

# Full audit chain: pick a Gold record, trace to Silver, trace to Bronze, confirm source_file
gold_row = conn.execute(\"SELECT transaction_date, _pipeline_run_id FROM read_parquet('/data/gold/daily_summary/data.parquet') LIMIT 1\").fetchone()
run_id = gold_row[1]
silver_count = conn.execute(f"SELECT COUNT(*) FROM read_parquet('/data/silver/transactions/data.parquet') WHERE _pipeline_run_id = '{run_id}'").fetchone()[0]
assert silver_count > 0, 'No Silver records for Gold run_id'
log_entry = conn.execute(f\\\"SELECT status FROM read_parquet('/data/pipeline/run_log.parquet') WHERE run_id = '{run_id}' AND model_name = 'silver_transactions'\\\").fetchone()
assert log_entry is not None and log_entry[0] == 'SUCCESS'
print('Session 5 integration check PASSED: full audit chain verified')
"
```

---

### Task 5.1 — Full Pipeline Run and Invariant Verification

**Description:** Run the complete historical pipeline over all 7 seed data days. Execute each verification query from INVARIANTS.md and Section 10 of the requirements brief. Document pass/fail per invariant.

**CC prompt:**
```
Create scripts/verify_invariants.py — a standalone verification script that runs all invariant checks against the fully loaded data lake.

The script must:
- Connect to DuckDB (no server — embedded, reading Parquet files directly)
- Run each check below as a named assertion
- Print PASS or FAIL for each invariant ID
- Exit with code 1 if any check fails

Checks to implement (one function per invariant):

INV-B1: For each Bronze partition, confirm the partition exists and was not overwritten — check _pipeline_run_id consistency within partition (all rows same run_id means it was loaded in one run).

INV-B2: No null _source_file, _ingested_at, or _pipeline_run_id in any Bronze partition.

INV-S1 (Conservation Law): For each date, COUNT(bronze partition for that date) = COUNT(silver WHERE transaction_date = that date) + COUNT(quarantine WHERE transaction_date = that date). Silver and quarantine are flat files; filter by the transaction_date column.

INV-S2: COUNT(*) = COUNT(DISTINCT transaction_id) in silver/transactions/data.parquet (flat file, all dates).

INV-S3: No transaction_id appears in both silver/transactions/data.parquet and silver/quarantine/data.parquet.

INV-S4: No null _signed_amount in silver/transactions/data.parquet. All _signed_amount values where debit_credit_indicator='DR' are positive; where 'CR' are negative. (Join to silver_transaction_codes to verify.)

INV-S5: No null _is_resolvable in silver/transactions/data.parquet.

INV-S7: No null _rejection_reason, _rejected_at, or _pipeline_run_id in silver/quarantine/data.parquet. All _rejection_reason values are from the defined list: NULL_REQUIRED_FIELD, INVALID_AMOUNT, DUPLICATE_TRANSACTION_ID, INVALID_TRANSACTION_CODE, INVALID_CHANNEL, UNRESOLVABLE_ACCOUNT_ID.

INV-S8: COUNT(*) = COUNT(DISTINCT account_id) in silver/accounts/data.parquet.

INV-G1: Gold daily_summary row count = COUNT(DISTINCT transaction_date WHERE _is_resolvable=true) in silver/transactions/data.parquet.

INV-G3: Run the gold models twice (call subprocess to run dbt) and confirm Gold row counts are identical. (Or: assert deterministic by checking counts match a prior snapshot.)

INV-G4: Proxy check: gold total_signed_amount matches SUM(_signed_amount) from silver/transactions/data.parquet for each date WHERE _is_resolvable=true, within 0.0001.

INV-G5: For every row in gold/daily_summary/data.parquet, transactions_by_type contains all five keys with non-null count and total.

INV-C1: Control table watermark equals the last date present in silver/transactions/data.parquet (derive dynamically — do not hardcode a date).

INV-C3 + INV-C5: No duplicate (run_id, model_name) in run_log.

INV-C6: No null _pipeline_run_id in any Bronze, Silver, or Gold record.

INV-C7: COUNT(*) = 1 in pipeline/control.parquet.

INV-T1: For any Gold daily_summary row, SUM(_signed_amount) from silver/transactions/data.parquet WHERE transaction_date = gold.transaction_date AND _is_resolvable = true = gold.total_signed_amount (within 0.0001).

Do not create any other files.
```

**Test cases:**
| Case | Scenario | Expected |
|---|---|---|
| TC-1 | All invariant checks on clean 7-day load | All print PASS; exit code 0 |
| TC-2 | Force a known conservation violation (manually add a row to Bronze after Silver run) | INV-S1 prints FAIL; exit code 1 |

**Verification command:**
```bash
# Replace 2024-01-01 and 2024-01-07 with actual first and last dates from source/ CSV files
docker compose run --rm pipeline python pipeline.py historical --start-date 2024-01-01 --end-date 2024-01-07 && \
docker compose run --rm pipeline python scripts/verify_invariants.py
```

**Invariant flag:** All invariants. This task is the verification surface for the entire system.

---

### Task 5.2 — Idempotency Verification

**Description:** Run the full pipeline twice against the same seed data. Confirm all layer row counts are identical. Confirm the watermark does not regress.

**CC prompt:**
```
Create scripts/verify_idempotency.py — a script that:

1. Captures current state snapshots:
   - Bronze row counts per entity (transactions, accounts, transaction_codes)
   - Silver row counts (transactions, accounts, quarantine)
   - Gold row counts (daily_summary, weekly_account_summary)
   - Current watermark date

2. Prints all captured counts to stdout with labels

3. Compares against a previously-captured snapshot if a snapshot file exists at /data/idempotency_snapshot.json. If no snapshot exists, saves the current snapshot and exits with a message "Snapshot saved — re-run to compare."

4. If a snapshot exists: assert each count matches exactly. Print PASS or FAIL per entity. Exit 1 if any mismatch.

The script does NOT re-run the pipeline itself. It only reads current Parquet state and compares.

Do not create any other files.
```

**Test cases:**
| Case | Scenario | Expected |
|---|---|---|
| TC-1 | Run pipeline → snapshot → run pipeline again → compare | All counts identical; watermark unchanged |
| TC-2 | Counts after second run match counts after first run | No count inflation anywhere |

**Verification command:**
```bash
# Replace 2024-01-01 and 2024-01-07 with actual first and last dates from source/ CSV files
# First run + snapshot
docker compose run --rm pipeline python pipeline.py historical --start-date 2024-01-01 --end-date 2024-01-07
docker compose run --rm pipeline python scripts/verify_idempotency.py

# Second run (should produce SKIPs at Bronze, identical Silver/Gold)
docker compose run --rm pipeline python pipeline.py historical --start-date 2024-01-01 --end-date 2024-01-07
docker compose run --rm pipeline python scripts/verify_idempotency.py
```

**Invariant flag:** INV-B1, INV-G3, INV-C8 (Historical Watermark Idempotency). Code review: not applicable — this is a verification script, not a production path.

---

## Requirements Traceability

| Requirement | Owner in Architecture | Execution Plan task |
|---|---|---|
| Bronze immutable raw landing zone | DD4 | Task 1.3, 1.4 |
| Bronze audit columns | INV-B2 | Task 1.3, 1.4 |
| Silver quality rules — all 6 transaction codes | DD5, INV-S1–S7 | Task 2.3, 2.4 |
| Silver accounts upsert | INV-S8 | Task 2.2 |
| Silver transaction codes loaded once | DD5, INV-S9 | Task 2.1 |
| Silver quarantine with reason codes | IC1, INV-S7 | Task 2.4 |
| Sign assignment from transaction_codes join | DD5, INV-S4 | Task 2.3 |
| `_is_resolvable` flag | DD9, INV-S5 | Task 2.3 |
| Gold daily summary with STRUCT | DD10, INV-G5 | Task 3.1 |
| Gold weekly account summary | Section 4.3.2 | Task 3.2 |
| Gold full rebuild | DD6, INV-G3 | Task 3.1, 3.2 |
| Historical pipeline with watermark | Section 3.1, DD2 | Task 4.2 |
| Incremental pipeline with watermark | Section 3.2, DD2, DD3 | Task 4.3 |
| Control table single-row | INV-C7 | Task 4.1 |
| Run log append-only | DD8, INV-C3 | Task 4.1 |
| Error sanitisation | DD7, INV-E1 | Task 4.1, 4.2, 4.3 |
| End-to-end audit chain | INV-T1, T2, T3 | Task 5.1 |
| Idempotency end-to-end | Section 8, INV-B1 | Task 5.2 |
| Docker single-command startup | Section 8 constraints | Task 1.1 |
| Path constants as single source of truth | IC13 | Task 1.2 |

---

*PBVI Phase 3 complete. Proceed to Phase 4 — Design Gate.*
