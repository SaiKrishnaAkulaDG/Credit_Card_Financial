# Claude.md — v1.0 · FROZEN · 2026-03-25

---

## 1. System Intent

This system is a Medallion data lake pipeline (Bronze → Silver → Gold) that ingests daily credit card transaction CSV extracts, enforces defined quality rules at each layer boundary, and produces Gold-layer aggregations queryable via DuckDB. It surfaces pre-existing data only — it does not compute risk, make credit decisions, or modify source system records. Success means: given any Gold aggregate, an analyst can trace it back to the Silver records that produced it, the Bronze records that fed Silver, and the exact source CSV file — the chain is unbroken and queryable.

---

## 2. Hard Invariants

INVARIANT: INV-B1 — A Bronze partition for a given date must be written at most once. If the partition already exists, ingestion must skip without modifying, appending to, or overwriting it. This is never negotiable.

INVARIANT: INV-B2 — Every Bronze record must have non-null values for `_source_file`, `_ingested_at`, and `_pipeline_run_id`. This is never negotiable.

INVARIANT: INV-B3 — Bronze must preserve source data per explicit schema contract. Field values must not be transformed. Type coercion must be deterministic and schema-defined. No loss of precision or truncation is permitted. This is never negotiable.

INVARIANT: INV-S1 — For each processing date, `COUNT(bronze) = COUNT(silver) + COUNT(quarantine)` for transactions. Every Bronze transaction record maps to exactly one outcome — Silver OR Quarantine — never both, never neither. This is never negotiable.

INVARIANT: INV-S2 — `transaction_id` must be globally unique across ALL Silver transaction partitions, not just the current date's partition. This is never negotiable.

INVARIANT: INV-S3 — If duplicate `transaction_id` values exist in Bronze (within a date or across dates), exactly one record is promoted to Silver and all others are written to Quarantine with `_rejection_reason = DUPLICATE_TRANSACTION_ID`. The deduplication check must span all existing Silver partitions. This is never negotiable.

INVARIANT: INV-S4 — `_signed_amount` is computed only in `silver_transactions.sql` via a JOIN to `silver_transaction_codes` on `debit_credit_indicator`. DR = source amount (positive). CR = source amount × −1. Gold and all downstream layers read `_signed_amount` directly and must not recompute sign. `_signed_amount` must never be null. A failed join produces an `INVALID_TRANSACTION_CODE` quarantine record — never a Silver record with null `_signed_amount`. This is never negotiable.

INVARIANT: INV-S5 — For every Silver transaction record: `_is_resolvable = true` if and only if `account_id` exists in Silver Accounts at promotion time; `_is_resolvable = false` if and only if it does not. `_is_resolvable` must never be null. This is never negotiable.

INVARIANT: INV-S6 — Every Silver transaction record must have non-null values for `_pipeline_run_id`, `_bronze_ingested_at`, `_promoted_at`, and `_is_resolvable`. This is never negotiable.

INVARIANT: INV-S7 — Every quarantined record must include all original source record fields, a non-null `_rejection_reason` from the pre-defined code list, a non-null `_rejected_at`, and a non-null `_pipeline_run_id`. This is never negotiable.

INVARIANT: INV-S8 — Silver Accounts must contain exactly one record per `account_id` at all times. Delta records replace existing records on conflict; new records are inserted. This is never negotiable.

INVARIANT: INV-S9 — Silver transaction codes must be successfully loaded before any Silver transaction promotion begins. No Silver transaction record may be promoted until `silver/transaction_codes/data.parquet` exists and is non-empty. This is never negotiable.

INVARIANT: INV-G1 — Gold aggregations must include only records where `_is_resolvable = true`. This filter must be applied at the Silver query level in the dbt model's WHERE clause, before aggregation — not as a post-aggregation filter. This is never negotiable.

INVARIANT: INV-G2 — Gold must be computed exclusively from Silver. No Gold dbt model may reference Bronze sources directly. This is never negotiable.

INVARIANT: INV-G3 — Gold must be fully rebuilt on every pipeline run (truncate and rewrite — no append, no merge). Given the same Silver inputs, all aggregate values must be identical across runs. This is never negotiable.

INVARIANT: INV-G4 — All Gold aggregations must be computed using `_signed_amount`. Raw `amount` must not appear in any Gold model. This is never negotiable.

INVARIANT: INV-G5 — The `transactions_by_type` STRUCT in `gold/daily_summary` must always contain all five keys: PURCHASE, PAYMENT, FEE, INTEREST, REFUND. Types with no transactions on a given day must emit `count = 0` and `sum = 0.00` — not a missing key. This is never negotiable.

INVARIANT: INV-C1 — The watermark must advance if and only if all pipeline layers — Bronze, Silver, and Gold — complete successfully for the target date. If any layer fails, the watermark must not change. This is never negotiable.

INVARIANT: INV-C2 — The incremental pipeline must process only `watermark + 1`. It must never skip dates or process more than one date per invocation. If no transactions file exists for `watermark + 1`, the pipeline must exit cleanly with no watermark change, no run log entry, and no error status. This is never negotiable.

INVARIANT: INV-C3 — The run log must be append-only — no updates or deletions of existing entries. Run log entries must be written exactly once, on model completion. No partial entry may be written on model start and updated on completion. This is never negotiable.

INVARIANT: INV-C4 — Every model invocation must produce exactly one run log entry, including deliberate skips. Each entry must include non-null `pipeline_run_id`, `status` (SUCCESS, FAILED, or SKIPPED), `started_at`, and `completed_at`. This is never negotiable.

INVARIANT: INV-C5 — The run log must contain at most one entry per `(run_id, model_name)` combination. This is never negotiable.

INVARIANT: INV-C6 — Every record in Bronze, Silver, and Gold must include a non-null `_pipeline_run_id`. This is never negotiable.

INVARIANT: INV-C7 — The pipeline control table (`pipeline/control.parquet`) must contain exactly one row at all times. Watermark advancement must replace the existing row — not append a new one. This is never negotiable.

INVARIANT: INV-C8 — On successful completion of the historical pipeline, the watermark must be set to `end_date`. On re-running the historical pipeline against dates already processed, the watermark must not regress. This is never negotiable.

INVARIANT: INV-T1 — Every Gold record must be traceable to the contributing Silver records via `_pipeline_run_id` and the record's grouping keys. This is never negotiable.

INVARIANT: INV-T2 — Every Silver transaction record must retain `_bronze_ingested_at` and `_source_file`, enabling traceability back to the Bronze record and its source partition. This is never negotiable.

INVARIANT: INV-T3 — Every Bronze record must retain `_source_file`, containing the originating CSV filename. This is never negotiable.

INVARIANT: INV-E1 — All error messages written to the run log must be sanitised to remove file paths, credentials, and internal system details before persistence. This is never negotiable.

INVARIANT: INV-E2 — Every pipeline failure must produce a run log entry with `status = FAILED` and a non-null sanitised `error_message`. No failure may be silently swallowed without a run log record. This is never negotiable.

---

## 3. Scope Boundary

### Files CC may create or modify

```
pipeline/__init__.py
pipeline/constants.py
pipeline/control.py
pipeline.py
bronze_loader/__init__.py
bronze_loader/loader.py
dbt_project/dbt_project.yml
dbt_project/profiles.yml
dbt_project/models/silver/silver_transaction_codes.sql
dbt_project/models/silver/silver_accounts.sql
dbt_project/models/silver/silver_transactions.sql
dbt_project/models/silver/silver_quarantine.sql
dbt_project/models/gold/gold_daily_summary.sql
dbt_project/models/gold/gold_weekly_account_summary.sql
scripts/verify_invariants.py
scripts/verify_idempotency.py
docker-compose.yml
Dockerfile
requirements.txt
```

### CC must not

- Modify or delete any file in the `source/` directory
- Create any file that writes to `source/`
- Create intermediate databases or any output format other than Parquet
- Implement sign logic in any file other than `silver_transactions.sql`
- Write to `pipeline/run_log.parquet` or `pipeline/control.parquet` from any file other than `pipeline/control.py` (called exclusively by `pipeline.py`)
- Reference Bronze paths in any Gold dbt model
- Apply a post-aggregation resolvability filter in any Gold model — the filter must be in the WHERE clause before GROUP BY
- Add inner try/except blocks in `pipeline.py` that could swallow module exceptions before the top-level handler
- Advance the watermark before all three layers (Bronze, Silver, Gold) have returned success
- Use `amount` (without underscore prefix) in any Gold model
- Construct paths as hardcoded strings — all paths must use `pipeline/constants.py`
- Write partial run log rows (start-then-update pattern)
- Overwrite or delete existing Bronze partitions
- Append to `pipeline/control.parquet` — it is always a single-row overwrite

### Conflict rule

If any task prompt conflicts with an invariant listed in Section 2, the invariant wins. Do not resolve the conflict silently. Flag it immediately and stop.

---

## 4. Fixed Stack

| Concern | Fixed choice |
|---|---|
| Python version | 3.11 |
| dbt-core | 1.7.x (pin: `dbt-core~=1.7`) |
| dbt-duckdb adapter | 1.7.x (pin: `dbt-duckdb~=1.7`) |
| DuckDB (Python) | `>=0.10` |
| Containerisation | Docker Compose — single-command startup via `docker compose up` |
| Bronze ingestion | Python + DuckDB directly — dbt is never used for Bronze |
| Silver and Gold | dbt models exclusively — no ad-hoc SQL outside dbt |
| Storage format | Parquet only — no intermediate databases, no CSV outputs |
| Control and run log | Parquet files — no metadata database |
| Query engine | DuckDB embedded — no separate server process |
| Source data location | `/source/` (read-only bind mount) |
| Data lake location | `/data/` (read-write bind mount) |
| dbt project directory | `dbt_project/` |
| dbt profile name | `credit_card_lake` |
| dbt target | `dev` |
| dbt database path | `/data/lake.duckdb` |
| dbt threads | 1 |

### Silver and Gold path contract (DD-P1 resolution)

dbt-duckdb does not natively produce Hive-style `date=YYYY-MM-DD` partition folders. Silver and quarantine are written as single flat Parquet files filtered by `transaction_date` column. Gold tables are single flat Parquet files. Bronze retains filesystem partitioning (Python + DuckDB). This boundary is intentional and must not be changed.

| Entity | Path |
|---|---|
| Bronze transactions partition | `/data/bronze/transactions/date=YYYY-MM-DD/data.parquet` |
| Bronze accounts partition | `/data/bronze/accounts/date=YYYY-MM-DD/data.parquet` |
| Bronze transaction codes | `/data/bronze/transaction_codes/data.parquet` |
| Silver transactions (flat) | `/data/silver/transactions/data.parquet` |
| Silver accounts (flat) | `/data/silver/accounts/data.parquet` |
| Silver transaction codes (flat) | `/data/silver/transaction_codes/data.parquet` |
| Silver quarantine (flat) | `/data/silver/quarantine/data.parquet` |
| Gold daily summary | `/data/gold/daily_summary/data.parquet` |
| Gold weekly account summary | `/data/gold/weekly_account_summary/data.parquet` |
| Pipeline control | `/data/pipeline/control.parquet` |
| Pipeline run log | `/data/pipeline/run_log.parquet` |

All paths must be constructed from constants defined in `pipeline/constants.py`. No path string may be hardcoded in any other file.

### dbt variable contract

Every dbt model invocation must pass the following variables as applicable:

| Variable | Required by | Value |
|---|---|---|
| `run_id` | All Silver and Gold models | `str(uuid.uuid4())` generated once per pipeline invocation |
| `processing_date` | `silver_accounts`, `silver_transactions`, `silver_quarantine` | `YYYY-MM-DD` string for the date being processed |

### Rejection reason codes (exhaustive — no additions permitted)

`NULL_REQUIRED_FIELD`, `INVALID_AMOUNT`, `DUPLICATE_TRANSACTION_ID`, `INVALID_TRANSACTION_CODE`, `INVALID_CHANNEL`, `UNRESOLVABLE_ACCOUNT_ID`

`UNRESOLVABLE_ACCOUNT_ID` is the only code that produces a Silver record (with `_is_resolvable = false`) rather than a quarantine record.
