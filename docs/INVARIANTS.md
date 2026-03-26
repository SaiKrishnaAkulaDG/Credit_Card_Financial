# INVARIANTS.md

**Credit Card Financial Transactions Lake**
*Version 1.0 · PBVI Phase 2 Output · Classification: Training Demo System*

---

## Overview

Invariants are conditions that must never break. If any invariant is violated, the system is broken regardless of what else works. These are not goals or design preferences — they are constraints. Violation of any invariant during Phase 8 sign-off is a build failure, not a finding.

**Authorship:** Engineer-authored and signed off. Claude challenged and checked sufficiency. Final set agreed before Phase 3 begins.

**Scope note:** INV-S1 (conservation law) applies to transactions only. Accounts follow upsert semantics with different rejection rules and no 1:1 conservation relationship.

---

## Bronze Layer Invariants

### INV-B1 — Bronze Immutability and Idempotency

**Condition:** A Bronze partition for a given date must be written at most once. If the partition already exists, ingestion must skip without modifying, appending to, or overwriting it.

**Category:** Data correctness

**Why this matters:** Overwriting a Bronze partition destroys the immutability guarantee and corrupts the audit trail. An append would silently double row counts, causing all downstream conservation checks to fail with no clear signal. A skip-on-exists pattern is the only mechanism that satisfies both immutability and idempotency simultaneously.

**Enforcement points:**
- `bronze_loader.py`: checks partition path existence before any write operation
- `bronze_loader.py`: returns SKIPPED status (not FAILED) when partition exists
- `pipeline.py`: writes a SKIPPED run log entry when bronze_loader returns SKIPPED

---

### INV-B2 — Bronze Audit Columns Completeness

**Condition:** Every Bronze record must have non-null values for:
- `_source_file`
- `_ingested_at`
- `_pipeline_run_id`

**Category:** Data correctness / Audit

**Why this matters:** These three columns are the foundation of the audit chain. A null `_pipeline_run_id` breaks traceability from Bronze to the run log. A null `_source_file` breaks traceability from Bronze to the source CSV. A null `_ingested_at` removes the temporal anchor for the ingestion event.

**Enforcement points:**
- `bronze_loader.py`: adds all three audit columns before writing the Parquet partition
- Verification query against all Bronze partitions: no nulls in any audit column

---

### INV-B3 — Raw Data Fidelity (Schema-Defined)

**Condition:** Bronze must preserve source data as defined by an explicit schema contract:
- Field values must not be transformed (no derived fields, no business logic applied)
- Type coercion must be deterministic and consistent with the defined schema
- No loss of precision or truncation is permitted

**Category:** Data correctness

**Why this matters:** Bronze is the immutable record of what arrived. Any uncontrolled transformation at this layer makes it impossible to distinguish source data errors from pipeline-introduced errors — which is the core purpose of the Bronze layer's existence.

**Enforcement points:**
- `bronze_loader.py`: reads source CSV with an explicit schema definition; no transformations applied beyond adding audit columns
- Schema contract is defined once and referenced by all Bronze loaders — not inferred at runtime
- Verification: field values in Bronze Parquet match source CSV for a sample of records

---

## Silver Layer Invariants

### INV-S1 — No Silent Drops (Conservation Law)

**Condition (transactions only):** For each processing date:

```
Bronze transaction row count = Silver transaction row count + Quarantine row count
```

Additionally, every Bronze transaction record must map to exactly one outcome — Silver OR Quarantine — never both, never neither.

**Category:** Data correctness

**Why this matters:** Silent drops are the most dangerous failure mode in a data pipeline — Gold aggregates are understated with no observable signal. The conservation law is the primary mechanism for detecting any record that fell through without a documented outcome.

**Enforcement points:**
- `silver_transactions.sql` and `silver_quarantine.sql`: together must account for every Bronze transaction record
- Verification query: `COUNT(bronze) = COUNT(silver) + COUNT(quarantine)` per date partition
- Note: this invariant applies to transactions only; accounts follow upsert semantics

---

### INV-S2 — Global Transaction ID Uniqueness

**Condition:** `transaction_id` must be globally unique across all Silver transaction partitions — not just within the current date's partition.

**Category:** Data correctness

**Why this matters:** A duplicate `transaction_id` in Silver means a transaction has been counted and summed twice. Gold aggregates (total_transactions, total_signed_amount) would be wrong with no row-count signal to indicate the problem.

**Enforcement points:**
- `silver_transactions.sql`: deduplication check scans ALL existing Silver transaction partitions before promoting any record from the current batch
- Verification query: `COUNT(*) = COUNT(DISTINCT transaction_id)` across all Silver partitions

---

### INV-S3 — Deduplication Correctness (Cross-Partition Scope)

**Condition:** If duplicate `transaction_id` values exist in Bronze (within a date or across dates):
- Exactly one record is promoted to Silver
- All others are written to Quarantine with `_rejection_reason = DUPLICATE_TRANSACTION_ID`
- The deduplication check must span all existing Silver partitions, not only the current batch

**Category:** Data correctness

**Why this matters:** A within-batch-only deduplication check would pass if the seed data has no same-date duplicates, but would silently allow a cross-date duplicate through. FM4 in ARCHITECTURE.md explicitly identifies this as a failure mode with a high-impact consequence: Gold `total_signed_amount` wrong with no row count signal.

**Enforcement points:**
- `silver_transactions.sql`: LEFT JOIN or EXISTS check against all existing Silver partitions before promotion decision
- Verification query: no `transaction_id` appears in both Silver and Quarantine; no `transaction_id` appears more than once in Silver

---

### INV-S4 — Sign Assignment Correctness (Single Source of Truth)

**Condition:** Sign must be applied exactly once, with two mandatory enforcement points:
1. `_signed_amount` is computed only in the Silver transactions dbt model, via a JOIN to `silver_transaction_codes` on `debit_credit_indicator`. DR = source amount (positive). CR = source amount × −1.
2. Gold and all downstream layers must use `_signed_amount` directly and must not recompute sign.

Additionally: `_signed_amount` must never be null in any Silver transaction record. A failed join (transaction_code not found in Silver transaction_codes) must produce an `INVALID_TRANSACTION_CODE` quarantine record — not a Silver record with a null `_signed_amount`.

**Category:** Data correctness

**Why this matters:** A null `_signed_amount` in Silver would silently exclude that transaction from Gold sums (NULL propagation in aggregates). An incorrect sign would corrupt balance calculations. Both failures are undetectable from row counts alone.

**Enforcement points:**
- `silver_transactions.sql`: LEFT JOIN to `silver_transaction_codes`; null join result → quarantine with `INVALID_TRANSACTION_CODE`, not Silver record
- `gold_daily_summary.sql` and `gold_weekly_account_summary.sql`: read `_signed_amount` directly; no sign logic present in Gold models
- Verification query: no null `_signed_amount` in any Silver transactions record

---

### INV-S5 — Resolvability Flag Correctness (Bidirectional)

**Condition:** For every Silver transaction record:
- `_is_resolvable = true` if and only if `account_id` exists in Silver Accounts at promotion time
- `_is_resolvable = false` if and only if `account_id` does not exist in Silver Accounts at promotion time
- `_is_resolvable` must never be null

**Category:** Data correctness

**Why this matters:** A null `_is_resolvable` would be excluded from Gold by the `WHERE _is_resolvable = true` filter — silently understating aggregates with no quarantine record and no diagnostic signal. The bidirectional constraint prevents both false positives (valid accounts excluded from Gold) and false negatives (unresolvable accounts included in Gold).

**Enforcement points:**
- `silver_transactions.sql`: LEFT JOIN to `silver_accounts`; flag set based on join result; COALESCE ensures no null
- Verification query: `COUNT(*) WHERE _is_resolvable IS NULL = 0` across all Silver transaction partitions

---

### INV-S6 — Silver Audit Columns Completeness

**Condition:** Every Silver transaction record must have non-null values for:
- `_pipeline_run_id`
- `_bronze_ingested_at`
- `_promoted_at`
- `_is_resolvable`

**Category:** Audit

**Why this matters:** These columns are the Silver layer's contribution to the end-to-end audit chain. A null `_is_resolvable` has a direct correctness impact (see INV-S5). A null `_pipeline_run_id` breaks traceability from Silver to the run log. A null `_bronze_ingested_at` breaks the Silver → Bronze link.

**Enforcement points:**
- `silver_transactions.sql`: all four columns populated before writing; no nullable defaults
- Verification query: no nulls in any of these columns across all Silver transaction partitions

---

### INV-S7 — Quarantine Integrity

**Condition:** Every quarantined record must include:
- All original source record fields (as received from Bronze — no source field may be dropped)
- Non-null `_rejection_reason` (from the pre-defined code list in requirements Section 5)
- Non-null `_rejected_at`
- Non-null `_pipeline_run_id`

**Category:** Data correctness / Audit

**Why this matters:** A quarantine record without source fields is useless for diagnosis and remediation. The quarantine is the audit trail for data quality failures — its evidential value depends entirely on the original record being preserved alongside the rejection metadata.

**Enforcement points:**
- `silver_quarantine.sql`: SELECT includes all source columns plus rejection audit columns; no column drops permitted
- Verification query: no null `_rejection_reason`, `_rejected_at`, or `_pipeline_run_id` in any quarantine record; `_rejection_reason` values must be members of the pre-defined code list

---

### INV-S8 — Accounts Upsert Correctness

**Condition:** Silver Accounts must contain exactly one record per `account_id`, representing the latest available state. Delta records from Bronze replace existing records on conflict; new records are inserted.

**Category:** Data correctness

**Why this matters:** Multiple records for the same `account_id` would produce non-deterministic `closing_balance` values in Gold weekly aggregates. The single-record guarantee is the foundation of the SCD Type 1 simplification documented in ARCHITECTURE.md.

**Enforcement points:**
- `silver_accounts.sql`: upsert logic — DELETE existing `account_id` then INSERT, or equivalent merge; no append-only path
- Verification query: `COUNT(*) = COUNT(DISTINCT account_id)` in `silver/accounts/data.parquet`

---

### INV-S9 — Transaction Codes Sequencing

**Condition:** Silver transaction codes must be successfully loaded before any Silver transaction promotion begins. No Silver transaction record may be promoted until `silver/transaction_codes/data.parquet` exists and is non-empty.

**Category:** Operational correctness

**Why this matters:** INV-S4 requires the sign assignment join to resolve against Silver transaction codes. If the reference table is absent, every transaction would fail with `INVALID_TRANSACTION_CODE` — a systematic rejection of all records that would be indistinguishable from genuine data quality failures.

**Enforcement points:**
- `pipeline.py` (historical pipeline): `silver_transaction_codes` dbt model runs and returns SUCCESS before the first Silver transaction promotion is invoked
- Structural: `silver_transactions.sql` declares a dbt source dependency on `silver_transaction_codes`

---

## Gold Layer Invariants

### INV-G1 — Resolvability Filter Enforcement (Pre-Aggregation)

**Condition:** Gold aggregations must:
- Include only records where `_is_resolvable = true`
- Apply this filter at the Silver query level (in the dbt model's WHERE clause), before aggregation — not as a post-aggregation filter

**Category:** Data correctness

**Why this matters:** A post-aggregation filter on a pre-summed result would produce incorrect sums if any unresolvable records had been included in the aggregation. The filter must be applied before any COUNT or SUM operation.

**Enforcement points:**
- `gold_daily_summary.sql`: `WHERE _is_resolvable = true` present in the base Silver query, before GROUP BY
- `gold_weekly_account_summary.sql`: same requirement
- Code review: confirm filter position in SQL — not in a HAVING clause, not in a wrapper SELECT

---

### INV-G2 — No Bronze Dependency

**Condition:** Gold must be computed exclusively from Silver. No Gold dbt model may reference Bronze sources directly.

**Category:** Data correctness / Architecture

**Why this matters:** Gold computed from Bronze would bypass all quality rules, sign assignment, and resolvability filtering — producing aggregates from raw, unvalidated data. This is the exact failure mode the Medallion architecture exists to prevent.

**Enforcement points:**
- dbt model lineage: Gold models reference only `silver/` sources in their `FROM` clauses and dbt source definitions
- No `bronze/` path appears in any Gold model file

---

### INV-G3 — Full Rebuild and Deterministic Values

**Condition:** Gold must be:
- Fully rebuilt on every pipeline run (truncate and rewrite — no append, no merge)
- Deterministic: given the same Silver inputs, all aggregate values (counts, sums, grouping keys) must be identical across runs

File-level byte identity is not required. Value identity is required.

**Category:** Data correctness / Operational

**Why this matters:** A partial rebuild (append) would accumulate duplicate Gold rows across runs, corrupting all aggregates. Non-determinism would make Gold untrustworthy for repeated queries — analysts would get different answers from the same data.

**Enforcement points:**
- `gold_daily_summary.sql` and `gold_weekly_account_summary.sql`: dbt materialisation set to `table` (full truncate + insert on every run)
- Verification: row counts and aggregate values identical after two consecutive pipeline runs on the same Silver state

---

### INV-G4 — Aggregation Correctness

**Condition:** All Gold aggregations must be computed using `_signed_amount`. Raw `amount` must not appear in any Gold model.

**Category:** Data correctness

**Why this matters:** Raw `amount` is always positive in the source. Using it in Gold would produce incorrect totals — payments, refunds, and fees would appear as positive charges, making balance calculations meaningless.

**Enforcement points:**
- `gold_daily_summary.sql` and `gold_weekly_account_summary.sql`: SUM computed on `_signed_amount`; no reference to `amount`
- Code review: grep for `amount` (without underscore prefix) in Gold model files

---

### INV-G5 — transactions_by_type STRUCT Completeness

**Condition:** The `transactions_by_type` STRUCT in `gold/daily_summary` must always contain all five defined transaction types: PURCHASE, PAYMENT, FEE, INTEREST, REFUND. Types with no transactions on a given day must emit `count = 0` and `sum = 0.00` — not a missing key.

**Category:** Data correctness

**Why this matters:** A sparse STRUCT (only types present that day) produces inconsistent query behaviour in DuckDB across days — queries that work on days with all five types would fail or return nulls on days with fewer. A fixed-schema STRUCT is queryable without conditional logic.

**Enforcement points:**
- `gold_daily_summary.sql`: explicit STRUCT construction with zero-fill for absent types — not a simple GROUP BY pivot
- Verification query: for any date in Gold daily_summary, STRUCT contains exactly five keys with non-null values

---

## Control and Audit Invariants

### INV-C1 — Watermark Advancement Gate

**Condition:** The watermark must advance if and only if all pipeline layers — Bronze, Silver, and Gold — complete successfully for the target date. If any layer fails, the watermark must not change.

**Category:** Operational correctness

**Why this matters:** An advanced watermark on partial success means the failed date is permanently skipped on the next incremental run — a silent data gap in Gold with no reprocessing path. This is FM1 in ARCHITECTURE.md: the most critical failure mode in the system.

**Enforcement points:**
- `pipeline.py`: watermark advancement is the final write of any pipeline run — structurally unreachable if any layer module raised an exception
- `pipeline.py`: no inner try/catch blocks that could swallow module exceptions before they reach the coordinator's top-level handler
- Control table write occurs after all run log entries are written

---

### INV-C2 — Sequential Progression and Clean Exit on No New Data

**Condition:** The incremental pipeline must process only `watermark + 1`. It must never skip dates or process more than one date per invocation. If no transactions file exists for `watermark + 1`, the pipeline must exit cleanly with no watermark change, no run log entry, and no error status.

**Category:** Operational correctness

**Why this matters:** Processing future dates or skipping dates would create gaps or out-of-order Gold aggregates. A missing transactions file is not an error — it is a "no new data" state. Treating it as a failure would generate false alerts in a scheduled environment and pollute the run log.

**Enforcement points:**
- `pipeline.py`: file existence pre-check for the transactions file before invoking any layer module
- `pipeline.py`: clean exit (no exception, no run log write, no watermark change) when transactions file is absent
- `pipeline.py`: incremental pipeline processes exactly one date per invocation — no loop

---

### INV-C3 — Run Log Append-Only and Write-on-Completion

**Condition:** The run log must be append-only — no updates or deletions of existing entries. Run log entries must be written exactly once, on model completion (SUCCESS, FAILED, or SKIPPED). No partial entry may be written on model start and updated on completion.

**Category:** Audit

**Why this matters:** A start-then-update pattern satisfies append-only semantics on the write but violates the write-on-completion guarantee. A partial row with stale timestamps is not diagnosable. A gap in the run log (process killed mid-execution) is diagnosable. The run log's value as an audit trail depends on every present entry being a complete, trustworthy record.

**Enforcement points:**
- `pipeline.py`: single append write per model, after the model returns its status — no two-phase write
- No component other than `pipeline.py` writes to `pipeline/run_log.parquet`
- Verification: no two rows share the same `(run_id, model_name)` — see INV-C5

---

### INV-C4 — Run Log Completeness

**Condition:** Every model invocation must produce exactly one run log entry, including deliberate skips. Each entry must include non-null:
- `pipeline_run_id`
- `status` (SUCCESS, FAILED, or SKIPPED)
- `started_at` and `completed_at` timestamps

**Category:** Audit

**Why this matters:** A missing run log entry for a model is an audit gap — it is impossible to distinguish a model that was never invoked from one that was invoked and killed mid-execution. SKIPPED entries are required because a skipped Bronze partition is a deliberate pipeline decision that must be traceable.

**Enforcement points:**
- `pipeline.py`: run log write after every module invocation, including SKIPPED returns from `bronze_loader.py`
- Verification query: for any `run_id` in the run log, the set of `model_name` values matches the expected set for that pipeline type (HISTORICAL or INCREMENTAL)

---

### INV-C5 — Run Log Entry Uniqueness

**Condition:** The run log must contain at most one entry per `(run_id, model_name)` combination. Duplicate entries for the same run and model are not permitted.

**Category:** Audit

**Why this matters:** Duplicate run log entries make the audit trail ambiguous — it is impossible to determine which entry represents the actual execution outcome. Traceability queries that join on `(run_id, model_name)` would return multiple rows.

**Enforcement points:**
- `pipeline.py`: run log write is the final act per model in a run; no retry-and-append pattern
- Verification query: `COUNT(*) = COUNT(DISTINCT run_id || model_name)` in `pipeline/run_log.parquet`

---

### INV-C6 — Audit Chain Completeness

**Condition:** Every record in Bronze, Silver, and Gold must include a non-null `_pipeline_run_id`, enabling traceability to a specific pipeline run entry in the run log.

**Category:** Audit

**Why this matters:** `_pipeline_run_id` is the connective tissue across all layers. Without it, it is impossible to answer the core traceability question: given a suspicious Gold aggregate, which pipeline run produced it, and what did that run's Silver and Bronze record counts look like?

**Enforcement points:**
- `bronze_loader.py`: `_pipeline_run_id` added as audit column on every write
- `silver_transactions.sql`, `silver_accounts.sql`, `silver_transaction_codes.sql`: `_pipeline_run_id` populated on every promotion
- `gold_daily_summary.sql`, `gold_weekly_account_summary.sql`: `_pipeline_run_id` included in every Gold record
- Verification query: no null `_pipeline_run_id` in any layer

---

### INV-C7 — Control Table Single-Row Invariant

**Condition:** The pipeline control table (`pipeline/control.parquet`) must contain exactly one row at all times. Watermark advancement must replace the existing row — not append a new one.

**Category:** Operational correctness

**Why this matters:** Multiple rows in the control table make watermark reads non-deterministic. Different read strategies (first row, last row, max date) would produce different watermarks, causing the incremental pipeline to process wrong dates or reprocess already-processed dates.

**Enforcement points:**
- `pipeline.py`: watermark advancement overwrites the control table with a single-row Parquet file — not an append operation
- Verification query: `COUNT(*) = 1` in `pipeline/control.parquet` after any pipeline run

---

### INV-C8 — Historical Pipeline Watermark Initialisation

**Condition:** On successful completion of the historical pipeline:
- The watermark must be set to `end_date`
- The control table must contain exactly one row (see INV-C7)

On re-running the historical pipeline against dates already processed:
- The watermark must remain unchanged (not regressed, not corrupted)
- The control table must still contain exactly one row after the re-run

**Category:** Operational correctness

**Why this matters:** An incorrect watermark after historical initialisation would cause the incremental pipeline to start from the wrong date — either reprocessing existing data (idempotency failure) or skipping dates (data gap). A watermark regression on re-run would silently undo all incremental progress made since the historical load.

**Enforcement points:**
- `pipeline.py` (historical path): control table write sets `last_processed_date = end_date` on success
- `pipeline.py` (historical path on re-run): Bronze skips trigger INV-B1; Silver is idempotent; watermark advancement logic reads current watermark before writing — does not regress if current value is already `>= end_date`
- Verification: watermark value equals `end_date` after first historical run; watermark value unchanged after second run on same date range

---

## Traceability Invariants

### INV-T1 — Gold → Silver Traceability

**Condition:** Every Gold record must be traceable to the contributing Silver records via `_pipeline_run_id` and the record's grouping keys (`transaction_date` for daily summary; `week_start_date` + `account_id` for weekly account summary).

**Category:** Audit

**Why this matters:** The core system requirement is decision confidence with traceability. An analyst must be able to start from any Gold aggregate and reconstruct the Silver records that produced it.

**Enforcement points:**
- `gold_daily_summary.sql` and `gold_weekly_account_summary.sql`: `_pipeline_run_id` included in output; grouping keys match Silver query predicates
- Verification: for any Gold row, a query against Silver using the Gold row's keys and `_pipeline_run_id` returns the records that sum to the Gold aggregate

---

### INV-T2 — Silver → Bronze Traceability

**Condition:** Every Silver transaction record must retain `_bronze_ingested_at` and `_source_file`, enabling traceability back to the Bronze record and its source partition.

**Category:** Audit

**Why this matters:** The middle link in the audit chain. Without it, Gold → Silver is traceable but Silver → Bronze is not — the chain is broken at the Silver layer boundary.

**Enforcement points:**
- `silver_transactions.sql`: carries forward `_bronze_ingested_at` and `_source_file` from Bronze; not recomputed
- Verification: no null `_source_file` or `_bronze_ingested_at` in Silver transactions

---

### INV-T3 — Bronze → Source Traceability

**Condition:** Every Bronze record must retain `_source_file`, containing the originating CSV filename, linking the Bronze record to the exact source file that produced it.

**Category:** Audit

**Why this matters:** The terminal link in the audit chain. Without it, the chain terminates at Bronze with no connection to the actual source file — making it impossible to verify or re-examine the raw data that entered the system.

**Enforcement points:**
- `bronze_loader.py`: `_source_file` populated with the CSV filename on every write
- Verification: `_source_file` value in Bronze matches the actual source filename for all records in the corresponding partition

---

## Error Handling Invariants

### INV-E1 — Error Sanitisation Enforcement

**Condition:** All error messages written to the run log must be sanitised to remove file paths, credentials, and internal system details before persistence.

**Category:** Operational / Security

**Why this matters:** Unsanitised error messages in an append-only run log create a permanent record of potentially sensitive internal detail (directory structures, credential fragments from environment variable errors). The run log is queryable by analysts — it is not a private system log.

**Enforcement points:**
- `pipeline.py`: single sanitisation function applied to all exceptions before `run_log` write — no module writes sanitised or unsanitised messages directly
- `pipeline.py`: dbt stderr output captured and sanitised before storage; not passed through raw

---

### INV-E2 — Failure Visibility

**Condition:** Every pipeline failure must produce a run log entry with:
- `status = FAILED`
- A non-null `error_message` (sanitised per INV-E1)

No failure may be silently swallowed without a run log record.

**Category:** Operational

**Why this matters:** A silent failure produces no run log entry and no watermark change (INV-C1 holds). The result is indistinguishable from a "no new data" state — the pipeline appears healthy while the failure goes undetected.

**Enforcement points:**
- `pipeline.py`: top-level exception handler catches all module and dbt subprocess failures; writes FAILED run log entry before re-raising or exiting
- No inner catch block may suppress an exception without writing a FAILED run log entry

---

## Invariant Index

| ID | Name | Category | Primary Enforcement |
|---|---|---|---|
| INV-B1 | Bronze Immutability and Idempotency | Data correctness | `bronze_loader.py` skip-on-exists |
| INV-B2 | Bronze Audit Columns Completeness | Audit | `bronze_loader.py` write path |
| INV-B3 | Raw Data Fidelity | Data correctness | `bronze_loader.py` schema contract |
| INV-S1 | No Silent Drops (Conservation Law) | Data correctness | `silver_transactions.sql` + `silver_quarantine.sql` |
| INV-S2 | Global Transaction ID Uniqueness | Data correctness | `silver_transactions.sql` cross-partition dedup |
| INV-S3 | Deduplication Correctness | Data correctness | `silver_transactions.sql` cross-partition scan |
| INV-S4 | Sign Assignment Correctness | Data correctness | `silver_transactions.sql` join path + Gold models |
| INV-S5 | Resolvability Flag Correctness | Data correctness | `silver_transactions.sql` LEFT JOIN to accounts |
| INV-S6 | Silver Audit Columns Completeness | Audit | `silver_transactions.sql` write path |
| INV-S7 | Quarantine Integrity | Audit | `silver_quarantine.sql` SELECT clause |
| INV-S8 | Accounts Upsert Correctness | Data correctness | `silver_accounts.sql` upsert logic |
| INV-S9 | Transaction Codes Sequencing | Operational | `pipeline.py` historical sequence |
| INV-G1 | Resolvability Filter Enforcement | Data correctness | Gold model WHERE clause position |
| INV-G2 | No Bronze Dependency | Architecture | dbt model lineage |
| INV-G3 | Full Rebuild and Deterministic Values | Data correctness | dbt `table` materialisation |
| INV-G4 | Aggregation Correctness | Data correctness | Gold model SUM on `_signed_amount` |
| INV-G5 | transactions_by_type STRUCT Completeness | Data correctness | `gold_daily_summary.sql` explicit STRUCT |
| INV-C1 | Watermark Advancement Gate | Operational | `pipeline.py` final-act advancement |
| INV-C2 | Sequential Progression and Clean Exit | Operational | `pipeline.py` file existence pre-check |
| INV-C3 | Run Log Append-Only and Write-on-Completion | Audit | `pipeline.py` single write on completion |
| INV-C4 | Run Log Completeness | Audit | `pipeline.py` post-invocation write |
| INV-C5 | Run Log Entry Uniqueness | Audit | `pipeline.py` one write per model per run |
| INV-C6 | Audit Chain Completeness | Audit | All layer write paths |
| INV-C7 | Control Table Single-Row Invariant | Operational | `pipeline.py` watermark overwrite |
| INV-C8 | Historical Pipeline Watermark Initialisation | Operational | `pipeline.py` historical completion path |
| INV-T1 | Gold → Silver Traceability | Audit | Gold model output columns |
| INV-T2 | Silver → Bronze Traceability | Audit | `silver_transactions.sql` carry-forward |
| INV-T3 | Bronze → Source Traceability | Audit | `bronze_loader.py` `_source_file` column |
| INV-E1 | Error Sanitisation Enforcement | Operational | `pipeline.py` sanitisation function |
| INV-E2 | Failure Visibility | Operational | `pipeline.py` top-level exception handler |

---

*PBVI Phase 2 complete. Engineer sign-off required before Phase 3 — Execution Planning begins.*
