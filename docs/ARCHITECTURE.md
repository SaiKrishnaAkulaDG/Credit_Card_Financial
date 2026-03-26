# ARCHITECTURE.md

**Credit Card Financial Transactions Lake**
*Version 1.0 · PBVI Phase 1 Output · Classification: Training Demo System*

**Selected Architecture: B — Coordinator + Modules**

---

## 1. Problem Framing

### What the system solves

Analysts and risk teams are making decisions from raw CSV extracts with no quality control, no version consistency, and no audit trail. The underlying need is decision confidence with traceability: given any Gold aggregate, a user must be able to trace it back to the Silver records that produced it, the Bronze records that fed Silver, and the source file those Bronze records came from. The chain must be unbroken and queryable.

A secondary need is operational consistency: a single authoritative Silver layer with governed promotion rules eliminates analyst divergence caused by working from different file versions.

### What the system explicitly does not solve

- Risk computation or credit decisioning
- Modification of source system records
- Resolution of unresolvable account references (backfill — out of scope)
- SCD Type 2 account history — Silver holds only the latest record per account
- Streaming or near-realtime ingestion — batch only
- Schema evolution — CSV schema is fixed for this exercise
- Production deployment, monitoring, or alerting infrastructure

---

## 2. Key Design Decisions

### DD1 — Architecture B: Coordinator + Discrete Layer Modules

| Aspect | Detail |
|---|---|
| Decision | pipeline.py acts as a thin coordinator. Bronze ingestion is a discrete module (bronze_loader.py). Silver and Gold are dbt models. The coordinator owns the control table, run log writes, and watermark advancement exclusively. |
| Rationale | Engineer selection: correctness by design, safe failure handling, independent testability, clean separation of responsibility, single place for control logic. |
| Alternative rejected | Architecture A (single script): IC2 watermark gate is convention-dependent, not structurally enforced. A try/catch gap advances the watermark on partial success. |
| Alternative rejected | Architecture C (dbt-centric): IC2 and IC11 are structurally difficult. dbt exit codes are unreliable per-layer. Run log write-on-completion requires non-trivial custom macros. |

### DD2 — Watermark Advancement is the Last Act of the Coordinator

| Aspect | Detail |
|---|---|
| Decision | The coordinator advances the watermark only after all three layer modules (Bronze, Silver, Gold) return a success signal. The advancement is the final write of any pipeline run. |
| Rationale | IC2: watermark must never advance past the last successfully completed full pipeline run. Structural enforcement — the coordinator cannot reach the advancement call if any module raised an exception. |
| Challenge | A module could return success falsely (swallowed exception). Rejected: modules are required to propagate exceptions upward. The coordinator catches at the top level only — no inner catches that could mask failure. |

### DD3 — File Existence is a Pre-Pipeline Gate, Not a Bronze Concern

| Aspect | Detail |
|---|---|
| Decision | The coordinator checks for the existence of the transactions file for watermark+1 before invoking any layer module. A missing file causes a clean exit with no watermark change and no run log entry. |
| Rationale | IC9 (MI1 resolved): a missing file is not a failure — it is a "no new data" state. Treating it as a Bronze failure would generate false error signals in a scheduled environment. |
| Challenge | Missing accounts file is a different case — it means no account changes, not a missing day. The existence check must distinguish between the two files. Resolved: accounts file absence = zero-row promotion; transactions file absence = pipeline exit. |

### DD4 — Bronze Partitions are Immutable; Idempotency via Skip

| Aspect | Detail |
|---|---|
| Decision | Bronze loader checks whether a partition already exists before writing. If it exists, the loader skips and returns success. It never overwrites an existing partition. |
| Rationale | C4 (Bronze partitions never overwritten). IC4: overwriting would destroy the immutability guarantee even with identical content. Idempotency is enforced at the partition level, not by downstream deduplication. |
| Challenge | A skipped Bronze partition still needs a run log entry to show the date was processed. Resolved: loader returns a SKIPPED status for existing partitions; coordinator writes a SKIPPED run log row. |

### DD5 — Sign Assignment is a Join-Dependent Silver Transformation

| Aspect | Detail |
|---|---|
| Decision | `_signed_amount` is computed in the Silver transactions dbt model by joining to Silver transaction_codes on `transaction_code` and reading `debit_credit_indicator`. DR = source amount (positive). CR = source amount × -1. No other logic applies. |
| Rationale | C10: the pipeline never applies sign logic using its own rules. IC12: a failed join is an INVALID_TRANSACTION_CODE rejection — not a null, not a default. IC5: sign is applied exactly once. |
| Challenge | Silver transaction_codes must be loaded before any transaction Silver promotion runs. Resolved: coordinator sequences transaction_codes Silver load as the first dbt invocation on historical pipeline initialisation. |

### DD6 — Gold is Fully Rebuilt on Every Run

| Aspect | Detail |
|---|---|
| Decision | Both Gold output files (daily_summary, weekly_account_summary) are truncated and rewritten on each pipeline run. No append, no merge, no deduplication key required. |
| Rationale | IC10 (MI4 resolved): full replacement guarantees idempotency without requiring a composite key definition. Gold is always a pure projection of current Silver state. |
| Challenge | If Silver grows large, full Gold recomputation is expensive. Accepted: out of scope for this exercise. Documented as a known production pattern requiring incremental materialisation. |

### DD7 — Error Sanitisation is Owned Exclusively by the Coordinator

| Aspect | Detail |
|---|---|
| Decision | pipeline.py catches all exceptions from layer modules and dbt subprocess calls, sanitises them (removes file paths, credentials, internal detail), and writes the cleaned message to the run log. No other component writes to the run log directly. |
| Rationale | IC11 (MI5 resolved): single sanitisation point prevents inconsistent or leaking error messages. Modules surface raw exceptions upward; the coordinator owns the scrub before persistence. |
| Challenge | dbt subprocess failures return stderr output that may contain path detail. Resolved: coordinator captures stderr, applies a sanitisation function, and stores only the cleaned string. |

### DD8 — Run Log is Write-on-Completion Only

| Aspect | Detail |
|---|---|
| Decision | The coordinator writes a run log row only after a model completes (SUCCESS, FAILED, or SKIPPED). No partial rows. A gap in the run log for a given model is the observable signal that the process was killed mid-execution. |
| Rationale | MI7 resolved. IC6: the run log is the audit trail — overwriting or partial rows destroy its evidential value. A gap is diagnosable; a partial row with stale timestamps is not. |
| Challenge | A gap makes the run log non-trivially queryable (must check for absence). Accepted: the run log schema and verification queries in Section 10 account for this. |

### DD9 — UNRESOLVABLE_ACCOUNT_ID is a Flag, Not a Quarantine

| Aspect | Detail |
|---|---|
| Decision | Transactions referencing an account_id not found in Silver Accounts at promotion time are written to Silver with `_is_resolvable = false`. They are excluded from Gold aggregations. They are not quarantined. |
| Rationale | Deliberate brief requirement (Section 5.1). A missing account may be a timing issue, not a data error. Quarantining it would permanently exclude potentially valid transactions since backfill is out of scope. |
| Challenge | `_is_resolvable = false` records accumulate silently, understating Gold. Resolved: the count of `_is_resolvable = false` records must be surfaced as a queryable metric — not hidden in implementation detail (FM3). |

### DD10 — transactions_by_type STRUCT Always Emits All Five Types

| Aspect | Detail |
|---|---|
| Decision | The Gold daily_summary `transactions_by_type` STRUCT always contains all five keys: PURCHASE, PAYMENT, FEE, INTEREST, REFUND. Types with no transactions on a given day emit count=0 and sum=0.00. |
| Rationale | MI6 resolved. A fixed-schema STRUCT is queryable without conditional logic. Sparse keys (only types present that day) would produce inconsistent query behaviour in DuckDB across days. |
| Challenge | Zero-filling requires explicit STRUCT construction in the dbt model rather than a simple GROUP BY. Accepted: the additional complexity is bounded and one-time. |

---

## 3. Challenges to Key Decisions

| Decision | Strongest Argument Against | Verdict |
|---|---|---|
| DD1 — Coordinator + Modules | Architecture A is simpler — fewer files, one execution path, easier for a new engineer to follow without knowing the module boundary contract. | Rejected. Simplicity that depends on try/catch discipline is not simplicity — it is hidden fragility. IC2 being convention-dependent in A means a single missed catch corrupts the watermark permanently. The module boundary is a one-time documentation cost; watermark corruption is a data integrity failure. |
| DD2 — Watermark last | Advancing the watermark before Gold means Gold recomputation failures don't block Silver from being queryable. Analysts could use Silver directly on Gold failure. | Rejected. Gold failure with an advanced watermark means the failed date is never reprocessed. Analysts querying Silver directly bypasses the entire governed layer structure the system exists to provide. The brief is explicit: all three layers must complete. |
| DD3 — File existence pre-check | Checking file existence in the coordinator adds a file system dependency before any layer logic. A race condition (file appears after check) could cause a false skip. | Accepted as valid but low-risk. The pipeline runs on a daily batch schedule with static CSV files — race conditions are not a realistic concern. If they were, the pre-check would move to the Bronze loader. Documented as a known limitation. |
| DD6 — Full Gold rebuild | Full rebuild is O(n) on all Silver data on every run. As Silver grows, Gold recomputation time grows unboundedly. Incremental Gold materialisation is the production pattern. | Accepted. Out of scope for this exercise. Documented in Future Enhancements. The brief's seed data is seven days — full rebuild cost is negligible at exercise scale. |
| DD9 — Flag not quarantine | Flagging unresolvable accounts in Silver rather than quarantine means Silver contains records that will never contribute to Gold. Silver's claim to be "clean and validated" is weakened. | Rejected as framing. Silver's quality guarantee is that every record passed all checkable rules at promotion time. Account resolvability is time-dependent — the account may arrive in a later delta. Quarantining it would be a permanent exclusion for a potentially temporary gap. The brief explicitly chose flag-not-quarantine for this reason. |

---

## 4. Key Risks

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| FM1 — Watermark drift: pipeline records success that did not happen | Low (structurally mitigated by DD2) | Critical — silent data gap in Gold | Coordinator advances watermark only as final act. Exception propagation from all modules is non-negotiable. |
| FM2 — Idempotency failure on Bronze re-run | Low (mitigated by DD4 skip-on-partition) | High — Bronze row counts double; audit trail fails even if Gold is correct | Bronze loader checks partition existence before any write. Verified by row count checks in Section 10.4. |
| FM3 — Resolvable account gap silently understates Gold | Medium — depends on accounts delta timing | High — Gold aggregates are incomplete with no visible warning | `_is_resolvable = false` count must be surfaced as a first-class queryable metric. Not an implementation detail. |
| FM4 — Sign corruption via double-promotion | Low (Silver deduplication is cross-partition) | High — Gold total_signed_amount wrong with no row count signal | Silver deduplication checks transaction_id across ALL existing partitions before promoting any record from current batch. |

---

## 5. Key Assumptions

- Source CSV files are always well-formed UTF-8. Encoding errors in source files are out of scope.
- The Docker container has read access to the `source/` directory and read-write access to the data lake directory.
- DuckDB can handle the exercise-scale data volume (7 days of seed data) within a single embedded process without memory pressure.
- The scaffold repository's directory layout and file naming conventions are fixed and correct — no pipeline logic derives paths dynamically beyond the defined patterns.
- The pipeline runs in a single-process, single-threaded context. Concurrent pipeline invocations are not a concern at exercise scale.

---

## 6. Accepted Limitations (Documented)

| Limitation | What it costs | Documented location |
|---|---|---|
| SCD Type 1 for Accounts (no history) | Cannot reconstruct account credit limit or status on a historical date. `closing_balance` in weekly Gold always reflects latest available balance, not week-specific balance. | Gold weekly_account_summary schema comment + this document |
| No backfill pipeline | `_is_resolvable = false` records are permanently excluded from Gold for this exercise. No mechanism to reprocess them if the account arrives later. | Section 9 of requirements brief + this document |
| Full Gold rebuild on every run | O(n) on all Silver data per run. Acceptable at exercise scale; would require incremental materialisation in production. | Future Enhancements below |
| Transaction codes treated as static | No mechanism to add or modify transaction codes during pipeline operation. A new code in a transaction file would trigger INVALID_TRANSACTION_CODE rejections. | Section 2.2 of requirements brief |

---

## 7. Future Enhancements (Parking Lot)

| Enhancement | Why deferred | What it would require |
|---|---|---|
| Incremental Gold materialisation | Out of scope at exercise scale. Full rebuild is correct and simple. | Composite deduplication key per Gold table. Merge-on-write or partition-level replacement strategy. |
| SCD Type 2 for Accounts | Explicitly out of scope in brief. Adds significant model complexity. | Surrogate key, effective_from/effective_to dates, current_flag. Separate historical and current views. |
| Backfill pipeline for unresolvable accounts | Out of scope. Requires watermark guard logic to prevent future-date processing. | Dedicated pipeline with date-range parameter, `_is_resolvable` re-evaluation, and Gold recomputation. |
| Transaction code dimension changes | Treated as static for exercise. Production would need a governed change process. | Versioned reference table, effective dates, Silver re-promotion for affected transactions. |
| Concurrent pipeline safety | Single-process assumption holds for exercise scale. | File locking or process mutex on control table writes. |

---

## 8. Component Architecture

### 8.1 Components and Responsibilities

| Component | Layer | Technology | Responsibility |
|---|---|---|---|
| pipeline.py | Coordinator | Python 3.11 | Orchestration, file existence pre-check, module invocation sequencing, run log writes, watermark advancement, error sanitisation |
| bronze_loader.py | Bronze | Python + DuckDB | Read source CSV, add audit columns (`_source_file`, `_ingested_at`, `_pipeline_run_id`), write Parquet partition, skip if partition exists |
| silver_transactions.sql | Silver | dbt model | Deduplicate vs all existing partitions, validate quality rules, join to transaction_codes for sign assignment, write resolvable records to Silver, rejections to quarantine |
| silver_accounts.sql | Silver | dbt model | Upsert account delta records — replace existing account_id on conflict, insert new accounts |
| silver_transaction_codes.sql | Silver | dbt model | Load once from Bronze reference file. Historical pipeline only. |
| silver_quarantine.sql | Silver | dbt model | Receive rejected records from silver_transactions and silver_accounts with rejection reason codes |
| gold_daily_summary.sql | Gold | dbt model | Full rebuild: one row per transaction_date from Silver where `_is_resolvable = true`. Fixed STRUCT for transactions_by_type. |
| gold_weekly_account_summary.sql | Gold | dbt model | Full rebuild: one row per (week_start_date, account_id) from Silver where `_is_resolvable = true`. Join to Silver Accounts for closing_balance. |
| pipeline/control.parquet | Control | Parquet file | Watermark: last_processed_date, updated_at, updated_by_run_id |
| pipeline/run_log.parquet | Control | Parquet file | Append-only execution log. One row per model per run. Write-on-completion only. |

### 8.2 Execution Sequence — Historical Pipeline

| Step | Actor | Action | Guard |
|---|---|---|---|
| 1 | pipeline.py | Generate run_id, record pipeline start | — |
| 2 | pipeline.py | Invoke bronze_loader for transaction_codes | — |
| 3 | dbt | Run silver_transaction_codes model | Must complete before any transaction Silver promotion |
| 4..N | pipeline.py | For each date in range: Bronze accounts → Bronze transactions → Silver accounts → Silver transactions → Silver quarantine | Date order enforced. Watermark not advanced until all dates complete. |
| N+1 | dbt | Run gold_daily_summary (full rebuild) | All Silver transactions loaded |
| N+2 | dbt | Run gold_weekly_account_summary (full rebuild) | All Silver transactions loaded |
| N+3 | pipeline.py | Advance watermark to end_date | Only reached if all prior steps succeeded |
| N+4 | pipeline.py | Write run log rows for all models | Write-on-completion — final act |

### 8.3 Execution Sequence — Incremental Pipeline

| Step | Actor | Action | Guard |
|---|---|---|---|
| 1 | pipeline.py | Read watermark from control.parquet | — |
| 2 | pipeline.py | Determine next_date = watermark + 1 day | — |
| 3 | pipeline.py | Check transactions file exists for next_date | Missing → clean exit, no watermark change, no run log entry |
| 4 | bronze_loader.py | Load accounts delta (or no-op if file absent) | File absent = zero-row promotion (not an error) |
| 5 | bronze_loader.py | Load transactions for next_date | Partition exists → SKIPPED status returned |
| 6 | dbt | Run silver_accounts | — |
| 7 | dbt | Run silver_transactions + silver_quarantine | — |
| 8 | dbt | Run gold_daily_summary (full rebuild) | — |
| 9 | dbt | Run gold_weekly_account_summary (full rebuild) | — |
| 10 | pipeline.py | Advance watermark to next_date | Only reached if steps 4–9 all succeeded |
| 11 | pipeline.py | Write run log rows for all models in this run | Write-on-completion only |

---

## 9. Data Model — First-Class Entities

| Entity | What it represents | Layer | Grain | Key |
|---|---|---|---|---|
| bronze/transactions | Exact copy of daily transaction CSV + audit columns | Bronze | One row per source record per day partition | (_source_file, row position — no deduplication) |
| bronze/accounts | Exact copy of daily accounts delta CSV + audit columns | Bronze | One row per source record per day partition | — |
| bronze/transaction_codes | Exact copy of transaction_codes.csv + audit columns | Bronze | One row per transaction code | — |
| silver/transactions | Validated, signed transactions promoted from Bronze | Silver | One row per unique transaction_id across all date partitions | transaction_id (globally unique enforced) |
| silver/accounts | Latest record per account_id | Silver | One row per account_id (SCD Type 1) | account_id |
| silver/transaction_codes | Reference table for sign assignment and type validation | Silver | One row per transaction_code | transaction_code |
| silver/quarantine | Records rejected during Silver promotion with reason codes | Silver | One row per rejected source record per day partition | — |
| gold/daily_summary | Aggregated transaction metrics per calendar day | Gold | One row per transaction_date | transaction_date |
| gold/weekly_account_summary | Per-account weekly transaction aggregates | Gold | One row per (week_start_date, account_id) | (week_start_date, account_id) |
| pipeline/control | Watermark and pipeline state | Control | Single row | — |
| pipeline/run_log | Execution metadata — append-only | Control | One row per model per pipeline run | (run_id, model_name) |

---

## 10. Constraint Traceability

| Constraint | Type | Enforced by |
|---|---|---|
| C1 — Never modify/delete source CSV files | Stated | Bronze loader opens files read-only. No component has a write path to source/ |
| C2 — No external service calls | Stated | All data is local. Docker Compose has no outbound network config. |
| C3 — No database server | Stated | DuckDB runs embedded in Python/dbt processes only. |
| C4 — Bronze partitions never overwritten | Stated | DD4: bronze_loader.py checks partition existence, skips if present. |
| C5 — Gold only from Silver | Stated | dbt model lineage: Gold models reference silver/ sources only. |
| C6 — dbt for Silver and Gold | Stated | Architecture enforces this boundary. Bronze loader is Python only. |
| C7 — Python+DuckDB for Bronze | Stated | bronze_loader.py is a standalone Python module. dbt has no Bronze models. |
| C8 — All outputs are Parquet | Stated | DuckDB COPY TO parquet in all write paths. No intermediate databases. |
| C9 — Watermark never advances past last successful full run | Stated | DD2: watermark advancement is the final act of the coordinator after all modules succeed. |
| C10 — Sign logic from Transaction Codes only | Stated | DD5: _signed_amount computed by join to silver_transaction_codes in dbt model only. |
| C11 — No risk, credit, or source modification | Stated | System scope. No model computes risk scores or writes to source/. |
| IC1 — No silent record drops | Implied | Every rejected record lands in quarantine via silver_quarantine model with a non-null _rejection_reason. |
| IC2 — Watermark gate: partial success is not success | Implied | DD2: structural enforcement. Coordinator cannot reach advancement call if any module raises. |
| IC3 — No in-pipeline quarantine rescue | Implied | No model has a path from quarantine back to Silver. Backfill is explicitly out of scope. |
| IC4 — Bronze skip, not overwrite | Implied | DD4. |
| IC5 — Sign applied exactly once | Implied | DD5: _signed_amount exists only in Silver. Gold reads it; does not recompute. |
| IC6 — Run log append-only | Implied | DD8: coordinator appends rows. No model truncates or rewrites run_log.parquet. |
| IC7 — _is_resolvable=false excluded from Gold | Implied | Gold dbt models filter WHERE _is_resolvable = true. |
| IC8 — No future-date processing | Implied | DD3: file existence pre-check. Incremental processes watermark+1 only if file exists. |
| IC9 — File existence is pre-pipeline gate | Resolved (MI1) | DD3. |
| IC10 — Gold fully replaced each run | Resolved (MI4) | DD6: dbt models use full table materialisation (truncate + insert). |
| IC11 — Sanitisation owned by pipeline.py | Resolved (MI5) | DD7: modules raise exceptions; coordinator catches, sanitises, writes to run log. |
| IC12 — Failed sign join = rejection not null | From CL2 | silver_transactions: LEFT JOIN to transaction_codes; NULL join result = INVALID_TRANSACTION_CODE quarantine. |
| IC13 — Bronze partition path is a fixed contract | Surfaced in Explore | Path structure defined in requirements brief Section 4.1. All components reference the same constants. |

---

## 11. Open Questions

None. All missing information items from Interrogate are resolved. All Explore-surfaced gaps are traced. This document is the authoritative basis for Phase 2 Invariant Definition.

---

*PBVI Phase 1 complete. Engineer selection recorded. Proceed to Phase 2 — Invariant Definition.*
