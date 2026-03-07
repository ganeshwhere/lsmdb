# SQL Conformance Suite

The SQL conformance suite verifies parser, validator, planner, and executor behavior against the supported subset documented in `docs/sql_subset.md`.

## Run

```bash
cargo test --test sql_conformance
```

## Fixtures

Fixture files live in:

- `tests/conformance/sql/01-core-supported.toml`
- `tests/conformance/sql/02-errors-and-boundaries.toml`

Each case includes:

- `id`: stable case identifier.
- `category`: compatibility category.
- `setup_sql`: optional setup statements executed before the case statement.
- `sql`: statement under test.
- `expect`: expected outcome (`affected_rows`, `query`, `transaction_state`, `error_contains`).

## Compatibility report artifact

The test emits a machine-readable report at:

- `target/sql-conformance/report.toml`

Report fields include:

- `schema_version`
- `generated_unix_seconds`
- `total_suites`, `total_cases`, `passed_cases`, `failed_cases`
- `covered_categories`
- `suite_summaries`
- `failures` (detailed per-case diagnostics)

This artifact is intended for release checks and compatibility tracking.
