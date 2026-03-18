# lsmdb

An LSM-tree relational database in Rust for write-heavy, single-node workloads with durable storage and MVCC semantics.

## Why lsmdb

`lsmdb` is designed for developers who want a modern storage architecture with a small, hackable Rust codebase.

- **Write-optimized architecture**: WAL + MemTable + SSTable pipeline minimizes random write cost on SSD.
- **Durability first**: WAL replay, manifest tracking, checksum validation, and recovery-focused tests.
- **MVCC transactions**: snapshot-isolation behavior with conflict detection.
- **SQL pipeline included**: parser, validator, planner, and executor for a practical SQL subset.
- **Flexible usage**: run embedded in a Rust service or expose over the built-in TCP server protocol.

## What it uses

- **Language/runtime**: Rust + Tokio
- **Storage stack**: WAL, MemTable, SSTable, Manifest, background flush/compaction
- **Concurrency/transactions**: MVCC (timestamps, snapshots, GC)
- **SQL stack**: lexer, parser, AST, validator, logical/physical planning, executor
- **Ops tooling**: CLI client, admin utility, diagnostics bundle, release gate checks

See:
- `docs/architecture.md`
- `docs/sql_subset.md`
- `docs/testing.md`
- `docs/release_gate.md`

## How this benefits developers

- **Simple local integration**: use embedded mode directly in Rust without external DB process.
- **Deterministic behavior**: explicit test coverage around recovery, MVCC, and SQL semantics.
- **Operational visibility**: health/readiness/admin status payloads and diagnostics bundle support.
- **Fast iteration**: modular code structure (`storage`, `mvcc`, `sql`, `planner`, `executor`, `server`).

## Current status

Core storage, SQL execution, and integration tests are implemented and actively exercised.

Known limitations for production adoption are tracked in `team_issues/` and release gate criteria in `docs/release_gate.md`.

## Use it now

### Option A: Embedded (recommended today)

Use `lsmdb` directly inside your Rust app:

```rust
use lsmdb::catalog::Catalog;
use lsmdb::executor::ExecutionSession;
use lsmdb::mvcc::MvccStore;

let store = MvccStore::open_persistent("./data")?;
let catalog = Catalog::open(store.clone())?;
let mut session = ExecutionSession::new(&catalog, &store);
// parse -> validate -> plan -> execute SQL using the session
```

Reference: `tests/sql_persistence.rs`.

### Option B: Server + CLI

`lsmdb` includes server components and a custom wire protocol (`src/server/*`) plus a CLI client (`tools/lsmdb-cli/`).

- Start a server from your application by calling `start_server_with_options(...)`.
- Connect using the CLI:

```bash
cargo run --bin lsmdb-cli -- --addr 127.0.0.1:7878
```

For authenticated/TLS modes, use `--user`/`--password` or `--token`, and optionally `--tls-ca-cert`.

> Note: PostgreSQL wire compatibility is not implemented yet. Existing Postgres drivers/ORMs will not work directly.

### Quickstart: run a local server now

Use the runnable example at `examples/server_quickstart.rs`.

1) Start server:

```bash
cargo run --example server_quickstart
```

2) Connect from another terminal:

```bash
cargo run --bin lsmdb-cli -- --addr 127.0.0.1:7878
```

The example uses `MvccStore::open_persistent("./data")`, so data is persisted in `./data` across restarts.

## Project layout

- `src/` core database code
- `tests/integration/` integration scenarios
- `tests/bench/` benchmark entry points
- `tools/lsmdb-cli/` interactive client
- `tools/lsmdb-admin/` config + diagnostics commands
- `docs/` architecture, SQL subset, testing, release gate

## Build and test

- library tests: `cargo test --lib --locked`
- integration suite gate: `./tools/ci/run_integration_tests.sh`
- full test run: `cargo test --locked`

## Operations and release readiness

- release readiness criteria: `docs/release_gate.md`
- local blocker check: `./tools/release/check_critical_blockers.sh <owner>/<repo>`
- CI release gate workflow: `.github/workflows/release-gate.yml`

## Collaborate

Contributions are welcome across storage, SQL, transactions, server protocol, testing, and operability.
