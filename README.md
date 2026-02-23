# lsmdb

An LSM-tree based relational database built in Rust.

## Current Status

- Initial project skeleton is in place.
- Storage modules are scaffolded (WAL, MemTable, SSTable, Manifest, Compaction, Engine).
- Transaction modules are scaffolded (MVCC).
- Relational modules are scaffolded (Catalog, SQL, Planner, Executor).
- Server and CLI modules are scaffolded.
- Docs, integration test files, and benchmark entry points are scaffolded.
- Core engine behavior is not fully implemented yet.

## Layout

- `src/` database core library code
- `tests/integration/` end-to-end and subsystem integration tests
- `tests/bench/` benchmark entry points
- `tools/lsmdb-cli/` CLI client
- `docs/` architecture and component docs

## Collaborate

You can collaborate on this repository to help build a production-capable database.
Feature contributions are welcome across storage, transactions, SQL, performance, and tooling.

## Target Features

- Durable WAL with crash recovery
- MemTable flush pipeline to SSTables
- Leveled and tiered compaction
- MVCC snapshot isolation
- SQL subset with planning and execution
- Network server and interactive CLI
- Benchmarks and observability
