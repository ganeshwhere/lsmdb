# Architecture

This document describes the current architecture implemented in `db/`.

## Scope and current reality

LSMDB currently has two major tracks:

1. Persistent storage engine track (`src/storage/*`)
- WAL, MemTable, SSTable, Manifest, and `StorageEngine`
- durable files and restart recovery
- graceful shutdown and checksum validation implemented

2. SQL execution track (`src/mvcc`, `src/catalog`, `src/sql`, `src/planner`, `src/executor`, `src/server`)
- SQL pipeline and transaction semantics
- currently backed by in-memory `MvccStore`
- not yet wired to persistent `StorageEngine`

This split is important for design decisions and debugging.

## Module map

- `src/storage/`
  - `wal/`: append log, segments, replay
  - `memtable/`: mutable + immutable in-memory ordered structures
  - `sstable/`: immutable sorted on-disk files, index/filter/footer
  - `manifest/`: version edit log for table lifecycle
  - `engine.rs`: integrated LSM write/read/flush runtime
  - `compaction/`: leveled/tiered planning and scheduler
- `src/mvcc/`
  - timestamp oracle, snapshots, transactions, GC worker
- `src/catalog/`
  - table/column schema descriptors and persisted catalog entries
- `src/sql/`
  - lexer, parser, AST, validator
- `src/planner/`
  - logical planning, rule rewrites, physical plan selection
- `src/executor/`
  - physical plan execution and transaction session behavior
- `src/server/`
  - TCP server and binary protocol framing
- `tools/lsmdb-cli/`
  - interactive client for server requests

## Storage engine data flow

### Write path (`StorageEngine`)

For each `put`/`delete`:
1. append operation to WAL
2. apply to mutable memtable
3. if memtable size threshold reached, promote to immutable
4. schedule immutable memtable flush in background thread
5. flush worker builds SSTable and returns result
6. engine applies manifest edit and updates in-memory table set

### Read path (`StorageEngine`)

Reads resolve newest value in this order:
1. mutable memtable
2. immutable memtables (newest first)
3. SSTables (newest first)

Internal keys encode:
- `user_key`
- `sequence` (big-endian `u64`)
- `value_type` (`Put` / `Delete`)

### Startup and recovery

On engine open:
1. read manifest and open existing SSTables
2. replay WAL segments
3. rebuild memtable state and recovered max sequence
4. start flush worker

### Shutdown

`StorageEngine::shutdown()`:
1. force flush mutable memtable
2. sync WAL
3. sync manifest
4. stop and join flush worker

`Drop` delegates to the same shutdown path.

## SQL request pipeline

For each query request in server:
1. parse SQL (`sql::parser`)
2. validate semantically (`sql::validator` against `Catalog`)
3. plan (`planner`)
4. execute (`executor::ExecutionSession`)
5. convert results to wire payload (`server::protocol`)

Server maintains per-connection session state (active transaction).

## Wire protocol (high-level)

Request types:
- `Query`
- `Begin`
- `Commit`
- `Rollback`
- `Explain`

Response payloads:
- rowset data
- affected row count
- transaction state
- explain text
- or error string

## Observability and config

- tracing instrumentation is enabled across major paths
- runtime options are loaded from `lsmdb.toml` via `src/config/mod.rs`
- key configurable knobs:
  - memtable sizing and flush timing
  - WAL segment size and sync mode
  - SSTable block/restart/bloom settings
  - compaction strategy and strategy parameters

## Known architectural gaps

- SQL execution durability gap: `MvccStore` is in-memory and not yet backed by `StorageEngine`
- compaction execution loop is not yet integrated end-to-end in engine runtime
- docs and README are being updated incrementally to match implementation
