# MVCC

This document describes the MVCC layer in `src/mvcc`.

## Model

The MVCC layer implements snapshot isolation semantics over versioned keys.

Each committed key version stores:
- `commit_ts: u64`
- `value: Option<Vec<u8>>` (`None` = tombstone/delete)

Transactions read at a fixed `read_ts` and stage writes locally until commit.

## Core components

- `timestamp.rs` (`TimestampOracle`)
  - monotonic timestamp source
  - concurrent-safe `next_timestamp()`
- `snapshot.rs` (`SnapshotRegistry`, `Snapshot`)
  - tracks pinned read timestamps
  - reports oldest active snapshot and active snapshot count
- `transaction.rs` (`MvccStore`, `Transaction`)
  - MVCC committed version store with optional durable backing
  - transaction API + conflict checks + metrics
- `gc.rs`
  - prune obsolete versions behind snapshot watermark
  - optional background worker loop

## Transaction lifecycle

1. `begin_transaction()`
- captures `read_ts = oracle.current()`
- pins snapshot in registry

2. Reads
- check local write buffer first
- otherwise return latest committed version with `commit_ts <= read_ts`

3. Writes
- buffered in transaction-local map (`BTreeMap<Vec<u8>, Option<Vec<u8>>>`)

4. Commit
- if write set empty, returns `read_ts`
- detects write-write conflicts:
  - for each written key, if latest committed `commit_ts > read_ts`, abort with conflict error
- otherwise allocates `commit_ts`, persists durable state (durable mode), then acknowledges commit

5. Rollback / drop
- discard write buffer
- release snapshot pin

## Commit durability contract

Durable mode defines two explicit points:

- Durability point:
  - committed version state is written to `StorageEngine`.
  - after this point, committed data must survive restart.
- Visibility / acknowledgment point:
  - `commit()` returns success to caller.
  - in-process metrics (`committed`) are incremented.

Crash behavior:

- Crash before durability point:
  - transaction may be retried; data is not guaranteed persisted.
- Crash after durability point but before acknowledgment:
  - data is recovered and visible after restart.
  - client may treat commit outcome as unknown and handle idempotently.
- Rollback or uncommitted transaction:
  - staged writes are never persisted and are absent after restart.

## Conflict detection

Current policy:
- write-write conflict only
- first writer to commit wins
- later overlapping writer aborts if it observed an older snapshot

## Garbage collection

Watermark:
- `oldest_active_snapshot_timestamp()`, else current timestamp

Pruning rule:
- keep newest visible version boundary
- remove older versions when safe with respect to watermark

`GcWorker`:
- periodic `run_gc_once()` loop on background thread
- tracks last removed-version count

## Metrics

`TransactionMetrics` includes:
- started
- committed
- rolled_back
- write_conflicts
- active_transactions
- recovered_keys
- recovered_versions

## Durability modes

`MvccStore` supports two modes:

- In-memory mode (`MvccStore::new()`)
  - Uses only in-process `HashMap` state.
  - Best for unit tests and fast local execution.
- Durable mode (`MvccStore::open_persistent*()` / `MvccStore::with_storage_engine()`)
  - Persists committed MVCC state into `StorageEngine` under an internal key.
  - Reloads committed versions and oracle position on restart.
  - Allows SQL/catalog state to survive process restarts while preserving MVCC semantics.
