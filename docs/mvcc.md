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
  - in-memory committed version store
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
- otherwise allocates `commit_ts` and appends versions

5. Rollback / drop
- discard write buffer
- release snapshot pin

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

## Current limitation

`MvccStore` is currently an in-memory `HashMap`-backed version store.

Implication:
- SQL/server transaction behavior is correct for in-process semantics
- committed SQL data is not yet durable across process restart via the LSM storage engine
