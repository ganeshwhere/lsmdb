# Config Preflight

Use `lsmdb-admin` to validate config files before startup and print resolved runtime diagnostics.

## Command

```bash
cargo run --bin lsmdb-admin -- config check --config ./lsmdb.toml
```

## Output format

The command prints stable `key=value` lines to stdout:

```text
config.check=ok
config.path=./lsmdb.toml
storage.memtable_size_bytes=...
wal.segment_size_bytes=...
compaction.strategy=...
```

This output is intended to be parsed by CI pipelines and deployment scripts.

## Exit codes

- `0`: validation passed.
- `2`: config load/parse/validation failed.
- `64`: command usage error.

## Validation highlights

- Unknown fields are rejected (`deny_unknown_fields`).
- Numeric bounds are enforced (memtable sizing, WAL segment size, bloom FPR).
- Cross-field constraints are enforced:
  - `storage.memtable_arena_block_size_bytes <= storage.memtable_size_bytes`
  - `storage.flush_timeout_ms >= storage.flush_poll_interval_ms`
