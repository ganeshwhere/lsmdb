# Diagnostics Bundle

Use `lsmdb-admin` to generate a support bundle for incident triage.

## Command

```bash
cargo run --bin lsmdb-admin -- diagnostics bundle \
  --config ./lsmdb.toml \
  --engine-root ./data \
  --output-dir ./diagnostics \
  --log-dir ./logs
```

## Bundle contents

A timestamped bundle directory is created under `--output-dir` with:

- `build_info.kv`: build and environment metadata.
- `config.redacted.toml`: redacted config snapshot.
- `startup_diagnostics.kv`: resolved runtime config diagnostics (if config loads).
- `storage_snapshot.kv`: WAL/SST/manifest file counts and bytes.
- `logs/`: redacted log captures (optional, size-limited).
- `bundle_manifest.kv`: manifest metadata with version and warning list.
- `checksums.crc32`: integrity checksums for bundle files.

## Redaction policy

Lines with keys matching these tokens are redacted:

- `password`
- `secret`
- `token`
- `credential`
- `api_key`
- `private_key`
- `access_key`

## Exit codes

- `0`: bundle generated.
- `2`: command failed.
- `64`: usage error.
