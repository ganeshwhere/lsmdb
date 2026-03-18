# Testing

This project splits test coverage into:

- library/unit tests in `src/**`
- integration tests in `tests/integration/*.rs`
- root-level conformance and persistence tests in `tests/*.rs`

## Commands

Run library tests:

```bash
cargo test --lib --locked
```

Run integration suite with target-registration checks:

```bash
./tools/ci/run_integration_tests.sh
```

Run full local test pass:

```bash
cargo test --locked
```

## Integration target policy

Files under `tests/integration/` are intentionally registered explicitly in `Cargo.toml` as `[[test]]` targets (`integration_*` naming).

`./tools/ci/run_integration_tests.sh` enforces that every integration file is registered, so new integration tests cannot be silently skipped by CI.
