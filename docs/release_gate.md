# Release Gate

This document defines the production-readiness gate for `lsmdb`. A release is blocked if any
critical gate check fails.

## Automated Gate

The release gate workflow (`.github/workflows/release-gate.yml`) enforces the following checks:

| Category | Check | Pass Condition |
| --- | --- | --- |
| Build | `cargo check --locked` | Succeeds |
| Reliability | `cargo test --lib --locked` | Succeeds |
| Reliability | `./tools/ci/run_integration_tests.sh` | Succeeds |
| Product Risk | `./tools/release/check_critical_blockers.sh` | No open critical `priority/high` issues |

Critical blocking areas are currently:

- `area/security`
- `area/recovery`
- `area/release`
- `area/ops`
- `area/server`
- `area/storage`
- `area/performance`

Any open issue with `priority/high` and one of these area labels blocks release.

## Manual Gate

Manual sign-off is required in addition to automated checks.

- Engineering sign-off is required.
- Operations sign-off is required.
- Release notes and upgrade notes must be updated.
- Rollback plan must be documented.

Use `.github/ISSUE_TEMPLATE/release-signoff.md` for release sign-off records.

## Readiness Status

Quick local check:

```bash
./tools/release/check_critical_blockers.sh ganeshwhere/lsmdb
```

CI check:

- Run the `Release Gate` workflow manually (`workflow_dispatch`) before creating a release tag.
- Any failure in that workflow means release readiness is `NOT READY`.
