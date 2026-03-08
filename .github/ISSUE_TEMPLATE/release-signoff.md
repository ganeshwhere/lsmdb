---
name: Release Sign-Off
about: Production release readiness checklist and approvals
title: "release(signoff): vX.Y.Z readiness"
labels: ["area/release", "area/ops"]
assignees: []
---

## Release Metadata

- target version:
- target date:
- release owner:
- rollback owner:

## Automated Gate Evidence

- [ ] `Release Gate` workflow passed on target commit
- [ ] `cargo check --locked` passed
- [ ] `cargo test --lib --locked` passed
- [ ] `./tools/ci/run_integration_tests.sh` passed
- [ ] `./tools/release/check_critical_blockers.sh` passed

## Engineering Sign-Off

- [ ] Schema/storage compatibility risk reviewed
- [ ] Known high-severity defects triaged and dispositioned
- [ ] Upgrade notes completed
- [ ] Rollback procedure validated
- approver:
- approval date:

## Operations Sign-Off

- [ ] Runbook updated for this release
- [ ] Monitoring/alerts reviewed
- [ ] Capacity/performance risk reviewed
- [ ] Backup/restore posture reviewed
- approver:
- approval date:

## Decision

- [ ] APPROVED FOR RELEASE
- [ ] BLOCKED
- blocking notes:
