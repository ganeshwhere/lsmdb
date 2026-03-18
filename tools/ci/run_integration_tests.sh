#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$repo_root"

missing=0
for file in tests/integration/*.rs; do
  rel_path="${file#./}"
  stem="$(basename "${file%.rs}")"
  target="integration_${stem}"

  if ! grep -Fq "name = \"$target\"" Cargo.toml; then
    echo "missing integration test target registration: $target ($rel_path)"
    missing=1
  fi

  if ! grep -Fq "path = \"$rel_path\"" Cargo.toml; then
    echo "missing integration test path registration: $rel_path"
    missing=1
  fi
done

if [[ "$missing" -ne 0 ]]; then
  exit 1
fi

cargo test --tests --locked
