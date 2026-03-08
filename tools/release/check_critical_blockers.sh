#!/usr/bin/env bash
set -euo pipefail

repo="${1:-${GITHUB_REPOSITORY:-}}"
if [[ -z "$repo" ]]; then
  echo "error: repository not provided. Pass '<owner>/<repo>' or set GITHUB_REPOSITORY."
  exit 2
fi

token="${GITHUB_TOKEN:-${GH_TOKEN:-}}"
if [[ -z "$token" ]] && command -v gh >/dev/null 2>&1; then
  token="$(gh auth token 2>/dev/null || true)"
fi
if [[ -z "$token" ]]; then
  echo "error: missing token. Set GITHUB_TOKEN/GH_TOKEN or authenticate with gh."
  exit 2
fi

fetch_open_high_priority_issues() {
  local page=1
  local all="[]"
  while :; do
    local url="https://api.github.com/repos/${repo}/issues?state=open&labels=priority/high&per_page=100&page=${page}"
    local response
    response="$(
      curl -fsSL \
        -H "Accept: application/vnd.github+json" \
        -H "Authorization: Bearer ${token}" \
        "$url"
    )"
    local count
    count="$(echo "$response" | jq 'length')"
    if [[ "$count" -eq 0 ]]; then
      break
    fi

    all="$(jq -s 'add' <(echo "$all") <(echo "$response"))"
    page=$((page + 1))
  done
  echo "$all"
}

all_open_high_priority="$(fetch_open_high_priority_issues)"

critical_blockers="$(
  echo "$all_open_high_priority" | jq '[
    .[]
    | select(has("pull_request") | not)
    | select(
        ([.labels[].name] | index("area/security")) != null
        or ([.labels[].name] | index("area/recovery")) != null
        or ([.labels[].name] | index("area/release")) != null
        or ([.labels[].name] | index("area/ops")) != null
        or ([.labels[].name] | index("area/server")) != null
        or ([.labels[].name] | index("area/storage")) != null
        or ([.labels[].name] | index("area/performance")) != null
      )
    | {
        number: .number,
        title: .title,
        url: .html_url,
        labels: [.labels[].name]
      }
  ]'
)"

blocker_count="$(echo "$critical_blockers" | jq 'length')"

if [[ -n "${GITHUB_STEP_SUMMARY:-}" ]]; then
  {
    echo "### Release Gate: Critical Open Issues"
    echo
    if [[ "$blocker_count" -eq 0 ]]; then
      echo "- status: PASS"
      echo "- critical open issues: 0"
    else
      echo "- status: FAIL"
      echo "- critical open issues: $blocker_count"
      echo
      echo "| Issue | Title | Labels |"
      echo "| --- | --- | --- |"
      echo "$critical_blockers" | jq -r '.[] | "| [#\(.number)](\(.url)) | \(.title) | \(.labels | join(", ")) |"'
    fi
  } >>"$GITHUB_STEP_SUMMARY"
fi

if [[ "$blocker_count" -ne 0 ]]; then
  echo "release gate failed: found $blocker_count critical high-priority open issue(s)."
  echo "$critical_blockers" | jq -r '.[] | "- #\(.number): \(.title) (\(.url))"'
  exit 1
fi

echo "release gate passed: no critical high-priority open issues."
