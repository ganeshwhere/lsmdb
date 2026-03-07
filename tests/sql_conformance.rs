use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use lsmdb::catalog::Catalog;
use lsmdb::executor::{ExecutionResult, ExecutionSession, ScalarValue};
use lsmdb::mvcc::MvccStore;
use lsmdb::planner::plan_statement;
use lsmdb::sql::{parse_statement, validate_statement};
use serde::{Deserialize, Serialize};

const FIXTURE_DIR: &str = "tests/conformance/sql";
const REPORT_RELATIVE_PATH: &str = "sql-conformance/report.toml";
const REPORT_SCHEMA_VERSION: u32 = 1;

const REQUIRED_CATEGORIES: &[&str] = &[
    "ddl.create_table",
    "ddl.drop_table",
    "dml.insert",
    "query.select",
    "dml.update",
    "dml.delete",
    "txn.begin",
    "txn.commit",
    "txn.rollback",
    "errors.invalid_where_type",
    "errors.unsupported_join",
    "errors.ddl_in_explicit_txn",
];

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct ConformanceSuite {
    suite_id: String,
    description: String,
    cases: Vec<ConformanceCase>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct ConformanceCase {
    id: String,
    category: String,
    #[serde(default)]
    tags: Vec<String>,
    #[serde(default)]
    setup_sql: Vec<String>,
    sql: String,
    expect: Expectation,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum Expectation {
    AffectedRows { count: u64 },
    Query { columns: Vec<String>, rows: Vec<Vec<String>> },
    TransactionState { state: String },
    ErrorContains { message: String },
}

#[derive(Debug, Clone)]
struct CaseOutcome {
    passed: bool,
    details: String,
}

#[derive(Debug, Clone, Default)]
struct SuiteStats {
    description: String,
    total_cases: usize,
    passed_cases: usize,
    failed_cases: usize,
}

#[derive(Debug, Clone, Serialize)]
struct ConformanceReport {
    schema_version: u32,
    generated_unix_seconds: u64,
    source_fixture_dir: String,
    total_suites: usize,
    total_cases: usize,
    passed_cases: usize,
    failed_cases: usize,
    covered_categories: Vec<String>,
    suite_summaries: Vec<SuiteSummary>,
    failures: Vec<CaseFailure>,
}

#[derive(Debug, Clone, Serialize)]
struct SuiteSummary {
    suite_id: String,
    description: String,
    total_cases: usize,
    passed_cases: usize,
    failed_cases: usize,
}

#[derive(Debug, Clone, Serialize)]
struct CaseFailure {
    suite_id: String,
    case_id: String,
    category: String,
    tags: Vec<String>,
    sql: String,
    details: String,
}

#[test]
fn sql_conformance_suite_matches_documented_subset() {
    let suites = load_suites(Path::new(FIXTURE_DIR)).expect("load SQL conformance fixtures");
    assert!(!suites.is_empty(), "expected at least one SQL conformance fixture suite");

    let mut covered_categories = BTreeSet::new();
    let mut suite_stats = BTreeMap::<String, SuiteStats>::new();
    let mut failures = Vec::<CaseFailure>::new();

    let mut total_cases = 0_usize;
    let mut passed_cases = 0_usize;

    for suite in &suites {
        let stats = suite_stats.entry(suite.suite_id.clone()).or_insert_with(|| SuiteStats {
            description: suite.description.clone(),
            ..SuiteStats::default()
        });

        for case in &suite.cases {
            covered_categories.insert(case.category.clone());

            total_cases = total_cases.saturating_add(1);
            stats.total_cases = stats.total_cases.saturating_add(1);

            let outcome = run_case(case);
            if outcome.passed {
                passed_cases = passed_cases.saturating_add(1);
                stats.passed_cases = stats.passed_cases.saturating_add(1);
            } else {
                stats.failed_cases = stats.failed_cases.saturating_add(1);
                failures.push(CaseFailure {
                    suite_id: suite.suite_id.clone(),
                    case_id: case.id.clone(),
                    category: case.category.clone(),
                    tags: case.tags.clone(),
                    sql: case.sql.clone(),
                    details: outcome.details,
                });
            }
        }
    }

    let missing_categories = REQUIRED_CATEGORIES
        .iter()
        .filter(|required| !covered_categories.contains(**required))
        .map(|value| (*value).to_string())
        .collect::<Vec<_>>();

    let report = ConformanceReport {
        schema_version: REPORT_SCHEMA_VERSION,
        generated_unix_seconds: now_unix_seconds(),
        source_fixture_dir: FIXTURE_DIR.to_string(),
        total_suites: suites.len(),
        total_cases,
        passed_cases,
        failed_cases: total_cases.saturating_sub(passed_cases),
        covered_categories: covered_categories.into_iter().collect(),
        suite_summaries: suite_stats
            .into_iter()
            .map(|(suite_id, stats)| SuiteSummary {
                suite_id,
                description: stats.description,
                total_cases: stats.total_cases,
                passed_cases: stats.passed_cases,
                failed_cases: stats.failed_cases,
            })
            .collect(),
        failures,
    };

    write_report(&report).expect("write SQL conformance report artifact");

    assert!(
        missing_categories.is_empty(),
        "SQL conformance fixture coverage is missing required categories: {}",
        missing_categories.join(", ")
    );

    assert!(
        report.failures.is_empty(),
        "SQL conformance failures (see {}):\n{}",
        report_path().display(),
        report
            .failures
            .iter()
            .map(|failure| {
                format!(
                    "- {}/{} [{}]: {}",
                    failure.suite_id, failure.case_id, failure.category, failure.details
                )
            })
            .collect::<Vec<_>>()
            .join("\n")
    );
}

fn run_case(case: &ConformanceCase) -> CaseOutcome {
    let store = MvccStore::new();
    let catalog = Catalog::open(store.clone()).expect("open catalog for conformance case");
    let mut session = ExecutionSession::new(&catalog, &store);

    for setup_sql in &case.setup_sql {
        if let Err(err) = execute_sql(&mut session, &catalog, setup_sql) {
            return CaseOutcome {
                passed: false,
                details: format!("setup statement failed (`{setup_sql}`): {err}"),
            };
        }
    }

    let execution = execute_sql(&mut session, &catalog, &case.sql);
    evaluate_expectation(execution, &case.expect)
}

fn execute_sql(
    session: &mut ExecutionSession<'_>,
    catalog: &Catalog,
    sql: &str,
) -> Result<ExecutionResult, String> {
    let statement = parse_statement(sql).map_err(|err| format!("parse error: {err}"))?;
    validate_statement(catalog, &statement).map_err(|err| format!("validation error: {err}"))?;
    let plan =
        plan_statement(catalog, &statement).map_err(|err| format!("planner error: {err}"))?;
    session.execute_plan(&plan).map_err(|err| format!("execution error: {err}"))
}

fn evaluate_expectation(
    execution: Result<ExecutionResult, String>,
    expect: &Expectation,
) -> CaseOutcome {
    match expect {
        Expectation::AffectedRows { count } => match execution {
            Ok(ExecutionResult::AffectedRows(actual)) if &actual == count => {
                CaseOutcome { passed: true, details: "ok".to_string() }
            }
            Ok(other) => CaseOutcome {
                passed: false,
                details: format!("expected affected_rows={count}, got {other:?}"),
            },
            Err(err) => CaseOutcome {
                passed: false,
                details: format!("expected affected_rows={count}, got error: {err}"),
            },
        },
        Expectation::Query { columns, rows } => match execution {
            Ok(ExecutionResult::Query(query)) => {
                let actual_rows = query
                    .rows
                    .iter()
                    .map(|row| row.iter().map(scalar_to_string).collect::<Vec<_>>())
                    .collect::<Vec<_>>();

                if &query.columns == columns && &actual_rows == rows {
                    CaseOutcome { passed: true, details: "ok".to_string() }
                } else {
                    CaseOutcome {
                        passed: false,
                        details: format!(
                            "query mismatch: expected columns={columns:?} rows={rows:?}, got columns={:?} rows={actual_rows:?}",
                            query.columns
                        ),
                    }
                }
            }
            Ok(other) => CaseOutcome {
                passed: false,
                details: format!("expected query result, got {other:?}"),
            },
            Err(err) => CaseOutcome {
                passed: false,
                details: format!("expected query result, got error: {err}"),
            },
        },
        Expectation::TransactionState { state } => match execution {
            Ok(result) => {
                if let Some(actual) = transaction_state_name(&result) {
                    if state.eq_ignore_ascii_case(actual) {
                        CaseOutcome { passed: true, details: "ok".to_string() }
                    } else {
                        CaseOutcome {
                            passed: false,
                            details: format!("expected transaction_state={state}, got {actual}"),
                        }
                    }
                } else {
                    CaseOutcome {
                        passed: false,
                        details: format!("expected transaction state, got {result:?}"),
                    }
                }
            }
            Err(err) => CaseOutcome {
                passed: false,
                details: format!("expected transaction state, got error: {err}"),
            },
        },
        Expectation::ErrorContains { message } => match execution {
            Ok(result) => CaseOutcome {
                passed: false,
                details: format!(
                    "expected error containing '{message}', got successful result {result:?}"
                ),
            },
            Err(err) => {
                if err.to_ascii_lowercase().contains(&message.to_ascii_lowercase()) {
                    CaseOutcome { passed: true, details: "ok".to_string() }
                } else {
                    CaseOutcome {
                        passed: false,
                        details: format!("expected error containing '{message}', got '{err}'"),
                    }
                }
            }
        },
    }
}

fn transaction_state_name(result: &ExecutionResult) -> Option<&'static str> {
    match result {
        ExecutionResult::TransactionBegun => Some("begun"),
        ExecutionResult::TransactionCommitted => Some("committed"),
        ExecutionResult::TransactionRolledBack => Some("rolled_back"),
        _ => None,
    }
}

fn scalar_to_string(value: &ScalarValue) -> String {
    match value {
        ScalarValue::Integer(value) => value.to_string(),
        ScalarValue::BigInt(value) => value.to_string(),
        ScalarValue::Float(value) => value.to_string(),
        ScalarValue::Text(value) => value.clone(),
        ScalarValue::Boolean(value) => value.to_string(),
        ScalarValue::Blob(bytes) => bytes_to_hex(bytes),
        ScalarValue::Timestamp(value) => value.to_string(),
        ScalarValue::Null => "NULL".to_string(),
    }
}

fn bytes_to_hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2 + 2);
    out.push_str("0x");
    for byte in bytes {
        out.push(hex_char(byte >> 4));
        out.push(hex_char(byte & 0x0F));
    }
    out
}

fn hex_char(value: u8) -> char {
    match value {
        0..=9 => (b'0' + value) as char,
        10..=15 => (b'a' + (value - 10)) as char,
        _ => unreachable!(),
    }
}

fn load_suites(path: &Path) -> Result<Vec<ConformanceSuite>, String> {
    let mut files = fs::read_dir(path)
        .map_err(|err| format!("failed to read fixture directory '{}': {err}", path.display()))?
        .filter_map(Result::ok)
        .filter(|entry| entry.path().extension().and_then(|ext| ext.to_str()) == Some("toml"))
        .map(|entry| entry.path())
        .collect::<Vec<_>>();
    files.sort();

    if files.is_empty() {
        return Err(format!("no TOML fixture files found in {}", path.display()));
    }

    let mut suites = Vec::new();
    for file in files {
        let raw = fs::read_to_string(&file)
            .map_err(|err| format!("failed reading fixture '{}': {err}", file.display()))?;
        let suite: ConformanceSuite = toml::from_str(&raw)
            .map_err(|err| format!("failed parsing fixture '{}': {err}", file.display()))?;
        if suite.cases.is_empty() {
            return Err(format!("fixture '{}' must include at least one case", file.display()));
        }
        suites.push(suite);
    }

    Ok(suites)
}

fn write_report(report: &ConformanceReport) -> Result<(), String> {
    let report_path = report_path();
    let parent = report_path.parent().ok_or_else(|| {
        format!("failed to resolve parent directory for report path {}", report_path.display())
    })?;

    fs::create_dir_all(parent).map_err(|err| {
        format!("failed to create report directory '{}': {err}", parent.display())
    })?;

    let serialized = toml::to_string_pretty(report)
        .map_err(|err| format!("failed to serialize conformance report: {err}"))?;

    fs::write(&report_path, serialized)
        .map_err(|err| format!("failed to write report '{}': {err}", report_path.display()))
}

fn report_path() -> PathBuf {
    let target_dir = std::env::var_os("CARGO_TARGET_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("target"));
    target_dir.join(REPORT_RELATIVE_PATH)
}

fn now_unix_seconds() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0)
}
