use std::fs;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use lsmdb::catalog::Catalog;
use lsmdb::executor::{ExecutionResult, ExecutionSession, ScalarValue};
use lsmdb::mvcc::MvccStore;
use lsmdb::planner::plan_statement;
use lsmdb::sql::{parse_statement, validate_statement};

fn execute_sql(
    session: &mut ExecutionSession<'_>,
    catalog: &Catalog,
    sql: &str,
) -> ExecutionResult {
    let statement = parse_statement(sql).expect("parse SQL");
    validate_statement(catalog, &statement).expect("validate SQL");
    let plan = plan_statement(catalog, &statement).expect("plan SQL");
    session.execute_plan(&plan).expect("execute SQL")
}

fn test_dir(label: &str) -> PathBuf {
    let mut path = std::env::temp_dir();
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time should be after epoch")
        .as_nanos();
    path.push(format!("lsmdb-sql-persistence-{label}-{}-{nanos}", std::process::id()));
    fs::create_dir_all(&path).expect("create temp dir");
    path
}

#[test]
fn sql_data_and_catalog_survive_restart_with_durable_mvcc_store() {
    let dir = test_dir("restart");

    {
        let store = MvccStore::open_persistent(&dir).expect("open durable store");
        let catalog = Catalog::open(store.clone()).expect("open catalog");
        let mut session = ExecutionSession::new(&catalog, &store);

        execute_sql(
            &mut session,
            &catalog,
            "CREATE TABLE users (
                id BIGINT NOT NULL,
                email TEXT NOT NULL,
                active BOOLEAN DEFAULT true,
                PRIMARY KEY (id)
            )",
        );

        assert!(matches!(
            execute_sql(
                &mut session,
                &catalog,
                "INSERT INTO users (id, email, active) VALUES (1, 'persisted@x.com', true)",
            ),
            ExecutionResult::AffectedRows(1)
        ));
    }

    {
        let store = MvccStore::open_persistent(&dir).expect("reopen durable store");
        let catalog = Catalog::open(store.clone()).expect("reopen catalog");
        let mut session = ExecutionSession::new(&catalog, &store);

        let result =
            execute_sql(&mut session, &catalog, "SELECT id, email FROM users WHERE id = 1");
        let ExecutionResult::Query(query) = result else {
            panic!("expected query result");
        };
        assert_eq!(
            query.rows,
            vec![vec![ScalarValue::BigInt(1), ScalarValue::Text("persisted@x.com".to_string())]]
        );
    }

    fs::remove_dir_all(dir).expect("cleanup temp dir");
}

#[test]
fn rolled_back_and_uncommitted_sql_writes_are_not_visible_after_restart() {
    let dir = test_dir("rollback");

    {
        let store = MvccStore::open_persistent(&dir).expect("open durable store");
        let catalog = Catalog::open(store.clone()).expect("open catalog");
        let mut session = ExecutionSession::new(&catalog, &store);

        execute_sql(
            &mut session,
            &catalog,
            "CREATE TABLE accounts (
                id BIGINT NOT NULL,
                email TEXT NOT NULL,
                PRIMARY KEY (id)
            )",
        );

        execute_sql(&mut session, &catalog, "BEGIN ISOLATION LEVEL SNAPSHOT");
        execute_sql(
            &mut session,
            &catalog,
            "INSERT INTO accounts (id, email) VALUES (10, 'rollback@x.com')",
        );
        execute_sql(&mut session, &catalog, "ROLLBACK");

        execute_sql(&mut session, &catalog, "BEGIN ISOLATION LEVEL SNAPSHOT");
        execute_sql(
            &mut session,
            &catalog,
            "INSERT INTO accounts (id, email) VALUES (11, 'uncommitted@x.com')",
        );
        // Intentionally drop the session without COMMIT to simulate crash before ack.
    }

    {
        let store = MvccStore::open_persistent(&dir).expect("reopen durable store");
        let catalog = Catalog::open(store.clone()).expect("reopen catalog");
        let mut session = ExecutionSession::new(&catalog, &store);

        let result = execute_sql(
            &mut session,
            &catalog,
            "SELECT id FROM accounts WHERE id = 10 OR id = 11 ORDER BY id ASC",
        );
        let ExecutionResult::Query(query) = result else {
            panic!("expected query result");
        };
        assert!(query.rows.is_empty());
    }

    fs::remove_dir_all(dir).expect("cleanup temp dir");
}
