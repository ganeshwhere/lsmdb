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

#[test]
fn executes_insert_select_update_and_delete() {
    let store = MvccStore::new();
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
            "INSERT INTO users (id, email, active) VALUES (1, 'a@x.com', true)",
        ),
        ExecutionResult::AffectedRows(1)
    ));
    assert!(matches!(
        execute_sql(&mut session, &catalog, "INSERT INTO users (id, email) VALUES (2, 'b@x.com')",),
        ExecutionResult::AffectedRows(1)
    ));

    let result = execute_sql(&mut session, &catalog, "SELECT id, email FROM users ORDER BY id ASC");
    let ExecutionResult::Query(query) = result else {
        panic!("expected query result");
    };
    assert_eq!(query.columns, vec!["id".to_string(), "email".to_string()]);
    assert_eq!(query.rows.len(), 2);
    assert_eq!(query.rows[0][0], ScalarValue::BigInt(1));
    assert_eq!(query.rows[1][0], ScalarValue::BigInt(2));

    assert!(matches!(
        execute_sql(&mut session, &catalog, "UPDATE users SET email = 'new@x.com' WHERE id = 1",),
        ExecutionResult::AffectedRows(1)
    ));
    let result = execute_sql(&mut session, &catalog, "SELECT email FROM users WHERE id = 1");
    let ExecutionResult::Query(query) = result else {
        panic!("expected query result");
    };
    assert_eq!(query.rows, vec![vec![ScalarValue::Text("new@x.com".to_string())]]);

    assert!(matches!(
        execute_sql(&mut session, &catalog, "DELETE FROM users WHERE id = 2"),
        ExecutionResult::AffectedRows(1)
    ));
    let result = execute_sql(&mut session, &catalog, "SELECT id FROM users ORDER BY id ASC");
    let ExecutionResult::Query(query) = result else {
        panic!("expected query result");
    };
    assert_eq!(query.rows, vec![vec![ScalarValue::BigInt(1)]]);
}

#[test]
fn transaction_commit_controls_visibility() {
    let store = MvccStore::new();
    let catalog = Catalog::open(store.clone()).expect("open catalog");
    let mut session_a = ExecutionSession::new(&catalog, &store);
    let mut session_b = ExecutionSession::new(&catalog, &store);

    execute_sql(
        &mut session_a,
        &catalog,
        "CREATE TABLE accounts (
            id BIGINT NOT NULL,
            email TEXT NOT NULL,
            PRIMARY KEY (id)
        )",
    );

    execute_sql(&mut session_a, &catalog, "BEGIN ISOLATION LEVEL SNAPSHOT");
    execute_sql(
        &mut session_a,
        &catalog,
        "INSERT INTO accounts (id, email) VALUES (10, 'pending@x.com')",
    );

    let before_commit =
        execute_sql(&mut session_b, &catalog, "SELECT id FROM accounts WHERE id = 10");
    let ExecutionResult::Query(before_commit) = before_commit else {
        panic!("expected query result");
    };
    assert!(before_commit.rows.is_empty());

    execute_sql(&mut session_a, &catalog, "COMMIT");

    let after_commit =
        execute_sql(&mut session_b, &catalog, "SELECT id FROM accounts WHERE id = 10");
    let ExecutionResult::Query(after_commit) = after_commit else {
        panic!("expected query result");
    };
    assert_eq!(after_commit.rows, vec![vec![ScalarValue::BigInt(10)]]);
}
