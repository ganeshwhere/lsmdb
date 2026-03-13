use std::net::SocketAddr;
use std::str::from_utf8;
use std::sync::Arc;
use std::time::Duration;

use lsmdb::catalog::Catalog;
use lsmdb::executor::{ExecutionResult, ExecutionSession};
use lsmdb::mvcc::MvccStore;
use lsmdb::planner::plan_statement;
use lsmdb::server::{
    ActiveStatementsPayload, AdminStatusPayload, ErrorCode, ErrorPayload, HealthPayload,
    PROTOCOL_VERSION, QueryPayload, ReadinessPayload, RequestFrame, RequestType, ResponseFrame,
    ResponsePayload, ServerLimits, ServerOptions, StatementCancellationPayload, TransactionState,
    read_response, start_server, start_server_with_options, write_request,
};
use lsmdb::sql::{parse_statement, validate_statement};
use tokio::net::TcpStream;
use tokio::time::sleep;

async fn send_request(stream: &mut TcpStream, request: RequestFrame) -> ResponseFrame {
    write_request(stream, &request).await.expect("write request");
    read_response(stream).await.expect("read response").expect("response")
}

fn response_to_query(response: ResponseFrame) -> QueryPayload {
    match response {
        ResponseFrame::Ok(ResponsePayload::Query(query)) => query,
        other => panic!("expected query payload, got {other:?}"),
    }
}

fn response_to_explain(response: ResponseFrame) -> String {
    match response {
        ResponseFrame::Ok(ResponsePayload::ExplainPlan(plan)) => plan,
        other => panic!("expected explain payload, got {other:?}"),
    }
}

fn response_to_health(response: ResponseFrame) -> HealthPayload {
    match response {
        ResponseFrame::Ok(ResponsePayload::Health(payload)) => payload,
        other => panic!("expected health payload, got {other:?}"),
    }
}

fn response_to_readiness(response: ResponseFrame) -> ReadinessPayload {
    match response {
        ResponseFrame::Ok(ResponsePayload::Readiness(payload)) => payload,
        other => panic!("expected readiness payload, got {other:?}"),
    }
}

fn response_to_admin_status(response: ResponseFrame) -> AdminStatusPayload {
    match response {
        ResponseFrame::Ok(ResponsePayload::AdminStatus(payload)) => payload,
        other => panic!("expected admin status payload, got {other:?}"),
    }
}

fn response_to_error(response: ResponseFrame) -> ErrorPayload {
    match response {
        ResponseFrame::Err(payload) => payload,
        other => panic!("expected error payload, got {other:?}"),
    }
}

fn response_to_active_statements(response: ResponseFrame) -> ActiveStatementsPayload {
    match response {
        ResponseFrame::Ok(ResponsePayload::ActiveStatements(payload)) => payload,
        other => panic!("expected active statements payload, got {other:?}"),
    }
}

fn response_to_statement_cancellation(response: ResponseFrame) -> StatementCancellationPayload {
    match response {
        ResponseFrame::Ok(ResponsePayload::StatementCancellation(payload)) => payload,
        other => panic!("expected statement cancellation payload, got {other:?}"),
    }
}

fn execute_setup_sql(catalog: &Catalog, store: &MvccStore, sql: &str) -> ExecutionResult {
    let statement = parse_statement(sql).expect("parse setup SQL");
    validate_statement(catalog, &statement).expect("validate setup SQL");
    let plan = plan_statement(catalog, &statement).expect("plan setup SQL");
    let mut session = ExecutionSession::new(catalog, store);
    session.execute_plan(&plan).expect("execute setup SQL")
}

fn populate_users(catalog: &Catalog, store: &MvccStore, rows: usize) {
    execute_setup_sql(
        catalog,
        store,
        "CREATE TABLE users (id BIGINT NOT NULL, email TEXT NOT NULL, PRIMARY KEY (id))",
    );
    for id in 1..=rows {
        execute_setup_sql(
            catalog,
            store,
            &format!(
                "INSERT INTO users (id, email) VALUES ({id}, '{}')",
                format!("user{id:05}@example.com")
            ),
        );
    }
}

async fn wait_for_active_statement_id(stream: &mut TcpStream, request_type: &str) -> u64 {
    for _ in 0..500 {
        let payload = response_to_active_statements(
            send_request(
                stream,
                RequestFrame { request_type: RequestType::ActiveStatements, sql: String::new() },
            )
            .await,
        );
        if let Some(statement) =
            payload.statements.into_iter().find(|statement| statement.request_type == request_type)
        {
            return statement.statement_id;
        }
        sleep(Duration::from_millis(1)).await;
    }

    panic!("timed out waiting for active statement");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn server_executes_query_requests_end_to_end() {
    let store = Arc::new(MvccStore::new());
    let catalog = Arc::new(Catalog::open((*store).clone()).expect("open catalog"));

    let bind_addr: SocketAddr = "127.0.0.1:0".parse().expect("parse socket addr");
    let server = start_server(bind_addr, Arc::clone(&catalog), Arc::clone(&store))
        .await
        .expect("start server");
    let server_addr = server.local_addr();

    let mut client = TcpStream::connect(server_addr).await.expect("connect client");

    let create_response = send_request(
        &mut client,
        RequestFrame {
            request_type: RequestType::Query,
            sql: "CREATE TABLE users (id BIGINT NOT NULL, email TEXT NOT NULL, PRIMARY KEY (id))"
                .to_string(),
        },
    )
    .await;
    assert!(matches!(create_response, ResponseFrame::Ok(ResponsePayload::AffectedRows(0))));

    let insert_response = send_request(
        &mut client,
        RequestFrame {
            request_type: RequestType::Query,
            sql: "INSERT INTO users (id, email) VALUES (1, 'alice@x.com')".to_string(),
        },
    )
    .await;
    assert!(matches!(insert_response, ResponseFrame::Ok(ResponsePayload::AffectedRows(1))));

    let select_response = send_request(
        &mut client,
        RequestFrame {
            request_type: RequestType::Query,
            sql: "SELECT id, email FROM users WHERE id = 1".to_string(),
        },
    )
    .await;
    let query = response_to_query(select_response);
    assert_eq!(query.columns, vec!["id".to_string(), "email".to_string()]);
    assert_eq!(query.rows.len(), 1);
    assert_eq!(from_utf8(&query.rows[0][0]).expect("utf8 cell"), "1");
    assert_eq!(from_utf8(&query.rows[0][1]).expect("utf8 cell"), "alice@x.com");

    server.shutdown().await.expect("shutdown server");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn server_returns_explain_plan_without_executing_statement() {
    let store = Arc::new(MvccStore::new());
    let catalog = Arc::new(Catalog::open((*store).clone()).expect("open catalog"));

    let bind_addr: SocketAddr = "127.0.0.1:0".parse().expect("parse socket addr");
    let server = start_server(bind_addr, Arc::clone(&catalog), Arc::clone(&store))
        .await
        .expect("start server");
    let server_addr = server.local_addr();

    let mut client = TcpStream::connect(server_addr).await.expect("connect client");

    let create_response = send_request(
        &mut client,
        RequestFrame {
            request_type: RequestType::Query,
            sql: "CREATE TABLE users (id BIGINT NOT NULL, email TEXT NOT NULL, PRIMARY KEY (id))"
                .to_string(),
        },
    )
    .await;
    assert!(matches!(create_response, ResponseFrame::Ok(ResponsePayload::AffectedRows(0))));

    let explain_response = send_request(
        &mut client,
        RequestFrame {
            request_type: RequestType::Explain,
            sql: "SELECT id FROM users WHERE id = 1".to_string(),
        },
    )
    .await;
    let explain = response_to_explain(explain_response);
    assert!(explain.contains("PrimaryKeyScan"));

    let select_response = send_request(
        &mut client,
        RequestFrame { request_type: RequestType::Query, sql: "SELECT id FROM users".to_string() },
    )
    .await;
    let query = response_to_query(select_response);
    assert!(query.rows.is_empty());

    server.shutdown().await.expect("shutdown server");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn server_tracks_transaction_state_per_connection() {
    let store = Arc::new(MvccStore::new());
    let catalog = Arc::new(Catalog::open((*store).clone()).expect("open catalog"));

    let bind_addr: SocketAddr = "127.0.0.1:0".parse().expect("parse socket addr");
    let server = start_server(bind_addr, Arc::clone(&catalog), Arc::clone(&store))
        .await
        .expect("start server");
    let server_addr = server.local_addr();

    let mut client_a = TcpStream::connect(server_addr).await.expect("connect client_a");
    let mut client_b = TcpStream::connect(server_addr).await.expect("connect client_b");

    let create_response = send_request(
        &mut client_a,
        RequestFrame {
            request_type: RequestType::Query,
            sql:
                "CREATE TABLE accounts (id BIGINT NOT NULL, email TEXT NOT NULL, PRIMARY KEY (id))"
                    .to_string(),
        },
    )
    .await;
    assert!(matches!(create_response, ResponseFrame::Ok(ResponsePayload::AffectedRows(0))));

    let begin_response = send_request(
        &mut client_a,
        RequestFrame { request_type: RequestType::Begin, sql: String::new() },
    )
    .await;
    assert!(matches!(
        begin_response,
        ResponseFrame::Ok(ResponsePayload::TransactionState(TransactionState::Begun))
    ));

    let insert_response = send_request(
        &mut client_a,
        RequestFrame {
            request_type: RequestType::Query,
            sql: "INSERT INTO accounts (id, email) VALUES (10, 'pending@x.com')".to_string(),
        },
    )
    .await;
    assert!(matches!(insert_response, ResponseFrame::Ok(ResponsePayload::AffectedRows(1))));

    let before_commit = send_request(
        &mut client_b,
        RequestFrame {
            request_type: RequestType::Query,
            sql: "SELECT id FROM accounts WHERE id = 10".to_string(),
        },
    )
    .await;
    let before_query = response_to_query(before_commit);
    assert!(before_query.rows.is_empty());

    let commit_response = send_request(
        &mut client_a,
        RequestFrame { request_type: RequestType::Commit, sql: String::new() },
    )
    .await;
    assert!(matches!(
        commit_response,
        ResponseFrame::Ok(ResponsePayload::TransactionState(TransactionState::Committed))
    ));

    let after_commit = send_request(
        &mut client_b,
        RequestFrame {
            request_type: RequestType::Query,
            sql: "SELECT id FROM accounts WHERE id = 10".to_string(),
        },
    )
    .await;
    let after_query = response_to_query(after_commit);
    assert_eq!(after_query.rows.len(), 1);
    assert_eq!(from_utf8(&after_query.rows[0][0]).expect("utf8 cell"), "10");

    server.shutdown().await.expect("shutdown server");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn server_exposes_health_readiness_and_admin_status() {
    let store = Arc::new(MvccStore::new());
    let catalog = Arc::new(Catalog::open((*store).clone()).expect("open catalog"));

    let bind_addr: SocketAddr = "127.0.0.1:0".parse().expect("parse socket addr");
    let server = start_server(bind_addr, Arc::clone(&catalog), Arc::clone(&store))
        .await
        .expect("start server");
    let server_addr = server.local_addr();

    let mut client = TcpStream::connect(server_addr).await.expect("connect client");

    let health = response_to_health(
        send_request(
            &mut client,
            RequestFrame { request_type: RequestType::Health, sql: String::new() },
        )
        .await,
    );
    assert!(health.ok);
    assert_eq!(health.status, "ok");

    let readiness = response_to_readiness(
        send_request(
            &mut client,
            RequestFrame { request_type: RequestType::Readiness, sql: String::new() },
        )
        .await,
    );
    assert!(readiness.ready);
    assert_eq!(readiness.status, "ready");

    let admin = response_to_admin_status(
        send_request(
            &mut client,
            RequestFrame { request_type: RequestType::AdminStatus, sql: String::new() },
        )
        .await,
    );
    assert_eq!(admin.protocol_version, PROTOCOL_VERSION);
    assert_eq!(admin.server_version, env!("CARGO_PKG_VERSION"));
    assert!(admin.accepting_connections);
    assert!(admin.total_connections >= 1);
    assert!(admin.active_connections >= 1);
    assert_eq!(admin.rejected_connections, 0);
    assert_eq!(admin.busy_requests, 0);
    assert_eq!(admin.resource_limit_requests, 0);
    assert_eq!(admin.quota_rejections, 0);
    assert_eq!(admin.timed_out_requests, 0);
    assert_eq!(admin.canceled_requests, 0);
    assert_eq!(admin.active_statements, 0);
    assert_eq!(admin.active_memory_intensive_requests, 0);
    assert_eq!(admin.mvcc_started, 0);
    assert_eq!(admin.mvcc_committed, 0);
    assert_eq!(admin.mvcc_rolled_back, 0);
    assert_eq!(admin.mvcc_write_conflicts, 0);
    assert_eq!(admin.mvcc_active_transactions, 0);

    server.shutdown().await.expect("shutdown server");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn server_rejects_connections_above_limit_and_keeps_existing_connection_responsive() {
    let store = Arc::new(MvccStore::new());
    let catalog = Arc::new(Catalog::open((*store).clone()).expect("open catalog"));
    let options = ServerOptions {
        limits: ServerLimits { max_concurrent_connections: 1, ..ServerLimits::default() },
    };

    let bind_addr: SocketAddr = "127.0.0.1:0".parse().expect("parse socket addr");
    let server =
        start_server_with_options(bind_addr, Arc::clone(&catalog), Arc::clone(&store), options)
            .await
            .expect("start server");
    let server_addr = server.local_addr();

    let mut first = TcpStream::connect(server_addr).await.expect("connect first client");
    let mut second = TcpStream::connect(server_addr).await.expect("connect second client");

    let rejected =
        read_response(&mut second).await.expect("read busy response").expect("busy response");
    let error = response_to_error(rejected);
    assert_eq!(error.code, ErrorCode::Busy);
    assert!(error.retryable);
    assert!(error.message.contains("max concurrent connections"));

    let health = response_to_health(
        send_request(
            &mut first,
            RequestFrame { request_type: RequestType::Health, sql: String::new() },
        )
        .await,
    );
    assert!(health.ok);

    let admin = response_to_admin_status(
        send_request(
            &mut first,
            RequestFrame { request_type: RequestType::AdminStatus, sql: String::new() },
        )
        .await,
    );
    assert_eq!(admin.rejected_connections, 1);

    server.shutdown().await.expect("shutdown server");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn server_rejects_oversized_request_frames_with_resource_limit_error() {
    let store = Arc::new(MvccStore::new());
    let catalog = Arc::new(Catalog::open((*store).clone()).expect("open catalog"));
    let options =
        ServerOptions { limits: ServerLimits { max_request_bytes: 32, ..ServerLimits::default() } };

    let bind_addr: SocketAddr = "127.0.0.1:0".parse().expect("parse socket addr");
    let server =
        start_server_with_options(bind_addr, Arc::clone(&catalog), Arc::clone(&store), options)
            .await
            .expect("start server");
    let server_addr = server.local_addr();

    let mut client = TcpStream::connect(server_addr).await.expect("connect client");
    let error = response_to_error(
        send_request(
            &mut client,
            RequestFrame {
                request_type: RequestType::Query,
                sql: "SELECT 1 FROM some_really_long_table_name_that_exceeds_the_limit".to_string(),
            },
        )
        .await,
    );
    assert_eq!(error.code, ErrorCode::ResourceLimit);
    assert!(!error.retryable);
    assert!(error.message.contains("request frame too large"));

    server.shutdown().await.expect("shutdown server");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn server_enforces_scan_and_sort_limits() {
    let store = Arc::new(MvccStore::new());
    let catalog = Arc::new(Catalog::open((*store).clone()).expect("open catalog"));
    let options = ServerOptions {
        limits: ServerLimits {
            max_scan_rows: 2,
            max_sort_rows: 2,
            max_query_result_rows: 2,
            ..ServerLimits::default()
        },
    };

    let bind_addr: SocketAddr = "127.0.0.1:0".parse().expect("parse socket addr");
    let server =
        start_server_with_options(bind_addr, Arc::clone(&catalog), Arc::clone(&store), options)
            .await
            .expect("start server");
    let server_addr = server.local_addr();

    let mut client = TcpStream::connect(server_addr).await.expect("connect client");
    let create_response = send_request(
        &mut client,
        RequestFrame {
            request_type: RequestType::Query,
            sql: "CREATE TABLE users (id BIGINT NOT NULL, email TEXT NOT NULL, PRIMARY KEY (id))"
                .to_string(),
        },
    )
    .await;
    assert!(matches!(create_response, ResponseFrame::Ok(ResponsePayload::AffectedRows(0))));

    for id in 1..=3 {
        let insert = send_request(
            &mut client,
            RequestFrame {
                request_type: RequestType::Query,
                sql: format!("INSERT INTO users (id, email) VALUES ({id}, 'user{id}@x.com')"),
            },
        )
        .await;
        assert!(matches!(insert, ResponseFrame::Ok(ResponsePayload::AffectedRows(1))));
    }

    let scan_error = response_to_error(
        send_request(
            &mut client,
            RequestFrame {
                request_type: RequestType::Query,
                sql: "SELECT id, email FROM users".to_string(),
            },
        )
        .await,
    );
    assert_eq!(scan_error.code, ErrorCode::ResourceLimit);
    assert!(scan_error.message.contains("scan rows"));

    let sort_error = response_to_error(
        send_request(
            &mut client,
            RequestFrame {
                request_type: RequestType::Query,
                sql: "SELECT id FROM users ORDER BY id DESC".to_string(),
            },
        )
        .await,
    );
    assert_eq!(sort_error.code, ErrorCode::ResourceLimit);
    assert!(sort_error.message.contains("scan rows") || sort_error.message.contains("sort rows"));

    server.shutdown().await.expect("shutdown server");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn server_enforces_statement_count_limit() {
    let store = Arc::new(MvccStore::new());
    let catalog = Arc::new(Catalog::open((*store).clone()).expect("open catalog"));
    let options = ServerOptions {
        limits: ServerLimits { max_statements_per_request: 1, ..ServerLimits::default() },
    };

    let bind_addr: SocketAddr = "127.0.0.1:0".parse().expect("parse socket addr");
    let server =
        start_server_with_options(bind_addr, Arc::clone(&catalog), Arc::clone(&store), options)
            .await
            .expect("start server");
    let server_addr = server.local_addr();

    let mut client = TcpStream::connect(server_addr).await.expect("connect client");

    let error = response_to_error(
        send_request(
            &mut client,
            RequestFrame {
                request_type: RequestType::Query,
                sql: "BEGIN ISOLATION LEVEL SNAPSHOT; COMMIT".to_string(),
            },
        )
        .await,
    );
    assert_eq!(error.code, ErrorCode::ResourceLimit);
    assert!(error.message.contains("statements"));

    server.shutdown().await.expect("shutdown server");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn server_times_out_long_running_scan_and_sort_queries() {
    let store = Arc::new(MvccStore::new());
    let catalog = Arc::new(Catalog::open((*store).clone()).expect("open catalog"));
    populate_users(&catalog, &store, 25_000);

    let options = ServerOptions {
        limits: ServerLimits { statement_timeout_ms: Some(1), ..ServerLimits::default() },
    };
    let bind_addr: SocketAddr = "127.0.0.1:0".parse().expect("parse socket addr");
    let server =
        start_server_with_options(bind_addr, Arc::clone(&catalog), Arc::clone(&store), options)
            .await
            .expect("start server");
    let server_addr = server.local_addr();

    let mut client = TcpStream::connect(server_addr).await.expect("connect client");

    let scan_error = response_to_error(
        send_request(
            &mut client,
            RequestFrame {
                request_type: RequestType::Query,
                sql: "SELECT id, email FROM users".to_string(),
            },
        )
        .await,
    );
    assert_eq!(scan_error.code, ErrorCode::Timeout);
    assert!(scan_error.message.contains("timed out"));

    let sort_error = response_to_error(
        send_request(
            &mut client,
            RequestFrame {
                request_type: RequestType::Query,
                sql: "SELECT id FROM users ORDER BY email DESC".to_string(),
            },
        )
        .await,
    );
    assert_eq!(sort_error.code, ErrorCode::Timeout);
    assert!(sort_error.message.contains("timed out"));

    let admin = response_to_admin_status(
        send_request(
            &mut client,
            RequestFrame { request_type: RequestType::AdminStatus, sql: String::new() },
        )
        .await,
    );
    assert!(admin.timed_out_requests >= 2);

    server.shutdown().await.expect("shutdown server");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn server_rejects_queries_when_identity_quota_is_reached() {
    let store = Arc::new(MvccStore::new());
    let catalog = Arc::new(Catalog::open((*store).clone()).expect("open catalog"));
    populate_users(&catalog, &store, 60_000);

    let options = ServerOptions {
        limits: ServerLimits {
            max_concurrent_queries_per_identity: Some(1),
            statement_timeout_ms: Some(5_000),
            ..ServerLimits::default()
        },
    };
    let bind_addr: SocketAddr = "127.0.0.1:0".parse().expect("parse socket addr");
    let server =
        start_server_with_options(bind_addr, Arc::clone(&catalog), Arc::clone(&store), options)
            .await
            .expect("start server");
    let server_addr = server.local_addr();

    let mut query_client = TcpStream::connect(server_addr).await.expect("connect query client");
    let query_task = tokio::spawn(async move {
        send_request(
            &mut query_client,
            RequestFrame {
                request_type: RequestType::Query,
                sql: "UPDATE users SET email = 'quota@example.com'".to_string(),
            },
        )
        .await
    });

    let mut admin_client = TcpStream::connect(server_addr).await.expect("connect admin client");
    let statement_id = wait_for_active_statement_id(&mut admin_client, "QUERY").await;

    let mut second_client = TcpStream::connect(server_addr).await.expect("connect second client");
    let quota_error = response_to_error(
        send_request(
            &mut second_client,
            RequestFrame {
                request_type: RequestType::Query,
                sql: "SELECT id FROM users ORDER BY email ASC".to_string(),
            },
        )
        .await,
    );
    assert_eq!(quota_error.code, ErrorCode::Quota);
    assert!(quota_error.retryable);

    let cancel = response_to_statement_cancellation(
        send_request(
            &mut admin_client,
            RequestFrame {
                request_type: RequestType::CancelStatement,
                sql: statement_id.to_string(),
            },
        )
        .await,
    );
    assert_eq!(cancel.statement_id, statement_id);
    assert!(cancel.accepted);

    let canceled = response_to_error(query_task.await.expect("query task"));
    assert_eq!(canceled.code, ErrorCode::Canceled);

    let admin = response_to_admin_status(
        send_request(
            &mut admin_client,
            RequestFrame { request_type: RequestType::AdminStatus, sql: String::new() },
        )
        .await,
    );
    assert!(admin.quota_rejections >= 1);
    assert!(admin.canceled_requests >= 1);

    server.shutdown().await.expect("shutdown server");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn server_cancellation_rolls_back_active_transaction_state() {
    let store = Arc::new(MvccStore::new());
    let catalog = Arc::new(Catalog::open((*store).clone()).expect("open catalog"));
    populate_users(&catalog, &store, 50_000);

    let options = ServerOptions {
        limits: ServerLimits { statement_timeout_ms: Some(5_000), ..ServerLimits::default() },
    };
    let bind_addr: SocketAddr = "127.0.0.1:0".parse().expect("parse socket addr");
    let server =
        start_server_with_options(bind_addr, Arc::clone(&catalog), Arc::clone(&store), options)
            .await
            .expect("start server");
    let server_addr = server.local_addr();

    let mut txn_client = TcpStream::connect(server_addr).await.expect("connect txn client");
    let begin = send_request(
        &mut txn_client,
        RequestFrame { request_type: RequestType::Begin, sql: String::new() },
    )
    .await;
    assert!(matches!(
        begin,
        ResponseFrame::Ok(ResponsePayload::TransactionState(TransactionState::Begun))
    ));

    let txn_task = tokio::spawn(async move {
        let update = send_request(
            &mut txn_client,
            RequestFrame {
                request_type: RequestType::Query,
                sql: "UPDATE users SET email = 'blocked@example.com'".to_string(),
            },
        )
        .await;
        let commit = send_request(
            &mut txn_client,
            RequestFrame { request_type: RequestType::Commit, sql: String::new() },
        )
        .await;
        (update, commit)
    });

    let mut admin_client = TcpStream::connect(server_addr).await.expect("connect admin client");
    let statement_id = wait_for_active_statement_id(&mut admin_client, "QUERY").await;
    let cancel = response_to_statement_cancellation(
        send_request(
            &mut admin_client,
            RequestFrame {
                request_type: RequestType::CancelStatement,
                sql: statement_id.to_string(),
            },
        )
        .await,
    );
    assert!(cancel.accepted);

    let (update, commit) = txn_task.await.expect("txn task");
    let update_error = response_to_error(update);
    assert_eq!(update_error.code, ErrorCode::Canceled);

    let commit_error = response_to_error(commit);
    assert_eq!(commit_error.code, ErrorCode::Execution);
    assert!(commit_error.message.contains("no active transaction"));

    let mut verify_client = TcpStream::connect(server_addr).await.expect("connect verify client");
    let result = response_to_query(
        send_request(
            &mut verify_client,
            RequestFrame {
                request_type: RequestType::Query,
                sql: "SELECT email FROM users WHERE id = 1".to_string(),
            },
        )
        .await,
    );
    assert_eq!(from_utf8(&result.rows[0][0]).expect("utf8 cell"), "user00001@example.com");

    server.shutdown().await.expect("shutdown server");
}
