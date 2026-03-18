use std::net::SocketAddr;
use std::path::PathBuf;
use std::str::from_utf8;
use std::sync::Arc;
use std::sync::OnceLock;
use std::time::Duration;

use lsmdb::catalog::Catalog;
use lsmdb::executor::{ExecutionResult, ExecutionSession};
use lsmdb::mvcc::MvccStore;
use lsmdb::planner::plan_statement;
use lsmdb::server::{
    ActiveStatementsPayload, AdminStatusPayload, AuthenticationPayload, ErrorCode, ErrorPayload,
    HealthPayload, PROTOCOL_VERSION, QueryPayload, ReadinessPayload, RequestFrame, RequestType,
    ResponseFrame, ResponsePayload, ServerAuthOptions, ServerError, ServerLimits, ServerOptions,
    ServerRole, ServerSecurityOptions, ServerTlsMode, ServerTlsOptions,
    StatementCancellationPayload, StaticPasswordUser, StaticTokenPrincipal, TransactionState,
    authentication_request_with_password, authentication_request_with_token, read_response,
    start_server, start_server_with_options, write_request,
};
use lsmdb::sql::{parse_statement, validate_statement};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::time::sleep;
use tokio_rustls::TlsConnector;
use tokio_rustls::rustls::{ClientConfig, RootCertStore, pki_types::ServerName};

const TLS_CERT_PATH: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/tls/server.crt");
const TLS_KEY_PATH: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/tls/server.key");
static HEAVY_SERVER_TEST_SEMAPHORE: OnceLock<Arc<Semaphore>> = OnceLock::new();

async fn send_request<S>(stream: &mut S, request: RequestFrame) -> ResponseFrame
where
    S: AsyncRead + AsyncWrite + Unpin,
{
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

fn response_to_authentication(response: ResponseFrame) -> AuthenticationPayload {
    match response {
        ResponseFrame::Ok(ResponsePayload::Authentication(payload)) => payload,
        other => panic!("expected authentication payload, got {other:?}"),
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

async fn wait_for_active_statement_id<S>(stream: &mut S, request_type: &str) -> u64
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    for _ in 0..5_000 {
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

fn password_server_options(users: Vec<StaticPasswordUser>) -> ServerOptions {
    ServerOptions {
        security: ServerSecurityOptions {
            auth: ServerAuthOptions::StaticPassword { users },
            tls: fixture_tls_server_options(),
            ..ServerSecurityOptions::default()
        },
        ..ServerOptions::default()
    }
}

fn token_server_options(principals: Vec<StaticTokenPrincipal>) -> ServerOptions {
    ServerOptions {
        security: ServerSecurityOptions {
            auth: ServerAuthOptions::StaticToken { principals },
            tls: fixture_tls_server_options(),
            ..ServerSecurityOptions::default()
        },
        ..ServerOptions::default()
    }
}

fn insecure_server_options() -> ServerOptions {
    ServerOptions::insecure_for_local_dev()
}

fn fixture_tls_server_options() -> ServerTlsOptions {
    ServerTlsOptions {
        mode: ServerTlsMode::Required,
        cert_path: Some(PathBuf::from(TLS_CERT_PATH)),
        key_path: Some(PathBuf::from(TLS_KEY_PATH)),
    }
}

async fn connect_tls_client(addr: SocketAddr) -> tokio_rustls::client::TlsStream<TcpStream> {
    let certificate = std::fs::File::open(TLS_CERT_PATH).expect("open tls certificate fixture");
    let mut reader = std::io::BufReader::new(certificate);
    let certificates = rustls_pemfile::certs(&mut reader)
        .collect::<Result<Vec<_>, _>>()
        .expect("parse tls certificate fixture");
    let mut roots = RootCertStore::empty();
    for certificate in certificates {
        roots.add(certificate).expect("add tls certificate to root store");
    }
    let config = ClientConfig::builder().with_root_certificates(roots).with_no_client_auth();
    let connector = TlsConnector::from(Arc::new(config));
    let stream = TcpStream::connect(addr).await.expect("connect tcp stream for tls");
    let server_name = ServerName::try_from("localhost").expect("valid server name").to_owned();
    connector.connect(server_name, stream).await.expect("complete tls handshake")
}

async fn acquire_heavy_server_test_permit() -> OwnedSemaphorePermit {
    HEAVY_SERVER_TEST_SEMAPHORE
        .get_or_init(|| Arc::new(Semaphore::new(1)))
        .clone()
        .acquire_owned()
        .await
        .expect("heavy server test semaphore should remain open")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn server_executes_query_requests_end_to_end() {
    let store = Arc::new(MvccStore::new());
    let catalog = Arc::new(Catalog::open((*store).clone()).expect("open catalog"));

    let bind_addr: SocketAddr = "127.0.0.1:0".parse().expect("parse socket addr");
    let server = start_server_with_options(
        bind_addr,
        Arc::clone(&catalog),
        Arc::clone(&store),
        insecure_server_options(),
    )
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
    let server = start_server_with_options(
        bind_addr,
        Arc::clone(&catalog),
        Arc::clone(&store),
        insecure_server_options(),
    )
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
    let server = start_server_with_options(
        bind_addr,
        Arc::clone(&catalog),
        Arc::clone(&store),
        insecure_server_options(),
    )
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
    let server = start_server_with_options(
        bind_addr,
        Arc::clone(&catalog),
        Arc::clone(&store),
        insecure_server_options(),
    )
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
async fn start_server_requires_explicit_security_configuration() {
    let store = Arc::new(MvccStore::new());
    let catalog = Arc::new(Catalog::open((*store).clone()).expect("open catalog"));

    let bind_addr: SocketAddr = "127.0.0.1:0".parse().expect("parse socket addr");
    let err = match start_server(bind_addr, Arc::clone(&catalog), Arc::clone(&store)).await {
        Ok(_) => panic!("default startup should be rejected"),
        Err(err) => err,
    };
    match err {
        ServerError::InvalidConfiguration(message) => {
            assert!(message.contains("allow_anonymous_access"));
        }
        other => panic!("expected invalid configuration, got {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn server_rejects_connections_above_limit_and_keeps_existing_connection_responsive() {
    let store = Arc::new(MvccStore::new());
    let catalog = Arc::new(Catalog::open((*store).clone()).expect("open catalog"));
    let options = ServerOptions {
        limits: ServerLimits { max_concurrent_connections: 1, ..ServerLimits::default() },
        ..insecure_server_options()
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
    let options = ServerOptions {
        limits: ServerLimits { max_request_bytes: 32, ..ServerLimits::default() },
        ..insecure_server_options()
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
        ..insecure_server_options()
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
        ..insecure_server_options()
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
    let _heavy_test_permit = acquire_heavy_server_test_permit().await;
    let store = Arc::new(MvccStore::new());
    let catalog = Arc::new(Catalog::open((*store).clone()).expect("open catalog"));
    populate_users(&catalog, &store, 25_000);

    let options = ServerOptions {
        limits: ServerLimits { statement_timeout_ms: Some(1), ..ServerLimits::default() },
        ..insecure_server_options()
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
    let _heavy_test_permit = acquire_heavy_server_test_permit().await;
    let store = Arc::new(MvccStore::new());
    let catalog = Arc::new(Catalog::open((*store).clone()).expect("open catalog"));
    populate_users(&catalog, &store, 250_000);

    let options = ServerOptions {
        limits: ServerLimits {
            max_concurrent_queries_per_identity: Some(1),
            statement_timeout_ms: Some(5_000),
            max_scan_rows: 300_000,
            max_sort_rows: 300_000,
            max_query_result_rows: 300_000,
            ..ServerLimits::default()
        },
        ..insecure_server_options()
    };
    let bind_addr: SocketAddr = "127.0.0.1:0".parse().expect("parse socket addr");
    let server =
        start_server_with_options(bind_addr, Arc::clone(&catalog), Arc::clone(&store), options)
            .await
            .expect("start server");
    let server_addr = server.local_addr();

    let mut query_client = TcpStream::connect(server_addr).await.expect("connect query client");
    write_request(
        &mut query_client,
        &RequestFrame { request_type: RequestType::Query, sql: "SELECT id FROM users".to_string() },
    )
    .await
    .expect("send blocking query");

    tokio::task::yield_now().await;
    sleep(Duration::from_millis(10)).await;

    let mut second_client = TcpStream::connect(server_addr).await.expect("connect second client");
    let quota_error = response_to_error(
        send_request(
            &mut second_client,
            RequestFrame {
                request_type: RequestType::Query,
                sql: "SELECT id FROM users LIMIT 1".to_string(),
            },
        )
        .await,
    );
    assert_eq!(quota_error.code, ErrorCode::Quota);
    assert!(quota_error.retryable);

    drop(query_client);

    let mut admin_client = TcpStream::connect(server_addr).await.expect("connect admin client");
    let admin = response_to_admin_status(
        send_request(
            &mut admin_client,
            RequestFrame { request_type: RequestType::AdminStatus, sql: String::new() },
        )
        .await,
    );
    assert!(admin.quota_rejections >= 1);

    server.shutdown().await.expect("shutdown server");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn server_cancellation_rolls_back_active_transaction_state() {
    let _heavy_test_permit = acquire_heavy_server_test_permit().await;
    let store = Arc::new(MvccStore::new());
    let catalog = Arc::new(Catalog::open((*store).clone()).expect("open catalog"));
    populate_users(&catalog, &store, 50_000);

    let options = ServerOptions {
        limits: ServerLimits {
            statement_timeout_ms: Some(5_000),
            max_scan_rows: 100_000,
            max_sort_rows: 100_000,
            max_query_result_rows: 100_000,
            ..ServerLimits::default()
        },
        ..insecure_server_options()
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
                sql: "SELECT id FROM users ORDER BY email DESC".to_string(),
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn secure_server_rejects_unauthenticated_requests_before_any_command() {
    let store = Arc::new(MvccStore::new());
    let catalog = Arc::new(Catalog::open((*store).clone()).expect("open catalog"));
    let options = password_server_options(vec![StaticPasswordUser {
        username: "admin".to_string(),
        password: "secret".to_string(),
        role: ServerRole::Admin,
    }]);

    let bind_addr: SocketAddr = "127.0.0.1:0".parse().expect("parse socket addr");
    let server =
        start_server_with_options(bind_addr, Arc::clone(&catalog), Arc::clone(&store), options)
            .await
            .expect("start secure server");
    let server_addr = server.local_addr();

    let mut client = connect_tls_client(server_addr).await;
    let error = response_to_error(
        send_request(
            &mut client,
            RequestFrame { request_type: RequestType::Health, sql: String::new() },
        )
        .await,
    );
    assert_eq!(error.code, ErrorCode::Unauthenticated);
    assert!(error.message.contains("requires authentication"));
    assert!(
        read_response(&mut client).await.expect("read connection closure").is_none(),
        "server should close the connection after an unauthenticated request"
    );

    server.shutdown().await.expect("shutdown server");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn secure_server_rejects_authenticated_mode_without_tls() {
    let store = Arc::new(MvccStore::new());
    let catalog = Arc::new(Catalog::open((*store).clone()).expect("open catalog"));
    let options = ServerOptions {
        security: ServerSecurityOptions {
            auth: ServerAuthOptions::StaticPassword {
                users: vec![StaticPasswordUser {
                    username: "admin".to_string(),
                    password: "secret".to_string(),
                    role: ServerRole::Admin,
                }],
            },
            ..ServerSecurityOptions::default()
        },
        ..ServerOptions::default()
    };

    let bind_addr: SocketAddr = "127.0.0.1:0".parse().expect("parse socket addr");
    let err = match start_server_with_options(
        bind_addr,
        Arc::clone(&catalog),
        Arc::clone(&store),
        options,
    )
    .await
    {
        Ok(_) => panic!("auth without tls should be rejected"),
        Err(err) => err,
    };
    match err {
        ServerError::InvalidConfiguration(message) => {
            assert!(message.contains("authentication requires tls"));
        }
        other => panic!("expected invalid configuration, got {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn secure_server_rejects_invalid_password_credentials() {
    let store = Arc::new(MvccStore::new());
    let catalog = Arc::new(Catalog::open((*store).clone()).expect("open catalog"));
    let options = password_server_options(vec![StaticPasswordUser {
        username: "admin".to_string(),
        password: "secret".to_string(),
        role: ServerRole::Admin,
    }]);

    let bind_addr: SocketAddr = "127.0.0.1:0".parse().expect("parse socket addr");
    let server =
        start_server_with_options(bind_addr, Arc::clone(&catalog), Arc::clone(&store), options)
            .await
            .expect("start secure server");
    let server_addr = server.local_addr();

    let mut client = connect_tls_client(server_addr).await;
    let error = response_to_error(
        send_request(&mut client, authentication_request_with_password("admin", "wrong")).await,
    );
    assert_eq!(error.code, ErrorCode::Unauthenticated);
    assert!(error.message.contains("invalid username or password"));

    server.shutdown().await.expect("shutdown server");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn secure_server_enforces_role_based_authorization() {
    let store = Arc::new(MvccStore::new());
    let catalog = Arc::new(Catalog::open((*store).clone()).expect("open catalog"));
    execute_setup_sql(
        &catalog,
        &store,
        "CREATE TABLE users (id BIGINT NOT NULL, email TEXT NOT NULL, PRIMARY KEY (id))",
    );
    execute_setup_sql(
        &catalog,
        &store,
        "INSERT INTO users (id, email) VALUES (1, 'alice@example.com')",
    );
    let options = password_server_options(vec![
        StaticPasswordUser {
            username: "reader".to_string(),
            password: "reader-secret".to_string(),
            role: ServerRole::Reader,
        },
        StaticPasswordUser {
            username: "admin".to_string(),
            password: "admin-secret".to_string(),
            role: ServerRole::Admin,
        },
    ]);

    let bind_addr: SocketAddr = "127.0.0.1:0".parse().expect("parse socket addr");
    let server =
        start_server_with_options(bind_addr, Arc::clone(&catalog), Arc::clone(&store), options)
            .await
            .expect("start secure server");
    let server_addr = server.local_addr();

    let mut reader = connect_tls_client(server_addr).await;
    let auth = response_to_authentication(
        send_request(&mut reader, authentication_request_with_password("reader", "reader-secret"))
            .await,
    );
    assert_eq!(auth.identity, "reader");
    assert_eq!(auth.role, "reader");

    let select = response_to_query(
        send_request(
            &mut reader,
            RequestFrame {
                request_type: RequestType::Query,
                sql: "SELECT email FROM users WHERE id = 1".to_string(),
            },
        )
        .await,
    );
    assert_eq!(from_utf8(&select.rows[0][0]).expect("utf8 cell"), "alice@example.com");

    let update_error = response_to_error(
        send_request(
            &mut reader,
            RequestFrame {
                request_type: RequestType::Query,
                sql: "UPDATE users SET email = 'blocked@example.com' WHERE id = 1".to_string(),
            },
        )
        .await,
    );
    assert_eq!(update_error.code, ErrorCode::PermissionDenied);
    assert!(update_error.message.contains("role 'reader'"));

    let admin_status_error = response_to_error(
        send_request(
            &mut reader,
            RequestFrame { request_type: RequestType::AdminStatus, sql: String::new() },
        )
        .await,
    );
    assert_eq!(admin_status_error.code, ErrorCode::PermissionDenied);

    let mut admin = connect_tls_client(server_addr).await;
    let admin_auth = response_to_authentication(
        send_request(&mut admin, authentication_request_with_password("admin", "admin-secret"))
            .await,
    );
    assert_eq!(admin_auth.role, "admin");
    let admin_status = response_to_admin_status(
        send_request(
            &mut admin,
            RequestFrame { request_type: RequestType::AdminStatus, sql: String::new() },
        )
        .await,
    );
    assert!(admin_status.accepting_connections);

    server.shutdown().await.expect("shutdown server");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn secure_server_supports_static_token_authentication() {
    let store = Arc::new(MvccStore::new());
    let catalog = Arc::new(Catalog::open((*store).clone()).expect("open catalog"));
    let options = token_server_options(vec![StaticTokenPrincipal {
        label: "ingest-bot".to_string(),
        token: "opaque-token".to_string(),
        role: ServerRole::Writer,
    }]);

    let bind_addr: SocketAddr = "127.0.0.1:0".parse().expect("parse socket addr");
    let server =
        start_server_with_options(bind_addr, Arc::clone(&catalog), Arc::clone(&store), options)
            .await
            .expect("start token-auth server");
    let server_addr = server.local_addr();

    let mut client = connect_tls_client(server_addr).await;
    let auth = response_to_authentication(
        send_request(&mut client, authentication_request_with_token("opaque-token")).await,
    );
    assert_eq!(auth.identity, "ingest-bot");
    assert_eq!(auth.role, "writer");
    assert_eq!(auth.auth_scheme, "token");

    let health = response_to_health(
        send_request(
            &mut client,
            RequestFrame { request_type: RequestType::Health, sql: String::new() },
        )
        .await,
    );
    assert!(health.ok);

    server.shutdown().await.expect("shutdown server");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn secure_server_accepts_tls_connections_when_required() {
    let store = Arc::new(MvccStore::new());
    let catalog = Arc::new(Catalog::open((*store).clone()).expect("open catalog"));
    let options = password_server_options(vec![StaticPasswordUser {
        username: "admin".to_string(),
        password: "secret".to_string(),
        role: ServerRole::Admin,
    }]);

    let bind_addr: SocketAddr = "127.0.0.1:0".parse().expect("parse socket addr");
    let server =
        start_server_with_options(bind_addr, Arc::clone(&catalog), Arc::clone(&store), options)
            .await
            .expect("start tls server");
    let server_addr = server.local_addr();

    let mut client = connect_tls_client(server_addr).await;
    let auth = response_to_authentication(
        send_request(&mut client, authentication_request_with_password("admin", "secret")).await,
    );
    assert_eq!(auth.identity, "admin");
    assert_eq!(auth.role, "admin");

    let readiness = response_to_readiness(
        send_request(
            &mut client,
            RequestFrame { request_type: RequestType::Readiness, sql: String::new() },
        )
        .await,
    );
    assert!(readiness.ready);

    server.shutdown().await.expect("shutdown tls server");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tls_required_mode_rejects_missing_certificate_files() {
    let store = Arc::new(MvccStore::new());
    let catalog = Arc::new(Catalog::open((*store).clone()).expect("open catalog"));
    let options = ServerOptions {
        security: ServerSecurityOptions {
            tls: ServerTlsOptions {
                mode: ServerTlsMode::Required,
                cert_path: Some(PathBuf::from("tests/fixtures/tls/missing.crt")),
                key_path: Some(PathBuf::from(TLS_KEY_PATH)),
            },
            allow_anonymous_access: true,
            ..ServerSecurityOptions::default()
        },
        ..insecure_server_options()
    };

    let bind_addr: SocketAddr = "127.0.0.1:0".parse().expect("parse socket addr");
    let result =
        start_server_with_options(bind_addr, Arc::clone(&catalog), Arc::clone(&store), options)
            .await;
    let err = match result {
        Ok(_) => panic!("missing certificate should fail"),
        Err(err) => err,
    };
    match err {
        ServerError::Tls(message) => assert!(message.contains("missing.crt")),
        other => panic!("expected tls error, got {other:?}"),
    }
}
