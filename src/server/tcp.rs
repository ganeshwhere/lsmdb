use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Instant;

use thiserror::Error;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tracing::{Instrument, debug, error, info, info_span, warn};

use crate::catalog::Catalog;
use crate::executor::{ExecutionError, ExecutionSession};
use crate::mvcc::MvccStore;
use crate::planner::{PlannerError, plan_statement};
use crate::sql::parser::{ParseError, parse_sql};
use crate::sql::validator::{ValidationError, validate_statement};

use super::protocol::{
    AdminStatusPayload, HealthPayload, PROTOCOL_VERSION, ProtocolError, ReadinessPayload,
    RequestFrame, RequestType, ResponseFrame, ResponsePayload, payload_from_execution_result,
    read_request, write_response,
};

static NEXT_CONNECTION_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Debug)]
struct ServerRuntimeState {
    started_at: Instant,
    accepting_connections: AtomicBool,
    active_connections: AtomicU64,
    total_connections: AtomicU64,
}

impl ServerRuntimeState {
    fn new() -> Self {
        Self {
            started_at: Instant::now(),
            accepting_connections: AtomicBool::new(true),
            active_connections: AtomicU64::new(0),
            total_connections: AtomicU64::new(0),
        }
    }
}

#[derive(Debug)]
struct ConnectionGuard {
    runtime_state: Arc<ServerRuntimeState>,
}

impl ConnectionGuard {
    fn new(runtime_state: Arc<ServerRuntimeState>) -> Self {
        runtime_state.active_connections.fetch_add(1, Ordering::Relaxed);
        Self { runtime_state }
    }
}

impl Drop for ConnectionGuard {
    fn drop(&mut self) {
        self.runtime_state.active_connections.fetch_sub(1, Ordering::Relaxed);
    }
}

#[derive(Debug, Error)]
pub enum ServerError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("protocol error: {0}")]
    Protocol(#[from] ProtocolError),
    #[error("accept loop task failed: {0}")]
    Join(String),
}

#[derive(Debug, Error)]
enum RequestError {
    #[error("invalid request: {0}")]
    InvalidRequest(String),
    #[error("parse error: {0}")]
    Parse(#[from] ParseError),
    #[error("validation error: {0}")]
    Validation(#[from] ValidationError),
    #[error("planner error: {0}")]
    Planner(#[from] PlannerError),
    #[error("execution error: {0}")]
    Execution(#[from] ExecutionError),
}

pub struct ServerHandle {
    local_addr: SocketAddr,
    shutdown_tx: Option<oneshot::Sender<()>>,
    task: Option<JoinHandle<Result<(), ServerError>>>,
}

impl ServerHandle {
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    pub async fn shutdown(mut self) -> Result<(), ServerError> {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }

        if let Some(task) = self.task.take() {
            match task.await {
                Ok(result) => result,
                Err(err) => Err(ServerError::Join(err.to_string())),
            }
        } else {
            Ok(())
        }
    }
}

pub async fn start_server(
    bind_addr: SocketAddr,
    catalog: Arc<Catalog>,
    store: Arc<MvccStore>,
) -> Result<ServerHandle, ServerError> {
    info!(%bind_addr, "starting tcp server");
    let listener = TcpListener::bind(bind_addr).await?;
    let local_addr = listener.local_addr()?;
    info!(%local_addr, "tcp server bound");

    let runtime_state = Arc::new(ServerRuntimeState::new());
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let task = tokio::spawn(async move {
        run_accept_loop(listener, catalog, store, runtime_state, shutdown_rx).await
    });

    Ok(ServerHandle { local_addr, shutdown_tx: Some(shutdown_tx), task: Some(task) })
}

async fn run_accept_loop(
    listener: TcpListener,
    catalog: Arc<Catalog>,
    store: Arc<MvccStore>,
    runtime_state: Arc<ServerRuntimeState>,
    mut shutdown_rx: oneshot::Receiver<()>,
) -> Result<(), ServerError> {
    loop {
        tokio::select! {
            _ = &mut shutdown_rx => {
                runtime_state.accepting_connections.store(false, Ordering::Relaxed);
                info!("tcp accept loop received shutdown signal");
                break;
            }
            accept_result = listener.accept() => {
                let (stream, peer_addr) = accept_result?;
                let connection_id = NEXT_CONNECTION_ID.fetch_add(1, Ordering::Relaxed);
                runtime_state.total_connections.fetch_add(1, Ordering::Relaxed);
                info!(connection_id, %peer_addr, "accepted tcp connection");
                let catalog = Arc::clone(&catalog);
                let store = Arc::clone(&store);
                let runtime_state = Arc::clone(&runtime_state);
                let span = info_span!("connection", connection_id, %peer_addr);
                tokio::spawn(async move {
                    if let Err(err) = handle_connection(stream, catalog, store, runtime_state).await {
                        warn!(error = %err, "connection task failed");
                    }
                }.instrument(span));
            }
        }
    }

    Ok(())
}

async fn handle_connection(
    mut stream: TcpStream,
    catalog: Arc<Catalog>,
    store: Arc<MvccStore>,
    runtime_state: Arc<ServerRuntimeState>,
) -> Result<(), ServerError> {
    let _connection_guard = ConnectionGuard::new(Arc::clone(&runtime_state));
    let mut session = ExecutionSession::new(catalog.as_ref(), store.as_ref());
    debug!("connection session created");

    loop {
        let Some(request) = read_request(&mut stream).await? else {
            debug!("client closed connection");
            return Ok(());
        };

        let request_type = request.request_type;
        let sql_len = request.sql.len();
        debug!(request_type = ?request_type, sql_len, "received request frame");

        let response =
            match execute_request(&mut session, &catalog, &store, &runtime_state, request) {
                Ok(payload) => {
                    debug!(request_type = ?request_type, "request handled successfully");
                    ResponseFrame::Ok(payload)
                }
                Err(err) => {
                    warn!(request_type = ?request_type, error = %err, "request failed");
                    ResponseFrame::Err(err.to_string())
                }
            };

        if let Err(err) = write_response(&mut stream, &response).await {
            error!(error = %err, "failed to write response");
            return Err(ServerError::Protocol(err));
        }
    }
}

fn execute_request(
    session: &mut ExecutionSession<'_>,
    catalog: &Catalog,
    store: &MvccStore,
    runtime_state: &ServerRuntimeState,
    request: RequestFrame,
) -> Result<super::protocol::ResponsePayload, RequestError> {
    debug!(request_type = ?request.request_type, "executing request");
    match request.request_type {
        RequestType::Query => {
            if request.sql.trim().is_empty() {
                return Err(RequestError::InvalidRequest(
                    "query request requires non-empty SQL payload".to_string(),
                ));
            }
            execute_sql(session, catalog, &request.sql)
        }
        RequestType::Begin => execute_sql(session, catalog, "BEGIN ISOLATION LEVEL SNAPSHOT"),
        RequestType::Commit => execute_sql(session, catalog, "COMMIT"),
        RequestType::Rollback => execute_sql(session, catalog, "ROLLBACK"),
        RequestType::Explain => explain_sql(catalog, &request.sql),
        RequestType::Health => Ok(health_payload()),
        RequestType::Readiness => Ok(readiness_payload(runtime_state)),
        RequestType::AdminStatus => Ok(admin_status_payload(store, runtime_state)),
    }
}

fn execute_sql(
    session: &mut ExecutionSession<'_>,
    catalog: &Catalog,
    sql: &str,
) -> Result<super::protocol::ResponsePayload, RequestError> {
    let statements = parse_sql(sql)?;
    let mut last_result = None;

    for statement in statements {
        validate_statement(catalog, &statement)?;
        let plan = plan_statement(catalog, &statement)?;
        let result = session.execute_plan(&plan)?;
        last_result = Some(result);
    }

    let result = last_result.ok_or_else(|| {
        RequestError::InvalidRequest("SQL payload produced no executable statement".to_string())
    })?;
    Ok(payload_from_execution_result(&result))
}

fn explain_sql(catalog: &Catalog, sql: &str) -> Result<ResponsePayload, RequestError> {
    if sql.trim().is_empty() {
        return Err(RequestError::InvalidRequest(
            "explain request requires non-empty SQL payload".to_string(),
        ));
    }

    let statements = parse_sql(sql)?;
    if statements.is_empty() {
        return Err(RequestError::InvalidRequest(
            "SQL payload produced no executable statement".to_string(),
        ));
    }

    let mut rendered = Vec::new();
    for (index, statement) in statements.into_iter().enumerate() {
        validate_statement(catalog, &statement)?;
        let plan = plan_statement(catalog, &statement)?;
        rendered.push(format!("Statement {}:\n{plan:#?}", index + 1));
    }

    Ok(ResponsePayload::ExplainPlan(rendered.join("\n\n")))
}

fn health_payload() -> ResponsePayload {
    ResponsePayload::Health(HealthPayload { ok: true, status: "ok".to_string() })
}

fn readiness_payload(runtime_state: &ServerRuntimeState) -> ResponsePayload {
    let ready = runtime_state.accepting_connections.load(Ordering::Relaxed);
    let status = if ready { "ready" } else { "draining" };
    ResponsePayload::Readiness(ReadinessPayload { ready, status: status.to_string() })
}

fn admin_status_payload(store: &MvccStore, runtime_state: &ServerRuntimeState) -> ResponsePayload {
    let metrics = store.metrics();
    ResponsePayload::AdminStatus(AdminStatusPayload {
        server_version: env!("CARGO_PKG_VERSION").to_string(),
        protocol_version: PROTOCOL_VERSION,
        uptime_seconds: runtime_state.started_at.elapsed().as_secs(),
        accepting_connections: runtime_state.accepting_connections.load(Ordering::Relaxed),
        active_connections: runtime_state.active_connections.load(Ordering::Relaxed),
        total_connections: runtime_state.total_connections.load(Ordering::Relaxed),
        mvcc_started: metrics.started,
        mvcc_committed: metrics.committed,
        mvcc_rolled_back: metrics.rolled_back,
        mvcc_write_conflicts: metrics.write_conflicts,
        mvcc_active_transactions: u64::try_from(metrics.active_transactions).unwrap_or(u64::MAX),
    })
}
