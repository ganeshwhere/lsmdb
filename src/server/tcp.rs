use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Instant;

use thiserror::Error;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, oneshot};
use tokio::task::JoinHandle;
use tracing::{Instrument, debug, error, info, info_span, warn};

use crate::catalog::Catalog;
use crate::executor::{ExecutionError, ExecutionLimits, ExecutionSession};
use crate::mvcc::MvccStore;
use crate::planner::{PhysicalPlan, PlannerError, plan_statement};
use crate::sql::parser::{ParseError, parse_sql};
use crate::sql::validator::{ValidationError, validate_statement};

use super::protocol::{
    AdminStatusPayload, ErrorCode, ErrorPayload, HealthPayload, PROTOCOL_VERSION, ProtocolError,
    ReadinessPayload, RequestFrame, RequestType, ResponseFrame, ResponsePayload,
    payload_from_execution_result, read_request_with_limit, write_response,
};

static NEXT_CONNECTION_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ServerLimits {
    pub max_concurrent_connections: usize,
    pub max_in_flight_requests_per_connection: usize,
    pub max_request_bytes: usize,
    pub max_statements_per_request: usize,
    pub max_memory_intensive_requests: usize,
    pub max_scan_rows: usize,
    pub max_sort_rows: usize,
    pub max_join_rows: usize,
    pub max_query_result_rows: usize,
}

impl Default for ServerLimits {
    fn default() -> Self {
        Self {
            max_concurrent_connections: 128,
            max_in_flight_requests_per_connection: 1,
            max_request_bytes: 256 * 1024,
            max_statements_per_request: 16,
            max_memory_intensive_requests: 8,
            max_scan_rows: 10_000,
            max_sort_rows: 10_000,
            max_join_rows: 10_000,
            max_query_result_rows: 10_000,
        }
    }
}

impl ServerLimits {
    fn validate(self) -> Result<(), ServerError> {
        for (field, value) in [
            ("max_concurrent_connections", self.max_concurrent_connections),
            ("max_in_flight_requests_per_connection", self.max_in_flight_requests_per_connection),
            ("max_request_bytes", self.max_request_bytes),
            ("max_statements_per_request", self.max_statements_per_request),
            ("max_memory_intensive_requests", self.max_memory_intensive_requests),
            ("max_scan_rows", self.max_scan_rows),
            ("max_sort_rows", self.max_sort_rows),
            ("max_join_rows", self.max_join_rows),
            ("max_query_result_rows", self.max_query_result_rows),
        ] {
            if value == 0 {
                return Err(ServerError::InvalidConfiguration(format!(
                    "server limit '{field}' must be > 0"
                )));
            }
        }

        Ok(())
    }

    fn execution_limits(self) -> ExecutionLimits {
        ExecutionLimits {
            max_scan_rows: self.max_scan_rows,
            max_sort_rows: self.max_sort_rows,
            max_join_rows: self.max_join_rows,
            max_query_result_rows: self.max_query_result_rows,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ServerOptions {
    pub limits: ServerLimits,
}

#[derive(Debug)]
struct ServerRuntimeState {
    started_at: Instant,
    accepting_connections: AtomicBool,
    active_connections: AtomicU64,
    total_connections: AtomicU64,
    rejected_connections: AtomicU64,
    busy_requests: AtomicU64,
    resource_limit_requests: AtomicU64,
    active_memory_intensive_requests: AtomicU64,
    limits: ServerLimits,
    connection_slots: Arc<Semaphore>,
    memory_intensive_slots: Arc<Semaphore>,
}

impl ServerRuntimeState {
    fn new(options: ServerOptions) -> Self {
        Self {
            started_at: Instant::now(),
            accepting_connections: AtomicBool::new(true),
            active_connections: AtomicU64::new(0),
            total_connections: AtomicU64::new(0),
            rejected_connections: AtomicU64::new(0),
            busy_requests: AtomicU64::new(0),
            resource_limit_requests: AtomicU64::new(0),
            active_memory_intensive_requests: AtomicU64::new(0),
            limits: options.limits,
            connection_slots: Arc::new(Semaphore::new(options.limits.max_concurrent_connections)),
            memory_intensive_slots: Arc::new(Semaphore::new(
                options.limits.max_memory_intensive_requests,
            )),
        }
    }

    fn try_acquire_connection(self: &Arc<Self>) -> Option<OwnedSemaphorePermit> {
        self.connection_slots.clone().try_acquire_owned().ok()
    }

    fn try_acquire_memory_intensive_slot(self: &Arc<Self>) -> Option<OwnedSemaphorePermit> {
        self.memory_intensive_slots.clone().try_acquire_owned().ok()
    }

    fn record_rejected_connection(&self) {
        self.rejected_connections.fetch_add(1, Ordering::Relaxed);
    }

    fn record_busy_request(&self) {
        self.busy_requests.fetch_add(1, Ordering::Relaxed);
    }

    fn record_resource_limit_request(&self) {
        self.resource_limit_requests.fetch_add(1, Ordering::Relaxed);
    }
}

#[derive(Debug)]
struct ConnectionGuard {
    runtime_state: Arc<ServerRuntimeState>,
    _permit: OwnedSemaphorePermit,
}

impl ConnectionGuard {
    fn new(runtime_state: Arc<ServerRuntimeState>, permit: OwnedSemaphorePermit) -> Self {
        runtime_state.active_connections.fetch_add(1, Ordering::Relaxed);
        Self { runtime_state, _permit: permit }
    }
}

impl Drop for ConnectionGuard {
    fn drop(&mut self) {
        self.runtime_state.active_connections.fetch_sub(1, Ordering::Relaxed);
    }
}

#[derive(Debug)]
struct ConnectionRequestGuard {
    _permit: OwnedSemaphorePermit,
}

#[derive(Debug)]
struct MemoryIntensiveRequestGuard {
    runtime_state: Arc<ServerRuntimeState>,
    _permit: OwnedSemaphorePermit,
}

impl MemoryIntensiveRequestGuard {
    fn new(runtime_state: Arc<ServerRuntimeState>, permit: OwnedSemaphorePermit) -> Self {
        runtime_state.active_memory_intensive_requests.fetch_add(1, Ordering::Relaxed);
        Self { runtime_state, _permit: permit }
    }
}

impl Drop for MemoryIntensiveRequestGuard {
    fn drop(&mut self) {
        self.runtime_state.active_memory_intensive_requests.fetch_sub(1, Ordering::Relaxed);
    }
}

#[derive(Debug)]
struct ConnectionContext {
    request_slots: Arc<Semaphore>,
}

impl ConnectionContext {
    fn new(limit: usize) -> Self {
        Self { request_slots: Arc::new(Semaphore::new(limit)) }
    }

    fn try_acquire_request_slot(&self) -> Option<ConnectionRequestGuard> {
        self.request_slots
            .clone()
            .try_acquire_owned()
            .ok()
            .map(|permit| ConnectionRequestGuard { _permit: permit })
    }
}

#[derive(Debug, Error)]
pub enum ServerError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("protocol error: {0}")]
    Protocol(#[from] ProtocolError),
    #[error("invalid server configuration: {0}")]
    InvalidConfiguration(String),
    #[error("accept loop task failed: {0}")]
    Join(String),
}

#[derive(Debug, Error)]
enum RequestError {
    #[error("invalid request: {0}")]
    InvalidRequest(String),
    #[error("server busy: {0}")]
    Busy(String),
    #[error("resource limit exceeded: {0}")]
    ResourceLimit(String),
    #[error("parse error: {0}")]
    Parse(#[from] ParseError),
    #[error("validation error: {0}")]
    Validation(#[from] ValidationError),
    #[error("planner error: {0}")]
    Planner(#[from] PlannerError),
    #[error("execution error: {0}")]
    Execution(#[from] ExecutionError),
}

impl RequestError {
    fn into_error_payload(self) -> ErrorPayload {
        match self {
            RequestError::InvalidRequest(message) => {
                ErrorPayload::new(ErrorCode::InvalidRequest, message, false)
            }
            RequestError::Busy(message) => ErrorPayload::new(ErrorCode::Busy, message, true),
            RequestError::ResourceLimit(message) => {
                ErrorPayload::new(ErrorCode::ResourceLimit, message, false)
            }
            RequestError::Parse(error) => {
                ErrorPayload::new(ErrorCode::Parse, error.to_string(), false)
            }
            RequestError::Validation(error) => {
                ErrorPayload::new(ErrorCode::Validation, error.to_string(), false)
            }
            RequestError::Planner(error) => {
                ErrorPayload::new(ErrorCode::Planner, error.to_string(), false)
            }
            RequestError::Execution(error) => match error {
                ExecutionError::ResourceLimitExceeded { .. } => {
                    ErrorPayload::new(ErrorCode::ResourceLimit, error.to_string(), false)
                }
                _ => ErrorPayload::new(ErrorCode::Execution, error.to_string(), false),
            },
        }
    }
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
    start_server_with_options(bind_addr, catalog, store, ServerOptions::default()).await
}

pub async fn start_server_with_options(
    bind_addr: SocketAddr,
    catalog: Arc<Catalog>,
    store: Arc<MvccStore>,
    options: ServerOptions,
) -> Result<ServerHandle, ServerError> {
    options.limits.validate()?;
    info!(%bind_addr, "starting tcp server");
    let listener = TcpListener::bind(bind_addr).await?;
    let local_addr = listener.local_addr()?;
    info!(%local_addr, "tcp server bound");

    let runtime_state = Arc::new(ServerRuntimeState::new(options));
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
                let (mut stream, peer_addr) = accept_result?;
                let connection_id = NEXT_CONNECTION_ID.fetch_add(1, Ordering::Relaxed);
                runtime_state.total_connections.fetch_add(1, Ordering::Relaxed);
                let Some(connection_permit) = runtime_state.try_acquire_connection() else {
                    runtime_state.record_rejected_connection();
                    warn!(
                        connection_id,
                        %peer_addr,
                        max_concurrent_connections = runtime_state.limits.max_concurrent_connections,
                        "rejecting connection because the server is at capacity"
                    );
                    let response = ResponseFrame::Err(ErrorPayload::new(
                        ErrorCode::Busy,
                        format!(
                            "server busy: max concurrent connections ({}) reached; retry later",
                            runtime_state.limits.max_concurrent_connections
                        ),
                        true,
                    ));
                    let _ = write_response(&mut stream, &response).await;
                    continue;
                };
                info!(connection_id, %peer_addr, "accepted tcp connection");
                let catalog = Arc::clone(&catalog);
                let store = Arc::clone(&store);
                let runtime_state = Arc::clone(&runtime_state);
                let span = info_span!("connection", connection_id, %peer_addr);
                tokio::spawn(async move {
                    if let Err(err) = handle_connection(
                        stream,
                        catalog,
                        store,
                        runtime_state,
                        connection_permit,
                    )
                    .await
                    {
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
    connection_permit: OwnedSemaphorePermit,
) -> Result<(), ServerError> {
    let _connection_guard = ConnectionGuard::new(Arc::clone(&runtime_state), connection_permit);
    let connection_context =
        ConnectionContext::new(runtime_state.limits.max_in_flight_requests_per_connection);
    let mut session = ExecutionSession::with_limits(
        catalog.as_ref(),
        store.as_ref(),
        runtime_state.limits.execution_limits(),
    );
    debug!("connection session created");

    loop {
        let request = match read_request_with_limit(
            &mut stream,
            runtime_state.limits.max_request_bytes,
        )
        .await
        {
            Ok(Some(request)) => request,
            Ok(None) => {
                debug!("client closed connection");
                return Ok(());
            }
            Err(ProtocolError::FrameTooLarge { length, max }) => {
                runtime_state.record_resource_limit_request();
                warn!(length, max, "rejecting oversized request frame");
                let response = ResponseFrame::Err(ErrorPayload::new(
                    ErrorCode::ResourceLimit,
                    format!("request frame too large: {length} bytes exceeds limit {max} bytes"),
                    false,
                ));
                let _ = write_response(&mut stream, &response).await;
                return Ok(());
            }
            Err(err) => return Err(ServerError::Protocol(err)),
        };

        let Some(_request_guard) = connection_context.try_acquire_request_slot() else {
            runtime_state.record_busy_request();
            let response = ResponseFrame::Err(ErrorPayload::new(
                ErrorCode::Busy,
                format!(
                    "server busy: max in-flight requests per connection ({}) exceeded; retry later",
                    runtime_state.limits.max_in_flight_requests_per_connection
                ),
                true,
            ));
            write_response(&mut stream, &response).await?;
            continue;
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
                    let payload = err.into_error_payload();
                    if payload.code == ErrorCode::Busy {
                        runtime_state.record_busy_request();
                    }
                    if payload.code == ErrorCode::ResourceLimit {
                        runtime_state.record_resource_limit_request();
                    }
                    ResponseFrame::Err(payload)
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
    runtime_state: &Arc<ServerRuntimeState>,
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
            execute_sql(session, catalog, runtime_state, &request.sql)
        }
        RequestType::Begin => {
            execute_sql(session, catalog, runtime_state, "BEGIN ISOLATION LEVEL SNAPSHOT")
        }
        RequestType::Commit => execute_sql(session, catalog, runtime_state, "COMMIT"),
        RequestType::Rollback => execute_sql(session, catalog, runtime_state, "ROLLBACK"),
        RequestType::Explain => explain_sql(catalog, runtime_state, &request.sql),
        RequestType::Health => Ok(health_payload()),
        RequestType::Readiness => Ok(readiness_payload(runtime_state)),
        RequestType::AdminStatus => Ok(admin_status_payload(store, runtime_state.as_ref())),
    }
}

fn execute_sql(
    session: &mut ExecutionSession<'_>,
    catalog: &Catalog,
    runtime_state: &Arc<ServerRuntimeState>,
    sql: &str,
) -> Result<super::protocol::ResponsePayload, RequestError> {
    let statements = parse_sql(sql)?;
    if statements.len() > runtime_state.limits.max_statements_per_request {
        return Err(RequestError::ResourceLimit(format!(
            "request contains {} statements, limit is {}",
            statements.len(),
            runtime_state.limits.max_statements_per_request
        )));
    }
    let mut last_result = None;

    for statement in statements {
        validate_statement(catalog, &statement)?;
        let plan = plan_statement(catalog, &statement)?;
        let _memory_guard = acquire_memory_intensive_guard(runtime_state, &plan)?;
        let result = session.execute_plan(&plan)?;
        last_result = Some(result);
    }

    let result = last_result.ok_or_else(|| {
        RequestError::InvalidRequest("SQL payload produced no executable statement".to_string())
    })?;
    Ok(payload_from_execution_result(&result))
}

fn explain_sql(
    catalog: &Catalog,
    runtime_state: &Arc<ServerRuntimeState>,
    sql: &str,
) -> Result<ResponsePayload, RequestError> {
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
    if statements.len() > runtime_state.limits.max_statements_per_request {
        return Err(RequestError::ResourceLimit(format!(
            "request contains {} statements, limit is {}",
            statements.len(),
            runtime_state.limits.max_statements_per_request
        )));
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

fn readiness_payload(runtime_state: &Arc<ServerRuntimeState>) -> ResponsePayload {
    let ready = runtime_state.accepting_connections.load(Ordering::Relaxed);
    let status = if ready { "ready" } else { "draining" };
    ResponsePayload::Readiness(ReadinessPayload { ready, status: status.to_string() })
}

fn acquire_memory_intensive_guard(
    runtime_state: &Arc<ServerRuntimeState>,
    plan: &PhysicalPlan,
) -> Result<Option<MemoryIntensiveRequestGuard>, RequestError> {
    if !plan_is_memory_intensive(plan) {
        return Ok(None);
    }

    let Some(permit) = runtime_state.try_acquire_memory_intensive_slot() else {
        return Err(RequestError::Busy(format!(
            "max memory-intensive requests ({}) reached; retry later",
            runtime_state.limits.max_memory_intensive_requests
        )));
    };

    Ok(Some(MemoryIntensiveRequestGuard::new(Arc::clone(runtime_state), permit)))
}

fn plan_is_memory_intensive(plan: &PhysicalPlan) -> bool {
    match plan {
        PhysicalPlan::SeqScan(_) | PhysicalPlan::Sort(_) | PhysicalPlan::Join(_) => true,
        PhysicalPlan::Filter(node) => plan_is_memory_intensive(&node.input),
        PhysicalPlan::Project(node) => plan_is_memory_intensive(&node.input),
        PhysicalPlan::Limit(node) => plan_is_memory_intensive(&node.input),
        _ => false,
    }
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
        rejected_connections: runtime_state.rejected_connections.load(Ordering::Relaxed),
        busy_requests: runtime_state.busy_requests.load(Ordering::Relaxed),
        resource_limit_requests: runtime_state.resource_limit_requests.load(Ordering::Relaxed),
        active_memory_intensive_requests: runtime_state
            .active_memory_intensive_requests
            .load(Ordering::Relaxed),
        mvcc_started: metrics.started,
        mvcc_committed: metrics.committed,
        mvcc_rolled_back: metrics.rolled_back,
        mvcc_write_conflicts: metrics.write_conflicts,
        mvcc_active_transactions: u64::try_from(metrics.active_transactions).unwrap_or(u64::MAX),
    })
}
