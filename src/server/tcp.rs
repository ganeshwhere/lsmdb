use std::collections::{BTreeMap, HashMap};
use std::fs::File;
use std::io::BufReader;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpListener;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, oneshot};
use tokio::task::JoinHandle;
use tokio_rustls::TlsAcceptor;
use tokio_rustls::rustls::{ServerConfig as RustlsServerConfig, pki_types::PrivateKeyDer};
use tracing::{Instrument, debug, error, info, info_span, warn};

use crate::catalog::Catalog;
use crate::executor::governance::{ExecutionGovernance, StatementCancellation};
use crate::executor::{ExecutionError, ExecutionLimits, ExecutionSession};
use crate::mvcc::MvccStore;
use crate::planner::{PhysicalPlan, PlannerError, plan_statement};
use crate::sql::ast::Statement;
use crate::sql::parser::{ParseError, parse_sql};
use crate::sql::validator::{ValidationError, validate_statement};

use super::protocol::{
    ActiveStatementPayload, ActiveStatementsPayload, AdminStatusPayload, AuthenticationPayload,
    AuthenticationRequest, ErrorCode, ErrorPayload, HealthPayload, PROTOCOL_VERSION, ProtocolError,
    ReadinessPayload, RequestFrame, RequestType, ResponseFrame, ResponsePayload,
    StatementCancellationPayload, decode_authentication_request, payload_from_execution_result,
    read_request_with_limit, write_response,
};

static NEXT_CONNECTION_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServerRole {
    Admin,
    Writer,
    Reader,
}

impl ServerRole {
    pub fn as_str(self) -> &'static str {
        match self {
            ServerRole::Admin => "admin",
            ServerRole::Writer => "writer",
            ServerRole::Reader => "reader",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StaticPasswordUser {
    pub username: String,
    pub password: String,
    pub role: ServerRole,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StaticTokenPrincipal {
    pub label: String,
    pub token: String,
    pub role: ServerRole,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum ServerAuthOptions {
    #[default]
    Disabled,
    StaticPassword {
        users: Vec<StaticPasswordUser>,
    },
    StaticToken {
        principals: Vec<StaticTokenPrincipal>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ServerTlsMode {
    #[default]
    Disabled,
    Required,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ServerTlsOptions {
    pub mode: ServerTlsMode,
    pub cert_path: Option<PathBuf>,
    pub key_path: Option<PathBuf>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ServerSecurityOptions {
    pub auth: ServerAuthOptions,
    pub tls: ServerTlsOptions,
    pub allow_anonymous_access: bool,
}

impl ServerSecurityOptions {
    pub fn anonymous_for_local_dev() -> Self {
        Self { allow_anonymous_access: true, ..Self::default() }
    }

    fn validate(&self) -> Result<(), ServerError> {
        match &self.auth {
            ServerAuthOptions::Disabled => {}
            ServerAuthOptions::StaticPassword { users } => {
                if users.is_empty() {
                    return Err(ServerError::InvalidConfiguration(
                        "password authentication requires at least one user".to_string(),
                    ));
                }
            }
            ServerAuthOptions::StaticToken { principals } => {
                if principals.is_empty() {
                    return Err(ServerError::InvalidConfiguration(
                        "token authentication requires at least one token principal".to_string(),
                    ));
                }
            }
        }

        if !matches!(self.auth, ServerAuthOptions::Disabled)
            && self.tls.mode != ServerTlsMode::Required
        {
            return Err(ServerError::InvalidConfiguration(
                "authentication requires tls mode 'required'".to_string(),
            ));
        }
        if matches!(self.auth, ServerAuthOptions::Disabled) && !self.allow_anonymous_access {
            return Err(ServerError::InvalidConfiguration(
                "authentication is disabled; set allow_anonymous_access only for local development or tests"
                    .to_string(),
            ));
        }

        if self.tls.mode == ServerTlsMode::Required {
            if self.tls.cert_path.is_none() {
                return Err(ServerError::InvalidConfiguration(
                    "tls mode 'required' needs a certificate path".to_string(),
                ));
            }
            if self.tls.key_path.is_none() {
                return Err(ServerError::InvalidConfiguration(
                    "tls mode 'required' needs a private key path".to_string(),
                ));
            }
        }

        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ServerLimits {
    pub max_concurrent_connections: usize,
    pub max_in_flight_requests_per_connection: usize,
    pub max_request_bytes: usize,
    pub max_statements_per_request: usize,
    pub statement_timeout_ms: Option<u64>,
    pub max_memory_intensive_requests: usize,
    pub max_scan_rows: usize,
    pub max_sort_rows: usize,
    pub max_join_rows: usize,
    pub max_query_result_rows: usize,
    pub max_query_result_bytes: usize,
    pub max_concurrent_queries_per_identity: Option<usize>,
}

impl Default for ServerLimits {
    fn default() -> Self {
        Self {
            max_concurrent_connections: 128,
            max_in_flight_requests_per_connection: 1,
            max_request_bytes: 256 * 1024,
            max_statements_per_request: 16,
            statement_timeout_ms: None,
            max_memory_intensive_requests: 8,
            max_scan_rows: 10_000,
            max_sort_rows: 10_000,
            max_join_rows: 10_000,
            max_query_result_rows: 10_000,
            max_query_result_bytes: 4 * 1024 * 1024,
            max_concurrent_queries_per_identity: None,
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
            ("max_query_result_bytes", self.max_query_result_bytes),
        ] {
            if value == 0 {
                return Err(ServerError::InvalidConfiguration(format!(
                    "server limit '{field}' must be > 0"
                )));
            }
        }

        if matches!(self.statement_timeout_ms, Some(0)) {
            return Err(ServerError::InvalidConfiguration(
                "server limit 'statement_timeout_ms' must be > 0 when set".to_string(),
            ));
        }
        if matches!(self.max_concurrent_queries_per_identity, Some(0)) {
            return Err(ServerError::InvalidConfiguration(
                "server limit 'max_concurrent_queries_per_identity' must be > 0 when set"
                    .to_string(),
            ));
        }

        Ok(())
    }

    fn execution_limits(self) -> ExecutionLimits {
        ExecutionLimits {
            max_scan_rows: self.max_scan_rows,
            max_sort_rows: self.max_sort_rows,
            max_join_rows: self.max_join_rows,
            max_query_result_rows: self.max_query_result_rows,
            max_query_result_bytes: self.max_query_result_bytes,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ServerOptions {
    pub limits: ServerLimits,
    pub security: ServerSecurityOptions,
}

impl ServerOptions {
    pub fn insecure_for_local_dev() -> Self {
        Self { security: ServerSecurityOptions::anonymous_for_local_dev(), ..Self::default() }
    }
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
    quota_rejections: AtomicU64,
    timed_out_requests: AtomicU64,
    canceled_requests: AtomicU64,
    active_memory_intensive_requests: AtomicU64,
    next_statement_id: AtomicU64,
    active_statements: Mutex<BTreeMap<u64, ActiveStatementEntry>>,
    identity_query_counts: Mutex<HashMap<String, usize>>,
    limits: ServerLimits,
    security: ServerSecurityOptions,
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
            quota_rejections: AtomicU64::new(0),
            timed_out_requests: AtomicU64::new(0),
            canceled_requests: AtomicU64::new(0),
            active_memory_intensive_requests: AtomicU64::new(0),
            next_statement_id: AtomicU64::new(1),
            active_statements: Mutex::new(BTreeMap::new()),
            identity_query_counts: Mutex::new(HashMap::new()),
            limits: options.limits,
            security: options.security,
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

    fn record_quota_rejection(&self) {
        self.quota_rejections.fetch_add(1, Ordering::Relaxed);
    }

    fn record_timed_out_request(&self) {
        self.timed_out_requests.fetch_add(1, Ordering::Relaxed);
    }

    fn record_canceled_request(&self) {
        self.canceled_requests.fetch_add(1, Ordering::Relaxed);
    }

    fn begin_statement(
        self: &Arc<Self>,
        connection_id: u64,
        identity: &str,
        request_type: RequestType,
        sql: &str,
    ) -> Result<StatementExecutionGuard, RequestError> {
        let statement_id = self.next_statement_id.fetch_add(1, Ordering::Relaxed);
        let cancellation = StatementCancellation::new();
        let entry = ActiveStatementEntry {
            statement_id,
            connection_id,
            identity: identity.to_string(),
            request_type,
            sql_preview: sql_preview(sql),
            started_at: Instant::now(),
            cancellation: cancellation.clone(),
        };
        self.active_statements.lock().insert(statement_id, entry);

        Ok(StatementExecutionGuard {
            runtime_state: Arc::clone(self),
            statement_id,
            identity: identity.to_string(),
            cancellation,
        })
    }

    fn begin_identity_query(
        self: &Arc<Self>,
        identity: &str,
    ) -> Result<IdentityQueryGuard, RequestError> {
        if let Some(limit) = self.limits.max_concurrent_queries_per_identity {
            let mut counts = self.identity_query_counts.lock();
            let current = counts.get(identity).copied().unwrap_or(0);
            if current >= limit {
                drop(counts);
                return Err(RequestError::Quota(format!(
                    "identity '{identity}' exceeded concurrent query quota ({limit})"
                )));
            }
            counts.insert(identity.to_string(), current + 1);
        }

        Ok(IdentityQueryGuard { runtime_state: Arc::clone(self), identity: identity.to_string() })
    }

    fn finish_statement(&self, statement_id: u64, _identity: &str) {
        self.active_statements.lock().remove(&statement_id);
    }

    fn finish_identity_query(&self, identity: &str) {
        if self.limits.max_concurrent_queries_per_identity.is_some() {
            let mut counts = self.identity_query_counts.lock();
            if let Some(current) = counts.get_mut(identity) {
                if *current <= 1 {
                    counts.remove(identity);
                } else {
                    *current -= 1;
                }
            }
        }
    }

    fn active_statement_payloads(&self) -> Vec<ActiveStatementPayload> {
        self.active_statements
            .lock()
            .values()
            .map(|entry| ActiveStatementPayload {
                statement_id: entry.statement_id,
                connection_id: entry.connection_id,
                identity: entry.identity.clone(),
                request_type: request_type_name(entry.request_type).to_string(),
                runtime_ms: duration_to_millis(entry.started_at.elapsed()),
                cancel_requested: entry.cancellation.reason().is_some(),
                sql_preview: entry.sql_preview.clone(),
            })
            .collect()
    }

    fn cancel_statement(&self, statement_id: u64) -> StatementCancellationPayload {
        let Some(statement) = self.active_statements.lock().get(&statement_id).cloned() else {
            return StatementCancellationPayload {
                statement_id,
                accepted: false,
                status: "statement not found".to_string(),
            };
        };

        let accepted = statement.cancellation.cancel();
        StatementCancellationPayload {
            statement_id,
            accepted,
            status: if accepted {
                "cancellation signaled".to_string()
            } else {
                "cancellation was already requested".to_string()
            },
        }
    }
}

#[derive(Debug, Clone)]
struct ActiveStatementEntry {
    statement_id: u64,
    connection_id: u64,
    identity: String,
    request_type: RequestType,
    sql_preview: String,
    started_at: Instant,
    cancellation: StatementCancellation,
}

#[derive(Debug)]
struct StatementExecutionGuard {
    runtime_state: Arc<ServerRuntimeState>,
    statement_id: u64,
    identity: String,
    cancellation: StatementCancellation,
}

impl StatementExecutionGuard {
    fn governance(&self, statement_timeout_ms: Option<u64>) -> ExecutionGovernance {
        let mut governance =
            ExecutionGovernance::default().with_cancellation(self.cancellation.clone());
        if let Some(timeout_ms) = statement_timeout_ms {
            governance = governance.with_timeout(Duration::from_millis(timeout_ms));
        }
        governance
    }
}

impl Drop for StatementExecutionGuard {
    fn drop(&mut self) {
        self.runtime_state.finish_statement(self.statement_id, &self.identity);
    }
}

#[derive(Debug)]
struct IdentityQueryGuard {
    runtime_state: Arc<ServerRuntimeState>,
    identity: String,
}

impl Drop for IdentityQueryGuard {
    fn drop(&mut self) {
        self.runtime_state.finish_identity_query(&self.identity);
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
    connection_id: u64,
    principal: AuthenticatedPrincipal,
    peer_identity: String,
}

impl ConnectionContext {
    fn new(
        limit: usize,
        connection_id: u64,
        peer_identity: String,
        principal: AuthenticatedPrincipal,
    ) -> Self {
        Self {
            request_slots: Arc::new(Semaphore::new(limit)),
            connection_id,
            principal,
            peer_identity,
        }
    }

    fn try_acquire_request_slot(&self) -> Option<ConnectionRequestGuard> {
        self.request_slots
            .clone()
            .try_acquire_owned()
            .ok()
            .map(|permit| ConnectionRequestGuard { _permit: permit })
    }

    fn identity(&self) -> &str {
        &self.principal.identity
    }

    fn role(&self) -> ServerRole {
        self.principal.role
    }
}

#[derive(Debug, Clone)]
struct AuthenticatedPrincipal {
    identity: String,
    role: ServerRole,
    auth_scheme: &'static str,
}

#[derive(Debug, Error)]
pub enum ServerError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("protocol error: {0}")]
    Protocol(#[from] ProtocolError),
    #[error("TLS error: {0}")]
    Tls(String),
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
    #[error("quota exceeded: {0}")]
    Quota(String),
    #[error("unauthenticated: {0}")]
    Unauthenticated(String),
    #[error("permission denied: {0}")]
    PermissionDenied(String),
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
            RequestError::Quota(message) => ErrorPayload::new(ErrorCode::Quota, message, true),
            RequestError::Unauthenticated(message) => {
                ErrorPayload::new(ErrorCode::Unauthenticated, message, false)
            }
            RequestError::PermissionDenied(message) => {
                ErrorPayload::new(ErrorCode::PermissionDenied, message, false)
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
                ExecutionError::StatementTimedOut { .. } => {
                    ErrorPayload::new(ErrorCode::Timeout, error.to_string(), false)
                }
                ExecutionError::StatementCanceled { .. } => {
                    ErrorPayload::new(ErrorCode::Canceled, error.to_string(), false)
                }
                _ => ErrorPayload::new(ErrorCode::Execution, error.to_string(), false),
            },
        }
    }
}

fn load_tls_acceptor(options: &ServerTlsOptions) -> Result<Option<TlsAcceptor>, ServerError> {
    if options.mode == ServerTlsMode::Disabled {
        return Ok(None);
    }

    let cert_path = options.cert_path.as_ref().ok_or_else(|| {
        ServerError::InvalidConfiguration(
            "tls mode 'required' needs a certificate path".to_string(),
        )
    })?;
    let key_path = options.key_path.as_ref().ok_or_else(|| {
        ServerError::InvalidConfiguration(
            "tls mode 'required' needs a private key path".to_string(),
        )
    })?;

    let mut cert_reader = BufReader::new(File::open(cert_path).map_err(|err| {
        ServerError::Tls(format!("failed to open TLS certificate '{}': {err}", cert_path.display()))
    })?);
    let certificates =
        rustls_pemfile::certs(&mut cert_reader).collect::<Result<Vec<_>, _>>().map_err(|err| {
            ServerError::Tls(format!(
                "failed to parse TLS certificate '{}': {err}",
                cert_path.display()
            ))
        })?;
    if certificates.is_empty() {
        return Err(ServerError::Tls(format!(
            "TLS certificate '{}' did not contain any certificate entries",
            cert_path.display()
        )));
    }

    let mut key_reader = BufReader::new(File::open(key_path).map_err(|err| {
        ServerError::Tls(format!("failed to open TLS private key '{}': {err}", key_path.display()))
    })?);
    let private_key = rustls_pemfile::private_key(&mut key_reader)
        .map_err(|err| {
            ServerError::Tls(format!(
                "failed to parse TLS private key '{}': {err}",
                key_path.display()
            ))
        })?
        .ok_or_else(|| {
            ServerError::Tls(format!(
                "TLS private key '{}' did not contain a supported key",
                key_path.display()
            ))
        })?;

    let config = build_tls_server_config(certificates, private_key)?;
    Ok(Some(TlsAcceptor::from(Arc::new(config))))
}

fn build_tls_server_config(
    certificates: Vec<tokio_rustls::rustls::pki_types::CertificateDer<'static>>,
    private_key: PrivateKeyDer<'static>,
) -> Result<RustlsServerConfig, ServerError> {
    RustlsServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certificates, private_key)
        .map_err(|err| ServerError::Tls(format!("failed to build TLS server config: {err}")))
}

async fn perform_authentication_handshake<S>(
    stream: &mut S,
    runtime_state: &Arc<ServerRuntimeState>,
    connection_id: u64,
    peer_identity: &str,
) -> Result<Option<AuthenticatedPrincipal>, ServerError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    match &runtime_state.security.auth {
        ServerAuthOptions::Disabled => Ok(Some(AuthenticatedPrincipal {
            identity: peer_identity.to_string(),
            role: ServerRole::Admin,
            auth_scheme: "disabled",
        })),
        auth_options => {
            let request = match read_request_with_limit(
                stream,
                runtime_state.limits.max_request_bytes,
            )
            .await
            {
                Ok(Some(request)) => request,
                Ok(None) => return Ok(None),
                Err(ProtocolError::FrameTooLarge { length, max }) => {
                    runtime_state.record_resource_limit_request();
                    let response = ResponseFrame::Err(ErrorPayload::new(
                        ErrorCode::ResourceLimit,
                        format!(
                            "authentication frame too large: {length} bytes exceeds limit {max} bytes"
                        ),
                        false,
                    ));
                    let _ = write_response(stream, &response).await;
                    return Ok(None);
                }
                Err(err) => return Err(ServerError::Protocol(err)),
            };

            if request.request_type != RequestType::Authenticate {
                warn!(
                    connection_id,
                    peer_identity,
                    request_type = ?request.request_type,
                    "rejecting unauthenticated request before session authentication"
                );
                let response = ResponseFrame::Err(ErrorPayload::new(
                    ErrorCode::Unauthenticated,
                    "secure mode requires authentication before any other request".to_string(),
                    false,
                ));
                let _ = write_response(stream, &response).await;
                return Ok(None);
            }

            let auth_request = match decode_authentication_request(&request.sql) {
                Ok(request) => request,
                Err(message) => {
                    warn!(connection_id, peer_identity, error = %message, "invalid authentication payload");
                    let response = ResponseFrame::Err(ErrorPayload::new(
                        ErrorCode::InvalidRequest,
                        message,
                        false,
                    ));
                    let _ = write_response(stream, &response).await;
                    return Ok(None);
                }
            };

            match authenticate_principal(auth_options, auth_request) {
                Ok(principal) => {
                    info!(
                        connection_id,
                        peer_identity,
                        identity = %principal.identity,
                        role = principal.role.as_str(),
                        auth_scheme = principal.auth_scheme,
                        "authenticated connection"
                    );
                    let response =
                        ResponseFrame::Ok(ResponsePayload::Authentication(AuthenticationPayload {
                            identity: principal.identity.clone(),
                            role: principal.role.as_str().to_string(),
                            auth_scheme: principal.auth_scheme.to_string(),
                        }));
                    write_response(stream, &response).await?;
                    Ok(Some(principal))
                }
                Err(err) => {
                    warn!(connection_id, peer_identity, error = %err, "authentication failed");
                    let response = ResponseFrame::Err(err.into_error_payload());
                    let _ = write_response(stream, &response).await;
                    Ok(None)
                }
            }
        }
    }
}

fn authenticate_principal(
    auth_options: &ServerAuthOptions,
    request: AuthenticationRequest,
) -> Result<AuthenticatedPrincipal, RequestError> {
    match (auth_options, request) {
        (
            ServerAuthOptions::StaticPassword { users },
            AuthenticationRequest::Password { username, password },
        ) => {
            let user = users
                .iter()
                .find(|user| user.username == username && user.password == password)
                .ok_or_else(|| {
                    RequestError::Unauthenticated(
                        "invalid username or password for secure server".to_string(),
                    )
                })?;
            Ok(AuthenticatedPrincipal {
                identity: user.username.clone(),
                role: user.role,
                auth_scheme: "password",
            })
        }
        (ServerAuthOptions::StaticToken { principals }, AuthenticationRequest::Token { token }) => {
            let principal =
                principals.iter().find(|principal| principal.token == token).ok_or_else(|| {
                    RequestError::Unauthenticated("invalid token for secure server".to_string())
                })?;
            Ok(AuthenticatedPrincipal {
                identity: principal.label.clone(),
                role: principal.role,
                auth_scheme: "token",
            })
        }
        (ServerAuthOptions::StaticPassword { .. }, AuthenticationRequest::Token { .. }) => {
            Err(RequestError::Unauthenticated(
                "secure server expects password authentication".to_string(),
            ))
        }
        (ServerAuthOptions::StaticToken { .. }, AuthenticationRequest::Password { .. }) => Err(
            RequestError::Unauthenticated("secure server expects token authentication".to_string()),
        ),
        (ServerAuthOptions::Disabled, _) => Ok(AuthenticatedPrincipal {
            identity: "anonymous".to_string(),
            role: ServerRole::Admin,
            auth_scheme: "disabled",
        }),
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
    options.security.validate()?;
    let tls_acceptor = load_tls_acceptor(&options.security.tls)?;
    info!(%bind_addr, "starting tcp server");
    let listener = TcpListener::bind(bind_addr).await?;
    let local_addr = listener.local_addr()?;
    info!(%local_addr, "tcp server bound");

    let runtime_state = Arc::new(ServerRuntimeState::new(options));
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let task = tokio::spawn(async move {
        run_accept_loop(listener, catalog, store, runtime_state, tls_acceptor, shutdown_rx).await
    });

    Ok(ServerHandle { local_addr, shutdown_tx: Some(shutdown_tx), task: Some(task) })
}

async fn run_accept_loop(
    listener: TcpListener,
    catalog: Arc<Catalog>,
    store: Arc<MvccStore>,
    runtime_state: Arc<ServerRuntimeState>,
    tls_acceptor: Option<TlsAcceptor>,
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
                let peer_identity = peer_addr.ip().to_string();
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
                let tls_acceptor = tls_acceptor.clone();
                let span = info_span!("connection", connection_id, %peer_addr);
                tokio::spawn(async move {
                    let result = if let Some(acceptor) = tls_acceptor {
                        match acceptor.accept(stream).await {
                            Ok(tls_stream) => {
                                handle_connection(
                                    tls_stream,
                                    catalog,
                                    store,
                                    runtime_state,
                                    connection_permit,
                                    connection_id,
                                    peer_identity,
                                )
                                .await
                            }
                            Err(err) => Err(ServerError::Tls(err.to_string())),
                        }
                    } else {
                        handle_connection(
                            stream,
                            catalog,
                            store,
                            runtime_state,
                            connection_permit,
                            connection_id,
                            peer_identity,
                        )
                        .await
                    };

                    if let Err(err) = result {
                        warn!(error = %err, "connection task failed");
                    }
                }.instrument(span));
            }
        }
    }

    Ok(())
}

async fn handle_connection<S>(
    mut stream: S,
    catalog: Arc<Catalog>,
    store: Arc<MvccStore>,
    runtime_state: Arc<ServerRuntimeState>,
    connection_permit: OwnedSemaphorePermit,
    connection_id: u64,
    peer_identity: String,
) -> Result<(), ServerError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let _connection_guard = ConnectionGuard::new(Arc::clone(&runtime_state), connection_permit);
    let Some(principal) = perform_authentication_handshake(
        &mut stream,
        &runtime_state,
        connection_id,
        &peer_identity,
    )
    .await?
    else {
        return Ok(());
    };
    let connection_context = ConnectionContext::new(
        runtime_state.limits.max_in_flight_requests_per_connection,
        connection_id,
        peer_identity,
        principal,
    );
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

        let identity_query_guard =
            if matches!(request_type, RequestType::Query | RequestType::Explain) {
                match runtime_state.begin_identity_query(connection_context.identity()) {
                    Ok(guard) => Some(guard),
                    Err(err) => {
                        warn!(request_type = ?request_type, error = %err, "request failed");
                        let payload = err.into_error_payload();
                        if payload.code == ErrorCode::Quota {
                            runtime_state.record_quota_rejection();
                        }
                        let response = ResponseFrame::Err(payload);
                        if let Err(err) = write_response(&mut stream, &response).await {
                            error!(error = %err, "failed to write response");
                            return Err(ServerError::Protocol(err));
                        }
                        continue;
                    }
                }
            } else {
                None
            };

        let response = match execute_request(
            &mut session,
            &catalog,
            &store,
            &runtime_state,
            &connection_context,
            request,
        ) {
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
                if payload.code == ErrorCode::Quota {
                    runtime_state.record_quota_rejection();
                }
                if payload.code == ErrorCode::Timeout {
                    runtime_state.record_timed_out_request();
                }
                if payload.code == ErrorCode::Canceled {
                    runtime_state.record_canceled_request();
                }
                ResponseFrame::Err(payload)
            }
        };

        if let Err(err) = write_response(&mut stream, &response).await {
            error!(error = %err, "failed to write response");
            return Err(ServerError::Protocol(err));
        }
        drop(identity_query_guard);
    }
}

fn execute_request(
    session: &mut ExecutionSession<'_>,
    catalog: &Catalog,
    store: &MvccStore,
    runtime_state: &Arc<ServerRuntimeState>,
    connection_context: &ConnectionContext,
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
            let statement = runtime_state.begin_statement(
                connection_context.connection_id,
                connection_context.identity(),
                RequestType::Query,
                &request.sql,
            )?;
            execute_sql(
                session,
                catalog,
                runtime_state,
                connection_context,
                &request.sql,
                &statement.governance(runtime_state.limits.statement_timeout_ms),
            )
        }
        RequestType::Begin => execute_sql(
            session,
            catalog,
            runtime_state,
            connection_context,
            "BEGIN ISOLATION LEVEL SNAPSHOT",
            &ExecutionGovernance::default(),
        ),
        RequestType::Commit => execute_sql(
            session,
            catalog,
            runtime_state,
            connection_context,
            "COMMIT",
            &ExecutionGovernance::default(),
        ),
        RequestType::Rollback => execute_sql(
            session,
            catalog,
            runtime_state,
            connection_context,
            "ROLLBACK",
            &ExecutionGovernance::default(),
        ),
        RequestType::Explain => {
            let statement = runtime_state.begin_statement(
                connection_context.connection_id,
                connection_context.identity(),
                RequestType::Explain,
                &request.sql,
            )?;
            explain_sql(
                catalog,
                runtime_state,
                connection_context,
                &request.sql,
                &statement.governance(runtime_state.limits.statement_timeout_ms),
            )
        }
        RequestType::Health => Ok(health_payload()),
        RequestType::Readiness => Ok(readiness_payload(runtime_state)),
        RequestType::AdminStatus => {
            authorize_admin_request(connection_context, "admin status")?;
            Ok(admin_status_payload(store, runtime_state.as_ref()))
        }
        RequestType::ActiveStatements => {
            authorize_admin_request(connection_context, "active statements")?;
            Ok(ResponsePayload::ActiveStatements(ActiveStatementsPayload {
                statements: runtime_state.active_statement_payloads(),
            }))
        }
        RequestType::CancelStatement => {
            authorize_admin_request(connection_context, "statement cancellation")?;
            cancel_statement(runtime_state, &request.sql)
        }
        RequestType::Authenticate => {
            Err(RequestError::InvalidRequest("connection is already authenticated".to_string()))
        }
    }
}

fn execute_sql(
    session: &mut ExecutionSession<'_>,
    catalog: &Catalog,
    runtime_state: &Arc<ServerRuntimeState>,
    connection_context: &ConnectionContext,
    sql: &str,
    governance: &ExecutionGovernance,
) -> Result<super::protocol::ResponsePayload, RequestError> {
    governance.checkpoint()?;
    let statements = parse_sql(sql)?;
    authorize_sql_statements(connection_context, &statements)?;
    if statements.len() > runtime_state.limits.max_statements_per_request {
        return Err(RequestError::ResourceLimit(format!(
            "request contains {} statements, limit is {}",
            statements.len(),
            runtime_state.limits.max_statements_per_request
        )));
    }
    let mut last_result = None;

    for statement in statements {
        governance.checkpoint()?;
        validate_statement(catalog, &statement)?;
        governance.checkpoint()?;
        let plan = plan_statement(catalog, &statement)?;
        let _memory_guard = acquire_memory_intensive_guard(runtime_state, &plan)?;
        let result = match session.execute_plan_with_governance(&plan, governance) {
            Ok(result) => result,
            Err(err) => {
                if matches!(
                    err,
                    ExecutionError::StatementTimedOut { .. }
                        | ExecutionError::StatementCanceled { .. }
                ) {
                    session.abort_active_transaction();
                }
                return Err(RequestError::Execution(err));
            }
        };
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
    connection_context: &ConnectionContext,
    sql: &str,
    governance: &ExecutionGovernance,
) -> Result<ResponsePayload, RequestError> {
    governance.checkpoint()?;
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
    authorize_sql_statements(connection_context, &statements)?;
    if statements.len() > runtime_state.limits.max_statements_per_request {
        return Err(RequestError::ResourceLimit(format!(
            "request contains {} statements, limit is {}",
            statements.len(),
            runtime_state.limits.max_statements_per_request
        )));
    }

    let mut rendered = Vec::new();
    for (index, statement) in statements.into_iter().enumerate() {
        governance.checkpoint()?;
        validate_statement(catalog, &statement)?;
        governance.checkpoint()?;
        let plan = plan_statement(catalog, &statement)?;
        rendered.push(format!("Statement {}:\n{plan:#?}", index + 1));
    }

    Ok(ResponsePayload::ExplainPlan(rendered.join("\n\n")))
}

fn health_payload() -> ResponsePayload {
    ResponsePayload::Health(HealthPayload { ok: true, status: "ok".to_string() })
}

fn cancel_statement(
    runtime_state: &Arc<ServerRuntimeState>,
    statement_id: &str,
) -> Result<ResponsePayload, RequestError> {
    let statement_id = statement_id.trim().parse::<u64>().map_err(|_| {
        RequestError::InvalidRequest(
            "cancel request requires a numeric statement id in the SQL payload".to_string(),
        )
    })?;
    Ok(ResponsePayload::StatementCancellation(runtime_state.cancel_statement(statement_id)))
}

fn readiness_payload(runtime_state: &Arc<ServerRuntimeState>) -> ResponsePayload {
    let ready = runtime_state.accepting_connections.load(Ordering::Relaxed);
    let status = if ready { "ready" } else { "draining" };
    ResponsePayload::Readiness(ReadinessPayload { ready, status: status.to_string() })
}

fn authorize_admin_request(
    connection_context: &ConnectionContext,
    operation: &str,
) -> Result<(), RequestError> {
    if connection_context.role() == ServerRole::Admin {
        return Ok(());
    }

    warn!(
        connection_id = connection_context.connection_id,
        peer_identity = %connection_context.peer_identity,
        identity = %connection_context.identity(),
        role = connection_context.role().as_str(),
        operation,
        "authorization denied for admin-only request"
    );
    Err(RequestError::PermissionDenied(format!(
        "role '{}' cannot access {operation}",
        connection_context.role().as_str()
    )))
}

fn authorize_sql_statements(
    connection_context: &ConnectionContext,
    statements: &[Statement],
) -> Result<(), RequestError> {
    for statement in statements {
        if role_allows_statement(connection_context.role(), statement) {
            continue;
        }

        warn!(
            connection_id = connection_context.connection_id,
            peer_identity = %connection_context.peer_identity,
            identity = %connection_context.identity(),
            role = connection_context.role().as_str(),
            statement_kind = statement_kind(statement),
            "authorization denied for SQL statement"
        );
        return Err(RequestError::PermissionDenied(format!(
            "role '{}' cannot execute {} statements",
            connection_context.role().as_str(),
            statement_kind(statement)
        )));
    }

    Ok(())
}

fn role_allows_statement(role: ServerRole, statement: &Statement) -> bool {
    match role {
        ServerRole::Admin => true,
        ServerRole::Writer => matches!(
            statement,
            Statement::Insert(_)
                | Statement::Select(_)
                | Statement::Update(_)
                | Statement::Delete(_)
                | Statement::Begin(_)
                | Statement::Commit
                | Statement::Rollback
        ),
        ServerRole::Reader => matches!(
            statement,
            Statement::Select(_) | Statement::Begin(_) | Statement::Commit | Statement::Rollback
        ),
    }
}

fn statement_kind(statement: &Statement) -> &'static str {
    match statement {
        Statement::CreateTable(_) => "CREATE TABLE",
        Statement::DropTable(_) => "DROP TABLE",
        Statement::Insert(_) => "INSERT",
        Statement::Select(_) => "SELECT",
        Statement::Update(_) => "UPDATE",
        Statement::Delete(_) => "DELETE",
        Statement::Begin(_) => "BEGIN",
        Statement::Commit => "COMMIT",
        Statement::Rollback => "ROLLBACK",
    }
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
        quota_rejections: runtime_state.quota_rejections.load(Ordering::Relaxed),
        timed_out_requests: runtime_state.timed_out_requests.load(Ordering::Relaxed),
        canceled_requests: runtime_state.canceled_requests.load(Ordering::Relaxed),
        active_statements: u64::try_from(runtime_state.active_statements.lock().len())
            .unwrap_or(u64::MAX),
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

fn request_type_name(request_type: RequestType) -> &'static str {
    match request_type {
        RequestType::Query => "QUERY",
        RequestType::Begin => "BEGIN",
        RequestType::Commit => "COMMIT",
        RequestType::Rollback => "ROLLBACK",
        RequestType::Explain => "EXPLAIN",
        RequestType::Health => "HEALTH",
        RequestType::Readiness => "READINESS",
        RequestType::AdminStatus => "ADMIN_STATUS",
        RequestType::ActiveStatements => "ACTIVE_STATEMENTS",
        RequestType::CancelStatement => "CANCEL_STATEMENT",
        RequestType::Authenticate => "AUTHENTICATE",
    }
}

fn sql_preview(sql: &str) -> String {
    const MAX_PREVIEW_CHARS: usize = 160;

    let mut preview = sql.split_whitespace().collect::<Vec<_>>().join(" ");
    if preview.chars().count() > MAX_PREVIEW_CHARS {
        preview = preview.chars().take(MAX_PREVIEW_CHARS).collect::<String>();
        preview.push_str("...");
    }
    preview
}

fn duration_to_millis(duration: Duration) -> u64 {
    let millis = duration.as_millis();
    u64::try_from(millis).unwrap_or(u64::MAX)
}
