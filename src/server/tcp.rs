use std::net::SocketAddr;
use std::sync::Arc;

use thiserror::Error;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

use crate::catalog::Catalog;
use crate::executor::{ExecutionError, ExecutionSession};
use crate::mvcc::MvccStore;
use crate::planner::{PlannerError, plan_statement};
use crate::sql::parser::{ParseError, parse_sql};
use crate::sql::validator::{ValidationError, validate_statement};

use super::protocol::{
    ProtocolError, RequestFrame, RequestType, ResponseFrame, payload_from_execution_result,
    read_request, write_response,
};

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
    let listener = TcpListener::bind(bind_addr).await?;
    let local_addr = listener.local_addr()?;

    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let task =
        tokio::spawn(async move { run_accept_loop(listener, catalog, store, shutdown_rx).await });

    Ok(ServerHandle { local_addr, shutdown_tx: Some(shutdown_tx), task: Some(task) })
}

async fn run_accept_loop(
    listener: TcpListener,
    catalog: Arc<Catalog>,
    store: Arc<MvccStore>,
    mut shutdown_rx: oneshot::Receiver<()>,
) -> Result<(), ServerError> {
    loop {
        tokio::select! {
            _ = &mut shutdown_rx => {
                break;
            }
            accept_result = listener.accept() => {
                let (stream, _) = accept_result?;
                let catalog = Arc::clone(&catalog);
                let store = Arc::clone(&store);
                tokio::spawn(async move {
                    if let Err(err) = handle_connection(stream, catalog, store).await {
                        eprintln!("connection error: {err}");
                    }
                });
            }
        }
    }

    Ok(())
}

async fn handle_connection(
    mut stream: TcpStream,
    catalog: Arc<Catalog>,
    store: Arc<MvccStore>,
) -> Result<(), ServerError> {
    let mut session = ExecutionSession::new(catalog.as_ref(), store.as_ref());

    loop {
        let Some(request) = read_request(&mut stream).await? else {
            return Ok(());
        };

        let response = match execute_request(&mut session, &catalog, request) {
            Ok(payload) => ResponseFrame::Ok(payload),
            Err(err) => ResponseFrame::Err(err.to_string()),
        };

        write_response(&mut stream, &response).await?;
    }
}

fn execute_request(
    session: &mut ExecutionSession<'_>,
    catalog: &Catalog,
    request: RequestFrame,
) -> Result<super::protocol::ResponsePayload, RequestError> {
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
