use std::env;
use std::fs::File;
use std::io::BufReader;
use std::io::{self, Write};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use lsmdb::observability::init_tracing_from_env;
use lsmdb::server::{
    QueryPayload, RequestFrame, RequestType, ResponseFrame, ResponsePayload, TransactionState,
    authentication_request_with_password, authentication_request_with_token, read_response,
    write_request,
};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;
use tokio_rustls::rustls::{ClientConfig, RootCertStore, pki_types::ServerName};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    if let Err(err) = init_tracing_from_env() {
        eprintln!("warning: failed to initialize tracing: {err}");
    }

    let options = parse_cli_options()?;
    let mut stream = connect(&options).await?;

    if let Some(auth) = &options.auth {
        authenticate(&mut *stream, auth).await?;
    }

    println!("Connected to lsmdb server at {}", options.addr);
    println!(
        "Type SQL to execute. Meta commands: \\help, \\q, \\timing, \\explain <sql>, \\health, \\ready, \\status, \\queries, \\cancel <id>"
    );

    let mut timing_enabled = false;
    let stdin = io::stdin();
    loop {
        print!("lsmdb> ");
        io::stdout().flush()?;

        let mut line = String::new();
        let bytes = stdin.read_line(&mut line)?;
        if bytes == 0 {
            println!();
            break;
        }

        let input = line.trim();
        if input.is_empty() {
            continue;
        }

        if input.starts_with('\\') {
            match handle_meta_command(input, &mut timing_enabled, &mut *stream).await? {
                ControlFlow::Continue => continue,
                ControlFlow::Break => break,
            }
        }

        let request = request_from_sql(input);
        let start = Instant::now();
        let response = send_request(&mut *stream, request).await?;
        let elapsed = start.elapsed();

        render_response(response);
        if timing_enabled {
            println!("Time: {:.3} ms", elapsed.as_secs_f64() * 1000.0);
        }
    }

    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ControlFlow {
    Continue,
    Break,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CliOptions {
    addr: SocketAddr,
    auth: Option<ClientAuth>,
    tls_ca_cert: Option<PathBuf>,
    tls_server_name: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ClientAuth {
    Password { username: String, password: String },
    Token { token: String },
}

fn parse_cli_options() -> Result<CliOptions, Box<dyn std::error::Error>> {
    let mut args = env::args().skip(1);
    let mut addr = "127.0.0.1:7878".to_string();
    let mut username = None;
    let mut password = None;
    let mut token = None;
    let mut tls_ca_cert = None;
    let mut tls_server_name = None;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--addr" => {
                let Some(value) = args.next() else {
                    return Err("--addr expects a value".into());
                };
                addr = value;
            }
            "--user" => {
                let Some(value) = args.next() else {
                    return Err("--user expects a value".into());
                };
                username = Some(value);
            }
            "--password" => {
                let Some(value) = args.next() else {
                    return Err("--password expects a value".into());
                };
                password = Some(value);
            }
            "--token" => {
                let Some(value) = args.next() else {
                    return Err("--token expects a value".into());
                };
                token = Some(value);
            }
            "--tls-ca-cert" => {
                let Some(value) = args.next() else {
                    return Err("--tls-ca-cert expects a value".into());
                };
                tls_ca_cert = Some(PathBuf::from(value));
            }
            "--tls-server-name" => {
                let Some(value) = args.next() else {
                    return Err("--tls-server-name expects a value".into());
                };
                tls_server_name = Some(value);
            }
            "--help" | "-h" => {
                print_help();
                std::process::exit(0);
            }
            other => {
                return Err(format!("unknown argument: {other}").into());
            }
        }
    }

    let auth = if let Some(token) = token {
        if username.is_some() || password.is_some() {
            return Err("--token cannot be combined with --user or --password".into());
        }
        Some(ClientAuth::Token { token })
    } else if let Some(username) = username {
        let password = match password {
            Some(password) => password,
            None => env::var("LSMDB_PASSWORD")
                .map_err(|_| "--password or LSMDB_PASSWORD is required when --user is set")?,
        };
        Some(ClientAuth::Password { username, password })
    } else if password.is_some() {
        return Err("--password requires --user".into());
    } else {
        None
    };

    Ok(CliOptions { addr: addr.parse()?, auth, tls_ca_cert, tls_server_name })
}

trait ClientIo: AsyncRead + AsyncWrite + Unpin + Send {}

impl<T> ClientIo for T where T: AsyncRead + AsyncWrite + Unpin + Send {}

async fn connect(options: &CliOptions) -> Result<Box<dyn ClientIo>, Box<dyn std::error::Error>> {
    let tcp = TcpStream::connect(options.addr).await?;
    let Some(ca_cert_path) = &options.tls_ca_cert else {
        return Ok(Box::new(tcp));
    };

    let mut root_store = RootCertStore::empty();
    let mut cert_reader = BufReader::new(File::open(ca_cert_path)?);
    let certificates = rustls_pemfile::certs(&mut cert_reader).collect::<Result<Vec<_>, _>>()?;
    if certificates.is_empty() {
        return Err(format!(
            "TLS CA bundle '{}' does not contain any certificates",
            ca_cert_path.display()
        )
        .into());
    }
    for certificate in certificates {
        root_store.add(certificate)?;
    }

    let server_name =
        options.tls_server_name.clone().unwrap_or_else(|| options.addr.ip().to_string());
    let server_name = ServerName::try_from(server_name.as_str())?.to_owned();
    let config = ClientConfig::builder().with_root_certificates(root_store).with_no_client_auth();
    let connector = TlsConnector::from(Arc::new(config));
    let tls = connector.connect(server_name, tcp).await?;
    Ok(Box::new(tls))
}

async fn authenticate<S>(
    stream: &mut S,
    auth: &ClientAuth,
) -> Result<(), Box<dyn std::error::Error>>
where
    S: AsyncRead + AsyncWrite + Unpin + ?Sized,
{
    let request = match auth {
        ClientAuth::Password { username, password } => {
            authentication_request_with_password(username.clone(), password.clone())
        }
        ClientAuth::Token { token } => authentication_request_with_token(token.clone()),
    };

    match send_request(stream, request).await? {
        ResponseFrame::Ok(ResponsePayload::Authentication(payload)) => {
            println!(
                "Authenticated as {} ({}) via {}",
                payload.identity, payload.role, payload.auth_scheme
            );
            Ok(())
        }
        ResponseFrame::Err(error) => Err(format!(
            "authentication failed [{}{}]: {}",
            error.code.as_str(),
            if error.retryable { ", retryable" } else { "" },
            error.message
        )
        .into()),
        other => Err(format!("unexpected authentication response: {other:?}").into()),
    }
}

async fn handle_meta_command(
    input: &str,
    timing_enabled: &mut bool,
    stream: &mut (impl AsyncRead + AsyncWrite + Unpin + ?Sized),
) -> Result<ControlFlow, Box<dyn std::error::Error>> {
    if input == "\\q" || input == "\\quit" {
        return Ok(ControlFlow::Break);
    }

    if input == "\\help" {
        print_help();
        return Ok(ControlFlow::Continue);
    }

    if input == "\\timing" {
        *timing_enabled = !*timing_enabled;
        println!("Timing is now {}", if *timing_enabled { "ON" } else { "OFF" });
        return Ok(ControlFlow::Continue);
    }

    if let Some(sql) = input.strip_prefix("\\explain") {
        let sql = sql.trim();
        if sql.is_empty() {
            println!("Usage: \\explain <sql>");
            return Ok(ControlFlow::Continue);
        }

        let start = Instant::now();
        let response = send_request(
            stream,
            RequestFrame { request_type: RequestType::Explain, sql: sql.to_string() },
        )
        .await?;
        let elapsed = start.elapsed();

        render_response(response);
        if *timing_enabled {
            println!("Time: {:.3} ms", elapsed.as_secs_f64() * 1000.0);
        }
        return Ok(ControlFlow::Continue);
    }

    if input == "\\health" {
        let start = Instant::now();
        let response = send_request(
            stream,
            RequestFrame { request_type: RequestType::Health, sql: String::new() },
        )
        .await?;
        let elapsed = start.elapsed();
        render_response(response);
        if *timing_enabled {
            println!("Time: {:.3} ms", elapsed.as_secs_f64() * 1000.0);
        }
        return Ok(ControlFlow::Continue);
    }

    if input == "\\ready" {
        let start = Instant::now();
        let response = send_request(
            stream,
            RequestFrame { request_type: RequestType::Readiness, sql: String::new() },
        )
        .await?;
        let elapsed = start.elapsed();
        render_response(response);
        if *timing_enabled {
            println!("Time: {:.3} ms", elapsed.as_secs_f64() * 1000.0);
        }
        return Ok(ControlFlow::Continue);
    }

    if input == "\\status" {
        let start = Instant::now();
        let response = send_request(
            stream,
            RequestFrame { request_type: RequestType::AdminStatus, sql: String::new() },
        )
        .await?;
        let elapsed = start.elapsed();
        render_response(response);
        if *timing_enabled {
            println!("Time: {:.3} ms", elapsed.as_secs_f64() * 1000.0);
        }
        return Ok(ControlFlow::Continue);
    }

    if input == "\\queries" {
        let start = Instant::now();
        let response = send_request(
            stream,
            RequestFrame { request_type: RequestType::ActiveStatements, sql: String::new() },
        )
        .await?;
        let elapsed = start.elapsed();
        render_response(response);
        if *timing_enabled {
            println!("Time: {:.3} ms", elapsed.as_secs_f64() * 1000.0);
        }
        return Ok(ControlFlow::Continue);
    }

    if let Some(statement_id) = input.strip_prefix("\\cancel") {
        let statement_id = statement_id.trim();
        if statement_id.is_empty() {
            println!("Usage: \\cancel <statement_id>");
            return Ok(ControlFlow::Continue);
        }

        let start = Instant::now();
        let response = send_request(
            stream,
            RequestFrame {
                request_type: RequestType::CancelStatement,
                sql: statement_id.to_string(),
            },
        )
        .await?;
        let elapsed = start.elapsed();
        render_response(response);
        if *timing_enabled {
            println!("Time: {:.3} ms", elapsed.as_secs_f64() * 1000.0);
        }
        return Ok(ControlFlow::Continue);
    }

    println!("Unknown command: {input}. Use \\help for available commands.");
    Ok(ControlFlow::Continue)
}

fn request_from_sql(sql: &str) -> RequestFrame {
    let normalized = sql.trim().trim_end_matches(';').trim().to_ascii_uppercase();
    let request_type = match normalized.as_str() {
        "BEGIN" | "BEGIN ISOLATION LEVEL SNAPSHOT" => RequestType::Begin,
        "COMMIT" => RequestType::Commit,
        "ROLLBACK" => RequestType::Rollback,
        _ => RequestType::Query,
    };

    RequestFrame {
        request_type,
        sql: if request_type == RequestType::Query { sql.to_string() } else { String::new() },
    }
}

async fn send_request<S>(
    stream: &mut S,
    request: RequestFrame,
) -> Result<ResponseFrame, Box<dyn std::error::Error>>
where
    S: AsyncRead + AsyncWrite + Unpin + ?Sized,
{
    write_request(stream, &request).await?;
    let response =
        read_response(stream).await?.ok_or_else(|| "server closed connection".to_string())?;
    Ok(response)
}

fn render_response(response: ResponseFrame) {
    match response {
        ResponseFrame::Ok(payload) => match payload {
            ResponsePayload::Query(query) => print_query_table(&query),
            ResponsePayload::AffectedRows(rows) => println!("{rows} rows affected"),
            ResponsePayload::TransactionState(state) => match state {
                TransactionState::Begun => println!("Transaction started"),
                TransactionState::Committed => println!("Transaction committed"),
                TransactionState::RolledBack => println!("Transaction rolled back"),
            },
            ResponsePayload::ExplainPlan(plan) => println!("{plan}"),
            ResponsePayload::Health(health) => {
                println!(
                    "health: {} ({})",
                    if health.ok { "ok" } else { "unhealthy" },
                    health.status
                )
            }
            ResponsePayload::Readiness(readiness) => {
                println!(
                    "readiness: {} ({})",
                    if readiness.ready { "ready" } else { "not-ready" },
                    readiness.status
                )
            }
            ResponsePayload::AdminStatus(status) => {
                println!("server_version: {}", status.server_version);
                println!("protocol_version: {}", status.protocol_version);
                println!("uptime_seconds: {}", status.uptime_seconds);
                println!("accepting_connections: {}", status.accepting_connections);
                println!("active_connections: {}", status.active_connections);
                println!("total_connections: {}", status.total_connections);
                println!("rejected_connections: {}", status.rejected_connections);
                println!("busy_requests: {}", status.busy_requests);
                println!("resource_limit_requests: {}", status.resource_limit_requests);
                println!("quota_rejections: {}", status.quota_rejections);
                println!("timed_out_requests: {}", status.timed_out_requests);
                println!("canceled_requests: {}", status.canceled_requests);
                println!("active_statements: {}", status.active_statements);
                println!(
                    "active_memory_intensive_requests: {}",
                    status.active_memory_intensive_requests
                );
                println!("mvcc_started: {}", status.mvcc_started);
                println!("mvcc_committed: {}", status.mvcc_committed);
                println!("mvcc_rolled_back: {}", status.mvcc_rolled_back);
                println!("mvcc_write_conflicts: {}", status.mvcc_write_conflicts);
                println!("mvcc_active_transactions: {}", status.mvcc_active_transactions);
            }
            ResponsePayload::ActiveStatements(payload) => {
                if payload.statements.is_empty() {
                    println!("No active statements");
                } else {
                    for statement in payload.statements {
                        println!("statement_id: {}", statement.statement_id);
                        println!("connection_id: {}", statement.connection_id);
                        println!("identity: {}", statement.identity);
                        println!("request_type: {}", statement.request_type);
                        println!("runtime_ms: {}", statement.runtime_ms);
                        println!("cancel_requested: {}", statement.cancel_requested);
                        println!("sql_preview: {}", statement.sql_preview);
                        println!();
                    }
                }
            }
            ResponsePayload::StatementCancellation(payload) => {
                println!("statement_id: {}", payload.statement_id);
                println!("accepted: {}", payload.accepted);
                println!("status: {}", payload.status);
            }
            ResponsePayload::Authentication(payload) => {
                println!("authenticated_identity: {}", payload.identity);
                println!("authenticated_role: {}", payload.role);
                println!("auth_scheme: {}", payload.auth_scheme);
            }
        },
        ResponseFrame::Err(error) => {
            eprintln!(
                "Error [{}{}]: {}",
                error.code.as_str(),
                if error.retryable { ", retryable" } else { "" },
                error.message
            );
        }
    }
}

fn print_query_table(query: &QueryPayload) {
    if query.columns.is_empty() {
        println!("(empty result)");
        return;
    }

    let rows = query
        .rows
        .iter()
        .map(|row| row.iter().map(|cell| cell_to_string(cell)).collect::<Vec<_>>())
        .collect::<Vec<_>>();

    let mut widths = query.columns.iter().map(|column| column.len()).collect::<Vec<_>>();
    for row in &rows {
        for (index, cell) in row.iter().enumerate() {
            if index < widths.len() {
                widths[index] = widths[index].max(cell.len());
            }
        }
    }

    print_separator(&widths);
    print_row(&query.columns, &widths);
    print_separator(&widths);
    for row in &rows {
        print_row(row, &widths);
    }
    print_separator(&widths);
    println!("{} rows", rows.len());
}

fn print_separator(widths: &[usize]) {
    print!("+");
    for width in widths {
        print!("{:-<1$}+", "-", width + 2);
    }
    println!();
}

fn print_row(cells: &[String], widths: &[usize]) {
    print!("|");
    for (index, width) in widths.iter().enumerate() {
        let cell = cells.get(index).map(String::as_str).unwrap_or("");
        print!(" {:<width$} |", cell, width = *width);
    }
    println!();
}

fn cell_to_string(bytes: &[u8]) -> String {
    match std::str::from_utf8(bytes) {
        Ok(value) => value.to_string(),
        Err(_) => bytes_to_hex(bytes),
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

fn print_help() {
    println!("Usage: lsmdb-cli [--addr HOST:PORT] [--user NAME --password VALUE] [--token VALUE]");
    println!("Meta commands:");
    println!("  \\help                 Show this help");
    println!("  \\q | \\quit            Exit CLI");
    println!("  \\timing               Toggle query timing display");
    println!("  \\explain <sql>        Print physical plan without executing SQL");
    println!("  \\health               Request liveness status");
    println!("  \\ready                Request readiness status");
    println!("  \\status               Request admin runtime diagnostics");
    println!("  \\queries              List active statements");
    println!("  \\cancel <id>          Signal cancellation for an active statement");
    println!("Auth options:");
    println!("  --user NAME           Authenticate with static username/password");
    println!("  --password VALUE      Password for --user (or set LSMDB_PASSWORD)");
    println!("  --token VALUE         Authenticate with a static token");
    println!("TLS options:");
    println!("  --tls-ca-cert PATH    Enable TLS and trust the PEM CA/cert at PATH");
    println!("  --tls-server-name N   Override the TLS server name (default: addr IP)");
}
