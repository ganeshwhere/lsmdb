use std::env;
use std::io::{self, Write};
use std::net::SocketAddr;
use std::time::Instant;

use lsmdb::observability::init_tracing_from_env;
use lsmdb::server::{
    QueryPayload, RequestFrame, RequestType, ResponseFrame, ResponsePayload, TransactionState,
    read_response, write_request,
};
use tokio::net::TcpStream;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    if let Err(err) = init_tracing_from_env() {
        eprintln!("warning: failed to initialize tracing: {err}");
    }

    let addr = parse_addr()?;
    let mut stream = TcpStream::connect(addr).await?;

    println!("Connected to lsmdb server at {addr}");
    println!(
        "Type SQL to execute. Meta commands: \\help, \\q, \\timing, \\explain <sql>, \\health, \\ready, \\status"
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
            match handle_meta_command(input, &mut timing_enabled, &mut stream).await? {
                ControlFlow::Continue => continue,
                ControlFlow::Break => break,
            }
        }

        let request = request_from_sql(input);
        let start = Instant::now();
        let response = send_request(&mut stream, request).await?;
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

fn parse_addr() -> Result<SocketAddr, Box<dyn std::error::Error>> {
    let mut args = env::args().skip(1);
    let mut addr = "127.0.0.1:7878".to_string();

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--addr" => {
                let Some(value) = args.next() else {
                    return Err("--addr expects a value".into());
                };
                addr = value;
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

    Ok(addr.parse()?)
}

async fn handle_meta_command(
    input: &str,
    timing_enabled: &mut bool,
    stream: &mut TcpStream,
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

async fn send_request(
    stream: &mut TcpStream,
    request: RequestFrame,
) -> Result<ResponseFrame, Box<dyn std::error::Error>> {
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
    println!("Usage: lsmdb-cli [--addr HOST:PORT]");
    println!("Meta commands:");
    println!("  \\help                 Show this help");
    println!("  \\q | \\quit            Exit CLI");
    println!("  \\timing               Toggle query timing display");
    println!("  \\explain <sql>        Print physical plan without executing SQL");
    println!("  \\health               Request liveness status");
    println!("  \\ready                Request readiness status");
    println!("  \\status               Request admin runtime diagnostics");
}
