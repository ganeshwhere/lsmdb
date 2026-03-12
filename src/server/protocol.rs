use std::io::{Cursor, Read};

use thiserror::Error;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::executor::{ExecutionResult, QueryResult, ScalarValue};

pub const PROTOCOL_VERSION: u16 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum RequestType {
    Query = 1,
    Begin = 2,
    Commit = 3,
    Rollback = 4,
    Explain = 5,
    Health = 6,
    Readiness = 7,
    AdminStatus = 8,
}

impl TryFrom<u8> for RequestType {
    type Error = ProtocolError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(RequestType::Query),
            2 => Ok(RequestType::Begin),
            3 => Ok(RequestType::Commit),
            4 => Ok(RequestType::Rollback),
            5 => Ok(RequestType::Explain),
            6 => Ok(RequestType::Health),
            7 => Ok(RequestType::Readiness),
            8 => Ok(RequestType::AdminStatus),
            other => {
                Err(ProtocolError::InvalidFrame(format!("unknown request type byte: {other}")))
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RequestFrame {
    pub request_type: RequestType,
    pub sql: String,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ResponseFrame {
    Ok(ResponsePayload),
    Err(ErrorPayload),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum ErrorCode {
    InvalidRequest = 1,
    Parse = 2,
    Validation = 3,
    Planner = 4,
    Execution = 5,
    Busy = 6,
    ResourceLimit = 7,
}

impl TryFrom<u8> for ErrorCode {
    type Error = ProtocolError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(ErrorCode::InvalidRequest),
            2 => Ok(ErrorCode::Parse),
            3 => Ok(ErrorCode::Validation),
            4 => Ok(ErrorCode::Planner),
            5 => Ok(ErrorCode::Execution),
            6 => Ok(ErrorCode::Busy),
            7 => Ok(ErrorCode::ResourceLimit),
            other => Err(ProtocolError::InvalidFrame(format!("unknown error code byte: {other}"))),
        }
    }
}

impl ErrorCode {
    pub fn as_str(self) -> &'static str {
        match self {
            ErrorCode::InvalidRequest => "INVALID_REQUEST",
            ErrorCode::Parse => "PARSE",
            ErrorCode::Validation => "VALIDATION",
            ErrorCode::Planner => "PLANNER",
            ErrorCode::Execution => "EXECUTION",
            ErrorCode::Busy => "BUSY",
            ErrorCode::ResourceLimit => "RESOURCE_LIMIT",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ErrorPayload {
    pub code: ErrorCode,
    pub message: String,
    pub retryable: bool,
}

impl ErrorPayload {
    pub fn new(code: ErrorCode, message: impl Into<String>, retryable: bool) -> Self {
        Self { code, message: message.into(), retryable }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum ResponsePayload {
    Query(QueryPayload),
    AffectedRows(u64),
    TransactionState(TransactionState),
    ExplainPlan(String),
    Health(HealthPayload),
    Readiness(ReadinessPayload),
    AdminStatus(AdminStatusPayload),
}

#[derive(Debug, Clone, PartialEq)]
pub struct QueryPayload {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<Vec<u8>>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HealthPayload {
    pub ok: bool,
    pub status: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadinessPayload {
    pub ready: bool,
    pub status: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdminStatusPayload {
    pub server_version: String,
    pub protocol_version: u16,
    pub uptime_seconds: u64,
    pub accepting_connections: bool,
    pub active_connections: u64,
    pub total_connections: u64,
    pub rejected_connections: u64,
    pub busy_requests: u64,
    pub resource_limit_requests: u64,
    pub active_memory_intensive_requests: u64,
    pub mvcc_started: u64,
    pub mvcc_committed: u64,
    pub mvcc_rolled_back: u64,
    pub mvcc_write_conflicts: u64,
    pub mvcc_active_transactions: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum TransactionState {
    Begun = 1,
    Committed = 2,
    RolledBack = 3,
}

#[derive(Debug, Error)]
pub enum ProtocolError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("invalid frame: {0}")]
    InvalidFrame(String),
    #[error("frame too large: length={length}, max={max}")]
    FrameTooLarge { length: usize, max: usize },
    #[error("utf8 decode error: {0}")]
    Utf8(#[from] std::string::FromUtf8Error),
}

pub async fn read_request<R: AsyncRead + Unpin>(
    reader: &mut R,
) -> Result<Option<RequestFrame>, ProtocolError> {
    let Some(body) = read_frame(reader, None).await? else {
        return Ok(None);
    };
    decode_request(&body).map(Some)
}

pub async fn read_request_with_limit<R: AsyncRead + Unpin>(
    reader: &mut R,
    max_body_bytes: usize,
) -> Result<Option<RequestFrame>, ProtocolError> {
    let Some(body) = read_frame(reader, Some(max_body_bytes)).await? else {
        return Ok(None);
    };
    decode_request(&body).map(Some)
}

pub async fn write_request<W: AsyncWrite + Unpin>(
    writer: &mut W,
    request: &RequestFrame,
) -> Result<(), ProtocolError> {
    let body = encode_request(request)?;
    write_frame(writer, &body).await
}

pub async fn read_response<R: AsyncRead + Unpin>(
    reader: &mut R,
) -> Result<Option<ResponseFrame>, ProtocolError> {
    let Some(body) = read_frame(reader, None).await? else {
        return Ok(None);
    };
    decode_response(&body).map(Some)
}

pub async fn write_response<W: AsyncWrite + Unpin>(
    writer: &mut W,
    response: &ResponseFrame,
) -> Result<(), ProtocolError> {
    let body = encode_response(response)?;
    write_frame(writer, &body).await
}

pub fn payload_from_execution_result(result: &ExecutionResult) -> ResponsePayload {
    match result {
        ExecutionResult::Query(query) => ResponsePayload::Query(query_to_payload(query)),
        ExecutionResult::AffectedRows(rows) => ResponsePayload::AffectedRows(*rows),
        ExecutionResult::TransactionBegun => {
            ResponsePayload::TransactionState(TransactionState::Begun)
        }
        ExecutionResult::TransactionCommitted => {
            ResponsePayload::TransactionState(TransactionState::Committed)
        }
        ExecutionResult::TransactionRolledBack => {
            ResponsePayload::TransactionState(TransactionState::RolledBack)
        }
    }
}

fn query_to_payload(query: &QueryResult) -> QueryPayload {
    let rows = query
        .rows
        .iter()
        .map(|row| row.iter().map(scalar_to_wire_bytes).collect::<Vec<_>>())
        .collect::<Vec<_>>();
    QueryPayload { columns: query.columns.clone(), rows }
}

fn scalar_to_wire_bytes(value: &ScalarValue) -> Vec<u8> {
    match value {
        ScalarValue::Integer(value) => value.to_string().into_bytes(),
        ScalarValue::BigInt(value) => value.to_string().into_bytes(),
        ScalarValue::Float(value) => value.to_string().into_bytes(),
        ScalarValue::Text(value) => value.clone().into_bytes(),
        ScalarValue::Boolean(value) => {
            if *value {
                b"true".to_vec()
            } else {
                b"false".to_vec()
            }
        }
        ScalarValue::Blob(bytes) => bytes_to_hex(bytes).into_bytes(),
        ScalarValue::Timestamp(value) => value.to_string().into_bytes(),
        ScalarValue::Null => b"NULL".to_vec(),
    }
}

fn bytes_to_hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
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

async fn read_frame<R: AsyncRead + Unpin>(
    reader: &mut R,
    max_body_bytes: Option<usize>,
) -> Result<Option<Vec<u8>>, ProtocolError> {
    let mut len_buf = [0_u8; 4];
    match reader.read_exact(&mut len_buf).await {
        Ok(_) => {}
        Err(err) if err.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(err) => return Err(ProtocolError::Io(err)),
    }

    let length = u32::from_be_bytes(len_buf) as usize;
    if length == 0 {
        return Err(ProtocolError::InvalidFrame(
            "frame length must be greater than zero".to_string(),
        ));
    }
    if let Some(max_body_bytes) = max_body_bytes {
        if length > max_body_bytes {
            let mut remaining = length;
            let mut discard_buf = [0_u8; 4096];
            while remaining > 0 {
                let chunk_len = remaining.min(discard_buf.len());
                reader.read_exact(&mut discard_buf[..chunk_len]).await?;
                remaining -= chunk_len;
            }
            return Err(ProtocolError::FrameTooLarge { length, max: max_body_bytes });
        }
    }

    let mut body = vec![0_u8; length];
    reader.read_exact(&mut body).await?;
    Ok(Some(body))
}

async fn write_frame<W: AsyncWrite + Unpin>(
    writer: &mut W,
    body: &[u8],
) -> Result<(), ProtocolError> {
    let len = u32::try_from(body.len())
        .map_err(|_| ProtocolError::InvalidFrame("frame is too large".to_string()))?;
    writer.write_all(&len.to_be_bytes()).await?;
    writer.write_all(body).await?;
    writer.flush().await?;
    Ok(())
}

fn encode_request(request: &RequestFrame) -> Result<Vec<u8>, ProtocolError> {
    let mut body = Vec::new();
    body.push(request.request_type as u8);
    body.extend(request.sql.as_bytes());
    Ok(body)
}

fn decode_request(body: &[u8]) -> Result<RequestFrame, ProtocolError> {
    let (&request_type, sql_bytes) = body
        .split_first()
        .ok_or_else(|| ProtocolError::InvalidFrame("request frame is empty".to_string()))?;
    let request_type = RequestType::try_from(request_type)?;
    let sql = String::from_utf8(sql_bytes.to_vec())?;

    Ok(RequestFrame { request_type, sql })
}

fn encode_response(response: &ResponseFrame) -> Result<Vec<u8>, ProtocolError> {
    let mut body = Vec::new();
    match response {
        ResponseFrame::Ok(payload) => {
            body.push(0_u8);
            encode_payload(payload, &mut body)?;
        }
        ResponseFrame::Err(error) => {
            body.push(1_u8);
            body.push(error.code as u8);
            body.push(u8::from(error.retryable));
            write_len_prefixed_bytes(&mut body, error.message.as_bytes())?;
        }
    }
    Ok(body)
}

fn decode_response(body: &[u8]) -> Result<ResponseFrame, ProtocolError> {
    let (&status, payload) = body
        .split_first()
        .ok_or_else(|| ProtocolError::InvalidFrame("response frame is empty".to_string()))?;

    match status {
        0 => {
            let mut cursor = Cursor::new(payload);
            let decoded_payload = decode_payload(&mut cursor)?;
            if (cursor.position() as usize) != payload.len() {
                return Err(ProtocolError::InvalidFrame(
                    "response payload has trailing bytes".to_string(),
                ));
            }
            Ok(ResponseFrame::Ok(decoded_payload))
        }
        1 => {
            let mut cursor = Cursor::new(payload);
            let code = ErrorCode::try_from(read_u8(&mut cursor)?)?;
            let retryable = read_bool(&mut cursor)?;
            let message = read_len_prefixed_string(&mut cursor)?;
            if (cursor.position() as usize) != payload.len() {
                return Err(ProtocolError::InvalidFrame(
                    "error payload has trailing bytes".to_string(),
                ));
            }
            Ok(ResponseFrame::Err(ErrorPayload { code, message, retryable }))
        }
        other => Err(ProtocolError::InvalidFrame(format!("unknown response status byte: {other}"))),
    }
}

fn encode_payload(payload: &ResponsePayload, out: &mut Vec<u8>) -> Result<(), ProtocolError> {
    match payload {
        ResponsePayload::Query(query) => {
            out.push(1_u8);
            write_u16(out, query.columns.len())?;
            for column in &query.columns {
                write_len_prefixed_bytes(out, column.as_bytes())?;
            }
            write_u32(out, query.rows.len())?;
            for row in &query.rows {
                write_u16(out, row.len())?;
                for value in row {
                    write_len_prefixed_bytes(out, value)?;
                }
            }
        }
        ResponsePayload::AffectedRows(affected) => {
            out.push(2_u8);
            out.extend(affected.to_be_bytes());
        }
        ResponsePayload::TransactionState(state) => {
            out.push(3_u8);
            out.push(*state as u8);
        }
        ResponsePayload::ExplainPlan(plan) => {
            out.push(4_u8);
            write_len_prefixed_bytes(out, plan.as_bytes())?;
        }
        ResponsePayload::Health(health) => {
            out.push(5_u8);
            out.push(u8::from(health.ok));
            write_len_prefixed_bytes(out, health.status.as_bytes())?;
        }
        ResponsePayload::Readiness(readiness) => {
            out.push(6_u8);
            out.push(u8::from(readiness.ready));
            write_len_prefixed_bytes(out, readiness.status.as_bytes())?;
        }
        ResponsePayload::AdminStatus(status) => {
            out.push(7_u8);
            write_len_prefixed_bytes(out, status.server_version.as_bytes())?;
            out.extend(status.protocol_version.to_be_bytes());
            out.extend(status.uptime_seconds.to_be_bytes());
            out.push(u8::from(status.accepting_connections));
            out.extend(status.active_connections.to_be_bytes());
            out.extend(status.total_connections.to_be_bytes());
            out.extend(status.rejected_connections.to_be_bytes());
            out.extend(status.busy_requests.to_be_bytes());
            out.extend(status.resource_limit_requests.to_be_bytes());
            out.extend(status.active_memory_intensive_requests.to_be_bytes());
            out.extend(status.mvcc_started.to_be_bytes());
            out.extend(status.mvcc_committed.to_be_bytes());
            out.extend(status.mvcc_rolled_back.to_be_bytes());
            out.extend(status.mvcc_write_conflicts.to_be_bytes());
            out.extend(status.mvcc_active_transactions.to_be_bytes());
        }
    }
    Ok(())
}

fn decode_payload(cursor: &mut Cursor<&[u8]>) -> Result<ResponsePayload, ProtocolError> {
    let payload_type = read_u8(cursor)?;
    match payload_type {
        1 => {
            let col_count = read_u16(cursor)? as usize;
            let mut columns = Vec::with_capacity(col_count);
            for _ in 0..col_count {
                columns.push(read_len_prefixed_string(cursor)?);
            }

            let row_count = read_u32(cursor)? as usize;
            let mut rows = Vec::with_capacity(row_count);
            for _ in 0..row_count {
                let cell_count = read_u16(cursor)? as usize;
                let mut row = Vec::with_capacity(cell_count);
                for _ in 0..cell_count {
                    row.push(read_len_prefixed_bytes(cursor)?);
                }
                rows.push(row);
            }
            Ok(ResponsePayload::Query(QueryPayload { columns, rows }))
        }
        2 => {
            let mut raw = [0_u8; 8];
            read_exact(cursor, &mut raw)?;
            Ok(ResponsePayload::AffectedRows(u64::from_be_bytes(raw)))
        }
        3 => {
            let state = match read_u8(cursor)? {
                1 => TransactionState::Begun,
                2 => TransactionState::Committed,
                3 => TransactionState::RolledBack,
                other => {
                    return Err(ProtocolError::InvalidFrame(format!(
                        "unknown transaction state byte: {other}"
                    )));
                }
            };
            Ok(ResponsePayload::TransactionState(state))
        }
        4 => {
            let plan = read_len_prefixed_string(cursor)?;
            Ok(ResponsePayload::ExplainPlan(plan))
        }
        5 => {
            let ok = read_bool(cursor)?;
            let status = read_len_prefixed_string(cursor)?;
            Ok(ResponsePayload::Health(HealthPayload { ok, status }))
        }
        6 => {
            let ready = read_bool(cursor)?;
            let status = read_len_prefixed_string(cursor)?;
            Ok(ResponsePayload::Readiness(ReadinessPayload { ready, status }))
        }
        7 => {
            let server_version = read_len_prefixed_string(cursor)?;
            let protocol_version = read_u16(cursor)?;
            let uptime_seconds = read_u64(cursor)?;
            let accepting_connections = read_bool(cursor)?;
            let active_connections = read_u64(cursor)?;
            let total_connections = read_u64(cursor)?;
            let rejected_connections = read_u64(cursor)?;
            let busy_requests = read_u64(cursor)?;
            let resource_limit_requests = read_u64(cursor)?;
            let active_memory_intensive_requests = read_u64(cursor)?;
            let mvcc_started = read_u64(cursor)?;
            let mvcc_committed = read_u64(cursor)?;
            let mvcc_rolled_back = read_u64(cursor)?;
            let mvcc_write_conflicts = read_u64(cursor)?;
            let mvcc_active_transactions = read_u64(cursor)?;
            Ok(ResponsePayload::AdminStatus(AdminStatusPayload {
                server_version,
                protocol_version,
                uptime_seconds,
                accepting_connections,
                active_connections,
                total_connections,
                rejected_connections,
                busy_requests,
                resource_limit_requests,
                active_memory_intensive_requests,
                mvcc_started,
                mvcc_committed,
                mvcc_rolled_back,
                mvcc_write_conflicts,
                mvcc_active_transactions,
            }))
        }
        other => {
            Err(ProtocolError::InvalidFrame(format!("unknown response payload type: {other}")))
        }
    }
}

fn write_len_prefixed_bytes(out: &mut Vec<u8>, bytes: &[u8]) -> Result<(), ProtocolError> {
    write_u32(out, bytes.len())?;
    out.extend(bytes);
    Ok(())
}

fn write_u16(out: &mut Vec<u8>, value: usize) -> Result<(), ProtocolError> {
    let value = u16::try_from(value)
        .map_err(|_| ProtocolError::InvalidFrame("value exceeds u16 range".to_string()))?;
    out.extend(value.to_be_bytes());
    Ok(())
}

fn write_u32(out: &mut Vec<u8>, value: usize) -> Result<(), ProtocolError> {
    let value = u32::try_from(value)
        .map_err(|_| ProtocolError::InvalidFrame("value exceeds u32 range".to_string()))?;
    out.extend(value.to_be_bytes());
    Ok(())
}

fn read_u8(cursor: &mut Cursor<&[u8]>) -> Result<u8, ProtocolError> {
    let mut raw = [0_u8; 1];
    read_exact(cursor, &mut raw)?;
    Ok(raw[0])
}

fn read_u16(cursor: &mut Cursor<&[u8]>) -> Result<u16, ProtocolError> {
    let mut raw = [0_u8; 2];
    read_exact(cursor, &mut raw)?;
    Ok(u16::from_be_bytes(raw))
}

fn read_u64(cursor: &mut Cursor<&[u8]>) -> Result<u64, ProtocolError> {
    let mut raw = [0_u8; 8];
    read_exact(cursor, &mut raw)?;
    Ok(u64::from_be_bytes(raw))
}

fn read_bool(cursor: &mut Cursor<&[u8]>) -> Result<bool, ProtocolError> {
    match read_u8(cursor)? {
        0 => Ok(false),
        1 => Ok(true),
        other => Err(ProtocolError::InvalidFrame(format!("invalid bool byte: {other}"))),
    }
}

fn read_u32(cursor: &mut Cursor<&[u8]>) -> Result<u32, ProtocolError> {
    let mut raw = [0_u8; 4];
    read_exact(cursor, &mut raw)?;
    Ok(u32::from_be_bytes(raw))
}

fn read_len_prefixed_string(cursor: &mut Cursor<&[u8]>) -> Result<String, ProtocolError> {
    let bytes = read_len_prefixed_bytes(cursor)?;
    String::from_utf8(bytes).map_err(ProtocolError::Utf8)
}

fn read_len_prefixed_bytes(cursor: &mut Cursor<&[u8]>) -> Result<Vec<u8>, ProtocolError> {
    let len = read_u32(cursor)? as usize;
    let mut bytes = vec![0_u8; len];
    read_exact(cursor, &mut bytes)?;
    Ok(bytes)
}

fn read_exact(cursor: &mut Cursor<&[u8]>, out: &mut [u8]) -> Result<(), ProtocolError> {
    Read::read_exact(cursor, out).map_err(|err| ProtocolError::InvalidFrame(err.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn request_round_trip() {
        let request =
            RequestFrame { request_type: RequestType::Query, sql: "SELECT 1".to_string() };

        let (mut client, mut server) = tokio::io::duplex(1024);
        write_request(&mut client, &request).await.expect("write request");
        let decoded = read_request(&mut server).await.expect("read request").expect("request");
        assert_eq!(decoded, request);
    }

    #[tokio::test]
    async fn response_round_trip() {
        let response = ResponseFrame::Ok(ResponsePayload::AffectedRows(3));
        let (mut client, mut server) = tokio::io::duplex(1024);
        write_response(&mut client, &response).await.expect("write response");
        let decoded = read_response(&mut server).await.expect("read response").expect("response");
        assert_eq!(decoded, response);
    }

    #[tokio::test]
    async fn error_response_round_trip() {
        let response = ResponseFrame::Err(ErrorPayload {
            code: ErrorCode::Busy,
            message: "server busy: retry later".to_string(),
            retryable: true,
        });
        let (mut client, mut server) = tokio::io::duplex(1024);
        write_response(&mut client, &response).await.expect("write response");
        let decoded = read_response(&mut server).await.expect("read response").expect("response");
        assert_eq!(decoded, response);
    }

    #[tokio::test]
    async fn explain_payload_round_trip() {
        let response =
            ResponseFrame::Ok(ResponsePayload::ExplainPlan("PrimaryKeyScan(users)".to_string()));
        let (mut client, mut server) = tokio::io::duplex(1024);
        write_response(&mut client, &response).await.expect("write response");
        let decoded = read_response(&mut server).await.expect("read response").expect("response");
        assert_eq!(decoded, response);
    }

    #[tokio::test]
    async fn health_payload_round_trip() {
        let response = ResponseFrame::Ok(ResponsePayload::Health(HealthPayload {
            ok: true,
            status: "ok".to_string(),
        }));
        let (mut client, mut server) = tokio::io::duplex(1024);
        write_response(&mut client, &response).await.expect("write response");
        let decoded = read_response(&mut server).await.expect("read response").expect("response");
        assert_eq!(decoded, response);
    }

    #[tokio::test]
    async fn admin_status_payload_round_trip() {
        let response = ResponseFrame::Ok(ResponsePayload::AdminStatus(AdminStatusPayload {
            server_version: "0.1.0".to_string(),
            protocol_version: PROTOCOL_VERSION,
            uptime_seconds: 42,
            accepting_connections: true,
            active_connections: 1,
            total_connections: 4,
            rejected_connections: 2,
            busy_requests: 3,
            resource_limit_requests: 1,
            active_memory_intensive_requests: 0,
            mvcc_started: 12,
            mvcc_committed: 9,
            mvcc_rolled_back: 2,
            mvcc_write_conflicts: 1,
            mvcc_active_transactions: 0,
        }));
        let (mut client, mut server) = tokio::io::duplex(2048);
        write_response(&mut client, &response).await.expect("write response");
        let decoded = read_response(&mut server).await.expect("read response").expect("response");
        assert_eq!(decoded, response);
    }

    #[tokio::test]
    async fn request_frame_limit_rejects_oversized_body() {
        let request = RequestFrame { request_type: RequestType::Query, sql: "SELECT 1".repeat(64) };

        let (mut client, mut server) = tokio::io::duplex(4096);
        write_request(&mut client, &request).await.expect("write request");
        let err = read_request_with_limit(&mut server, 16).await.expect_err("frame too large");
        assert!(matches!(err, ProtocolError::FrameTooLarge { .. }));
    }
}
