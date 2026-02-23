use std::net::SocketAddr;
use std::str::from_utf8;
use std::sync::Arc;

use lsmdb::catalog::Catalog;
use lsmdb::mvcc::MvccStore;
use lsmdb::server::{
    QueryPayload, RequestFrame, RequestType, ResponseFrame, ResponsePayload, TransactionState,
    read_response, start_server, write_request,
};
use tokio::net::TcpStream;

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
