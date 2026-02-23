use std::sync::{Arc, Barrier};
use std::thread;

use lsmdb::catalog::column::ColumnDescriptor;
use lsmdb::catalog::schema::ColumnType;
use lsmdb::catalog::table::TableDescriptor;
use lsmdb::catalog::{Catalog, CatalogError};
use lsmdb::mvcc::MvccStore;

fn users_table() -> TableDescriptor {
    TableDescriptor {
        name: "users".to_string(),
        columns: vec![
            ColumnDescriptor {
                name: "id".to_string(),
                column_type: ColumnType::BigInt,
                nullable: false,
                default: None,
            },
            ColumnDescriptor {
                name: "email".to_string(),
                column_type: ColumnType::Text,
                nullable: false,
                default: None,
            },
        ],
        primary_key: vec!["id".to_string()],
    }
}

#[test]
fn catalog_round_trip_persists_descriptors() {
    let store = MvccStore::new();
    let catalog = Catalog::open(store.clone()).expect("open catalog");
    catalog.create_table(users_table()).expect("create users table");

    let reopened = Catalog::open(store).expect("reopen catalog");
    let table = reopened.get_table("users").expect("users table should exist");

    assert_eq!(table.name, "users");
    assert_eq!(table.primary_key, vec!["id".to_string()]);
    assert_eq!(table.columns.len(), 2);
}

#[test]
fn concurrent_create_table_allows_only_one_winner() {
    let store = MvccStore::new();
    let barrier = Arc::new(Barrier::new(3));

    let mut handles = Vec::new();
    for _ in 0..2 {
        let store = store.clone();
        let barrier = Arc::clone(&barrier);
        handles.push(thread::spawn(move || {
            let catalog = Catalog::open(store).expect("open catalog");
            barrier.wait();
            catalog.create_table(users_table())
        }));
    }

    barrier.wait();

    let mut successes = 0;
    let mut already_exists = 0;
    for handle in handles {
        match handle.join().expect("worker should not panic") {
            Ok(()) => successes += 1,
            Err(CatalogError::TableAlreadyExists(name)) => {
                assert_eq!(name, "users");
                already_exists += 1;
            }
            Err(other) => panic!("unexpected catalog error: {other}"),
        }
    }

    assert_eq!(successes, 1);
    assert_eq!(already_exists, 1);
}
