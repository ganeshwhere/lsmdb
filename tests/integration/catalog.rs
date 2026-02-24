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

#[test]
fn concurrent_drop_table_allows_only_one_winner() {
    let store = MvccStore::new();
    let catalog = Catalog::open(store.clone()).expect("open catalog");
    catalog.create_table(users_table()).expect("seed users table");

    let barrier = Arc::new(Barrier::new(3));
    let mut handles = Vec::new();
    for _ in 0..2 {
        let store = store.clone();
        let barrier = Arc::clone(&barrier);
        handles.push(thread::spawn(move || {
            let catalog = Catalog::open(store).expect("open catalog");
            barrier.wait();
            catalog.drop_table("users")
        }));
    }

    barrier.wait();

    let mut successes = 0;
    let mut not_found = 0;
    for handle in handles {
        match handle.join().expect("worker should not panic") {
            Ok(()) => successes += 1,
            Err(CatalogError::TableNotFound(name)) => {
                assert_eq!(name, "users");
                not_found += 1;
            }
            Err(other) => panic!("unexpected catalog error: {other}"),
        }
    }

    assert_eq!(successes, 1);
    assert_eq!(not_found, 1);

    let reopened = Catalog::open(store).expect("reopen catalog");
    assert!(reopened.get_table("users").is_none());
}

#[test]
fn concurrent_drop_and_create_table_produces_consistent_final_state() {
    let store = MvccStore::new();
    let catalog = Catalog::open(store.clone()).expect("open catalog");
    catalog.create_table(users_table()).expect("seed users table");

    let barrier = Arc::new(Barrier::new(3));

    let drop_handle = {
        let store = store.clone();
        let barrier = Arc::clone(&barrier);
        thread::spawn(move || {
            let catalog = Catalog::open(store).expect("open catalog for drop");
            barrier.wait();
            catalog.drop_table("users")
        })
    };

    let create_handle = {
        let store = store.clone();
        let barrier = Arc::clone(&barrier);
        thread::spawn(move || {
            let catalog = Catalog::open(store).expect("open catalog for create");
            barrier.wait();
            catalog.create_table(users_table())
        })
    };

    barrier.wait();

    let drop_result = drop_handle.join().expect("drop worker should not panic");
    let create_result = create_handle.join().expect("create worker should not panic");

    assert!(drop_result.is_ok(), "drop should always succeed in this race");
    let create_succeeded = match create_result {
        Ok(()) => true,
        Err(CatalogError::TableAlreadyExists(_)) => false,
        Err(other) => panic!("unexpected catalog create result: {other}"),
    };

    let reopened = Catalog::open(store).expect("reopen catalog");
    let exists = reopened.get_table("users").is_some();

    if create_succeeded {
        assert!(exists, "successful concurrent create must persist table");
    } else {
        assert!(!exists, "already-exists response means create did not re-add table");
    }
}
