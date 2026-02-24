use lsmdb::mvcc::{MvccStore, TransactionError};

#[test]
fn snapshot_reader_sees_stable_view() {
    let store = MvccStore::new();

    let mut seed = store.begin_transaction();
    seed.put(b"user:1", b"v1").expect("seed put");
    seed.commit().expect("seed commit");

    let snapshot_reader = store.begin_transaction();

    let mut writer = store.begin_transaction();
    writer.put(b"user:1", b"v2").expect("writer put");
    writer.commit().expect("writer commit");

    assert_eq!(snapshot_reader.get(b"user:1").expect("snapshot read"), Some(b"v1".to_vec()));

    let latest_reader = store.begin_transaction();
    assert_eq!(latest_reader.get(b"user:1").expect("latest read"), Some(b"v2".to_vec()));
}

#[test]
fn write_write_conflict_aborts_second_committer() {
    let store = MvccStore::new();

    let mut tx_a = store.begin_transaction();
    let mut tx_b = store.begin_transaction();

    tx_a.put(b"counter", b"1").expect("tx_a write");
    tx_b.put(b"counter", b"2").expect("tx_b write");

    tx_a.commit().expect("tx_a commit");

    let err = tx_b.commit().expect_err("tx_b must conflict");
    assert!(matches!(err, TransactionError::WriteWriteConflict { .. }));
}

#[test]
fn deletes_are_versioned_and_visible_after_commit() {
    let store = MvccStore::new();

    let mut writer = store.begin_transaction();
    writer.put(b"k", b"value").expect("put k");
    writer.commit().expect("commit put");

    let reader_before_delete = store.begin_transaction();

    let mut deleter = store.begin_transaction();
    deleter.delete(b"k").expect("delete k");
    deleter.commit().expect("commit delete");

    assert_eq!(
        reader_before_delete.get(b"k").expect("snapshot read before delete"),
        Some(b"value".to_vec())
    );

    let reader_after_delete = store.begin_transaction();
    assert_eq!(reader_after_delete.get(b"k").expect("read after delete"), None);
}

#[test]
fn mvcc_exposes_transaction_metrics() {
    let store = MvccStore::new();

    let mut tx_commit = store.begin_transaction();
    tx_commit.put(b"m/key", b"v1").expect("tx_commit write");
    tx_commit.commit().expect("tx_commit commit");

    let mut tx_rollback = store.begin_transaction();
    tx_rollback.put(b"m/key2", b"v2").expect("tx_rollback write");
    tx_rollback.rollback();

    let mut tx_a = store.begin_transaction();
    let mut tx_b = store.begin_transaction();
    tx_a.put(b"m/conflict", b"a").expect("tx_a write");
    tx_b.put(b"m/conflict", b"b").expect("tx_b write");
    tx_a.commit().expect("tx_a commit");
    let err = tx_b.commit().expect_err("tx_b conflict");
    assert!(matches!(err, TransactionError::WriteWriteConflict { .. }));
    tx_b.rollback();

    let metrics = store.metrics();
    assert_eq!(metrics.started, 4);
    assert_eq!(metrics.committed, 2);
    assert_eq!(metrics.rolled_back, 2);
    assert_eq!(metrics.write_conflicts, 1);
    assert_eq!(metrics.active_transactions, 0);
}
