use lsmdb::catalog::Catalog;
use lsmdb::catalog::column::ColumnDescriptor;
use lsmdb::catalog::schema::ColumnType;
use lsmdb::catalog::table::TableDescriptor;
use lsmdb::mvcc::MvccStore;
use lsmdb::planner::{PhysicalPlan, plan_statement};
use lsmdb::sql::parse_statement;

fn seeded_catalog() -> Catalog {
    let store = MvccStore::new();
    let catalog = Catalog::open(store).expect("open catalog");
    catalog
        .create_table(TableDescriptor {
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
        })
        .expect("create users table");
    catalog
}

#[test]
fn planner_uses_primary_key_scan_for_pk_equality_filter() {
    let catalog = seeded_catalog();
    let statement = parse_statement("SELECT id, email FROM users WHERE id = 7").expect("parse");

    let plan = plan_statement(&catalog, &statement).expect("plan");
    match plan {
        PhysicalPlan::PrimaryKeyScan(scan) => {
            assert_eq!(scan.table, "users");
            assert_eq!(scan.key_values.len(), 1);
            assert_eq!(scan.key_values[0].0, "id");
        }
        other => panic!("expected primary key scan, got {other:?}"),
    }
}

#[test]
fn planner_keeps_seq_scan_for_non_pk_filter() {
    let catalog = seeded_catalog();
    let statement =
        parse_statement("SELECT id, email FROM users WHERE email = 'a@b.com'").expect("parse");

    let plan = plan_statement(&catalog, &statement).expect("plan");
    let PhysicalPlan::Filter(filter) = plan else {
        panic!("expected filter");
    };
    assert!(matches!(*filter.input, PhysicalPlan::SeqScan(_)));
}
