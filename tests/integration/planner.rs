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
    assert!(
        contains_primary_key_scan(&plan),
        "expected primary key scan in plan subtree, got {plan:?}"
    );
}

#[test]
fn planner_keeps_seq_scan_for_non_pk_filter() {
    let catalog = seeded_catalog();
    let statement =
        parse_statement("SELECT id, email FROM users WHERE email = 'a@b.com'").expect("parse");

    let plan = plan_statement(&catalog, &statement).expect("plan");
    assert!(
        contains_filter_over_seq_scan(&plan),
        "expected filter over seq scan in plan subtree, got {plan:?}"
    );
}

fn contains_primary_key_scan(plan: &PhysicalPlan) -> bool {
    match plan {
        PhysicalPlan::PrimaryKeyScan(_) => true,
        PhysicalPlan::Filter(filter) => contains_primary_key_scan(&filter.input),
        PhysicalPlan::Project(project) => contains_primary_key_scan(&project.input),
        PhysicalPlan::Sort(sort) => contains_primary_key_scan(&sort.input),
        PhysicalPlan::Limit(limit) => contains_primary_key_scan(&limit.input),
        PhysicalPlan::Join(join) => {
            contains_primary_key_scan(&join.left) || contains_primary_key_scan(&join.right)
        }
        _ => false,
    }
}

fn contains_filter_over_seq_scan(plan: &PhysicalPlan) -> bool {
    match plan {
        PhysicalPlan::Filter(filter) => {
            matches!(*filter.input, PhysicalPlan::SeqScan(_))
                || contains_filter_over_seq_scan(&filter.input)
        }
        PhysicalPlan::Project(project) => contains_filter_over_seq_scan(&project.input),
        PhysicalPlan::Sort(sort) => contains_filter_over_seq_scan(&sort.input),
        PhysicalPlan::Limit(limit) => contains_filter_over_seq_scan(&limit.input),
        PhysicalPlan::Join(join) => {
            contains_filter_over_seq_scan(&join.left) || contains_filter_over_seq_scan(&join.right)
        }
        _ => false,
    }
}
