use lsmdb::catalog::Catalog;
use lsmdb::catalog::column::ColumnDescriptor;
use lsmdb::catalog::schema::ColumnType;
use lsmdb::catalog::table::TableDescriptor;
use lsmdb::mvcc::MvccStore;
use lsmdb::sql::{Statement, parse_sql, parse_statement, validate_statement, validate_statements};

fn seeded_catalog() -> Catalog {
    let store = MvccStore::new();
    let catalog = Catalog::open(store).expect("open catalog");
    let users = TableDescriptor {
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
            ColumnDescriptor {
                name: "active".to_string(),
                column_type: ColumnType::Boolean,
                nullable: false,
                default: None,
            },
        ],
        primary_key: vec!["id".to_string()],
    };
    catalog.create_table(users).expect("create users table");
    catalog
}

#[test]
fn parses_supported_statement_forms() {
    let sql = r#"
        CREATE TABLE accounts (
            id BIGINT NOT NULL,
            email TEXT NOT NULL,
            active BOOLEAN DEFAULT true,
            PRIMARY KEY (id)
        );
        DROP TABLE accounts;
        INSERT INTO users (id, email, active) VALUES (1, 'a@b.com', true);
        SELECT id, email FROM users WHERE active = true ORDER BY email DESC LIMIT 25;
        UPDATE users SET email = 'x@y.com' WHERE id = 1;
        DELETE FROM users WHERE id = 1;
        BEGIN ISOLATION LEVEL SNAPSHOT;
        COMMIT;
        ROLLBACK;
    "#;

    let statements = parse_sql(sql).expect("parse should succeed");
    assert_eq!(statements.len(), 9);
    assert!(matches!(statements[0], Statement::CreateTable(_)));
    assert!(matches!(statements[1], Statement::DropTable(_)));
    assert!(matches!(statements[2], Statement::Insert(_)));
    assert!(matches!(statements[3], Statement::Select(_)));
    assert!(matches!(statements[4], Statement::Update(_)));
    assert!(matches!(statements[5], Statement::Delete(_)));
    assert!(matches!(statements[6], Statement::Begin(_)));
    assert!(matches!(statements[7], Statement::Commit));
    assert!(matches!(statements[8], Statement::Rollback));
}

#[test]
fn rejects_malformed_sql() {
    let err = parse_statement("SELECT FROM users").expect_err("should fail");
    let rendered = err.to_string();
    assert!(rendered.contains("expected expression") || rendered.contains("unexpected token"));
}

#[test]
fn validates_select_insert_and_update_against_catalog() {
    let catalog = seeded_catalog();

    let select = parse_statement(
        "SELECT id, email FROM users WHERE active = true ORDER BY email ASC LIMIT 10",
    )
    .expect("parse select");
    validate_statement(&catalog, &select).expect("select validation should pass");

    let insert = parse_statement("INSERT INTO users (id, email, active) VALUES (1, 'x', true)")
        .expect("parse insert");
    validate_statement(&catalog, &insert).expect("insert validation should pass");

    let update = parse_statement("UPDATE users SET email = 'new' WHERE id = 1").expect("parse");
    validate_statement(&catalog, &update).expect("update validation should pass");
}

#[test]
fn validates_batch_statements() {
    let catalog = seeded_catalog();
    let statements = parse_sql(
        "BEGIN ISOLATION LEVEL SNAPSHOT; SELECT * FROM users WHERE active = true; COMMIT;",
    )
    .expect("parse batch");

    validate_statements(&catalog, &statements).expect("batch validation should pass");
}
