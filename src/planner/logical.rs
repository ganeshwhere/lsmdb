use crate::catalog::Catalog;
use crate::catalog::table::TableDescriptor;
use crate::sql::ast::{
    Assignment, BeginStatement, CreateTableStatement, DeleteStatement, DropTableStatement, Expr,
    InsertStatement, OrderByExpr, SelectItem, SelectStatement, Statement, UpdateStatement,
};
use crate::sql::validator::{ValidationError, validate_statement};
use thiserror::Error;

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum LogicalPlanError {
    #[error("statement validation failed: {0}")]
    Validation(#[from] ValidationError),
    #[error("table '{0}' does not exist in catalog")]
    TableNotFound(String),
}

#[derive(Debug, Clone, PartialEq)]
pub enum LogicalPlan {
    Scan(LogicalScan),
    Filter(LogicalFilter),
    Project(LogicalProject),
    Sort(LogicalSort),
    Limit(LogicalLimit),
    Join(LogicalJoin),
    Insert(LogicalInsert),
    Update(LogicalUpdate),
    Delete(LogicalDelete),
    CreateTable(LogicalCreateTable),
    DropTable(LogicalDropTable),
    Begin(LogicalBegin),
    Commit,
    Rollback,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogicalScan {
    pub table: String,
    pub columns: Vec<String>,
    pub primary_key: Vec<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct LogicalFilter {
    pub input: Box<LogicalPlan>,
    pub predicate: Expr,
}

#[derive(Debug, Clone, PartialEq)]
pub struct LogicalProject {
    pub input: Box<LogicalPlan>,
    pub projection: Vec<SelectItem>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct LogicalSort {
    pub input: Box<LogicalPlan>,
    pub order_by: Vec<OrderByExpr>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct LogicalLimit {
    pub input: Box<LogicalPlan>,
    pub limit: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct LogicalJoin {
    pub left: Box<LogicalPlan>,
    pub right: Box<LogicalPlan>,
    pub on: Expr,
}

#[derive(Debug, Clone, PartialEq)]
pub struct LogicalInsert {
    pub table: String,
    pub columns: Vec<String>,
    pub values: Vec<Expr>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct LogicalUpdate {
    pub table: String,
    pub assignments: Vec<Assignment>,
    pub predicate: Option<Expr>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct LogicalDelete {
    pub table: String,
    pub predicate: Option<Expr>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct LogicalCreateTable {
    pub create: CreateTableStatement,
}

#[derive(Debug, Clone, PartialEq)]
pub struct LogicalDropTable {
    pub drop: DropTableStatement,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogicalBegin {
    pub begin: BeginStatement,
}

pub fn build_logical_plan(
    catalog: &Catalog,
    statement: &Statement,
) -> Result<LogicalPlan, LogicalPlanError> {
    validate_statement(catalog, statement)?;

    match statement {
        Statement::Select(select) => build_select_plan(catalog, select),
        Statement::Insert(insert) => Ok(build_insert_plan(insert)),
        Statement::Update(update) => Ok(build_update_plan(update)),
        Statement::Delete(delete) => Ok(build_delete_plan(delete)),
        Statement::CreateTable(create) => {
            Ok(LogicalPlan::CreateTable(LogicalCreateTable { create: create.clone() }))
        }
        Statement::DropTable(drop) => {
            Ok(LogicalPlan::DropTable(LogicalDropTable { drop: drop.clone() }))
        }
        Statement::Begin(begin) => Ok(LogicalPlan::Begin(LogicalBegin { begin: begin.clone() })),
        Statement::Commit => Ok(LogicalPlan::Commit),
        Statement::Rollback => Ok(LogicalPlan::Rollback),
    }
}

fn build_select_plan(
    catalog: &Catalog,
    select: &SelectStatement,
) -> Result<LogicalPlan, LogicalPlanError> {
    let table = catalog
        .get_table(&select.from.name)
        .ok_or_else(|| LogicalPlanError::TableNotFound(select.from.name.clone()))?;

    let mut plan = LogicalPlan::Scan(build_scan_node(&table));

    if let Some(predicate) = &select.where_clause {
        plan = LogicalPlan::Filter(LogicalFilter {
            input: Box::new(plan),
            predicate: predicate.clone(),
        });
    }

    if !matches!(select.projection.as_slice(), [SelectItem::Wildcard]) {
        plan = LogicalPlan::Project(LogicalProject {
            input: Box::new(plan),
            projection: select.projection.clone(),
        });
    }

    if !select.order_by.is_empty() {
        plan = LogicalPlan::Sort(LogicalSort {
            input: Box::new(plan),
            order_by: select.order_by.clone(),
        });
    }

    if let Some(limit) = select.limit {
        plan = LogicalPlan::Limit(LogicalLimit { input: Box::new(plan), limit });
    }

    Ok(plan)
}

fn build_insert_plan(insert: &InsertStatement) -> LogicalPlan {
    LogicalPlan::Insert(LogicalInsert {
        table: insert.table.name.clone(),
        columns: insert.columns.clone(),
        values: insert.values.clone(),
    })
}

fn build_update_plan(update: &UpdateStatement) -> LogicalPlan {
    LogicalPlan::Update(LogicalUpdate {
        table: update.table.name.clone(),
        assignments: update.assignments.clone(),
        predicate: update.where_clause.clone(),
    })
}

fn build_delete_plan(delete: &DeleteStatement) -> LogicalPlan {
    LogicalPlan::Delete(LogicalDelete {
        table: delete.table.name.clone(),
        predicate: delete.where_clause.clone(),
    })
}

fn build_scan_node(table: &TableDescriptor) -> LogicalScan {
    LogicalScan {
        table: table.name.clone(),
        columns: table.columns.iter().map(|column| column.name.clone()).collect(),
        primary_key: table.primary_key.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::column::ColumnDescriptor;
    use crate::catalog::schema::ColumnType;
    use crate::catalog::table::TableDescriptor;
    use crate::mvcc::MvccStore;
    use crate::sql::parser::parse_statement;

    fn seeded_catalog() -> Catalog {
        let store = MvccStore::new();
        let catalog = Catalog::open(store).expect("catalog open");
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
            .expect("create users");
        catalog
    }

    #[test]
    fn builds_select_pipeline() {
        let catalog = seeded_catalog();
        let statement =
            parse_statement("SELECT id FROM users WHERE id = 42 ORDER BY id DESC LIMIT 1")
                .expect("parse");

        let plan = build_logical_plan(&catalog, &statement).expect("plan");
        let LogicalPlan::Limit(limit) = plan else {
            panic!("expected limit at root");
        };
        assert_eq!(limit.limit, 1);
        assert!(matches!(*limit.input, LogicalPlan::Sort(_)));
    }

    #[test]
    fn builds_dml_nodes() {
        let catalog = seeded_catalog();

        let insert =
            parse_statement("INSERT INTO users (id, email) VALUES (1, 'a')").expect("parse");
        assert!(matches!(
            build_logical_plan(&catalog, &insert).expect("plan"),
            LogicalPlan::Insert(_)
        ));

        let update = parse_statement("UPDATE users SET email = 'b' WHERE id = 1").expect("parse");
        assert!(matches!(
            build_logical_plan(&catalog, &update).expect("plan"),
            LogicalPlan::Update(_)
        ));

        let delete = parse_statement("DELETE FROM users WHERE id = 1").expect("parse");
        assert!(matches!(
            build_logical_plan(&catalog, &delete).expect("plan"),
            LogicalPlan::Delete(_)
        ));
    }
}
