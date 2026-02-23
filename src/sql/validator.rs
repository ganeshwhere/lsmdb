use std::collections::HashSet;

use thiserror::Error;

use crate::catalog::Catalog;
use crate::catalog::column::ColumnDescriptor;
use crate::catalog::schema::{ColumnType, DefaultValue};
use crate::catalog::table::TableDescriptor;

use super::ast::{BinaryOp, ColumnDef, Expr, LiteralValue, SelectItem, Statement, UnaryOp};

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum ValidationError {
    #[error("table '{0}' already exists")]
    TableAlreadyExists(String),
    #[error("table '{0}' not found")]
    TableNotFound(String),
    #[error("column '{column}' not found in table '{table}'")]
    ColumnNotFound { table: String, column: String },
    #[error("column '{0}' is duplicated in statement")]
    DuplicateColumn(String),
    #[error("column/value count mismatch: columns={columns}, values={values}")]
    ColumnValueCountMismatch { columns: usize, values: usize },
    #[error("NULL is not allowed for non-nullable column '{column}'")]
    NonNullableNull { column: String },
    #[error("type mismatch for column '{column}': expected {expected:?}, found {found}")]
    TypeMismatch { column: String, expected: ColumnType, found: String },
    #[error("WHERE clause must evaluate to BOOLEAN")]
    InvalidWhereType,
    #[error("unsupported compound identifier")]
    UnsupportedCompoundIdentifier,
    #[error("invalid CREATE TABLE definition: {0}")]
    InvalidCreate(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ExprType {
    Concrete(ColumnType),
    Null,
    Unknown,
}

pub fn validate_statement(catalog: &Catalog, statement: &Statement) -> Result<(), ValidationError> {
    match statement {
        Statement::CreateTable(create) => {
            if catalog.get_table(&create.name).is_some() {
                return Err(ValidationError::TableAlreadyExists(create.name.clone()));
            }

            let columns = create
                .columns
                .iter()
                .map(column_def_to_descriptor)
                .collect::<Result<Vec<_>, _>>()?;
            let descriptor = TableDescriptor {
                name: create.name.clone(),
                columns,
                primary_key: create.primary_key.clone(),
            };
            descriptor.validate().map_err(|err| ValidationError::InvalidCreate(err.to_string()))?;
            Ok(())
        }
        Statement::DropTable(drop) => {
            if catalog.get_table(&drop.name).is_none() {
                return Err(ValidationError::TableNotFound(drop.name.clone()));
            }
            Ok(())
        }
        Statement::Insert(insert) => {
            let table = resolve_table(catalog, &insert.table.name)?;
            if insert.columns.len() != insert.values.len() {
                return Err(ValidationError::ColumnValueCountMismatch {
                    columns: insert.columns.len(),
                    values: insert.values.len(),
                });
            }

            let mut seen = HashSet::new();
            for (column_name, expr) in insert.columns.iter().zip(insert.values.iter()) {
                if !seen.insert(column_name.clone()) {
                    return Err(ValidationError::DuplicateColumn(column_name.clone()));
                }

                let column = find_column(&table, column_name)?;
                ensure_assignable(&table, column, expr)?;
            }
            Ok(())
        }
        Statement::Select(select) => {
            let table = resolve_table(catalog, &select.from.name)?;
            for item in &select.projection {
                if let SelectItem::Expr(expr) = item {
                    let _ = infer_expr_type(&table, expr)?;
                }
            }
            if let Some(expr) = &select.where_clause {
                ensure_boolean_expr(&table, expr)?;
            }
            for order in &select.order_by {
                let _ = infer_expr_type(&table, &order.expr)?;
            }
            Ok(())
        }
        Statement::Update(update) => {
            let table = resolve_table(catalog, &update.table.name)?;
            let mut seen = HashSet::new();
            for assignment in &update.assignments {
                if !seen.insert(assignment.column.clone()) {
                    return Err(ValidationError::DuplicateColumn(assignment.column.clone()));
                }
                let column = find_column(&table, &assignment.column)?;
                ensure_assignable(&table, column, &assignment.value)?;
            }

            if let Some(expr) = &update.where_clause {
                ensure_boolean_expr(&table, expr)?;
            }
            Ok(())
        }
        Statement::Delete(delete) => {
            let table = resolve_table(catalog, &delete.table.name)?;
            if let Some(expr) = &delete.where_clause {
                ensure_boolean_expr(&table, expr)?;
            }
            Ok(())
        }
        Statement::Begin(_) | Statement::Commit | Statement::Rollback => Ok(()),
    }
}

pub fn validate_statements(
    catalog: &Catalog,
    statements: &[Statement],
) -> Result<(), ValidationError> {
    for statement in statements {
        validate_statement(catalog, statement)?;
    }
    Ok(())
}

fn resolve_table(catalog: &Catalog, table_name: &str) -> Result<TableDescriptor, ValidationError> {
    catalog
        .get_table(table_name)
        .ok_or_else(|| ValidationError::TableNotFound(table_name.to_string()))
}

fn find_column<'a>(
    table: &'a TableDescriptor,
    column_name: &str,
) -> Result<&'a ColumnDescriptor, ValidationError> {
    table.column(column_name).ok_or_else(|| ValidationError::ColumnNotFound {
        table: table.name.clone(),
        column: column_name.to_string(),
    })
}

fn column_def_to_descriptor(def: &ColumnDef) -> Result<ColumnDescriptor, ValidationError> {
    let default = match &def.default {
        Some(literal) => literal_to_default(&def.name, def.column_type, def.nullable, literal)?,
        None => None,
    };
    Ok(ColumnDescriptor {
        name: def.name.clone(),
        column_type: def.column_type,
        nullable: def.nullable,
        default,
    })
}

fn literal_to_default(
    column_name: &str,
    target: ColumnType,
    nullable: bool,
    literal: &LiteralValue,
) -> Result<Option<DefaultValue>, ValidationError> {
    match literal {
        LiteralValue::Null => {
            if nullable {
                Ok(None)
            } else {
                Err(ValidationError::NonNullableNull { column: column_name.to_string() })
            }
        }
        LiteralValue::Integer(value) => {
            let default = match target {
                ColumnType::Integer => {
                    let casted =
                        i32::try_from(*value).map_err(|_| ValidationError::TypeMismatch {
                            column: column_name.to_string(),
                            expected: target,
                            found: value.to_string(),
                        })?;
                    DefaultValue::Integer(casted)
                }
                ColumnType::BigInt => DefaultValue::BigInt(*value),
                ColumnType::Float => DefaultValue::Float(*value as f64),
                ColumnType::Timestamp => DefaultValue::Timestamp(*value),
                _ => {
                    return Err(ValidationError::TypeMismatch {
                        column: column_name.to_string(),
                        expected: target,
                        found: "INTEGER".to_string(),
                    });
                }
            };
            Ok(Some(default))
        }
        LiteralValue::Float(value) => match target {
            ColumnType::Float => Ok(Some(DefaultValue::Float(*value))),
            _ => Err(ValidationError::TypeMismatch {
                column: column_name.to_string(),
                expected: target,
                found: "FLOAT".to_string(),
            }),
        },
        LiteralValue::String(value) => match target {
            ColumnType::Text => Ok(Some(DefaultValue::Text(value.clone()))),
            _ => Err(ValidationError::TypeMismatch {
                column: column_name.to_string(),
                expected: target,
                found: "TEXT".to_string(),
            }),
        },
        LiteralValue::Boolean(value) => match target {
            ColumnType::Boolean => Ok(Some(DefaultValue::Boolean(*value))),
            _ => Err(ValidationError::TypeMismatch {
                column: column_name.to_string(),
                expected: target,
                found: "BOOLEAN".to_string(),
            }),
        },
    }
}

fn ensure_assignable(
    table: &TableDescriptor,
    column: &ColumnDescriptor,
    expr: &Expr,
) -> Result<(), ValidationError> {
    let value_type = infer_expr_type(table, expr)?;
    match value_type {
        ExprType::Null => {
            if column.nullable {
                Ok(())
            } else {
                Err(ValidationError::NonNullableNull { column: column.name.clone() })
            }
        }
        ExprType::Concrete(found) if is_assignable_type(column.column_type, found) => Ok(()),
        ExprType::Concrete(found) => Err(ValidationError::TypeMismatch {
            column: column.name.clone(),
            expected: column.column_type,
            found: format!("{found:?}"),
        }),
        ExprType::Unknown => Ok(()),
    }
}

fn ensure_boolean_expr(table: &TableDescriptor, expr: &Expr) -> Result<(), ValidationError> {
    match infer_expr_type(table, expr)? {
        ExprType::Concrete(ColumnType::Boolean) | ExprType::Unknown => Ok(()),
        _ => Err(ValidationError::InvalidWhereType),
    }
}

fn infer_expr_type(table: &TableDescriptor, expr: &Expr) -> Result<ExprType, ValidationError> {
    match expr {
        Expr::Identifier(name) => {
            let column = find_column(table, name)?;
            Ok(ExprType::Concrete(column.column_type))
        }
        Expr::CompoundIdentifier(parts) => {
            if parts.len() != 2 {
                return Err(ValidationError::UnsupportedCompoundIdentifier);
            }
            if parts[0] != table.name {
                return Err(ValidationError::TableNotFound(parts[0].clone()));
            }
            let column = find_column(table, &parts[1])?;
            Ok(ExprType::Concrete(column.column_type))
        }
        Expr::Literal(literal) => Ok(match literal {
            LiteralValue::Integer(_) => ExprType::Concrete(ColumnType::Integer),
            LiteralValue::Float(_) => ExprType::Concrete(ColumnType::Float),
            LiteralValue::String(_) => ExprType::Concrete(ColumnType::Text),
            LiteralValue::Boolean(_) => ExprType::Concrete(ColumnType::Boolean),
            LiteralValue::Null => ExprType::Null,
        }),
        Expr::Unary { op, expr } => match op {
            UnaryOp::Not => {
                let _ = infer_expr_type(table, expr)?;
                Ok(ExprType::Concrete(ColumnType::Boolean))
            }
            UnaryOp::Negate => match infer_expr_type(table, expr)? {
                ExprType::Concrete(found)
                    if matches!(
                        found,
                        ColumnType::Integer | ColumnType::BigInt | ColumnType::Float
                    ) =>
                {
                    Ok(ExprType::Concrete(found))
                }
                ExprType::Unknown => Ok(ExprType::Unknown),
                other => Err(ValidationError::TypeMismatch {
                    column: "<expression>".to_string(),
                    expected: ColumnType::Float,
                    found: format!("{other:?}"),
                }),
            },
        },
        Expr::Binary { left, op, right } => {
            let left_type = infer_expr_type(table, left)?;
            let right_type = infer_expr_type(table, right)?;
            match op {
                BinaryOp::And | BinaryOp::Or => Ok(ExprType::Concrete(ColumnType::Boolean)),
                BinaryOp::Equal
                | BinaryOp::NotEqual
                | BinaryOp::LessThan
                | BinaryOp::LessThanOrEqual
                | BinaryOp::GreaterThan
                | BinaryOp::GreaterThanOrEqual => {
                    let _ = (left_type, right_type);
                    Ok(ExprType::Concrete(ColumnType::Boolean))
                }
                BinaryOp::Add | BinaryOp::Subtract | BinaryOp::Multiply | BinaryOp::Divide => {
                    let result = combine_numeric_types(left_type, right_type)?;
                    Ok(result)
                }
            }
        }
    }
}

fn combine_numeric_types(left: ExprType, right: ExprType) -> Result<ExprType, ValidationError> {
    match (left, right) {
        (ExprType::Concrete(lhs), ExprType::Concrete(rhs))
            if is_numeric_type(lhs) && is_numeric_type(rhs) =>
        {
            if lhs == ColumnType::Float || rhs == ColumnType::Float {
                Ok(ExprType::Concrete(ColumnType::Float))
            } else if lhs == ColumnType::BigInt || rhs == ColumnType::BigInt {
                Ok(ExprType::Concrete(ColumnType::BigInt))
            } else {
                Ok(ExprType::Concrete(ColumnType::Integer))
            }
        }
        (ExprType::Unknown, _) | (_, ExprType::Unknown) => Ok(ExprType::Unknown),
        (lhs, rhs) => Err(ValidationError::TypeMismatch {
            column: "<expression>".to_string(),
            expected: ColumnType::Float,
            found: format!("{lhs:?} and {rhs:?}"),
        }),
    }
}

fn is_numeric_type(column_type: ColumnType) -> bool {
    matches!(column_type, ColumnType::Integer | ColumnType::BigInt | ColumnType::Float)
}

fn is_assignable_type(expected: ColumnType, found: ColumnType) -> bool {
    match expected {
        ColumnType::Integer => found == ColumnType::Integer,
        ColumnType::BigInt => matches!(found, ColumnType::Integer | ColumnType::BigInt),
        ColumnType::Float => {
            matches!(found, ColumnType::Integer | ColumnType::BigInt | ColumnType::Float)
        }
        ColumnType::Text => found == ColumnType::Text,
        ColumnType::Boolean => found == ColumnType::Boolean,
        ColumnType::Blob => found == ColumnType::Blob,
        ColumnType::Timestamp => {
            matches!(found, ColumnType::Integer | ColumnType::BigInt | ColumnType::Timestamp)
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::catalog::column::ColumnDescriptor;
    use crate::catalog::table::TableDescriptor;
    use crate::mvcc::MvccStore;
    use crate::sql::parser::parse_statement;

    use super::*;

    fn setup_catalog() -> Catalog {
        let store = MvccStore::new();
        let catalog = Catalog::open(store).expect("catalog open");
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
    fn validates_insert_types() {
        let catalog = setup_catalog();
        let statement =
            parse_statement("INSERT INTO users (id, email, active) VALUES (1, 'a', true)")
                .expect("parse");
        assert!(validate_statement(&catalog, &statement).is_ok());
    }

    #[test]
    fn rejects_missing_table() {
        let catalog = setup_catalog();
        let statement = parse_statement("SELECT * FROM missing").expect("parse");
        let err = validate_statement(&catalog, &statement).expect_err("should fail");
        assert!(matches!(err, ValidationError::TableNotFound(name) if name == "missing"));
    }

    #[test]
    fn rejects_non_nullable_null_insert() {
        let catalog = setup_catalog();
        let statement =
            parse_statement("INSERT INTO users (id, email, active) VALUES (1, NULL, true)")
                .expect("parse");
        let err = validate_statement(&catalog, &statement).expect_err("should fail");
        assert!(matches!(err, ValidationError::NonNullableNull { column } if column == "email"));
    }

    #[test]
    fn rejects_unknown_column_reference() {
        let catalog = setup_catalog();
        let statement = parse_statement("SELECT id FROM users WHERE unknown = 1").expect("parse");
        let err = validate_statement(&catalog, &statement).expect_err("should fail");
        assert!(
            matches!(err, ValidationError::ColumnNotFound { column, .. } if column == "unknown")
        );
    }
}
