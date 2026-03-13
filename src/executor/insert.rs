use std::collections::HashSet;

use crate::catalog::Catalog;
use crate::mvcc::Transaction;
use crate::planner::InsertNode;

use super::filter::evaluate_const_expr;
use super::{
    ExecutionContext, ExecutionError, Row, apply_staged_writes, build_row_key,
    coerce_row_for_table, coerce_scalar_for_column, encode_row, staged_value_for_key,
};

pub(crate) fn execute_insert(
    catalog: &Catalog,
    tx: &mut Transaction,
    node: &InsertNode,
    context: &ExecutionContext<'_>,
) -> Result<u64, ExecutionError> {
    context.checkpoint()?;
    let table = catalog
        .get_table(&node.table)
        .ok_or_else(|| ExecutionError::TableNotFound(node.table.clone()))?;

    if node.columns.len() != node.values.len() {
        return Err(ExecutionError::ColumnValueCountMismatch {
            columns: node.columns.len(),
            values: node.values.len(),
        });
    }

    let mut row = Row::new();
    let mut seen = HashSet::new();

    for (column_name, expr) in node.columns.iter().zip(node.values.iter()) {
        context.checkpoint()?;
        if !seen.insert(column_name.clone()) {
            return Err(ExecutionError::DuplicateColumn(column_name.clone()));
        }

        let column = table.column(column_name).ok_or_else(|| ExecutionError::ColumnNotFound {
            table: table.name.clone(),
            column: column_name.clone(),
        })?;
        let value = evaluate_const_expr(expr)?;
        let coerced = coerce_scalar_for_column(&value, column)?;
        row.insert(column_name.clone(), coerced);
    }

    let normalized = coerce_row_for_table(&table, &row)?;
    let key = build_row_key(&table, &normalized)?;
    let mut staged = std::collections::BTreeMap::new();
    if staged_value_for_key(tx, &staged, &key)?.is_some() {
        return Err(ExecutionError::PrimaryKeyConflict { table: table.name.clone() });
    }

    let payload = encode_row(&table, &normalized)?;
    staged.insert(key, Some(payload));
    apply_staged_writes(tx, staged)?;
    Ok(1)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::column::ColumnDescriptor;
    use crate::catalog::schema::{ColumnType, DefaultValue};
    use crate::catalog::table::TableDescriptor;
    use crate::executor::ExecutionLimits;
    use crate::executor::governance::ExecutionGovernance;
    use crate::mvcc::MvccStore;
    use crate::sql::ast::{Expr, LiteralValue};

    fn test_context() -> ExecutionContext<'static> {
        ExecutionContext {
            limits: Box::leak(Box::new(ExecutionLimits::default())),
            governance: Box::leak(Box::new(ExecutionGovernance::default())),
        }
    }

    #[test]
    fn inserts_row_with_default_values() {
        let store = MvccStore::new();
        let catalog = Catalog::open(store.clone()).expect("catalog open");
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
                        name: "active".to_string(),
                        column_type: ColumnType::Boolean,
                        nullable: false,
                        default: Some(DefaultValue::Boolean(true)),
                    },
                ],
                primary_key: vec!["id".to_string()],
            })
            .expect("create users");

        let node = InsertNode {
            table: "users".to_string(),
            columns: vec!["id".to_string()],
            values: vec![Expr::Literal(LiteralValue::Integer(1))],
        };

        let mut tx = store.begin_transaction();
        let affected = execute_insert(&catalog, &mut tx, &node, &test_context()).expect("insert");
        assert_eq!(affected, 1);
    }
}
