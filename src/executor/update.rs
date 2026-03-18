use crate::catalog::Catalog;
use crate::mvcc::Transaction;
use crate::planner::UpdateNode;

use super::filter::{evaluate_expr, evaluate_predicate};
use super::scan::scan_table_rows;
use super::{
    ExecutionContext, ExecutionError, apply_staged_writes, build_row_key, coerce_row_for_table,
    coerce_scalar_for_column, encode_row, staged_value_for_key,
};

pub(crate) fn execute_update(
    catalog: &Catalog,
    tx: &mut Transaction,
    node: &UpdateNode,
    context: &ExecutionContext<'_>,
) -> Result<u64, ExecutionError> {
    context.checkpoint()?;
    let table = catalog
        .get_table(&node.table)
        .ok_or_else(|| ExecutionError::TableNotFound(node.table.clone()))?;
    let (_, rows) = scan_table_rows(catalog, tx, &table.name, usize::MAX, context)?;

    let mut affected = 0_u64;
    let mut staged = std::collections::BTreeMap::new();
    for stored in rows {
        context.checkpoint()?;
        if let Some(predicate) = &node.predicate {
            if !evaluate_predicate(predicate, &stored.values, Some(&table.name))? {
                continue;
            }
        }

        let source = stored.values.clone();
        let mut updated = source.clone();

        for assignment in &node.assignments {
            let column =
                table.column(&assignment.column).ok_or_else(|| ExecutionError::ColumnNotFound {
                    table: table.name.clone(),
                    column: assignment.column.clone(),
                })?;
            let value = evaluate_expr(&assignment.value, &source, Some(&table.name))?;
            let coerced = coerce_scalar_for_column(&value, column)?;
            updated.insert(assignment.column.clone(), coerced);
        }

        let normalized = coerce_row_for_table(&table, &updated)?;
        let new_key = build_row_key(&table, &normalized)?;
        if new_key != stored.key {
            if staged_value_for_key(tx, &staged, &new_key)?.is_some() {
                return Err(ExecutionError::PrimaryKeyConflict { table: table.name.clone() });
            }
            staged.insert(stored.key.clone(), None);
        }

        let payload = encode_row(&table, &normalized)?;
        staged.insert(new_key, Some(payload));
        affected = affected.saturating_add(1);
    }

    apply_staged_writes(tx, staged)?;
    Ok(affected)
}
