use crate::catalog::Catalog;
use crate::mvcc::Transaction;
use crate::planner::DeleteNode;

use super::filter::evaluate_predicate;
use super::scan::scan_table_rows;
use super::{ExecutionContext, ExecutionError, apply_staged_writes};

pub(crate) fn execute_delete(
    catalog: &Catalog,
    tx: &mut Transaction,
    node: &DeleteNode,
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

        staged.insert(stored.key, None);
        affected = affected.saturating_add(1);
    }

    apply_staged_writes(tx, staged)?;
    Ok(affected)
}
