use crate::catalog::Catalog;
use crate::mvcc::Transaction;
use crate::planner::DeleteNode;

use super::ExecutionError;
use super::filter::evaluate_predicate;
use super::scan::scan_table_rows;

pub(crate) fn execute_delete(
    catalog: &Catalog,
    tx: &mut Transaction,
    node: &DeleteNode,
) -> Result<u64, ExecutionError> {
    let table = catalog
        .get_table(&node.table)
        .ok_or_else(|| ExecutionError::TableNotFound(node.table.clone()))?;
    let (_, rows) = scan_table_rows(catalog, tx, &table.name, usize::MAX)?;

    let mut affected = 0_u64;
    for stored in rows {
        if let Some(predicate) = &node.predicate {
            if !evaluate_predicate(predicate, &stored.values, Some(&table.name))? {
                continue;
            }
        }

        tx.delete(&stored.key)?;
        affected = affected.saturating_add(1);
    }

    Ok(affected)
}
