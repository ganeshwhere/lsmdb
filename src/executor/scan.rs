use crate::catalog::Catalog;
use crate::catalog::table::TableDescriptor;
use crate::mvcc::Transaction;
use crate::planner::{PrimaryKeyScanNode, SeqScanNode};

use super::{
    ExecutionContext, ExecutionError, Row, RowSet, StoredRow, build_row_key,
    coerce_scalar_for_column, decode_row, literal_to_scalar, table_rows_prefix,
};

pub(crate) fn execute_seq_scan(
    catalog: &Catalog,
    tx: &Transaction,
    node: &SeqScanNode,
    context: &ExecutionContext<'_>,
) -> Result<RowSet, ExecutionError> {
    let (_, stored_rows) =
        scan_table_rows(catalog, tx, &node.table, context.limits.max_scan_rows, context)?;
    let rows = stored_rows.into_iter().map(|stored| stored.values).collect::<Vec<_>>();
    context.limits.ensure_scan_rows(rows.len())?;

    Ok(RowSet { columns: node.output_columns.clone(), rows, table_name: Some(node.table.clone()) })
}

pub(crate) fn execute_primary_key_scan(
    catalog: &Catalog,
    tx: &Transaction,
    node: &PrimaryKeyScanNode,
    context: &ExecutionContext<'_>,
) -> Result<RowSet, ExecutionError> {
    context.checkpoint()?;
    let table = get_table(catalog, &node.table)?;
    let mut key_row = Row::new();

    for primary_key in &table.primary_key {
        let (_, literal) = node
            .key_values
            .iter()
            .find(|(column, _)| column == primary_key)
            .ok_or_else(|| ExecutionError::MissingPrimaryKeyValue(primary_key.clone()))?;
        let column = table
            .column(primary_key)
            .ok_or_else(|| ExecutionError::MissingPrimaryKeyValue(primary_key.clone()))?;
        let value = coerce_scalar_for_column(&literal_to_scalar(literal), column)?;
        key_row.insert(primary_key.clone(), value);
    }

    let key = build_row_key(&table, &key_row)?;
    let mut rows = Vec::new();
    if let Some(payload) = tx.get(&key)? {
        rows.push(decode_row(&table, &payload)?);
    }

    Ok(RowSet { columns: node.output_columns.clone(), rows, table_name: Some(table.name) })
}

pub(crate) fn scan_table_rows(
    catalog: &Catalog,
    tx: &Transaction,
    table_name: &str,
    max_rows: usize,
    context: &ExecutionContext<'_>,
) -> Result<(TableDescriptor, Vec<StoredRow>), ExecutionError> {
    let table = get_table(catalog, table_name)?;
    let prefix = table_rows_prefix(&table.name);
    let mut governance_error = None;
    let rows = if max_rows == usize::MAX {
        tx.scan_prefix_with_observer(&prefix, |seen| {
            if seen == 0 {
                return true;
            }
            match context.checkpoint() {
                Ok(()) => true,
                Err(err) => {
                    governance_error = Some(err);
                    false
                }
            }
        })?
    } else {
        tx.scan_prefix_limited_with_observer(&prefix, max_rows, |seen| {
            if seen == 0 {
                return true;
            }
            match context.checkpoint() {
                Ok(()) => true,
                Err(err) => {
                    governance_error = Some(err);
                    false
                }
            }
        })?
    };
    if let Some(err) = governance_error {
        return Err(err);
    }

    if rows.len() > max_rows {
        return Err(ExecutionError::ResourceLimitExceeded {
            resource: "scan rows",
            actual: rows.len(),
            limit: max_rows,
        });
    }

    let mut decoded_rows = Vec::with_capacity(rows.len());
    for (key, payload) in rows {
        context.checkpoint()?;
        let values = decode_row(&table, &payload)?;
        decoded_rows.push(StoredRow { key, values });
    }

    Ok((table, decoded_rows))
}

fn get_table(catalog: &Catalog, table_name: &str) -> Result<TableDescriptor, ExecutionError> {
    catalog
        .get_table(table_name)
        .ok_or_else(|| ExecutionError::TableNotFound(table_name.to_string()))
}
