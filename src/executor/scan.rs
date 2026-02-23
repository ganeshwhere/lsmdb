use crate::catalog::Catalog;
use crate::catalog::table::TableDescriptor;
use crate::mvcc::Transaction;
use crate::planner::{PrimaryKeyScanNode, SeqScanNode};

use super::{
    ExecutionError, Row, RowSet, StoredRow, build_row_key, coerce_scalar_for_column, decode_row,
    literal_to_scalar, table_rows_prefix,
};

pub(crate) fn execute_seq_scan(
    catalog: &Catalog,
    tx: &Transaction,
    node: &SeqScanNode,
) -> Result<RowSet, ExecutionError> {
    let (_, stored_rows) = scan_table_rows(catalog, tx, &node.table)?;
    let rows = stored_rows.into_iter().map(|stored| stored.values).collect::<Vec<_>>();

    Ok(RowSet { columns: node.output_columns.clone(), rows, table_name: Some(node.table.clone()) })
}

pub(crate) fn execute_primary_key_scan(
    catalog: &Catalog,
    tx: &Transaction,
    node: &PrimaryKeyScanNode,
) -> Result<RowSet, ExecutionError> {
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
) -> Result<(TableDescriptor, Vec<StoredRow>), ExecutionError> {
    let table = get_table(catalog, table_name)?;
    let prefix = table_rows_prefix(&table.name);
    let rows = tx.scan_prefix(&prefix)?;

    let decoded_rows = rows
        .into_iter()
        .map(|(key, payload)| decode_row(&table, &payload).map(|values| StoredRow { key, values }))
        .collect::<Result<Vec<_>, _>>()?;

    Ok((table, decoded_rows))
}

fn get_table(catalog: &Catalog, table_name: &str) -> Result<TableDescriptor, ExecutionError> {
    catalog
        .get_table(table_name)
        .ok_or_else(|| ExecutionError::TableNotFound(table_name.to_string()))
}
