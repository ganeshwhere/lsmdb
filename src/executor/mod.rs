pub mod delete;
pub mod filter;
pub mod insert;
pub mod join;
pub mod projection;
pub mod scan;
pub mod update;

use std::collections::BTreeMap;

use thiserror::Error;

use crate::catalog::column::ColumnDescriptor;
use crate::catalog::schema::{ColumnType, DefaultValue};
use crate::catalog::table::TableDescriptor;
use crate::catalog::{Catalog, CatalogError};
use crate::mvcc::{MvccStore, Transaction, TransactionError};
use crate::planner::PhysicalPlan;
use crate::sql::ast::LiteralValue;

#[derive(Debug, Clone, PartialEq)]
pub enum ScalarValue {
    Integer(i32),
    BigInt(i64),
    Float(f64),
    Text(String),
    Boolean(bool),
    Blob(Vec<u8>),
    Timestamp(i64),
    Null,
}

impl ScalarValue {
    pub fn is_null(&self) -> bool {
        matches!(self, ScalarValue::Null)
    }
}

pub type Row = BTreeMap<String, ScalarValue>;

#[derive(Debug, Clone, PartialEq)]
pub struct QueryResult {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<ScalarValue>>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ExecutionResult {
    Query(QueryResult),
    AffectedRows(u64),
    TransactionBegun,
    TransactionCommitted,
    TransactionRolledBack,
}

#[derive(Debug, Error)]
pub enum ExecutionError {
    #[error("transaction error: {0}")]
    Transaction(#[from] TransactionError),
    #[error("catalog error: {0}")]
    Catalog(#[from] CatalogError),
    #[error("table '{0}' not found")]
    TableNotFound(String),
    #[error("column '{column}' not found in table '{table}'")]
    ColumnNotFound { table: String, column: String },
    #[error("column '{0}' is duplicated in statement")]
    DuplicateColumn(String),
    #[error("column/value count mismatch: columns={columns}, values={values}")]
    ColumnValueCountMismatch { columns: usize, values: usize },
    #[error("type mismatch for column '{column}': expected {expected}, found {found}")]
    TypeMismatch { column: String, expected: String, found: String },
    #[error("NULL is not allowed for column '{column}'")]
    NullViolation { column: String },
    #[error("missing primary key value for column '{0}'")]
    MissingPrimaryKeyValue(String),
    #[error("primary key conflict on table '{table}'")]
    PrimaryKeyConflict { table: String },
    #[error("predicate did not evaluate to BOOLEAN")]
    InvalidPredicateType,
    #[error("failed to encode row: {0}")]
    Encode(String),
    #[error("failed to decode row: {0}")]
    Decode(String),
    #[error("cannot divide by zero")]
    DivisionByZero,
    #[error("no active transaction")]
    NoActiveTransaction,
    #[error("transaction already active")]
    TransactionAlreadyActive,
    #[error("DDL in explicit transaction is not supported yet")]
    DdlInTransactionUnsupported,
    #[error("resource limit exceeded for {resource}: actual={actual}, limit={limit}")]
    ResourceLimitExceeded { resource: &'static str, actual: usize, limit: usize },
    #[error("unsupported plan: {0}")]
    UnsupportedPlan(&'static str),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExecutionLimits {
    pub max_scan_rows: usize,
    pub max_sort_rows: usize,
    pub max_join_rows: usize,
    pub max_query_result_rows: usize,
}

impl Default for ExecutionLimits {
    fn default() -> Self {
        Self {
            max_scan_rows: usize::MAX,
            max_sort_rows: usize::MAX,
            max_join_rows: usize::MAX,
            max_query_result_rows: usize::MAX,
        }
    }
}

impl ExecutionLimits {
    fn ensure_within(
        &self,
        resource: &'static str,
        actual: usize,
        limit: usize,
    ) -> Result<(), ExecutionError> {
        if actual > limit {
            return Err(ExecutionError::ResourceLimitExceeded { resource, actual, limit });
        }
        Ok(())
    }

    fn ensure_scan_rows(&self, actual: usize) -> Result<(), ExecutionError> {
        self.ensure_within("scan rows", actual, self.max_scan_rows)
    }

    fn ensure_sort_rows(&self, actual: usize) -> Result<(), ExecutionError> {
        self.ensure_within("sort rows", actual, self.max_sort_rows)
    }

    fn ensure_join_rows(&self, actual: usize) -> Result<(), ExecutionError> {
        self.ensure_within("join rows", actual, self.max_join_rows)
    }

    fn ensure_query_result_rows(&self, actual: usize) -> Result<(), ExecutionError> {
        self.ensure_within("query result rows", actual, self.max_query_result_rows)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct RowSet {
    pub columns: Vec<String>,
    pub rows: Vec<Row>,
    pub table_name: Option<String>,
}

impl RowSet {
    pub(crate) fn into_query_result(self) -> QueryResult {
        let mut materialized_rows = Vec::with_capacity(self.rows.len());
        for row in self.rows {
            let values = self
                .columns
                .iter()
                .map(|column| row.get(column).cloned().unwrap_or(ScalarValue::Null))
                .collect::<Vec<_>>();
            materialized_rows.push(values);
        }

        QueryResult { columns: self.columns, rows: materialized_rows }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct StoredRow {
    pub key: Vec<u8>,
    pub values: Row,
}

pub struct ExecutionSession<'a> {
    catalog: &'a Catalog,
    store: &'a MvccStore,
    limits: ExecutionLimits,
    active_tx: Option<Transaction>,
}

impl<'a> ExecutionSession<'a> {
    pub fn new(catalog: &'a Catalog, store: &'a MvccStore) -> Self {
        Self::with_limits(catalog, store, ExecutionLimits::default())
    }

    pub fn with_limits(
        catalog: &'a Catalog,
        store: &'a MvccStore,
        limits: ExecutionLimits,
    ) -> Self {
        Self { catalog, store, limits, active_tx: None }
    }

    pub fn has_active_transaction(&self) -> bool {
        self.active_tx.is_some()
    }

    pub fn execute_plan(&mut self, plan: &PhysicalPlan) -> Result<ExecutionResult, ExecutionError> {
        match plan {
            PhysicalPlan::Begin(_) => {
                if self.active_tx.is_some() {
                    return Err(ExecutionError::TransactionAlreadyActive);
                }
                self.active_tx = Some(self.store.begin_transaction());
                Ok(ExecutionResult::TransactionBegun)
            }
            PhysicalPlan::Commit => {
                let mut tx = self.active_tx.take().ok_or(ExecutionError::NoActiveTransaction)?;
                tx.commit()?;
                Ok(ExecutionResult::TransactionCommitted)
            }
            PhysicalPlan::Rollback => {
                let mut tx = self.active_tx.take().ok_or(ExecutionError::NoActiveTransaction)?;
                tx.rollback();
                Ok(ExecutionResult::TransactionRolledBack)
            }
            PhysicalPlan::CreateTable(node) => {
                if self.active_tx.is_some() {
                    return Err(ExecutionError::DdlInTransactionUnsupported);
                }
                let descriptor = create_statement_to_descriptor(&node.create)?;
                self.catalog.create_table(descriptor)?;
                Ok(ExecutionResult::AffectedRows(0))
            }
            PhysicalPlan::DropTable(node) => {
                if self.active_tx.is_some() {
                    return Err(ExecutionError::DdlInTransactionUnsupported);
                }
                self.catalog.drop_table(&node.drop.name)?;
                Ok(ExecutionResult::AffectedRows(0))
            }
            PhysicalPlan::Insert(node) => self
                .with_write_tx(|catalog, tx| insert::execute_insert(catalog, tx, node))
                .map(ExecutionResult::AffectedRows),
            PhysicalPlan::Update(node) => self
                .with_write_tx(|catalog, tx| update::execute_update(catalog, tx, node))
                .map(ExecutionResult::AffectedRows),
            PhysicalPlan::Delete(node) => self
                .with_write_tx(|catalog, tx| delete::execute_delete(catalog, tx, node))
                .map(ExecutionResult::AffectedRows),
            PhysicalPlan::SeqScan(_)
            | PhysicalPlan::PrimaryKeyScan(_)
            | PhysicalPlan::Filter(_)
            | PhysicalPlan::Project(_)
            | PhysicalPlan::Sort(_)
            | PhysicalPlan::Limit(_)
            | PhysicalPlan::Join(_) => {
                let limits = self.limits;
                self.with_read_tx(|catalog, tx| execute_query_plan(catalog, tx, plan, &limits))
                    .and_then(|rows| {
                        limits.ensure_query_result_rows(rows.rows.len())?;
                        Ok(ExecutionResult::Query(rows.into_query_result()))
                    })
            }
        }
    }

    fn with_read_tx<T, F>(&mut self, f: F) -> Result<T, ExecutionError>
    where
        F: FnOnce(&Catalog, &mut Transaction) -> Result<T, ExecutionError>,
    {
        if let Some(tx) = self.active_tx.as_mut() {
            return f(self.catalog, tx);
        }

        let mut tx = self.store.begin_transaction();
        let result = f(self.catalog, &mut tx);
        tx.rollback();
        result
    }

    fn with_write_tx<T, F>(&mut self, f: F) -> Result<T, ExecutionError>
    where
        F: FnOnce(&Catalog, &mut Transaction) -> Result<T, ExecutionError>,
    {
        if let Some(tx) = self.active_tx.as_mut() {
            return f(self.catalog, tx);
        }

        let mut tx = self.store.begin_transaction();
        let result = f(self.catalog, &mut tx);
        match result {
            Ok(output) => {
                tx.commit()?;
                Ok(output)
            }
            Err(err) => {
                tx.rollback();
                Err(err)
            }
        }
    }
}

fn execute_query_plan(
    catalog: &Catalog,
    tx: &mut Transaction,
    plan: &PhysicalPlan,
    limits: &ExecutionLimits,
) -> Result<RowSet, ExecutionError> {
    match plan {
        PhysicalPlan::SeqScan(node) => scan::execute_seq_scan(catalog, tx, node, limits),
        PhysicalPlan::PrimaryKeyScan(node) => scan::execute_primary_key_scan(catalog, tx, node),
        PhysicalPlan::Filter(node) => {
            let input = execute_query_plan(catalog, tx, &node.input, limits)?;
            filter::apply_filter(input, &node.predicate)
        }
        PhysicalPlan::Project(node) => {
            let input = execute_query_plan(catalog, tx, &node.input, limits)?;
            projection::apply_projection(input, &node.projection)
        }
        PhysicalPlan::Sort(node) => {
            let input = execute_query_plan(catalog, tx, &node.input, limits)?;
            projection::apply_sort(input, &node.order_by, limits)
        }
        PhysicalPlan::Limit(node) => {
            let input = execute_query_plan(catalog, tx, &node.input, limits)?;
            projection::apply_limit(input, node.limit)
        }
        PhysicalPlan::Join(node) => {
            let left = execute_query_plan(catalog, tx, &node.left, limits)?;
            let right = execute_query_plan(catalog, tx, &node.right, limits)?;
            join::execute_join(left, right, &node.on, limits)
        }
        _ => Err(ExecutionError::UnsupportedPlan("non-query node used in query execution path")),
    }
}

fn create_statement_to_descriptor(
    create: &crate::sql::ast::CreateTableStatement,
) -> Result<TableDescriptor, ExecutionError> {
    let columns = create
        .columns
        .iter()
        .map(|column| {
            Ok(ColumnDescriptor {
                name: column.name.clone(),
                column_type: column.column_type,
                nullable: column.nullable,
                default: column
                    .default
                    .as_ref()
                    .map(|literal| literal_to_default(column.column_type, literal))
                    .transpose()?
                    .flatten(),
            })
        })
        .collect::<Result<Vec<_>, ExecutionError>>()?;

    Ok(TableDescriptor {
        name: create.name.clone(),
        columns,
        primary_key: create.primary_key.clone(),
    })
}

pub(crate) fn literal_to_scalar(literal: &LiteralValue) -> ScalarValue {
    match literal {
        LiteralValue::Integer(value) => ScalarValue::BigInt(*value),
        LiteralValue::Float(value) => ScalarValue::Float(*value),
        LiteralValue::String(value) => ScalarValue::Text(value.clone()),
        LiteralValue::Boolean(value) => ScalarValue::Boolean(*value),
        LiteralValue::Null => ScalarValue::Null,
    }
}

pub(crate) fn literal_to_default(
    column_type: ColumnType,
    literal: &LiteralValue,
) -> Result<Option<DefaultValue>, ExecutionError> {
    match literal {
        LiteralValue::Null => Ok(None),
        LiteralValue::Integer(value) => {
            let default = match column_type {
                ColumnType::Integer => {
                    let value =
                        i32::try_from(*value).map_err(|_| ExecutionError::TypeMismatch {
                            column: "<default>".to_string(),
                            expected: "INTEGER".to_string(),
                            found: "BIGINT".to_string(),
                        })?;
                    DefaultValue::Integer(value)
                }
                ColumnType::BigInt => DefaultValue::BigInt(*value),
                ColumnType::Float => DefaultValue::Float(*value as f64),
                ColumnType::Timestamp => DefaultValue::Timestamp(*value),
                _ => {
                    return Err(ExecutionError::TypeMismatch {
                        column: "<default>".to_string(),
                        expected: column_type_name(column_type).to_string(),
                        found: "BIGINT".to_string(),
                    });
                }
            };
            Ok(Some(default))
        }
        LiteralValue::Float(value) => {
            if column_type != ColumnType::Float {
                return Err(ExecutionError::TypeMismatch {
                    column: "<default>".to_string(),
                    expected: column_type_name(column_type).to_string(),
                    found: "FLOAT".to_string(),
                });
            }
            Ok(Some(DefaultValue::Float(*value)))
        }
        LiteralValue::String(value) => {
            if column_type != ColumnType::Text {
                return Err(ExecutionError::TypeMismatch {
                    column: "<default>".to_string(),
                    expected: column_type_name(column_type).to_string(),
                    found: "TEXT".to_string(),
                });
            }
            Ok(Some(DefaultValue::Text(value.clone())))
        }
        LiteralValue::Boolean(value) => {
            if column_type != ColumnType::Boolean {
                return Err(ExecutionError::TypeMismatch {
                    column: "<default>".to_string(),
                    expected: column_type_name(column_type).to_string(),
                    found: "BOOLEAN".to_string(),
                });
            }
            Ok(Some(DefaultValue::Boolean(*value)))
        }
    }
}

pub(crate) fn default_to_scalar(default: &DefaultValue) -> ScalarValue {
    match default {
        DefaultValue::Integer(value) => ScalarValue::Integer(*value),
        DefaultValue::BigInt(value) => ScalarValue::BigInt(*value),
        DefaultValue::Float(value) => ScalarValue::Float(*value),
        DefaultValue::Text(value) => ScalarValue::Text(value.clone()),
        DefaultValue::Boolean(value) => ScalarValue::Boolean(*value),
        DefaultValue::Blob(value) => ScalarValue::Blob(value.clone()),
        DefaultValue::Timestamp(value) => ScalarValue::Timestamp(*value),
    }
}

pub(crate) fn coerce_row_for_table(
    table: &TableDescriptor,
    candidate: &Row,
) -> Result<Row, ExecutionError> {
    let mut normalized = Row::new();
    for column in &table.columns {
        let value = candidate
            .get(&column.name)
            .cloned()
            .or_else(|| column.default.as_ref().map(default_to_scalar))
            .unwrap_or(ScalarValue::Null);
        let coerced = coerce_scalar_for_column(&value, column)?;
        normalized.insert(column.name.clone(), coerced);
    }
    Ok(normalized)
}

pub(crate) fn coerce_scalar_for_column(
    value: &ScalarValue,
    column: &ColumnDescriptor,
) -> Result<ScalarValue, ExecutionError> {
    if value.is_null() {
        if column.nullable {
            return Ok(ScalarValue::Null);
        }
        return Err(ExecutionError::NullViolation { column: column.name.clone() });
    }

    let coerced = match (&column.column_type, value) {
        (ColumnType::Integer, ScalarValue::Integer(value)) => ScalarValue::Integer(*value),
        (ColumnType::Integer, ScalarValue::BigInt(value)) => {
            let converted = i32::try_from(*value).map_err(|_| ExecutionError::TypeMismatch {
                column: column.name.clone(),
                expected: "INTEGER".to_string(),
                found: "BIGINT".to_string(),
            })?;
            ScalarValue::Integer(converted)
        }
        (ColumnType::Integer, ScalarValue::Float(value)) if value.fract() == 0.0 => {
            if *value >= i32::MIN as f64 && *value <= i32::MAX as f64 {
                ScalarValue::Integer(*value as i32)
            } else {
                return Err(ExecutionError::TypeMismatch {
                    column: column.name.clone(),
                    expected: "INTEGER".to_string(),
                    found: "FLOAT".to_string(),
                });
            }
        }
        (ColumnType::BigInt, ScalarValue::Integer(value)) => ScalarValue::BigInt(*value as i64),
        (ColumnType::BigInt, ScalarValue::BigInt(value)) => ScalarValue::BigInt(*value),
        (ColumnType::BigInt, ScalarValue::Float(value)) if value.fract() == 0.0 => {
            if *value >= i64::MIN as f64 && *value <= i64::MAX as f64 {
                ScalarValue::BigInt(*value as i64)
            } else {
                return Err(ExecutionError::TypeMismatch {
                    column: column.name.clone(),
                    expected: "BIGINT".to_string(),
                    found: "FLOAT".to_string(),
                });
            }
        }
        (ColumnType::Float, ScalarValue::Integer(value)) => ScalarValue::Float(*value as f64),
        (ColumnType::Float, ScalarValue::BigInt(value)) => ScalarValue::Float(*value as f64),
        (ColumnType::Float, ScalarValue::Float(value)) => ScalarValue::Float(*value),
        (ColumnType::Text, ScalarValue::Text(value)) => ScalarValue::Text(value.clone()),
        (ColumnType::Boolean, ScalarValue::Boolean(value)) => ScalarValue::Boolean(*value),
        (ColumnType::Blob, ScalarValue::Blob(value)) => ScalarValue::Blob(value.clone()),
        (ColumnType::Timestamp, ScalarValue::Integer(value)) => {
            ScalarValue::Timestamp(*value as i64)
        }
        (ColumnType::Timestamp, ScalarValue::BigInt(value)) => ScalarValue::Timestamp(*value),
        (ColumnType::Timestamp, ScalarValue::Timestamp(value)) => ScalarValue::Timestamp(*value),
        (_, other) => {
            return Err(ExecutionError::TypeMismatch {
                column: column.name.clone(),
                expected: column_type_name(column.column_type).to_string(),
                found: scalar_type_name(other).to_string(),
            });
        }
    };

    Ok(coerced)
}

pub(crate) fn column_type_name(column_type: ColumnType) -> &'static str {
    match column_type {
        ColumnType::Integer => "INTEGER",
        ColumnType::BigInt => "BIGINT",
        ColumnType::Float => "FLOAT",
        ColumnType::Text => "TEXT",
        ColumnType::Boolean => "BOOLEAN",
        ColumnType::Blob => "BLOB",
        ColumnType::Timestamp => "TIMESTAMP",
    }
}

pub(crate) fn scalar_type_name(value: &ScalarValue) -> &'static str {
    match value {
        ScalarValue::Integer(_) => "INTEGER",
        ScalarValue::BigInt(_) => "BIGINT",
        ScalarValue::Float(_) => "FLOAT",
        ScalarValue::Text(_) => "TEXT",
        ScalarValue::Boolean(_) => "BOOLEAN",
        ScalarValue::Blob(_) => "BLOB",
        ScalarValue::Timestamp(_) => "TIMESTAMP",
        ScalarValue::Null => "NULL",
    }
}

pub(crate) fn encode_row(table: &TableDescriptor, row: &Row) -> Result<Vec<u8>, ExecutionError> {
    let mut bytes = Vec::new();
    for column in &table.columns {
        let value = row.get(&column.name).ok_or_else(|| ExecutionError::ColumnNotFound {
            table: table.name.clone(),
            column: column.name.clone(),
        })?;
        if value.is_null() {
            bytes.push(0);
            continue;
        }

        bytes.push(1);
        encode_non_null_value(&mut bytes, column.column_type, value)?;
    }
    Ok(bytes)
}

pub(crate) fn decode_row(table: &TableDescriptor, bytes: &[u8]) -> Result<Row, ExecutionError> {
    let mut offset = 0_usize;
    let mut row = Row::new();

    for column in &table.columns {
        let marker = bytes
            .get(offset)
            .copied()
            .ok_or_else(|| ExecutionError::Decode("missing null marker".to_string()))?;
        offset += 1;

        let value = if marker == 0 {
            ScalarValue::Null
        } else {
            decode_non_null_value(column.column_type, bytes, &mut offset)?
        };
        row.insert(column.name.clone(), value);
    }

    if offset != bytes.len() {
        return Err(ExecutionError::Decode("row payload has trailing bytes".to_string()));
    }

    Ok(row)
}

fn encode_non_null_value(
    out: &mut Vec<u8>,
    column_type: ColumnType,
    value: &ScalarValue,
) -> Result<(), ExecutionError> {
    match (column_type, value) {
        (ColumnType::Integer, ScalarValue::Integer(value)) => out.extend(value.to_be_bytes()),
        (ColumnType::BigInt, ScalarValue::BigInt(value)) => out.extend(value.to_be_bytes()),
        (ColumnType::Float, ScalarValue::Float(value)) => out.extend(value.to_be_bytes()),
        (ColumnType::Text, ScalarValue::Text(value)) => {
            let bytes = value.as_bytes();
            out.extend((bytes.len() as u32).to_be_bytes());
            out.extend(bytes);
        }
        (ColumnType::Boolean, ScalarValue::Boolean(value)) => out.push(u8::from(*value)),
        (ColumnType::Blob, ScalarValue::Blob(value)) => {
            out.extend((value.len() as u32).to_be_bytes());
            out.extend(value);
        }
        (ColumnType::Timestamp, ScalarValue::Timestamp(value)) => out.extend(value.to_be_bytes()),
        (_, other) => {
            return Err(ExecutionError::Encode(format!(
                "cannot encode {} as {}",
                scalar_type_name(other),
                column_type_name(column_type)
            )));
        }
    }
    Ok(())
}

fn decode_non_null_value(
    column_type: ColumnType,
    bytes: &[u8],
    offset: &mut usize,
) -> Result<ScalarValue, ExecutionError> {
    match column_type {
        ColumnType::Integer => read_fixed::<4>(bytes, offset).map(|raw| {
            let value = i32::from_be_bytes(raw);
            ScalarValue::Integer(value)
        }),
        ColumnType::BigInt => read_fixed::<8>(bytes, offset).map(|raw| {
            let value = i64::from_be_bytes(raw);
            ScalarValue::BigInt(value)
        }),
        ColumnType::Float => read_fixed::<8>(bytes, offset).map(|raw| {
            let value = f64::from_be_bytes(raw);
            ScalarValue::Float(value)
        }),
        ColumnType::Text => {
            let len = read_u32(bytes, offset)? as usize;
            let payload = read_slice(bytes, offset, len)?;
            let value = String::from_utf8(payload.to_vec())
                .map_err(|err| ExecutionError::Decode(err.to_string()))?;
            Ok(ScalarValue::Text(value))
        }
        ColumnType::Boolean => {
            let raw = read_slice(bytes, offset, 1)?[0];
            Ok(ScalarValue::Boolean(raw != 0))
        }
        ColumnType::Blob => {
            let len = read_u32(bytes, offset)? as usize;
            let payload = read_slice(bytes, offset, len)?;
            Ok(ScalarValue::Blob(payload.to_vec()))
        }
        ColumnType::Timestamp => read_fixed::<8>(bytes, offset).map(|raw| {
            let value = i64::from_be_bytes(raw);
            ScalarValue::Timestamp(value)
        }),
    }
}

fn read_u32(bytes: &[u8], offset: &mut usize) -> Result<u32, ExecutionError> {
    let raw = read_fixed::<4>(bytes, offset)?;
    Ok(u32::from_be_bytes(raw))
}

fn read_fixed<const N: usize>(bytes: &[u8], offset: &mut usize) -> Result<[u8; N], ExecutionError> {
    let slice = read_slice(bytes, offset, N)?;
    let mut out = [0_u8; N];
    out.copy_from_slice(slice);
    Ok(out)
}

fn read_slice<'a>(
    bytes: &'a [u8],
    offset: &mut usize,
    len: usize,
) -> Result<&'a [u8], ExecutionError> {
    let start = *offset;
    let end = start.saturating_add(len);
    if end > bytes.len() {
        return Err(ExecutionError::Decode("row payload truncated".to_string()));
    }
    *offset = end;
    Ok(&bytes[start..end])
}

pub(crate) fn table_rows_prefix(table_name: &str) -> Vec<u8> {
    format!("__data__/tables/{table_name}/rows/").into_bytes()
}

pub(crate) fn build_row_key(table: &TableDescriptor, row: &Row) -> Result<Vec<u8>, ExecutionError> {
    let mut key = table_rows_prefix(&table.name);

    for primary_key in &table.primary_key {
        let column = table
            .column(primary_key)
            .ok_or_else(|| ExecutionError::MissingPrimaryKeyValue(primary_key.clone()))?;
        let raw_value = row
            .get(primary_key)
            .ok_or_else(|| ExecutionError::MissingPrimaryKeyValue(primary_key.clone()))?;
        let value = coerce_scalar_for_column(raw_value, column)?;
        if value.is_null() {
            return Err(ExecutionError::NullViolation { column: primary_key.clone() });
        }
        let component = encode_key_component(column.column_type, &value)?;
        key.extend((component.len() as u16).to_be_bytes());
        key.extend(component);
    }

    Ok(key)
}

fn encode_key_component(
    column_type: ColumnType,
    value: &ScalarValue,
) -> Result<Vec<u8>, ExecutionError> {
    let mut out = Vec::new();
    encode_non_null_value(&mut out, column_type, value)?;
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::column::ColumnDescriptor;

    fn table() -> TableDescriptor {
        TableDescriptor {
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
                    default: Some(DefaultValue::Boolean(true)),
                },
            ],
            primary_key: vec!["id".to_string()],
        }
    }

    #[test]
    fn encodes_and_decodes_row_payload() {
        let mut row = Row::new();
        row.insert("id".to_string(), ScalarValue::BigInt(10));
        row.insert("email".to_string(), ScalarValue::Text("a@b.com".to_string()));
        row.insert("active".to_string(), ScalarValue::Boolean(false));

        let table = table();
        let encoded = encode_row(&table, &row).expect("encode");
        let decoded = decode_row(&table, &encoded).expect("decode");
        assert_eq!(decoded, row);
    }

    #[test]
    fn coerces_defaults_for_missing_columns() {
        let mut row = Row::new();
        row.insert("id".to_string(), ScalarValue::BigInt(1));
        row.insert("email".to_string(), ScalarValue::Text("x".to_string()));

        let normalized = coerce_row_for_table(&table(), &row).expect("normalize");
        assert_eq!(normalized.get("active"), Some(&ScalarValue::Boolean(true)));
    }

    #[test]
    fn builds_row_key_from_primary_key_columns() {
        let mut row = Row::new();
        row.insert("id".to_string(), ScalarValue::BigInt(42));
        row.insert("email".to_string(), ScalarValue::Text("a".to_string()));
        row.insert("active".to_string(), ScalarValue::Boolean(true));

        let key = build_row_key(&table(), &row).expect("key");
        assert!(key.starts_with(table_rows_prefix("users").as_slice()));
    }
}
