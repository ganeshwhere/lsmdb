use std::cmp::Ordering;

use crate::sql::ast::{Expr, OrderByExpr, SelectItem, SortDirection};

use super::filter::evaluate_expr;
use super::{ExecutionContext, ExecutionError, RowSet, ScalarValue};

pub(crate) fn apply_projection(
    input: RowSet,
    projection: &[SelectItem],
    context: &ExecutionContext<'_>,
) -> Result<RowSet, ExecutionError> {
    if projection.len() == 1 && matches!(projection[0], SelectItem::Wildcard) {
        return Ok(input);
    }

    let projected_columns = projection
        .iter()
        .enumerate()
        .map(|(index, item)| projection_column_name(item, index))
        .collect::<Vec<_>>();

    let RowSet { rows, table_name, .. } = input;

    let mut projected_rows = Vec::with_capacity(rows.len());
    for row in rows {
        context.checkpoint()?;
        let source = row.clone();
        let mut materialized = row;
        for (index, item) in projection.iter().enumerate() {
            let SelectItem::Expr(expr) = item else {
                continue;
            };
            let value = evaluate_expr(expr, &source, table_name.as_deref())?;
            let column_name = projection_column_name(item, index);
            materialized.insert(column_name, value);
        }
        projected_rows.push(materialized);
    }

    Ok(RowSet { columns: projected_columns, rows: projected_rows, table_name })
}

pub(crate) fn apply_sort(
    input: RowSet,
    order_by: &[OrderByExpr],
    context: &ExecutionContext<'_>,
) -> Result<RowSet, ExecutionError> {
    if order_by.is_empty() {
        return Ok(input);
    }

    let RowSet { columns, rows, table_name } = input;
    context.limits.ensure_sort_rows(rows.len())?;

    let mut keyed = rows
        .into_iter()
        .map(|row| {
            context.checkpoint()?;
            let sort_keys = order_by
                .iter()
                .map(|entry| evaluate_expr(&entry.expr, &row, table_name.as_deref()))
                .collect::<Result<Vec<_>, _>>()?;
            Ok((row, sort_keys))
        })
        .collect::<Result<Vec<_>, ExecutionError>>()?;

    keyed.sort_by(|(_, left_keys), (_, right_keys)| {
        compare_sort_keys(left_keys, right_keys, order_by)
    });

    let rows = keyed.into_iter().map(|(row, _)| row).collect::<Vec<_>>();
    Ok(RowSet { columns, rows, table_name })
}

pub(crate) fn apply_limit(
    mut input: RowSet,
    limit: u64,
    context: &ExecutionContext<'_>,
) -> Result<RowSet, ExecutionError> {
    context.checkpoint()?;
    let limit = usize::try_from(limit).unwrap_or(usize::MAX);
    input.rows.truncate(limit);
    Ok(input)
}

fn projection_column_name(item: &SelectItem, index: usize) -> String {
    match item {
        SelectItem::Wildcard => "*".to_string(),
        SelectItem::Expr(Expr::Identifier(name)) => name.clone(),
        SelectItem::Expr(Expr::CompoundIdentifier(parts)) => parts.join("."),
        SelectItem::Expr(_) => format!("expr_{index}"),
    }
}

fn compare_sort_keys(
    left: &[ScalarValue],
    right: &[ScalarValue],
    order_by: &[OrderByExpr],
) -> Ordering {
    for ((left_value, right_value), order) in left.iter().zip(right.iter()).zip(order_by.iter()) {
        let ordering = compare_scalar_for_sort(left_value, right_value);
        if ordering == Ordering::Equal {
            continue;
        }

        return if order.direction == SortDirection::Desc { ordering.reverse() } else { ordering };
    }

    Ordering::Equal
}

fn compare_scalar_for_sort(left: &ScalarValue, right: &ScalarValue) -> Ordering {
    match (left, right) {
        (ScalarValue::Null, ScalarValue::Null) => return Ordering::Equal,
        (ScalarValue::Null, _) => return Ordering::Greater,
        (_, ScalarValue::Null) => return Ordering::Less,
        _ => {}
    }

    if let (Some(left_numeric), Some(right_numeric)) = (as_numeric(left), as_numeric(right)) {
        return left_numeric.partial_cmp(&right_numeric).unwrap_or(Ordering::Equal);
    }

    match (left, right) {
        (ScalarValue::Text(a), ScalarValue::Text(b)) => a.cmp(b),
        (ScalarValue::Boolean(a), ScalarValue::Boolean(b)) => a.cmp(b),
        (ScalarValue::Blob(a), ScalarValue::Blob(b)) => a.cmp(b),
        _ => scalar_sort_tag(left).cmp(scalar_sort_tag(right)),
    }
}

fn as_numeric(value: &ScalarValue) -> Option<f64> {
    match value {
        ScalarValue::Integer(value) => Some(*value as f64),
        ScalarValue::BigInt(value) => Some(*value as f64),
        ScalarValue::Float(value) => Some(*value),
        ScalarValue::Timestamp(value) => Some(*value as f64),
        _ => None,
    }
}

fn scalar_sort_tag(value: &ScalarValue) -> &'static str {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::executor::ExecutionLimits;
    use crate::executor::governance::ExecutionGovernance;
    use crate::executor::{Row, ScalarValue};
    use crate::sql::ast::BinaryOp;

    fn test_context() -> ExecutionContext<'static> {
        ExecutionContext {
            limits: Box::leak(Box::new(ExecutionLimits::default())),
            governance: Box::leak(Box::new(ExecutionGovernance::default())),
        }
    }

    #[test]
    fn projects_expression_columns() {
        let mut row = Row::new();
        row.insert("a".to_string(), ScalarValue::BigInt(1));
        row.insert("b".to_string(), ScalarValue::BigInt(2));

        let input = RowSet {
            columns: vec!["a".to_string(), "b".to_string()],
            rows: vec![row],
            table_name: Some("t".to_string()),
        };

        let projected = apply_projection(
            input,
            &[SelectItem::Expr(Expr::Binary {
                left: Box::new(Expr::Identifier("a".to_string())),
                op: BinaryOp::Add,
                right: Box::new(Expr::Identifier("b".to_string())),
            })],
            &test_context(),
        )
        .expect("projection");

        assert_eq!(projected.columns, vec!["expr_0".to_string()]);
        assert_eq!(projected.rows[0].get("expr_0"), Some(&ScalarValue::BigInt(3)));
    }

    #[test]
    fn sorts_rows_in_descending_order() {
        let mut row_a = Row::new();
        row_a.insert("id".to_string(), ScalarValue::BigInt(1));
        let mut row_b = Row::new();
        row_b.insert("id".to_string(), ScalarValue::BigInt(3));

        let input = RowSet {
            columns: vec!["id".to_string()],
            rows: vec![row_a, row_b],
            table_name: Some("users".to_string()),
        };

        let sorted = apply_sort(
            input,
            &[OrderByExpr {
                expr: Expr::Identifier("id".to_string()),
                direction: SortDirection::Desc,
            }],
            &test_context(),
        )
        .expect("sort");

        assert_eq!(sorted.rows[0].get("id"), Some(&ScalarValue::BigInt(3)));
    }
}
