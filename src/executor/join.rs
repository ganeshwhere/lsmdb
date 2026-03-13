use crate::sql::ast::Expr;

use super::filter::evaluate_predicate;
use super::{ExecutionContext, ExecutionError, Row, RowSet};

pub(crate) fn execute_join(
    left: RowSet,
    right: RowSet,
    predicate: &Expr,
    context: &ExecutionContext<'_>,
) -> Result<RowSet, ExecutionError> {
    let right_prefix = right.table_name.clone().unwrap_or_else(|| "right".to_string());
    let output_columns = build_join_columns(&left.columns, &right.columns, &right_prefix);
    context.limits.ensure_join_rows(left.rows.len())?;
    context.limits.ensure_join_rows(right.rows.len())?;

    let candidate_pairs = left.rows.len().saturating_mul(right.rows.len());
    context.limits.ensure_join_rows(candidate_pairs)?;

    let mut joined_rows = Vec::new();
    for left_row in &left.rows {
        context.checkpoint()?;
        for right_row in &right.rows {
            context.checkpoint()?;
            let merged = merge_rows(left_row, right_row, &right_prefix);
            if evaluate_predicate(predicate, &merged, None)? {
                joined_rows.push(merged);
                context.limits.ensure_join_rows(joined_rows.len())?;
            }
        }
    }

    Ok(RowSet { columns: output_columns, rows: joined_rows, table_name: None })
}

fn build_join_columns(left: &[String], right: &[String], right_prefix: &str) -> Vec<String> {
    let mut columns = left.to_vec();
    for column in right {
        if columns.contains(column) {
            columns.push(format!("{right_prefix}.{column}"));
        } else {
            columns.push(column.clone());
        }
    }
    columns
}

fn merge_rows(left: &Row, right: &Row, right_prefix: &str) -> Row {
    let mut merged = left.clone();
    for (column, value) in right {
        if merged.contains_key(column) {
            merged.insert(format!("{right_prefix}.{column}"), value.clone());
        } else {
            merged.insert(column.clone(), value.clone());
        }
    }
    merged
}

#[cfg(test)]
mod tests {
    use std::thread;
    use std::time::Duration;

    use super::*;
    use crate::executor::governance::{ExecutionGovernance, StatementCancellation};
    use crate::executor::{ExecutionLimits, ScalarValue};
    use crate::sql::ast::{BinaryOp, LiteralValue};

    fn test_context(limits: ExecutionLimits) -> ExecutionContext<'static> {
        ExecutionContext {
            limits: Box::leak(Box::new(limits)),
            governance: Box::leak(Box::new(ExecutionGovernance::default())),
        }
    }

    #[test]
    fn joins_rows_with_predicate() {
        let mut left_row = Row::new();
        left_row.insert("id".to_string(), ScalarValue::BigInt(1));
        left_row.insert("name".to_string(), ScalarValue::Text("alice".to_string()));

        let mut right_row = Row::new();
        right_row.insert("id".to_string(), ScalarValue::BigInt(1));
        right_row.insert("city".to_string(), ScalarValue::Text("blr".to_string()));

        let left = RowSet {
            columns: vec!["id".to_string(), "name".to_string()],
            rows: vec![left_row],
            table_name: Some("users".to_string()),
        };
        let right = RowSet {
            columns: vec!["id".to_string(), "city".to_string()],
            rows: vec![right_row],
            table_name: Some("profiles".to_string()),
        };

        let predicate = Expr::Binary {
            left: Box::new(Expr::Identifier("id".to_string())),
            op: BinaryOp::Equal,
            right: Box::new(Expr::Literal(LiteralValue::Integer(1))),
        };

        let joined =
            execute_join(left, right, &predicate, &test_context(ExecutionLimits::default()))
                .expect("join");
        assert_eq!(joined.rows.len(), 1);
        assert!(joined.rows[0].contains_key("profiles.id"));
    }

    #[test]
    fn rejects_join_that_exceeds_row_limit() {
        let mut left_row_a = Row::new();
        left_row_a.insert("id".to_string(), ScalarValue::BigInt(1));
        let mut left_row_b = Row::new();
        left_row_b.insert("id".to_string(), ScalarValue::BigInt(2));

        let mut right_row_a = Row::new();
        right_row_a.insert("id".to_string(), ScalarValue::BigInt(1));
        let mut right_row_b = Row::new();
        right_row_b.insert("id".to_string(), ScalarValue::BigInt(2));

        let left = RowSet {
            columns: vec!["id".to_string()],
            rows: vec![left_row_a, left_row_b],
            table_name: Some("users".to_string()),
        };
        let right = RowSet {
            columns: vec!["id".to_string()],
            rows: vec![right_row_a, right_row_b],
            table_name: Some("profiles".to_string()),
        };

        let predicate = Expr::Binary {
            left: Box::new(Expr::Identifier("id".to_string())),
            op: BinaryOp::Equal,
            right: Box::new(Expr::Literal(LiteralValue::Integer(1))),
        };

        let err = execute_join(
            left,
            right,
            &predicate,
            &test_context(ExecutionLimits { max_join_rows: 3, ..ExecutionLimits::default() }),
        )
        .expect_err("join limit should fail");
        assert!(matches!(
            err,
            ExecutionError::ResourceLimitExceeded { resource, limit, .. }
            if resource == "join rows" && limit == 3
        ));
    }

    #[test]
    fn cancels_long_running_join() {
        let mut left_rows = Vec::new();
        let mut right_rows = Vec::new();
        for id in 0..500_i64 {
            let mut row = Row::new();
            row.insert("id".to_string(), ScalarValue::BigInt(id));
            left_rows.push(row.clone());
            right_rows.push(row);
        }

        let left = RowSet {
            columns: vec!["id".to_string()],
            rows: left_rows,
            table_name: Some("users".to_string()),
        };
        let right = RowSet {
            columns: vec!["id".to_string()],
            rows: right_rows,
            table_name: Some("profiles".to_string()),
        };

        let cancellation = StatementCancellation::new();
        let worker_cancellation = cancellation.clone();
        let handle = thread::spawn(move || {
            let limits = Box::leak(Box::new(ExecutionLimits::default()));
            let governance = Box::leak(Box::new(
                ExecutionGovernance::default().with_cancellation(worker_cancellation),
            ));
            execute_join(
                left,
                right,
                &Expr::Literal(LiteralValue::Boolean(true)),
                &ExecutionContext { limits, governance },
            )
        });

        thread::sleep(Duration::from_millis(1));
        cancellation.cancel();
        let err = handle.join().expect("join thread").expect_err("join should be canceled");
        assert!(matches!(err, ExecutionError::StatementCanceled { .. }));
    }
}
