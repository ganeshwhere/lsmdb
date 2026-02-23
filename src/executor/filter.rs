use crate::sql::ast::{BinaryOp, Expr, UnaryOp};

use super::{ExecutionError, Row, RowSet, ScalarValue, literal_to_scalar, scalar_type_name};

pub(crate) fn apply_filter(input: RowSet, predicate: &Expr) -> Result<RowSet, ExecutionError> {
    let RowSet { columns, rows, table_name } = input;

    let filtered_rows = rows
        .into_iter()
        .filter_map(|row| match evaluate_predicate(predicate, &row, table_name.as_deref()) {
            Ok(true) => Some(Ok(row)),
            Ok(false) => None,
            Err(err) => Some(Err(err)),
        })
        .collect::<Result<Vec<_>, _>>()?;

    Ok(RowSet { columns, rows: filtered_rows, table_name })
}

pub(crate) fn evaluate_const_expr(expr: &Expr) -> Result<ScalarValue, ExecutionError> {
    evaluate_expr(expr, &Row::new(), None)
}

pub(crate) fn evaluate_predicate(
    expr: &Expr,
    row: &Row,
    table_name: Option<&str>,
) -> Result<bool, ExecutionError> {
    let value = evaluate_expr(expr, row, table_name)?;
    match value {
        ScalarValue::Boolean(value) => Ok(value),
        ScalarValue::Null => Ok(false),
        other => Err(ExecutionError::TypeMismatch {
            column: "<predicate>".to_string(),
            expected: "BOOLEAN".to_string(),
            found: scalar_type_name(&other).to_string(),
        }),
    }
}

pub(crate) fn evaluate_expr(
    expr: &Expr,
    row: &Row,
    table_name: Option<&str>,
) -> Result<ScalarValue, ExecutionError> {
    match expr {
        Expr::Identifier(name) => {
            row.get(name).cloned().ok_or_else(|| ExecutionError::ColumnNotFound {
                table: table_name.unwrap_or("<row>").to_string(),
                column: name.clone(),
            })
        }
        Expr::CompoundIdentifier(parts) => {
            if parts.is_empty() {
                return Err(ExecutionError::ColumnNotFound {
                    table: table_name.unwrap_or("<row>").to_string(),
                    column: "<empty>".to_string(),
                });
            }

            if parts.len() == 2 {
                if let Some(expected_table) = table_name {
                    if parts[0] != expected_table {
                        let composed = format!("{}.{}", parts[0], parts[1]);
                        if let Some(value) = row.get(&composed) {
                            return Ok(value.clone());
                        }
                    }
                }
            }

            if let Some(value) = row.get(parts.last().expect("parts is not empty")) {
                return Ok(value.clone());
            }

            let composed = parts.join(".");
            row.get(&composed).cloned().ok_or_else(|| ExecutionError::ColumnNotFound {
                table: table_name.unwrap_or("<row>").to_string(),
                column: composed,
            })
        }
        Expr::Literal(literal) => Ok(literal_to_scalar(literal)),
        Expr::Unary { op, expr } => {
            let value = evaluate_expr(expr, row, table_name)?;
            match (op, value) {
                (_, ScalarValue::Null) => Ok(ScalarValue::Null),
                (UnaryOp::Not, ScalarValue::Boolean(value)) => Ok(ScalarValue::Boolean(!value)),
                (UnaryOp::Negate, ScalarValue::Integer(value)) => Ok(ScalarValue::Integer(-value)),
                (UnaryOp::Negate, ScalarValue::BigInt(value)) => Ok(ScalarValue::BigInt(-value)),
                (UnaryOp::Negate, ScalarValue::Float(value)) => Ok(ScalarValue::Float(-value)),
                (UnaryOp::Negate, ScalarValue::Timestamp(value)) => {
                    Ok(ScalarValue::Timestamp(-value))
                }
                (UnaryOp::Not, other) => Err(ExecutionError::TypeMismatch {
                    column: "<expression>".to_string(),
                    expected: "BOOLEAN".to_string(),
                    found: scalar_type_name(&other).to_string(),
                }),
                (UnaryOp::Negate, other) => Err(ExecutionError::TypeMismatch {
                    column: "<expression>".to_string(),
                    expected: "numeric".to_string(),
                    found: scalar_type_name(&other).to_string(),
                }),
            }
        }
        Expr::Binary { left, op, right } => {
            let left = evaluate_expr(left, row, table_name)?;
            let right = evaluate_expr(right, row, table_name)?;
            evaluate_binary(*op, left, right)
        }
    }
}

fn evaluate_binary(
    op: BinaryOp,
    left: ScalarValue,
    right: ScalarValue,
) -> Result<ScalarValue, ExecutionError> {
    match op {
        BinaryOp::And => eval_bool_and(left, right),
        BinaryOp::Or => eval_bool_or(left, right),
        BinaryOp::Add | BinaryOp::Subtract | BinaryOp::Multiply | BinaryOp::Divide => {
            eval_arithmetic(op, left, right)
        }
        BinaryOp::Equal
        | BinaryOp::NotEqual
        | BinaryOp::LessThan
        | BinaryOp::LessThanOrEqual
        | BinaryOp::GreaterThan
        | BinaryOp::GreaterThanOrEqual => eval_comparison(op, left, right),
    }
}

fn eval_bool_and(left: ScalarValue, right: ScalarValue) -> Result<ScalarValue, ExecutionError> {
    let left = as_optional_bool(&left)?;
    let right = as_optional_bool(&right)?;

    let value = match (left, right) {
        (Some(false), _) | (_, Some(false)) => Some(false),
        (Some(true), Some(true)) => Some(true),
        _ => None,
    };

    Ok(value.map(ScalarValue::Boolean).unwrap_or(ScalarValue::Null))
}

fn eval_bool_or(left: ScalarValue, right: ScalarValue) -> Result<ScalarValue, ExecutionError> {
    let left = as_optional_bool(&left)?;
    let right = as_optional_bool(&right)?;

    let value = match (left, right) {
        (Some(true), _) | (_, Some(true)) => Some(true),
        (Some(false), Some(false)) => Some(false),
        _ => None,
    };

    Ok(value.map(ScalarValue::Boolean).unwrap_or(ScalarValue::Null))
}

fn eval_arithmetic(
    op: BinaryOp,
    left: ScalarValue,
    right: ScalarValue,
) -> Result<ScalarValue, ExecutionError> {
    if left.is_null() || right.is_null() {
        return Ok(ScalarValue::Null);
    }

    let left_num = as_numeric(&left)?;
    let right_num = as_numeric(&right)?;

    match (left_num, right_num) {
        (Numeric::Int(a), Numeric::Int(b)) if op != BinaryOp::Divide => {
            let value = match op {
                BinaryOp::Add => a.saturating_add(b),
                BinaryOp::Subtract => a.saturating_sub(b),
                BinaryOp::Multiply => a.saturating_mul(b),
                _ => unreachable!(),
            };
            Ok(ScalarValue::BigInt(value))
        }
        (left, right) => {
            let a = left.as_f64();
            let b = right.as_f64();
            if op == BinaryOp::Divide && b == 0.0 {
                return Err(ExecutionError::DivisionByZero);
            }
            let value = match op {
                BinaryOp::Add => a + b,
                BinaryOp::Subtract => a - b,
                BinaryOp::Multiply => a * b,
                BinaryOp::Divide => a / b,
                _ => unreachable!(),
            };
            Ok(ScalarValue::Float(value))
        }
    }
}

fn eval_comparison(
    op: BinaryOp,
    left: ScalarValue,
    right: ScalarValue,
) -> Result<ScalarValue, ExecutionError> {
    if left.is_null() || right.is_null() {
        return Ok(ScalarValue::Null);
    }

    let ordering = compare_values(&left, &right)?;
    let result = match op {
        BinaryOp::Equal => ordering == 0,
        BinaryOp::NotEqual => ordering != 0,
        BinaryOp::LessThan => ordering < 0,
        BinaryOp::LessThanOrEqual => ordering <= 0,
        BinaryOp::GreaterThan => ordering > 0,
        BinaryOp::GreaterThanOrEqual => ordering >= 0,
        _ => unreachable!(),
    };

    Ok(ScalarValue::Boolean(result))
}

fn as_optional_bool(value: &ScalarValue) -> Result<Option<bool>, ExecutionError> {
    match value {
        ScalarValue::Boolean(value) => Ok(Some(*value)),
        ScalarValue::Null => Ok(None),
        other => Err(ExecutionError::TypeMismatch {
            column: "<expression>".to_string(),
            expected: "BOOLEAN".to_string(),
            found: scalar_type_name(other).to_string(),
        }),
    }
}

fn compare_values(left: &ScalarValue, right: &ScalarValue) -> Result<i8, ExecutionError> {
    if let (Ok(left), Ok(right)) = (as_numeric(left), as_numeric(right)) {
        return Ok(if left.as_f64() < right.as_f64() {
            -1
        } else if left.as_f64() > right.as_f64() {
            1
        } else {
            0
        });
    }

    match (left, right) {
        (ScalarValue::Text(a), ScalarValue::Text(b)) => Ok(if a < b {
            -1
        } else if a > b {
            1
        } else {
            0
        }),
        (ScalarValue::Boolean(a), ScalarValue::Boolean(b)) => Ok(if a == b {
            0
        } else if !*a && *b {
            -1
        } else {
            1
        }),
        (ScalarValue::Blob(a), ScalarValue::Blob(b)) => Ok(if a < b {
            -1
        } else if a > b {
            1
        } else {
            0
        }),
        (_, _) => Err(ExecutionError::TypeMismatch {
            column: "<expression>".to_string(),
            expected: scalar_type_name(left).to_string(),
            found: scalar_type_name(right).to_string(),
        }),
    }
}

#[derive(Debug, Clone, Copy)]
enum Numeric {
    Int(i64),
    Float(f64),
}

impl Numeric {
    fn as_f64(self) -> f64 {
        match self {
            Numeric::Int(value) => value as f64,
            Numeric::Float(value) => value,
        }
    }
}

fn as_numeric(value: &ScalarValue) -> Result<Numeric, ExecutionError> {
    match value {
        ScalarValue::Integer(value) => Ok(Numeric::Int(*value as i64)),
        ScalarValue::BigInt(value) => Ok(Numeric::Int(*value)),
        ScalarValue::Float(value) => Ok(Numeric::Float(*value)),
        ScalarValue::Timestamp(value) => Ok(Numeric::Int(*value)),
        other => Err(ExecutionError::TypeMismatch {
            column: "<expression>".to_string(),
            expected: "numeric".to_string(),
            found: scalar_type_name(other).to_string(),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::ast::LiteralValue;

    #[test]
    fn evaluates_arithmetic_expression() {
        let expr = Expr::Binary {
            left: Box::new(Expr::Literal(LiteralValue::Integer(1))),
            op: BinaryOp::Add,
            right: Box::new(Expr::Literal(LiteralValue::Integer(2))),
        };
        assert_eq!(evaluate_const_expr(&expr).expect("evaluate"), ScalarValue::BigInt(3));
    }

    #[test]
    fn evaluates_null_sensitive_boolean_logic() {
        let expr = Expr::Binary {
            left: Box::new(Expr::Literal(LiteralValue::Boolean(false))),
            op: BinaryOp::And,
            right: Box::new(Expr::Literal(LiteralValue::Null)),
        };
        assert_eq!(evaluate_const_expr(&expr).expect("evaluate"), ScalarValue::Boolean(false));
    }

    #[test]
    fn filters_rows_using_predicate() {
        let mut row_a = Row::new();
        row_a.insert("id".to_string(), ScalarValue::BigInt(1));
        let mut row_b = Row::new();
        row_b.insert("id".to_string(), ScalarValue::BigInt(2));

        let row_set = RowSet {
            columns: vec!["id".to_string()],
            rows: vec![row_a, row_b],
            table_name: Some("users".to_string()),
        };

        let predicate = Expr::Binary {
            left: Box::new(Expr::Identifier("id".to_string())),
            op: BinaryOp::Equal,
            right: Box::new(Expr::Literal(LiteralValue::Integer(2))),
        };

        let filtered = apply_filter(row_set, &predicate).expect("filter");
        assert_eq!(filtered.rows.len(), 1);
    }
}
