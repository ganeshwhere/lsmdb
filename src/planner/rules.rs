use crate::sql::ast::{BinaryOp, Expr, LiteralValue, OrderByExpr, SelectItem, UnaryOp};

use super::logical::{LogicalFilter, LogicalPlan, LogicalProject, LogicalScan, LogicalSort};

const MAX_REWRITE_PASSES: usize = 8;

pub fn optimize_logical_plan(plan: LogicalPlan) -> LogicalPlan {
    let mut current = plan;
    for _ in 0..MAX_REWRITE_PASSES {
        let next = apply_rules(current.clone());
        if next == current {
            return next;
        }
        current = next;
    }
    current
}

fn apply_rules(plan: LogicalPlan) -> LogicalPlan {
    let plan = rewrite_children(plan);
    let plan = fold_constants(plan);
    let plan = pushdown_predicates(plan);
    prune_projection(plan)
}

fn rewrite_children(plan: LogicalPlan) -> LogicalPlan {
    match plan {
        LogicalPlan::Filter(mut filter) => {
            filter.input = Box::new(apply_rules(*filter.input));
            LogicalPlan::Filter(filter)
        }
        LogicalPlan::Project(mut project) => {
            project.input = Box::new(apply_rules(*project.input));
            LogicalPlan::Project(project)
        }
        LogicalPlan::Sort(mut sort) => {
            sort.input = Box::new(apply_rules(*sort.input));
            LogicalPlan::Sort(sort)
        }
        LogicalPlan::Limit(mut limit) => {
            limit.input = Box::new(apply_rules(*limit.input));
            LogicalPlan::Limit(limit)
        }
        LogicalPlan::Join(mut join) => {
            join.left = Box::new(apply_rules(*join.left));
            join.right = Box::new(apply_rules(*join.right));
            LogicalPlan::Join(join)
        }
        other => other,
    }
}

fn fold_constants(plan: LogicalPlan) -> LogicalPlan {
    match plan {
        LogicalPlan::Filter(mut filter) => {
            filter.predicate = fold_expr(filter.predicate);
            LogicalPlan::Filter(filter)
        }
        LogicalPlan::Project(mut project) => {
            project.projection = project
                .projection
                .into_iter()
                .map(|item| match item {
                    SelectItem::Wildcard => SelectItem::Wildcard,
                    SelectItem::Expr(expr) => SelectItem::Expr(fold_expr(expr)),
                })
                .collect();
            LogicalPlan::Project(project)
        }
        LogicalPlan::Sort(mut sort) => {
            sort.order_by = sort
                .order_by
                .into_iter()
                .map(|entry| OrderByExpr {
                    expr: fold_expr(entry.expr),
                    direction: entry.direction,
                })
                .collect();
            LogicalPlan::Sort(sort)
        }
        LogicalPlan::Update(mut update) => {
            update.assignments = update
                .assignments
                .into_iter()
                .map(|mut assignment| {
                    assignment.value = fold_expr(assignment.value);
                    assignment
                })
                .collect();
            update.predicate = update.predicate.map(fold_expr);
            LogicalPlan::Update(update)
        }
        LogicalPlan::Delete(mut delete) => {
            delete.predicate = delete.predicate.map(fold_expr);
            LogicalPlan::Delete(delete)
        }
        LogicalPlan::Insert(mut insert) => {
            insert.values = insert.values.into_iter().map(fold_expr).collect();
            LogicalPlan::Insert(insert)
        }
        LogicalPlan::Join(mut join) => {
            join.on = fold_expr(join.on);
            LogicalPlan::Join(join)
        }
        other => other,
    }
}

fn pushdown_predicates(plan: LogicalPlan) -> LogicalPlan {
    match plan {
        LogicalPlan::Filter(filter) => {
            let predicate = filter.predicate;
            match *filter.input {
                LogicalPlan::Project(project)
                    if can_push_filter_through_projection(&project.projection, &predicate) =>
                {
                    LogicalPlan::Project(LogicalProject {
                        input: Box::new(LogicalPlan::Filter(LogicalFilter {
                            input: project.input,
                            predicate,
                        })),
                        projection: project.projection,
                    })
                }
                LogicalPlan::Sort(sort) => LogicalPlan::Sort(LogicalSort {
                    input: Box::new(LogicalPlan::Filter(LogicalFilter {
                        input: sort.input,
                        predicate,
                    })),
                    order_by: sort.order_by,
                }),
                input => LogicalPlan::Filter(LogicalFilter { input: Box::new(input), predicate }),
            }
        }
        other => other,
    }
}

fn prune_projection(plan: LogicalPlan) -> LogicalPlan {
    match plan {
        LogicalPlan::Project(project) => match *project.input {
            LogicalPlan::Scan(scan) if is_identity_projection(&project.projection, &scan) => {
                LogicalPlan::Scan(scan)
            }
            input => LogicalPlan::Project(LogicalProject {
                input: Box::new(input),
                projection: project.projection,
            }),
        },
        other => other,
    }
}

fn is_identity_projection(projection: &[SelectItem], scan: &LogicalScan) -> bool {
    if projection.len() != scan.columns.len() {
        return false;
    }

    projection
        .iter()
        .zip(scan.columns.iter())
        .all(|(item, column_name)| matches!(item, SelectItem::Expr(Expr::Identifier(name)) if name == column_name))
}

fn can_push_filter_through_projection(projection: &[SelectItem], predicate: &Expr) -> bool {
    let available_columns = projection
        .iter()
        .filter_map(|item| match item {
            SelectItem::Expr(Expr::Identifier(name)) => Some(name.as_str()),
            _ => None,
        })
        .collect::<std::collections::HashSet<_>>();

    if available_columns.len() != projection.len() {
        return false;
    }

    predicate.referenced_columns().iter().all(|column| available_columns.contains(column.as_str()))
}

pub fn fold_expr(expr: Expr) -> Expr {
    match expr {
        Expr::Unary { op, expr } => {
            let folded_expr = fold_expr(*expr);
            if let Some(value) = evaluate_unary_literal(op, &folded_expr) {
                return Expr::Literal(value);
            }
            Expr::Unary { op, expr: Box::new(folded_expr) }
        }
        Expr::Binary { left, op, right } => {
            let folded_left = fold_expr(*left);
            let folded_right = fold_expr(*right);
            if let Some(value) = evaluate_binary_literal(op, &folded_left, &folded_right) {
                return Expr::Literal(value);
            }
            Expr::Binary { left: Box::new(folded_left), op, right: Box::new(folded_right) }
        }
        other => other,
    }
}

fn evaluate_unary_literal(op: UnaryOp, expr: &Expr) -> Option<LiteralValue> {
    let literal = match expr {
        Expr::Literal(literal) => literal,
        _ => return None,
    };

    match (op, literal) {
        (UnaryOp::Not, LiteralValue::Boolean(value)) => Some(LiteralValue::Boolean(!value)),
        (UnaryOp::Negate, LiteralValue::Integer(value)) => Some(LiteralValue::Integer(-value)),
        (UnaryOp::Negate, LiteralValue::Float(value)) => Some(LiteralValue::Float(-value)),
        _ => None,
    }
}

fn evaluate_binary_literal(op: BinaryOp, left: &Expr, right: &Expr) -> Option<LiteralValue> {
    let (left, right) = match (left, right) {
        (Expr::Literal(left), Expr::Literal(right)) => (left, right),
        _ => return None,
    };

    match op {
        BinaryOp::And => match (left, right) {
            (LiteralValue::Boolean(a), LiteralValue::Boolean(b)) => {
                Some(LiteralValue::Boolean(*a && *b))
            }
            _ => None,
        },
        BinaryOp::Or => match (left, right) {
            (LiteralValue::Boolean(a), LiteralValue::Boolean(b)) => {
                Some(LiteralValue::Boolean(*a || *b))
            }
            _ => None,
        },
        BinaryOp::Add => fold_numeric(left, right, |a, b| a + b),
        BinaryOp::Subtract => fold_numeric(left, right, |a, b| a - b),
        BinaryOp::Multiply => fold_numeric(left, right, |a, b| a * b),
        BinaryOp::Divide => {
            if matches!(right, LiteralValue::Integer(0))
                || matches!(right, LiteralValue::Float(value) if *value == 0.0)
            {
                return None;
            }
            fold_numeric(left, right, |a, b| a / b)
        }
        BinaryOp::Equal => Some(LiteralValue::Boolean(left == right)),
        BinaryOp::NotEqual => Some(LiteralValue::Boolean(left != right)),
        BinaryOp::LessThan => compare_literals(left, right, |o| o < 0),
        BinaryOp::LessThanOrEqual => compare_literals(left, right, |o| o <= 0),
        BinaryOp::GreaterThan => compare_literals(left, right, |o| o > 0),
        BinaryOp::GreaterThanOrEqual => compare_literals(left, right, |o| o >= 0),
    }
}

fn fold_numeric<F>(left: &LiteralValue, right: &LiteralValue, op: F) -> Option<LiteralValue>
where
    F: Fn(f64, f64) -> f64,
{
    match (left, right) {
        (LiteralValue::Integer(a), LiteralValue::Integer(b)) => {
            let result = op(*a as f64, *b as f64);
            if result.fract() == 0.0 {
                Some(LiteralValue::Integer(result as i64))
            } else {
                Some(LiteralValue::Float(result))
            }
        }
        (LiteralValue::Integer(a), LiteralValue::Float(b)) => {
            Some(LiteralValue::Float(op(*a as f64, *b)))
        }
        (LiteralValue::Float(a), LiteralValue::Integer(b)) => {
            Some(LiteralValue::Float(op(*a, *b as f64)))
        }
        (LiteralValue::Float(a), LiteralValue::Float(b)) => Some(LiteralValue::Float(op(*a, *b))),
        _ => None,
    }
}

fn compare_literals<F>(left: &LiteralValue, right: &LiteralValue, cmp: F) -> Option<LiteralValue>
where
    F: Fn(i8) -> bool,
{
    let ordering = match (left, right) {
        (LiteralValue::Integer(a), LiteralValue::Integer(b)) => compare_f64(*a as f64, *b as f64),
        (LiteralValue::Integer(a), LiteralValue::Float(b)) => compare_f64(*a as f64, *b),
        (LiteralValue::Float(a), LiteralValue::Integer(b)) => compare_f64(*a, *b as f64),
        (LiteralValue::Float(a), LiteralValue::Float(b)) => compare_f64(*a, *b),
        (LiteralValue::String(a), LiteralValue::String(b)) => Some(if a < b {
            -1
        } else if a > b {
            1
        } else {
            0
        }),
        (LiteralValue::Boolean(a), LiteralValue::Boolean(b)) => Some(if a == b {
            0
        } else if !*a && *b {
            -1
        } else {
            1
        }),
        _ => None,
    }?;

    Some(LiteralValue::Boolean(cmp(ordering)))
}

fn compare_f64(left: f64, right: f64) -> Option<i8> {
    if left.is_nan() || right.is_nan() {
        return None;
    }

    Some(if left < right {
        -1
    } else if left > right {
        1
    } else {
        0
    })
}

trait ReferencedColumns {
    fn referenced_columns(&self) -> Vec<String>;
}

impl ReferencedColumns for Expr {
    fn referenced_columns(&self) -> Vec<String> {
        let mut columns = Vec::new();
        collect_referenced_columns(self, &mut columns);
        columns
    }
}

fn collect_referenced_columns(expr: &Expr, columns: &mut Vec<String>) {
    match expr {
        Expr::Identifier(name) => columns.push(name.clone()),
        Expr::CompoundIdentifier(parts) => {
            if let Some(column) = parts.last() {
                columns.push(column.clone());
            }
        }
        Expr::Literal(_) => {}
        Expr::Unary { expr, .. } => collect_referenced_columns(expr, columns),
        Expr::Binary { left, right, .. } => {
            collect_referenced_columns(left, columns);
            collect_referenced_columns(right, columns);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::planner::logical::{LogicalLimit, LogicalProject, LogicalScan};
    use crate::sql::ast::SortDirection;

    #[test]
    fn folds_arithmetic_and_boolean_literals() {
        let expr = Expr::Binary {
            left: Box::new(Expr::Binary {
                left: Box::new(Expr::Literal(LiteralValue::Integer(1))),
                op: BinaryOp::Add,
                right: Box::new(Expr::Literal(LiteralValue::Integer(2))),
            }),
            op: BinaryOp::Equal,
            right: Box::new(Expr::Literal(LiteralValue::Integer(3))),
        };

        assert_eq!(fold_expr(expr), Expr::Literal(LiteralValue::Boolean(true)));
    }

    #[test]
    fn pushes_filter_below_sort() {
        let plan = LogicalPlan::Filter(LogicalFilter {
            predicate: Expr::Binary {
                left: Box::new(Expr::Identifier("id".to_string())),
                op: BinaryOp::Equal,
                right: Box::new(Expr::Literal(LiteralValue::Integer(1))),
            },
            input: Box::new(LogicalPlan::Sort(LogicalSort {
                input: Box::new(LogicalPlan::Scan(LogicalScan {
                    table: "users".to_string(),
                    columns: vec!["id".to_string(), "email".to_string()],
                    primary_key: vec!["id".to_string()],
                })),
                order_by: vec![OrderByExpr {
                    expr: Expr::Identifier("id".to_string()),
                    direction: SortDirection::Asc,
                }],
            })),
        });

        let optimized = optimize_logical_plan(plan);
        let LogicalPlan::Sort(sort) = optimized else {
            panic!("sort should remain at root");
        };
        assert!(matches!(*sort.input, LogicalPlan::Filter(_)));
    }

    #[test]
    fn prunes_identity_projection_over_scan() {
        let plan = LogicalPlan::Project(LogicalProject {
            input: Box::new(LogicalPlan::Scan(LogicalScan {
                table: "users".to_string(),
                columns: vec!["id".to_string(), "email".to_string()],
                primary_key: vec!["id".to_string()],
            })),
            projection: vec![
                SelectItem::Expr(Expr::Identifier("id".to_string())),
                SelectItem::Expr(Expr::Identifier("email".to_string())),
            ],
        });

        let optimized = optimize_logical_plan(plan);
        assert!(matches!(optimized, LogicalPlan::Scan(_)));
    }

    #[test]
    fn keeps_limit_above_filter() {
        let plan = LogicalPlan::Limit(LogicalLimit {
            limit: 10,
            input: Box::new(LogicalPlan::Filter(LogicalFilter {
                predicate: Expr::Binary {
                    left: Box::new(Expr::Identifier("id".to_string())),
                    op: BinaryOp::GreaterThan,
                    right: Box::new(Expr::Literal(LiteralValue::Integer(10))),
                },
                input: Box::new(LogicalPlan::Scan(LogicalScan {
                    table: "users".to_string(),
                    columns: vec!["id".to_string()],
                    primary_key: vec!["id".to_string()],
                })),
            })),
        });

        let optimized = optimize_logical_plan(plan);
        let LogicalPlan::Limit(limit) = optimized else {
            panic!("limit should remain at root");
        };
        assert!(matches!(*limit.input, LogicalPlan::Filter(_)));
    }
}
