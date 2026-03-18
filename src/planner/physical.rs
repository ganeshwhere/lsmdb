use std::collections::HashMap;

use crate::sql::ast::{
    Assignment, BeginStatement, BinaryOp, CreateTableStatement, DropTableStatement, Expr,
    LiteralValue, OrderByExpr, SelectItem,
};

use super::logical::{LogicalFilter, LogicalPlan, LogicalScan};

#[derive(Debug, Clone, PartialEq)]
pub enum PhysicalPlan {
    SeqScan(SeqScanNode),
    PrimaryKeyScan(PrimaryKeyScanNode),
    Filter(FilterNode),
    Project(ProjectNode),
    Sort(SortNode),
    Limit(LimitNode),
    Join(JoinNode),
    Insert(InsertNode),
    Update(UpdateNode),
    Delete(DeleteNode),
    CreateTable(CreateTableNode),
    DropTable(DropTableNode),
    Begin(BeginNode),
    Commit,
    Rollback,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SeqScanNode {
    pub table: String,
    pub output_columns: Vec<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PrimaryKeyScanNode {
    pub table: String,
    pub output_columns: Vec<String>,
    pub key_values: Vec<(String, LiteralValue)>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct FilterNode {
    pub input: Box<PhysicalPlan>,
    pub predicate: Expr,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ProjectNode {
    pub input: Box<PhysicalPlan>,
    pub projection: Vec<SelectItem>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SortNode {
    pub input: Box<PhysicalPlan>,
    pub order_by: Vec<OrderByExpr>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct LimitNode {
    pub input: Box<PhysicalPlan>,
    pub limit: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct JoinNode {
    pub left: Box<PhysicalPlan>,
    pub right: Box<PhysicalPlan>,
    pub on: Expr,
}

#[derive(Debug, Clone, PartialEq)]
pub struct InsertNode {
    pub table: String,
    pub columns: Vec<String>,
    pub values: Vec<Expr>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct UpdateNode {
    pub table: String,
    pub assignments: Vec<Assignment>,
    pub predicate: Option<Expr>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DeleteNode {
    pub table: String,
    pub predicate: Option<Expr>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CreateTableNode {
    pub create: CreateTableStatement,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DropTableNode {
    pub drop: DropTableStatement,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BeginNode {
    pub begin: BeginStatement,
}

pub fn build_physical_plan(plan: LogicalPlan) -> PhysicalPlan {
    match plan {
        LogicalPlan::Scan(scan) => {
            PhysicalPlan::SeqScan(SeqScanNode { table: scan.table, output_columns: scan.columns })
        }
        LogicalPlan::Filter(filter) => build_filter_plan(filter),
        LogicalPlan::Project(project) => PhysicalPlan::Project(ProjectNode {
            input: Box::new(build_physical_plan(*project.input)),
            projection: project.projection,
        }),
        LogicalPlan::Sort(sort) => PhysicalPlan::Sort(SortNode {
            input: Box::new(build_physical_plan(*sort.input)),
            order_by: sort.order_by,
        }),
        LogicalPlan::Limit(limit) => PhysicalPlan::Limit(LimitNode {
            input: Box::new(build_physical_plan(*limit.input)),
            limit: limit.limit,
        }),
        LogicalPlan::Join(join) => PhysicalPlan::Join(JoinNode {
            left: Box::new(build_physical_plan(*join.left)),
            right: Box::new(build_physical_plan(*join.right)),
            on: join.on,
        }),
        LogicalPlan::Insert(insert) => PhysicalPlan::Insert(InsertNode {
            table: insert.table,
            columns: insert.columns,
            values: insert.values,
        }),
        LogicalPlan::Update(update) => PhysicalPlan::Update(UpdateNode {
            table: update.table,
            assignments: update.assignments,
            predicate: update.predicate,
        }),
        LogicalPlan::Delete(delete) => {
            PhysicalPlan::Delete(DeleteNode { table: delete.table, predicate: delete.predicate })
        }
        LogicalPlan::CreateTable(create) => {
            PhysicalPlan::CreateTable(CreateTableNode { create: create.create })
        }
        LogicalPlan::DropTable(drop) => PhysicalPlan::DropTable(DropTableNode { drop: drop.drop }),
        LogicalPlan::Begin(begin) => PhysicalPlan::Begin(BeginNode { begin: begin.begin }),
        LogicalPlan::Commit => PhysicalPlan::Commit,
        LogicalPlan::Rollback => PhysicalPlan::Rollback,
    }
}

fn build_filter_plan(filter: LogicalFilter) -> PhysicalPlan {
    let input = *filter.input;
    if let LogicalPlan::Scan(scan) = input {
        if let Some(key_values) = extract_primary_key_equalities(&filter.predicate, &scan) {
            return PhysicalPlan::PrimaryKeyScan(PrimaryKeyScanNode {
                table: scan.table,
                output_columns: scan.columns,
                key_values,
            });
        }

        return PhysicalPlan::Filter(FilterNode {
            input: Box::new(PhysicalPlan::SeqScan(SeqScanNode {
                table: scan.table,
                output_columns: scan.columns,
            })),
            predicate: filter.predicate,
        });
    }

    PhysicalPlan::Filter(FilterNode {
        input: Box::new(build_physical_plan(input)),
        predicate: filter.predicate,
    })
}

fn extract_primary_key_equalities(
    predicate: &Expr,
    scan: &LogicalScan,
) -> Option<Vec<(String, LiteralValue)>> {
    if scan.primary_key.is_empty() {
        return None;
    }

    let conjuncts = split_conjuncts(predicate);
    if conjuncts.is_empty() {
        return None;
    }

    let mut matches = HashMap::<String, LiteralValue>::new();
    for conjunct in conjuncts {
        let Expr::Binary { left, op: BinaryOp::Equal, right } = conjunct else {
            return None;
        };

        let pair = match (&**left, &**right) {
            (Expr::Identifier(column), Expr::Literal(value)) => {
                Some((column.clone(), value.clone()))
            }
            (Expr::Literal(value), Expr::Identifier(column)) => {
                Some((column.clone(), value.clone()))
            }
            (Expr::CompoundIdentifier(parts), Expr::Literal(value)) => {
                parts.last().map(|column| (column.clone(), value.clone()))
            }
            (Expr::Literal(value), Expr::CompoundIdentifier(parts)) => {
                parts.last().map(|column| (column.clone(), value.clone()))
            }
            _ => None,
        }?;

        if !scan.primary_key.contains(&pair.0) {
            return None;
        }

        if matches.insert(pair.0, pair.1).is_some() {
            return None;
        }
    }

    if matches.len() != scan.primary_key.len() {
        return None;
    }

    let mut ordered = Vec::with_capacity(scan.primary_key.len());
    for column in &scan.primary_key {
        let value = matches.remove(column)?;
        ordered.push((column.clone(), value));
    }
    Some(ordered)
}

fn split_conjuncts(expr: &Expr) -> Vec<&Expr> {
    let mut out = Vec::new();
    collect_conjuncts(expr, &mut out);
    out
}

fn collect_conjuncts<'a>(expr: &'a Expr, out: &mut Vec<&'a Expr>) {
    match expr {
        Expr::Binary { left, op: BinaryOp::And, right } => {
            collect_conjuncts(left, out);
            collect_conjuncts(right, out);
        }
        _ => out.push(expr),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::planner::{LogicalLimit, LogicalSort};
    use crate::sql::ast::SortDirection;

    fn scan_node() -> LogicalScan {
        LogicalScan {
            table: "users".to_string(),
            columns: vec!["id".to_string(), "email".to_string()],
            primary_key: vec!["id".to_string()],
        }
    }

    #[test]
    fn selects_primary_key_scan_for_pk_equality() {
        let plan = LogicalPlan::Filter(LogicalFilter {
            input: Box::new(LogicalPlan::Scan(scan_node())),
            predicate: Expr::Binary {
                left: Box::new(Expr::Identifier("id".to_string())),
                op: BinaryOp::Equal,
                right: Box::new(Expr::Literal(LiteralValue::Integer(7))),
            },
        });

        let physical = build_physical_plan(plan);
        let PhysicalPlan::PrimaryKeyScan(scan) = physical else {
            panic!("expected primary key scan");
        };
        assert_eq!(scan.key_values.len(), 1);
        assert_eq!(scan.key_values[0].0, "id");
    }

    #[test]
    fn keeps_seq_scan_for_non_pk_predicate() {
        let plan = LogicalPlan::Filter(LogicalFilter {
            input: Box::new(LogicalPlan::Scan(scan_node())),
            predicate: Expr::Binary {
                left: Box::new(Expr::Identifier("email".to_string())),
                op: BinaryOp::Equal,
                right: Box::new(Expr::Literal(LiteralValue::String("a".to_string()))),
            },
        });

        let physical = build_physical_plan(plan);
        let PhysicalPlan::Filter(filter) = physical else {
            panic!("expected filter on seq scan");
        };
        assert!(matches!(*filter.input, PhysicalPlan::SeqScan(_)));
    }

    #[test]
    fn converts_nested_tree() {
        let plan = LogicalPlan::Limit(LogicalLimit {
            limit: 5,
            input: Box::new(LogicalPlan::Sort(LogicalSort {
                input: Box::new(LogicalPlan::Filter(LogicalFilter {
                    input: Box::new(LogicalPlan::Scan(scan_node())),
                    predicate: Expr::Binary {
                        left: Box::new(Expr::Identifier("id".to_string())),
                        op: BinaryOp::Equal,
                        right: Box::new(Expr::Literal(LiteralValue::Integer(1))),
                    },
                })),
                order_by: vec![OrderByExpr {
                    expr: Expr::Identifier("id".to_string()),
                    direction: SortDirection::Desc,
                }],
            })),
        });

        let physical = build_physical_plan(plan);
        let PhysicalPlan::Limit(limit) = physical else {
            panic!("expected limit");
        };
        assert!(matches!(*limit.input, PhysicalPlan::Sort(_)));
    }
}
