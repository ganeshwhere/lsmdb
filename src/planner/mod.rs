pub mod logical;
pub mod physical;
pub mod rules;

use thiserror::Error;

use crate::catalog::Catalog;
use crate::sql::ast::Statement;

pub use logical::{
    LogicalDelete, LogicalFilter, LogicalInsert, LogicalJoin, LogicalLimit, LogicalPlan,
    LogicalPlanError, LogicalProject, LogicalScan, LogicalSort, LogicalUpdate, build_logical_plan,
};
pub use physical::{
    BeginNode, CreateTableNode, DeleteNode, DropTableNode, FilterNode, InsertNode, JoinNode,
    LimitNode, PhysicalPlan, PrimaryKeyScanNode, ProjectNode, SeqScanNode, SortNode, UpdateNode,
    build_physical_plan,
};
pub use rules::{fold_expr, optimize_logical_plan};

#[derive(Debug, Error)]
pub enum PlannerError {
    #[error("logical planning failed: {0}")]
    Logical(#[from] LogicalPlanError),
}

pub fn plan_statement(
    catalog: &Catalog,
    statement: &Statement,
) -> Result<PhysicalPlan, PlannerError> {
    let logical = build_logical_plan(catalog, statement)?;
    let optimized = optimize_logical_plan(logical);
    Ok(build_physical_plan(optimized))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::Catalog;
    use crate::catalog::column::ColumnDescriptor;
    use crate::catalog::schema::ColumnType;
    use crate::catalog::table::TableDescriptor;
    use crate::mvcc::MvccStore;
    use crate::sql::parser::parse_statement;

    fn seeded_catalog() -> Catalog {
        let store = MvccStore::new();
        let catalog = Catalog::open(store).expect("catalog open");
        catalog
            .create_table(TableDescriptor {
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
                ],
                primary_key: vec!["id".to_string()],
            })
            .expect("create users");
        catalog
    }

    #[test]
    fn plans_select_to_primary_key_scan() {
        let catalog = seeded_catalog();
        let statement =
            parse_statement("SELECT id FROM users WHERE id = 1 ORDER BY id DESC LIMIT 1")
                .expect("parse");

        let plan = plan_statement(&catalog, &statement).expect("plan");
        let PhysicalPlan::Limit(limit) = plan else {
            panic!("expected limit");
        };
        let PhysicalPlan::Sort(sort) = *limit.input else {
            panic!("expected sort");
        };
        assert!(
            contains_primary_key_scan(&sort.input),
            "expected primary key scan in sort subtree"
        );
    }

    fn contains_primary_key_scan(plan: &PhysicalPlan) -> bool {
        match plan {
            PhysicalPlan::PrimaryKeyScan(_) => true,
            PhysicalPlan::Filter(filter) => contains_primary_key_scan(&filter.input),
            PhysicalPlan::Project(project) => contains_primary_key_scan(&project.input),
            PhysicalPlan::Sort(sort) => contains_primary_key_scan(&sort.input),
            PhysicalPlan::Limit(limit) => contains_primary_key_scan(&limit.input),
            PhysicalPlan::Join(join) => {
                contains_primary_key_scan(&join.left) || contains_primary_key_scan(&join.right)
            }
            _ => false,
        }
    }
}
