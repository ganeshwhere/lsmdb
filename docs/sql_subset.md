# SQL Subset

This document reflects the SQL grammar and semantics currently implemented in `src/sql`, `src/planner`, and `src/executor`.

## Statement families

Implemented:

```sql
CREATE TABLE t (
  col TYPE [NOT NULL] [DEFAULT literal],
  ...,
  PRIMARY KEY (col, ...)
)

DROP TABLE t

INSERT INTO t (col, ...) VALUES (expr, ...)

SELECT [* | expr, ...]
FROM t
[WHERE expr]
[ORDER BY expr [ASC|DESC], ...]
[LIMIT n]

UPDATE t SET col = expr, ... [WHERE expr]

DELETE FROM t [WHERE expr]

BEGIN [ISOLATION LEVEL SNAPSHOT]
COMMIT
ROLLBACK
```

## Data types

Supported column types:
- `INTEGER`
- `BIGINT`
- `FLOAT`
- `TEXT`
- `BOOLEAN`
- `BLOB`
- `TIMESTAMP`

## Expressions

Supported expression forms:
- identifiers: `col`
- compound identifiers: `table.col` (validated against target table)
- literals: integer, float, string, boolean, `NULL`
- unary: `NOT`, unary `-`
- binary arithmetic: `+`, `-`, `*`, `/`
- binary comparisons: `=`, `!=`, `<`, `<=`, `>`, `>=`
- boolean: `AND`, `OR`

## Validation rules currently enforced

- table existence / duplicate table checks for DDL
- create-table descriptor validity (including primary key requirements)
- column existence checks
- duplicate column detection in `INSERT`/`UPDATE`
- insert column/value count matching
- assignability and type checks for writes/defaults
- non-null constraint validation
- `WHERE` must evaluate to boolean-compatible result

## Planner behavior (v1 style)

- logical planning + simple rule rewrites
- physical selection can emit:
  - `SeqScan`
  - `PrimaryKeyScan` for suitable equality predicates on all PK columns
- sort/limit/filter/projection operators are supported in physical plan

## Execution behavior

- DML and query operators are executed through `ExecutionSession`
- autocommit behavior for statements when no explicit transaction is active
- explicit transaction support with `BEGIN`/`COMMIT`/`ROLLBACK`
- DDL inside explicit transactions is currently rejected (`DdlInTransactionUnsupported`)

## Not implemented / out of scope right now

- joins in SQL grammar (planner/executor have join operator types but parser does not expose SQL JOIN syntax)
- subqueries
- aggregates (`COUNT`, `SUM`, etc.)
- group by / having
- multi-table query planning
- secondary indexes
- prepared statements
- SQL-backed persistence through `StorageEngine` (executor currently uses in-memory MVCC store)
