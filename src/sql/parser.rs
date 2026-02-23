use thiserror::Error;

use crate::catalog::schema::ColumnType;

use super::ast::{
    Assignment, BeginStatement, BinaryOp, ColumnDef, CreateTableStatement, DeleteStatement,
    DropTableStatement, Expr, InsertStatement, IsolationLevel, LiteralValue, OrderByExpr,
    SelectItem, SelectStatement, SortDirection, Statement, TableRef, UnaryOp, UpdateStatement,
};
use super::lexer::{LexError, Span, Token, TokenKind, tokenize};

#[derive(Debug, Error, Clone, PartialEq)]
pub enum ParseError {
    #[error("lexing failed: {0}")]
    Lex(#[from] LexError),
    #[error("unexpected token at {span:?}: expected {expected}, found {found}")]
    UnexpectedToken { expected: String, found: String, span: Span },
    #[error("unexpected end of input")]
    UnexpectedEof,
    #[error("invalid LIMIT value at {span:?}: {value}")]
    InvalidLimit { value: String, span: Span },
    #[error("invalid statement: {0}")]
    InvalidStatement(String),
}

pub fn parse_statement(sql: &str) -> Result<Statement, ParseError> {
    let statements = parse_sql(sql)?;
    match statements.as_slice() {
        [statement] => Ok(statement.clone()),
        [] => Err(ParseError::InvalidStatement("no statement found".to_string())),
        _ => Err(ParseError::InvalidStatement("expected exactly one statement".to_string())),
    }
}

pub fn parse_sql(sql: &str) -> Result<Vec<Statement>, ParseError> {
    let tokens = tokenize(sql)?;
    Parser::new(tokens).parse_sql()
}

struct Parser {
    tokens: Vec<Token>,
    index: usize,
}

impl Parser {
    fn new(tokens: Vec<Token>) -> Self {
        Self { tokens, index: 0 }
    }

    fn parse_sql(&mut self) -> Result<Vec<Statement>, ParseError> {
        let mut statements = Vec::new();

        while !self.is_eof() {
            while self.consume_if_simple(TokenKind::Semicolon) {}
            if self.is_eof() {
                break;
            }

            statements.push(self.parse_statement()?);
            while self.consume_if_simple(TokenKind::Semicolon) {}
        }

        if statements.is_empty() {
            return Err(ParseError::InvalidStatement("no statement found".to_string()));
        }

        Ok(statements)
    }

    fn parse_statement(&mut self) -> Result<Statement, ParseError> {
        if self.consume_if_simple(TokenKind::Create) {
            return self.parse_create_table();
        }
        if self.consume_if_simple(TokenKind::Drop) {
            return self.parse_drop_table();
        }
        if self.consume_if_simple(TokenKind::Insert) {
            return self.parse_insert();
        }
        if self.consume_if_simple(TokenKind::Select) {
            return self.parse_select();
        }
        if self.consume_if_simple(TokenKind::Update) {
            return self.parse_update();
        }
        if self.consume_if_simple(TokenKind::Delete) {
            return self.parse_delete();
        }
        if self.consume_if_simple(TokenKind::Begin) {
            return self.parse_begin();
        }
        if self.consume_if_simple(TokenKind::Commit) {
            return Ok(Statement::Commit);
        }
        if self.consume_if_simple(TokenKind::Rollback) {
            return Ok(Statement::Rollback);
        }

        let token = self.peek_token()?;
        Err(ParseError::UnexpectedToken {
            expected: "a SQL statement".to_string(),
            found: token_description(token),
            span: token.span,
        })
    }

    fn parse_create_table(&mut self) -> Result<Statement, ParseError> {
        self.expect_simple(TokenKind::Table)?;
        let name = self.expect_identifier()?;
        self.expect_simple(TokenKind::LParen)?;

        let mut columns = Vec::new();
        let mut primary_key = Vec::new();
        let mut parsed_pk = false;

        loop {
            if self.consume_if_simple(TokenKind::Primary) {
                if parsed_pk {
                    return Err(ParseError::InvalidStatement(
                        "CREATE TABLE cannot define PRIMARY KEY more than once".to_string(),
                    ));
                }
                self.expect_simple(TokenKind::Key)?;
                self.expect_simple(TokenKind::LParen)?;
                primary_key = self.parse_identifier_list()?;
                self.expect_simple(TokenKind::RParen)?;
                parsed_pk = true;
            } else {
                columns.push(self.parse_column_def()?);
            }

            if self.consume_if_simple(TokenKind::Comma) {
                continue;
            }
            break;
        }

        self.expect_simple(TokenKind::RParen)?;

        if !parsed_pk {
            return Err(ParseError::InvalidStatement(
                "CREATE TABLE requires a PRIMARY KEY clause".to_string(),
            ));
        }

        Ok(Statement::CreateTable(CreateTableStatement { name, columns, primary_key }))
    }

    fn parse_drop_table(&mut self) -> Result<Statement, ParseError> {
        self.expect_simple(TokenKind::Table)?;
        let name = self.expect_identifier()?;
        Ok(Statement::DropTable(DropTableStatement { name }))
    }

    fn parse_insert(&mut self) -> Result<Statement, ParseError> {
        self.expect_simple(TokenKind::Into)?;
        let table = TableRef { name: self.expect_identifier()? };
        self.expect_simple(TokenKind::LParen)?;
        let columns = self.parse_identifier_list()?;
        self.expect_simple(TokenKind::RParen)?;
        self.expect_simple(TokenKind::Values)?;
        self.expect_simple(TokenKind::LParen)?;
        let values = self.parse_expr_list()?;
        self.expect_simple(TokenKind::RParen)?;

        Ok(Statement::Insert(InsertStatement { table, columns, values }))
    }

    fn parse_select(&mut self) -> Result<Statement, ParseError> {
        let projection = self.parse_projection_list()?;
        self.expect_simple(TokenKind::From)?;
        let from = TableRef { name: self.expect_identifier()? };

        let where_clause =
            if self.consume_if_simple(TokenKind::Where) { Some(self.parse_expr()?) } else { None };

        let mut order_by = Vec::new();
        if self.consume_if_simple(TokenKind::Order) {
            self.expect_simple(TokenKind::By)?;
            loop {
                let expr = self.parse_expr()?;
                let direction = if self.consume_if_simple(TokenKind::Asc) {
                    SortDirection::Asc
                } else if self.consume_if_simple(TokenKind::Desc) {
                    SortDirection::Desc
                } else {
                    SortDirection::Asc
                };
                order_by.push(OrderByExpr { expr, direction });

                if self.consume_if_simple(TokenKind::Comma) {
                    continue;
                }
                break;
            }
        }

        let limit = if self.consume_if_simple(TokenKind::Limit) {
            let token = self.peek_token()?.clone();
            match token.kind {
                TokenKind::Integer(value) if value >= 0 => {
                    self.advance();
                    Some(value as u64)
                }
                TokenKind::Integer(value) => {
                    return Err(ParseError::InvalidLimit {
                        value: value.to_string(),
                        span: token.span,
                    });
                }
                _ => {
                    return Err(ParseError::InvalidLimit {
                        value: token_description(&token),
                        span: token.span,
                    });
                }
            }
        } else {
            None
        };

        Ok(Statement::Select(SelectStatement { projection, from, where_clause, order_by, limit }))
    }

    fn parse_update(&mut self) -> Result<Statement, ParseError> {
        let table = TableRef { name: self.expect_identifier()? };
        self.expect_simple(TokenKind::Set)?;

        let mut assignments = Vec::new();
        loop {
            let column = self.expect_identifier()?;
            self.expect_simple(TokenKind::Equal)?;
            let value = self.parse_expr()?;
            assignments.push(Assignment { column, value });

            if self.consume_if_simple(TokenKind::Comma) {
                continue;
            }
            break;
        }

        let where_clause =
            if self.consume_if_simple(TokenKind::Where) { Some(self.parse_expr()?) } else { None };

        Ok(Statement::Update(UpdateStatement { table, assignments, where_clause }))
    }

    fn parse_delete(&mut self) -> Result<Statement, ParseError> {
        self.expect_simple(TokenKind::From)?;
        let table = TableRef { name: self.expect_identifier()? };
        let where_clause =
            if self.consume_if_simple(TokenKind::Where) { Some(self.parse_expr()?) } else { None };

        Ok(Statement::Delete(DeleteStatement { table, where_clause }))
    }

    fn parse_begin(&mut self) -> Result<Statement, ParseError> {
        let isolation_level = if self.consume_if_simple(TokenKind::Isolation) {
            self.expect_simple(TokenKind::Level)?;
            self.expect_simple(TokenKind::Snapshot)?;
            Some(IsolationLevel::Snapshot)
        } else {
            None
        };

        Ok(Statement::Begin(BeginStatement { isolation_level }))
    }

    fn parse_column_def(&mut self) -> Result<ColumnDef, ParseError> {
        let name = self.expect_identifier()?;
        let column_type = self.parse_column_type()?;

        let mut nullable = true;
        if self.consume_if_simple(TokenKind::Not) {
            self.expect_simple(TokenKind::Null)?;
            nullable = false;
        }

        let default = if self.consume_if_simple(TokenKind::Default) {
            Some(self.parse_literal()?)
        } else {
            None
        };

        Ok(ColumnDef { name, column_type, nullable, default })
    }

    fn parse_column_type(&mut self) -> Result<ColumnType, ParseError> {
        let token = self.peek_token()?.clone();
        let column_type = match token.kind {
            TokenKind::IntegerType => ColumnType::Integer,
            TokenKind::BigIntType => ColumnType::BigInt,
            TokenKind::FloatType => ColumnType::Float,
            TokenKind::TextType => ColumnType::Text,
            TokenKind::BooleanType => ColumnType::Boolean,
            TokenKind::BlobType => ColumnType::Blob,
            TokenKind::TimestampType => ColumnType::Timestamp,
            _ => {
                return Err(ParseError::UnexpectedToken {
                    expected: "column type".to_string(),
                    found: token_description(&token),
                    span: token.span,
                });
            }
        };
        self.advance();
        Ok(column_type)
    }

    fn parse_projection_list(&mut self) -> Result<Vec<SelectItem>, ParseError> {
        if self.consume_if_simple(TokenKind::Asterisk) {
            return Ok(vec![SelectItem::Wildcard]);
        }

        let mut projection = Vec::new();
        loop {
            projection.push(SelectItem::Expr(self.parse_expr()?));
            if self.consume_if_simple(TokenKind::Comma) {
                continue;
            }
            break;
        }
        Ok(projection)
    }

    fn parse_identifier_list(&mut self) -> Result<Vec<String>, ParseError> {
        let mut values = Vec::new();
        loop {
            values.push(self.expect_identifier()?);
            if self.consume_if_simple(TokenKind::Comma) {
                continue;
            }
            break;
        }
        Ok(values)
    }

    fn parse_expr_list(&mut self) -> Result<Vec<Expr>, ParseError> {
        let mut values = Vec::new();
        loop {
            values.push(self.parse_expr()?);
            if self.consume_if_simple(TokenKind::Comma) {
                continue;
            }
            break;
        }
        Ok(values)
    }

    fn parse_expr(&mut self) -> Result<Expr, ParseError> {
        self.parse_or()
    }

    fn parse_or(&mut self) -> Result<Expr, ParseError> {
        let mut left = self.parse_and()?;
        while self.consume_if_simple(TokenKind::Or) {
            let right = self.parse_and()?;
            left = Expr::Binary { left: Box::new(left), op: BinaryOp::Or, right: Box::new(right) };
        }
        Ok(left)
    }

    fn parse_and(&mut self) -> Result<Expr, ParseError> {
        let mut left = self.parse_comparison()?;
        while self.consume_if_simple(TokenKind::And) {
            let right = self.parse_comparison()?;
            left = Expr::Binary { left: Box::new(left), op: BinaryOp::And, right: Box::new(right) };
        }
        Ok(left)
    }

    fn parse_comparison(&mut self) -> Result<Expr, ParseError> {
        let mut left = self.parse_additive()?;
        loop {
            let op = if self.consume_if_simple(TokenKind::Equal) {
                Some(BinaryOp::Equal)
            } else if self.consume_if_simple(TokenKind::NotEqual) {
                Some(BinaryOp::NotEqual)
            } else if self.consume_if_simple(TokenKind::LessThan) {
                Some(BinaryOp::LessThan)
            } else if self.consume_if_simple(TokenKind::LessThanOrEqual) {
                Some(BinaryOp::LessThanOrEqual)
            } else if self.consume_if_simple(TokenKind::GreaterThan) {
                Some(BinaryOp::GreaterThan)
            } else if self.consume_if_simple(TokenKind::GreaterThanOrEqual) {
                Some(BinaryOp::GreaterThanOrEqual)
            } else {
                None
            };

            let Some(op) = op else { break };
            let right = self.parse_additive()?;
            left = Expr::Binary { left: Box::new(left), op, right: Box::new(right) };
        }
        Ok(left)
    }

    fn parse_additive(&mut self) -> Result<Expr, ParseError> {
        let mut left = self.parse_multiplicative()?;
        loop {
            let op = if self.consume_if_simple(TokenKind::Plus) {
                Some(BinaryOp::Add)
            } else if self.consume_if_simple(TokenKind::Minus) {
                Some(BinaryOp::Subtract)
            } else {
                None
            };

            let Some(op) = op else { break };
            let right = self.parse_multiplicative()?;
            left = Expr::Binary { left: Box::new(left), op, right: Box::new(right) };
        }
        Ok(left)
    }

    fn parse_multiplicative(&mut self) -> Result<Expr, ParseError> {
        let mut left = self.parse_unary()?;
        loop {
            let op = if self.consume_if_simple(TokenKind::Asterisk) {
                Some(BinaryOp::Multiply)
            } else if self.consume_if_simple(TokenKind::Slash) {
                Some(BinaryOp::Divide)
            } else {
                None
            };

            let Some(op) = op else { break };
            let right = self.parse_unary()?;
            left = Expr::Binary { left: Box::new(left), op, right: Box::new(right) };
        }
        Ok(left)
    }

    fn parse_unary(&mut self) -> Result<Expr, ParseError> {
        if self.consume_if_simple(TokenKind::Not) {
            return Ok(Expr::Unary { op: UnaryOp::Not, expr: Box::new(self.parse_unary()?) });
        }

        if self.consume_if_simple(TokenKind::Minus) {
            return Ok(Expr::Unary { op: UnaryOp::Negate, expr: Box::new(self.parse_unary()?) });
        }

        self.parse_primary()
    }

    fn parse_primary(&mut self) -> Result<Expr, ParseError> {
        if self.consume_if_simple(TokenKind::LParen) {
            let expr = self.parse_expr()?;
            self.expect_simple(TokenKind::RParen)?;
            return Ok(expr);
        }

        if let Some(literal) = self.consume_literal() {
            return Ok(Expr::Literal(literal));
        }

        if matches!(self.peek_token()?.kind, TokenKind::Identifier(_)) {
            return self.parse_identifier_expr();
        }

        let token = self.peek_token()?.clone();
        Err(ParseError::UnexpectedToken {
            expected: "expression".to_string(),
            found: token_description(&token),
            span: token.span,
        })
    }

    fn parse_identifier_expr(&mut self) -> Result<Expr, ParseError> {
        let mut parts = Vec::new();
        parts.push(self.expect_identifier()?);

        while self.consume_if_simple(TokenKind::Dot) {
            parts.push(self.expect_identifier()?);
        }

        if parts.len() == 1 {
            Ok(Expr::Identifier(parts.remove(0)))
        } else {
            Ok(Expr::CompoundIdentifier(parts))
        }
    }

    fn parse_literal(&mut self) -> Result<LiteralValue, ParseError> {
        self.consume_literal().ok_or_else(|| {
            let token = self
                .peek_token()
                .cloned()
                .unwrap_or(Token { kind: TokenKind::Eof, span: Span { start: 0, end: 0 } });
            ParseError::UnexpectedToken {
                expected: "literal".to_string(),
                found: token_description(&token),
                span: token.span,
            }
        })
    }

    fn consume_literal(&mut self) -> Option<LiteralValue> {
        let token = self.tokens.get(self.index)?;
        let literal = match &token.kind {
            TokenKind::Integer(value) => LiteralValue::Integer(*value),
            TokenKind::Float(value) => LiteralValue::Float(*value),
            TokenKind::String(value) => LiteralValue::String(value.clone()),
            TokenKind::True => LiteralValue::Boolean(true),
            TokenKind::False => LiteralValue::Boolean(false),
            TokenKind::Null => LiteralValue::Null,
            _ => return None,
        };
        self.advance();
        Some(literal)
    }

    fn expect_identifier(&mut self) -> Result<String, ParseError> {
        let token = self.peek_token()?.clone();
        match token.kind {
            TokenKind::Identifier(value) => {
                self.advance();
                Ok(value)
            }
            _ => Err(ParseError::UnexpectedToken {
                expected: "identifier".to_string(),
                found: token_description(&token),
                span: token.span,
            }),
        }
    }

    fn expect_simple(&mut self, expected: TokenKind) -> Result<(), ParseError> {
        if self.consume_if_simple(expected.clone()) {
            return Ok(());
        }

        let token = self.peek_token()?.clone();
        Err(ParseError::UnexpectedToken {
            expected: token_kind_name(&expected).to_string(),
            found: token_description(&token),
            span: token.span,
        })
    }

    fn consume_if_simple(&mut self, expected: TokenKind) -> bool {
        if let Some(token) = self.tokens.get(self.index) {
            if token.kind == expected {
                self.advance();
                return true;
            }
        }
        false
    }

    fn peek_token(&self) -> Result<&Token, ParseError> {
        self.tokens.get(self.index).ok_or(ParseError::UnexpectedEof)
    }

    fn is_eof(&self) -> bool {
        matches!(self.tokens.get(self.index).map(|token| &token.kind), Some(TokenKind::Eof))
    }

    fn advance(&mut self) {
        self.index = self.index.saturating_add(1);
    }
}

fn token_description(token: &Token) -> String {
    match &token.kind {
        TokenKind::Identifier(value) => format!("identifier({value})"),
        TokenKind::Integer(value) => format!("integer({value})"),
        TokenKind::Float(value) => format!("float({value})"),
        TokenKind::String(value) => format!("string({value})"),
        other => token_kind_name(other).to_string(),
    }
}

fn token_kind_name(kind: &TokenKind) -> &'static str {
    match kind {
        TokenKind::Identifier(_) => "identifier",
        TokenKind::Integer(_) => "integer",
        TokenKind::Float(_) => "float",
        TokenKind::String(_) => "string",
        TokenKind::Create => "CREATE",
        TokenKind::Table => "TABLE",
        TokenKind::Drop => "DROP",
        TokenKind::Insert => "INSERT",
        TokenKind::Into => "INTO",
        TokenKind::Values => "VALUES",
        TokenKind::Select => "SELECT",
        TokenKind::From => "FROM",
        TokenKind::Where => "WHERE",
        TokenKind::Order => "ORDER",
        TokenKind::By => "BY",
        TokenKind::Asc => "ASC",
        TokenKind::Desc => "DESC",
        TokenKind::Limit => "LIMIT",
        TokenKind::Update => "UPDATE",
        TokenKind::Set => "SET",
        TokenKind::Delete => "DELETE",
        TokenKind::Begin => "BEGIN",
        TokenKind::Isolation => "ISOLATION",
        TokenKind::Level => "LEVEL",
        TokenKind::Snapshot => "SNAPSHOT",
        TokenKind::Commit => "COMMIT",
        TokenKind::Rollback => "ROLLBACK",
        TokenKind::Primary => "PRIMARY",
        TokenKind::Key => "KEY",
        TokenKind::Not => "NOT",
        TokenKind::Null => "NULL",
        TokenKind::Default => "DEFAULT",
        TokenKind::And => "AND",
        TokenKind::Or => "OR",
        TokenKind::True => "TRUE",
        TokenKind::False => "FALSE",
        TokenKind::IntegerType => "INTEGER",
        TokenKind::BigIntType => "BIGINT",
        TokenKind::FloatType => "FLOAT",
        TokenKind::TextType => "TEXT",
        TokenKind::BooleanType => "BOOLEAN",
        TokenKind::BlobType => "BLOB",
        TokenKind::TimestampType => "TIMESTAMP",
        TokenKind::LParen => "(",
        TokenKind::RParen => ")",
        TokenKind::Comma => ",",
        TokenKind::Dot => ".",
        TokenKind::Semicolon => ";",
        TokenKind::Asterisk => "*",
        TokenKind::Plus => "+",
        TokenKind::Minus => "-",
        TokenKind::Slash => "/",
        TokenKind::Equal => "=",
        TokenKind::NotEqual => "!=",
        TokenKind::LessThan => "<",
        TokenKind::LessThanOrEqual => "<=",
        TokenKind::GreaterThan => ">",
        TokenKind::GreaterThanOrEqual => ">=",
        TokenKind::Eof => "<eof>",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_create_table_statement() {
        let sql =
            "CREATE TABLE users (id BIGINT NOT NULL, email TEXT DEFAULT 'x', PRIMARY KEY (id));";
        let statement = parse_statement(sql).expect("parse should succeed");

        match statement {
            Statement::CreateTable(create) => {
                assert_eq!(create.name, "users");
                assert_eq!(create.columns.len(), 2);
                assert_eq!(create.primary_key, vec!["id".to_string()]);
            }
            other => panic!("unexpected statement: {other:?}"),
        }
    }

    #[test]
    fn parses_select_with_predicates_and_limit() {
        let sql = "SELECT id, email FROM users WHERE id = 7 ORDER BY email DESC LIMIT 10";
        let statement = parse_statement(sql).expect("parse should succeed");

        match statement {
            Statement::Select(select) => {
                assert_eq!(select.from.name, "users");
                assert_eq!(select.projection.len(), 2);
                assert_eq!(select.order_by.len(), 1);
                assert_eq!(select.limit, Some(10));
                assert!(select.where_clause.is_some());
            }
            other => panic!("unexpected statement: {other:?}"),
        }
    }

    #[test]
    fn parses_transaction_statements() {
        let sql = "BEGIN ISOLATION LEVEL SNAPSHOT; COMMIT; ROLLBACK;";
        let statements = parse_sql(sql).expect("parse should succeed");
        assert_eq!(statements.len(), 3);
        assert!(matches!(statements[0], Statement::Begin(_)));
        assert!(matches!(statements[1], Statement::Commit));
        assert!(matches!(statements[2], Statement::Rollback));
    }

    #[test]
    fn rejects_missing_primary_key_clause() {
        let err = parse_statement("CREATE TABLE users (id BIGINT)").expect_err("must fail");
        assert!(matches!(err, ParseError::InvalidStatement(_)));
    }
}
