use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Span {
    pub start: usize,
    pub end: usize,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Token {
    pub kind: TokenKind,
    pub span: Span,
}

#[derive(Debug, Clone, PartialEq)]
pub enum TokenKind {
    Identifier(String),
    Integer(i64),
    Float(f64),
    String(String),

    // Keywords
    Create,
    Table,
    Drop,
    Insert,
    Into,
    Values,
    Select,
    From,
    Where,
    Order,
    By,
    Asc,
    Desc,
    Limit,
    Update,
    Set,
    Delete,
    Begin,
    Isolation,
    Level,
    Snapshot,
    Commit,
    Rollback,
    Primary,
    Key,
    Not,
    Null,
    Default,
    And,
    Or,
    True,
    False,
    IntegerType,
    BigIntType,
    FloatType,
    TextType,
    BooleanType,
    BlobType,
    TimestampType,

    // Punctuation / operators
    LParen,
    RParen,
    Comma,
    Dot,
    Semicolon,
    Asterisk,
    Plus,
    Minus,
    Slash,
    Equal,
    NotEqual,
    LessThan,
    LessThanOrEqual,
    GreaterThan,
    GreaterThanOrEqual,

    Eof,
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum LexError {
    #[error("unexpected character '{ch}' at byte {position}")]
    UnexpectedCharacter { ch: char, position: usize },
    #[error("unterminated string starting at byte {position}")]
    UnterminatedString { position: usize },
    #[error("invalid numeric literal '{literal}' at byte {position}")]
    InvalidNumber { literal: String, position: usize },
}

pub fn tokenize(input: &str) -> Result<Vec<Token>, LexError> {
    Lexer::new(input).tokenize()
}

struct Lexer<'a> {
    input: &'a str,
    pos: usize,
}

impl<'a> Lexer<'a> {
    fn new(input: &'a str) -> Self {
        Self { input, pos: 0 }
    }

    fn tokenize(mut self) -> Result<Vec<Token>, LexError> {
        let mut tokens = Vec::new();

        while let Some(ch) = self.peek_char() {
            if ch.is_whitespace() {
                self.bump_char();
                continue;
            }

            let start = self.pos;
            let kind = match ch {
                '(' => {
                    self.bump_char();
                    TokenKind::LParen
                }
                ')' => {
                    self.bump_char();
                    TokenKind::RParen
                }
                ',' => {
                    self.bump_char();
                    TokenKind::Comma
                }
                '.' => {
                    self.bump_char();
                    TokenKind::Dot
                }
                ';' => {
                    self.bump_char();
                    TokenKind::Semicolon
                }
                '*' => {
                    self.bump_char();
                    TokenKind::Asterisk
                }
                '+' => {
                    self.bump_char();
                    TokenKind::Plus
                }
                '-' => {
                    self.bump_char();
                    TokenKind::Minus
                }
                '/' => {
                    self.bump_char();
                    TokenKind::Slash
                }
                '=' => {
                    self.bump_char();
                    TokenKind::Equal
                }
                '<' => {
                    self.bump_char();
                    match self.peek_char() {
                        Some('=') => {
                            self.bump_char();
                            TokenKind::LessThanOrEqual
                        }
                        Some('>') => {
                            self.bump_char();
                            TokenKind::NotEqual
                        }
                        _ => TokenKind::LessThan,
                    }
                }
                '>' => {
                    self.bump_char();
                    if self.peek_char() == Some('=') {
                        self.bump_char();
                        TokenKind::GreaterThanOrEqual
                    } else {
                        TokenKind::GreaterThan
                    }
                }
                '!' => {
                    self.bump_char();
                    if self.peek_char() == Some('=') {
                        self.bump_char();
                        TokenKind::NotEqual
                    } else {
                        return Err(LexError::UnexpectedCharacter { ch, position: start });
                    }
                }
                '\'' => self.lex_string(start)?,
                c if c.is_ascii_digit() => self.lex_number(start)?,
                c if is_identifier_start(c) => self.lex_identifier_or_keyword(start),
                other => {
                    return Err(LexError::UnexpectedCharacter { ch: other, position: start });
                }
            };

            tokens.push(Token { kind, span: Span { start, end: self.pos } });
        }

        tokens.push(Token { kind: TokenKind::Eof, span: Span { start: self.pos, end: self.pos } });
        Ok(tokens)
    }

    fn lex_string(&mut self, start: usize) -> Result<TokenKind, LexError> {
        // Consume opening quote.
        self.bump_char();

        let mut value = String::new();
        loop {
            let Some(ch) = self.peek_char() else {
                return Err(LexError::UnterminatedString { position: start });
            };

            self.bump_char();
            if ch == '\'' {
                if self.peek_char() == Some('\'') {
                    self.bump_char();
                    value.push('\'');
                    continue;
                }
                break;
            }

            value.push(ch);
        }

        Ok(TokenKind::String(value))
    }

    fn lex_number(&mut self, start: usize) -> Result<TokenKind, LexError> {
        while matches!(self.peek_char(), Some(ch) if ch.is_ascii_digit()) {
            self.bump_char();
        }

        let mut is_float = false;
        if self.peek_char() == Some('.') {
            let mut clone = self.clone();
            clone.bump_char();
            if matches!(clone.peek_char(), Some(ch) if ch.is_ascii_digit()) {
                is_float = true;
                self.bump_char();
                while matches!(self.peek_char(), Some(ch) if ch.is_ascii_digit()) {
                    self.bump_char();
                }
            }
        }

        let literal = &self.input[start..self.pos];
        if is_float {
            let value = literal.parse::<f64>().map_err(|_| LexError::InvalidNumber {
                literal: literal.to_string(),
                position: start,
            })?;
            Ok(TokenKind::Float(value))
        } else {
            let value = literal.parse::<i64>().map_err(|_| LexError::InvalidNumber {
                literal: literal.to_string(),
                position: start,
            })?;
            Ok(TokenKind::Integer(value))
        }
    }

    fn lex_identifier_or_keyword(&mut self, start: usize) -> TokenKind {
        while matches!(self.peek_char(), Some(ch) if is_identifier_part(ch)) {
            self.bump_char();
        }

        let raw = &self.input[start..self.pos];
        let normalized = raw.to_ascii_uppercase();
        match normalized.as_str() {
            "CREATE" => TokenKind::Create,
            "TABLE" => TokenKind::Table,
            "DROP" => TokenKind::Drop,
            "INSERT" => TokenKind::Insert,
            "INTO" => TokenKind::Into,
            "VALUES" => TokenKind::Values,
            "SELECT" => TokenKind::Select,
            "FROM" => TokenKind::From,
            "WHERE" => TokenKind::Where,
            "ORDER" => TokenKind::Order,
            "BY" => TokenKind::By,
            "ASC" => TokenKind::Asc,
            "DESC" => TokenKind::Desc,
            "LIMIT" => TokenKind::Limit,
            "UPDATE" => TokenKind::Update,
            "SET" => TokenKind::Set,
            "DELETE" => TokenKind::Delete,
            "BEGIN" => TokenKind::Begin,
            "ISOLATION" => TokenKind::Isolation,
            "LEVEL" => TokenKind::Level,
            "SNAPSHOT" => TokenKind::Snapshot,
            "COMMIT" => TokenKind::Commit,
            "ROLLBACK" => TokenKind::Rollback,
            "PRIMARY" => TokenKind::Primary,
            "KEY" => TokenKind::Key,
            "NOT" => TokenKind::Not,
            "NULL" => TokenKind::Null,
            "DEFAULT" => TokenKind::Default,
            "AND" => TokenKind::And,
            "OR" => TokenKind::Or,
            "TRUE" => TokenKind::True,
            "FALSE" => TokenKind::False,
            "INTEGER" => TokenKind::IntegerType,
            "BIGINT" => TokenKind::BigIntType,
            "FLOAT" => TokenKind::FloatType,
            "TEXT" => TokenKind::TextType,
            "BOOLEAN" => TokenKind::BooleanType,
            "BLOB" => TokenKind::BlobType,
            "TIMESTAMP" => TokenKind::TimestampType,
            _ => TokenKind::Identifier(raw.to_string()),
        }
    }

    fn peek_char(&self) -> Option<char> {
        self.input[self.pos..].chars().next()
    }

    fn bump_char(&mut self) {
        let Some(ch) = self.peek_char() else {
            return;
        };
        self.pos += ch.len_utf8();
    }
}

impl Clone for Lexer<'_> {
    fn clone(&self) -> Self {
        Self { input: self.input, pos: self.pos }
    }
}

fn is_identifier_start(ch: char) -> bool {
    ch.is_ascii_alphabetic() || ch == '_'
}

fn is_identifier_part(ch: char) -> bool {
    ch.is_ascii_alphanumeric() || ch == '_'
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tokenizes_create_table_statement() {
        let sql = "CREATE TABLE users (id BIGINT NOT NULL, email TEXT, PRIMARY KEY (id));";
        let tokens = tokenize(sql).expect("lex should succeed");

        assert!(tokens.iter().any(|token| token.kind == TokenKind::Create));
        assert!(tokens.iter().any(|token| token.kind == TokenKind::BigIntType));
        assert!(tokens.iter().any(|token| token.kind == TokenKind::Primary));
        assert!(matches!(tokens.last(), Some(Token { kind: TokenKind::Eof, .. })));
    }

    #[test]
    fn tokenizes_literals_and_operators() {
        let sql = "WHERE age >= 18 AND name != 'o''hara'";
        let tokens = tokenize(sql).expect("lex should succeed");

        assert!(tokens.iter().any(|token| token.kind == TokenKind::GreaterThanOrEqual));
        assert!(tokens.iter().any(|token| token.kind == TokenKind::And));
        assert!(tokens.iter().any(|token| token.kind == TokenKind::NotEqual));
        assert!(tokens.iter().any(|token| token.kind == TokenKind::String("o'hara".to_string())));
    }

    #[test]
    fn reports_unterminated_strings() {
        let err = tokenize("SELECT 'abc").expect_err("lex should fail");
        assert!(matches!(err, LexError::UnterminatedString { .. }));
    }
}
