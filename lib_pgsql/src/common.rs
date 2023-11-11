use core::fmt;
use std::io;

/// This `enum` can handle any error generated druing PostgreSQL operations
///
/// You can use it for error handle in `Result<_, SQLError>`
#[derive(Debug)]
pub enum SQLError {
    TkError(tokio_postgres::Error),
    IoError(io::Error),
    PoolError(deadpool_postgres::PoolError),
    StringError(String),
}

/// Convert `tokio_postgre` Error to `SQLError`
impl From<tokio_postgres::Error> for SQLError {
    fn from(value: tokio_postgres::Error) -> Self {
        Self::TkError(value)
    }
}

/// Convert `io` (file operation) Error to `SQLError`
impl From<io::Error> for SQLError {
    fn from(value: io::Error) -> Self {
        Self::IoError(value)
    }
}

/// Convert `deadpool_postgres` Pool Error to `SQLError`
impl From<deadpool_postgres::PoolError> for SQLError {
    fn from(value: deadpool_postgres::PoolError) -> Self {
        Self::PoolError(value)
    }
}

impl From<String> for SQLError {
    fn from(value: String) -> Self {
        Self::StringError(value)
    }
}

/// This `enum` provides different type input as query
///
/// `RAW("SQL query")` is string query type
///
/// `FILE("file full path")` reads query from file address
///
/// `LIB("file name in lib folder")` reads query from lib folder set in `PgPool` config
#[derive(Debug, Clone)]
pub enum QueryType {
    RAW(String),
    FILE(String),
    LIB(String),
}

/// This `enum` provides sorting kind in SQL queries
#[derive(Debug, Clone, Copy)]
pub enum SQLSort {
    ASC,
    DESC,
}

impl fmt::Display for SQLSort {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::ASC => write!(f, "ASC"),
            Self::DESC => write!(f, "DESC"),
        }
    }
}

/// This `enum` provides condition for SQL queries. `SQLCondition::EQUAL("id")` means `id = $1`
pub enum SQLCondition<'a> {
    EQUAL(&'a str),
    NEQ(&'a str),
    LESS(&'a str),
    LE(&'a str),
    GREATER(&'a str),
    GE(&'a str),
    AND,
    OR,
}

impl<'a> fmt::Display for SQLCondition<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::EQUAL(id) => write!(f, " {} = ##ID## ", id),
            Self::NEQ(id) => write!(f, " {} <> ##ID## ", id),
            Self::LESS(id) => write!(f, " {} < ##ID## ", id),
            Self::LE(id) => write!(f, " {} <= ##ID## ", id),
            Self::GREATER(id) => write!(f, " {} > ##ID## ", id),
            Self::GE(id) => write!(f, " {} >= ##ID## ", id),
            Self::AND => write!(f, " AND "),
            Self::OR => write!(f, " OR "),
        }
    }
}
