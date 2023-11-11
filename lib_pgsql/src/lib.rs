//! A set of functions and libraries to use PostgreSQL in Rust
//!
//! `PgPools` provides a seprated read/write pool connection
//!
//! `Queryable` is an async trait that can connect a general struct to PostgreSQL
//!
//! ```
//! use postgres_from_row::FromRow;
//! use serde::{Deserialize, Serialize};
//! use tokio;
//! use tokio_postgres::types::{FromSql, ToSql};
//! #[derive(Debug, FromRow, ToSql, FromSql, Serialize, Deserialize)]
//! struct ExampleTable {
//!    id: i64,
//!    name: String,
//! }
//!
//! impl Queryable<'_> for ExampleTable {
//!    type RowType = Self;
//!    fn table_name() -> &'static str {
//!        "public.example_table"
//!    }
//! }
//!
//! #[tokio::main]
//! async fn main() -> Result<(), SQLError> {
//!     let pool = PgPools::new(
//!         "postgres", // PostgreSQL username
//!         "postgres", // PostgreSQL password
//!         "mydb", // PostgreSQL database name
//!         "localhost", // Host address for read pool
//!         5432, // Host port for read pool
//!         "localhost", // Host address for write pool
//!         "5432", // Host port for write pool
//!         "/SQL", // Path that SQL files are stored in server
//!     );
//!     let results = ExampleTable::select_typed(&pool, None, None, None, &[], None, None).await?;
//!     println!("{:#?}", results);
//! }
//! ```
//!
//!
//!
//! Recommended crates that should include in client side:
//!```
//! log = ""
//! dotenv = ""
//! serde_json = ""
//! futures-util = ""
//! tokio-postgres = ""
//! tokio-pg-mapper = ""
//! postgres-from-row = ""
//! tokio = {version = "", features = ["full"]}
//! serde = {version = "", features = ["derive"]}
//! postgres-types = { version = "", features = ["derive"] }
//!```

/// This module provides common `enum` and `struct` for PostgreSQL operations
pub mod common;
/// This module provides libraries and functions to generate PostgreSQL connection pools
pub mod pool;
/// This module provides an async trait for PostgreSQL operations for Rust structs
pub mod queryable;

pub use common::{QueryType, SQLCondition, SQLError, SQLSort};
pub use futures_util::pin_mut;
pub use pool::PgPools;
pub use postgres_from_row::FromRow;
pub use queryable::Queryable;
pub use serde::{Deserialize, Serialize};
pub use tokio;
pub use tokio_postgres::types::{FromSql, ToSql};
