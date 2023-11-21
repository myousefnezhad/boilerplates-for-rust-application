use crate::common::SQLError;
use tokio_postgres::{connect, tls::NoTlsStream, Client, Connection, NoTls, Socket};

/// This struct provides tokio client for PostgreSQL and path of query libraris
#[derive(Debug)]
pub struct PgClient {
    pub read_connection: String,
    pub write_connection: String,
    pub lib_path: String,
}

impl PgClient {
    /// This function generates the PostgreSQL pools based on provided settings
    ///
    /// ```no_run
    /// let client = PgClient::new(
    ///    "postgresql://user:password@localhost:5432/test"
    ///    "postgresql://user:password@localhost:5432/test"
    ///    "/SQL", // Path that SQL files are stored in server
    /// );
    /// ```
    pub fn new(read_connection: &str, write_connection: &str, lib_path: &str) -> Self {
        Self {
            read_connection: read_connection.to_string(),
            write_connection: write_connection.to_string(),
            lib_path: lib_path.to_string(),
        }
    }

    /// This function returns either a read (if `is_read_only = true`) or write pool
    ///
    /// ```no_run
    /// let pool = PgPools::new(...)
    /// let read_client = pool.connection(true).get().await?;
    /// ```
    pub async fn connection(
        &self,
        is_read_only: bool,
    ) -> Result<(Client, Connection<Socket, NoTlsStream>), SQLError> {
        if is_read_only {
            Ok(connect(&self.read_connection, NoTls).await?)
        } else {
            Ok(connect(&self.write_connection, NoTls).await?)
        }
    }
}
