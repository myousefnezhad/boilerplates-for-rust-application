use deadpool_postgres::{Config, Pool, Runtime::Tokio1};
use tokio_postgres::NoTls;

/// This struct provides read/write pools for PostgreSQL and path of query libraris
pub struct PgPools {
    pub read_pool: Pool,
    pub write_pool: Pool,
    pub query_lib_path: &'static str,
}

impl PgPools {
    /// This function generates the PostgreSQL pools based on provided settings
    ///
    /// ```
    /// let pool = PgPools::new(
    ///    "postgres", // PostgreSQL username
    ///    "postgres", // PostgreSQL password
    ///    "mydb", // PostgreSQL database name
    ///    "localhost", // Host address for read pool
    ///    5432, // Host port for read pool
    ///    "localhost", // Host address for write pool
    ///    "5432", // Host port for write pool
    ///    "/SQL", // Path that SQL files are stored in server
    /// );
    /// ```
    pub fn new(
        user: &str,
        pass: &str,
        db_name: &str,
        read_host: &str,
        read_port: u16,
        write_host: &str,
        write_port: u16,
        lib_path: &'static str,
    ) -> Self {
        let mut pg_read_config = Config::new();
        pg_read_config.user = Some(user.to_string());
        pg_read_config.password = Some(pass.to_string());
        pg_read_config.dbname = Some(db_name.to_string());
        pg_read_config.port = Some(read_port);
        pg_read_config.host = Some(read_host.to_string());
        let mut pg_write_config = Config::new();
        pg_write_config.user = Some(user.to_string());
        pg_write_config.password = Some(pass.to_string());
        pg_write_config.dbname = Some(db_name.to_string());
        pg_write_config.port = Some(write_port);
        pg_write_config.host = Some(write_host.to_string());
        Self {
            read_pool: pg_read_config.create_pool(Some(Tokio1), NoTls).unwrap(),
            write_pool: pg_write_config.create_pool(Some(Tokio1), NoTls).unwrap(),
            query_lib_path: lib_path,
        }
    }

    /// This function returns either a read (if `is_read_only = true`) or write pool
    ///
    /// ```
    /// let pool = PgPools::new(...)
    /// let read_client = pool.connection(true).get().await?;
    /// ```
    pub fn connection(&self, is_read_only: bool) -> &Pool {
        if is_read_only {
            &self.read_pool
        } else {
            &self.write_pool
        }
    }
}