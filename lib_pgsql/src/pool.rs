use std::usize;

use deadpool_postgres::{Manager, ManagerConfig, Pool, RecyclingMethod};
use tokio_postgres::NoTls;

/// This struct provides read/write pools for PostgreSQL and path of query libraris
pub struct PgPools {
    pub read_pool: Pool,
    pub write_pool: Pool,
    pub query_lib_path: String,
}

impl PgPools {
    /// This function generates the PostgreSQL pools based on provided settings
    ///
    /// ```no_run
    /// let pool = PgPools::new(
    ///    "postgres", // PostgreSQL username
    ///    "postgres", // PostgreSQL password
    ///    "mydb", // PostgreSQL database name
    ///    "localhost", // Host address for read pool
    ///    5432, // Host port for read pool
    ///    5, // read pool size
    ///    "localhost", // Host address for write pool
    ///    "5432", // Host port for write pool
    ///    5, // write pool size
    ///    "/SQL", // Path that SQL files are stored in server
    /// );
    /// ```
    pub fn new(
        user: &str,
        pass: &str,
        db_name: &str,
        read_host: &str,
        read_port: u16,
        read_pool_size: usize,
        write_host: &str,
        write_port: u16,
        write_pool_size: usize,
        lib_path: String,
    ) -> Self {
        let mut pg_read_config = tokio_postgres::Config::new();
        pg_read_config.user(user);
        pg_read_config.password(pass);
        pg_read_config.dbname(db_name);
        pg_read_config.port(read_port);
        pg_read_config.host(read_host);
        let pg_read_mgr_cfg = ManagerConfig {
            recycling_method: RecyclingMethod::Fast,
        };
        let pg_read_mgr = Manager::from_config(pg_read_config, NoTls, pg_read_mgr_cfg);
        let mut pg_write_config = tokio_postgres::Config::new();
        pg_write_config.user(user);
        pg_write_config.password(pass);
        pg_write_config.dbname(db_name);
        pg_write_config.port(write_port);
        pg_write_config.host(write_host);
        let pg_write_mgr_cfg = ManagerConfig {
            recycling_method: RecyclingMethod::Clean,
        };
        let pg_write_mgr = Manager::from_config(pg_write_config, NoTls, pg_write_mgr_cfg);
        Self {
            read_pool: Pool::builder(pg_read_mgr)
                .max_size(read_pool_size)
                .build()
                .unwrap(),
            write_pool: Pool::builder(pg_write_mgr)
                .max_size(write_pool_size)
                .build()
                .unwrap(),
            query_lib_path: lib_path,
        }
    }

    /// This function returns either a read (if `is_read_only = true`) or write pool
    ///
    /// ```no_run
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
