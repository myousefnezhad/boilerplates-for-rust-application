use crate::common::{QueryType, SQLCondition, SQLError, SQLSort};
use crate::pool::PgPools;
use async_trait::async_trait;
use core::iter::IntoIterator;
use core::marker::Sync;
use deadpool_postgres::Client;
use futures_util::{pin_mut, TryStreamExt};
use log::{self, debug};
use num::One;
use postgres_from_row::FromRow;
use serde::Serialize;
use std::fs::read_to_string;
use std::ops::Add;
use tokio_postgres::Statement;
use tokio_postgres::{
    types::{FromSql, ToSql},
    Row, RowStream,
};

/// This is an async trait that can implement PostgreSQL operation for a Rust struct
#[async_trait]
pub trait DPQueryable<'a> {
    /// This should be `Self` for each struct in `impl` section
    ///
    /// ```
    /// impl Queryable<'_> for ExampleTable {
    ///    type RowType = Self;
    ///    fn table_name() -> &'static str {
    ///        "public.example_table"
    ///    }
    /// }
    /// ```
    type RowType: FromSql<'a> + FromRow + ToSql + Serialize + Send;

    /// Set PostgreSQL table name for each struct
    ///
    /// It should be reimplemented for each struct to assign corresponding PostgreSQL table
    ///
    /// ```
    /// fn table_name() -> &'static str {
    ///     "public.example_table"
    ///}
    ///```
    fn table_name() -> &'static str {
        ""
    }

    /// Creates a new prepared statement.
    ///
    /// Prepared statements can be executed repeatedly, and may contain query parameters (indicated by `$1`, `$2`, etc),
    /// which are set when executed. Prepared statements can only be used with the connection that created them.
    async fn prepare(client: &Client, query: &str) -> Result<Statement, SQLError> {
        Ok(client.prepare(query).await?)
    }

    /// Like [`prepare`], but reads cached statements first
    ///
    /// [`prepare`]: #method.prepare
    async fn prepare_cached(client: &Client, query: &str) -> Result<Statement, SQLError> {
        Ok(client.prepare_cached(query).await?)
    }

    /// This function reads SQL file (text format) from provided full path
    fn read_sql_file(file: &str) -> Result<String, SQLError> {
        Ok(read_to_string(file)?)
    }

    /// Parse QueryType to load a query from raw string, file, or lib folder
    async fn query_as_string(
        query: &QueryType,
        pool: Option<&PgPools>,
    ) -> Result<String, SQLError> {
        Ok(match query {
            QueryType::RAW(query) => query.to_string(),
            QueryType::FILE(file) => Self::read_sql_file(file)?,
            QueryType::LIB(lib) => {
                Self::read_sql_file(&format!("{}/{}", pool.unwrap().query_lib_path, lib))?
            }
        })
    }

    /// Executes a statement, returning the number of rows modified.
    ///
    /// A statement may contain parameters, specified by `$n`, where `n` is the index of the parameter of the list
    /// provided, 1-indexed.
    ///
    /// The `statement` argument can either be a `Statement`, or a raw query string. If the same statement will be
    /// repeatedly executed (perhaps with different query parameters), consider preparing the statement up front
    /// with the `prepare` method.
    ///
    /// If the statement does not modify any rows (e.g. `SELECT`), 0 is returned.
    async fn execute(
        pool: &PgPools,
        query: QueryType,
        params: &[&(dyn ToSql + Sync)],
        is_read_only: bool,
    ) -> Result<u64, SQLError> {
        let client = pool.connection(is_read_only).get().await?;
        let query_str = Self::query_as_string(&query, Some(&pool)).await?;
        let statement = Self::prepare_cached(&client, &query_str).await?;
        debug!("Execute {}", query_str);
        Ok(client.execute(&statement, params).await?)
    }

    /// The maximally flexible version of [`execute`].
    ///
    /// A statement may contain parameters, specified by `$n`, where `n` is the index of the parameter of the list
    /// provided, 1-indexed.
    ///
    /// [`execute`]: #method.execute
    async fn execute_raw<P, I>(
        pool: &PgPools,
        query: QueryType,
        params: I,
        is_read_only: bool,
    ) -> Result<u64, SQLError>
    where
        P: ToSql,
        I: IntoIterator<Item = P> + Sync + Send,
        I::IntoIter: ExactSizeIterator,
    {
        let client = pool.connection(is_read_only).get().await?;
        let query_str = Self::query_as_string(&query, Some(&pool)).await?;
        let statement = Self::prepare_cached(&client, &query_str).await?;
        debug!("Execute raw {}", query_str);
        Ok(client.execute_raw(&statement, params).await?)
    }

    /// Executes a statement, returning a vector of the resulting rows.
    ///
    /// A statement may contain parameters, specified by `$n`, where `n` is the index of the parameter of the list
    /// provided, 1-indexed.
    async fn query(
        pool: &PgPools,
        query: QueryType,
        params: &[&(dyn ToSql + Sync)],
        is_read_only: bool,
    ) -> Result<Vec<Row>, SQLError> {
        let client = pool.connection(is_read_only).get().await?;
        let query_str = Self::query_as_string(&query, Some(&pool)).await?;
        let statement = Self::prepare_cached(&client, &query_str).await?;
        debug!("Query {}", query_str);
        Ok(client.query(&statement, params).await?)
    }

    /// Executes a statement which returns a single row, returning it.
    ///
    /// Returns an error if the query does not return exactly one row.
    ///
    /// A statement may contain parameters, specified by `$n`, where `n` is the index of the parameter of the list
    /// provided, 1-indexed.
    async fn query_one(
        pool: &PgPools,
        query: QueryType,
        params: &[&(dyn ToSql + Sync)],
        is_read_only: bool,
    ) -> Result<Row, SQLError> {
        let client = pool.connection(is_read_only).get().await?;
        let query_str = Self::query_as_string(&query, Some(&pool)).await?;
        let statement = Self::prepare_cached(&client, &query_str).await?;
        debug!("Query one {}", query_str);
        Ok(client.query_one(&statement, params).await?)
    }

    /// Executes a statements which returns zero or one rows, returning it.
    ///
    /// Returns an error if the query returns more than one row.
    ///
    /// A statement may contain parameters, specified by `$n`, where `n` is the index of the parameter of the list
    /// provided, 1-indexed.
    async fn query_opt(
        pool: &PgPools,
        query: QueryType,
        params: &[&(dyn ToSql + Sync)],
        is_read_only: bool,
    ) -> Result<Option<Row>, SQLError> {
        let client = pool.connection(is_read_only).get().await?;
        let query_str = Self::query_as_string(&query, Some(&pool)).await?;
        let statement = Self::prepare_cached(&client, &query_str).await?;
        debug!("Query opt {}", query_str);
        Ok(client.query_opt(&statement, params).await?)
    }

    /// The maximally flexible version of [`query`].
    ///
    /// A statement may contain parameters, specified by `$n`, where `n` is the index of the parameter of the list
    /// provided, 1-indexed.
    ///
    /// [`query`]: #method.query
    async fn query_raw<I, P>(
        pool: &PgPools,
        query: QueryType,
        params: I,
        is_read_only: bool,
    ) -> Result<RowStream, SQLError>
    where
        P: ToSql,
        I: IntoIterator<Item = P> + Sync + Send,
        I::IntoIter: ExactSizeIterator,
    {
        let client = pool.connection(is_read_only).get().await?;
        let query_str = Self::query_as_string(&query, Some(&pool)).await?;
        let statement = Self::prepare_cached(&client, &query_str).await?;
        debug!("Query raw {}", query_str);
        Ok(client.query_raw(&statement, params).await?)
    }

    /// This function converts PostgreSQL Row type to provided type in RowType section (Rust struct type)
    fn parse_type(row: &Row) -> Result<Self::RowType, SQLError> {
        Self::parse_generic_type::<Self::RowType>(row)
    }

    /// This function converts PostgreSQL Row type to any generic Rust type with FromRow trait implementation
    fn parse_generic_type<T>(row: &Row) -> Result<T, SQLError>
    where
        T: FromRow,
    {
        Ok(T::try_from_row(row)?)
    }

    /// Like [`query`], but parse result to a vector of RowType
    ///
    /// [`query`]: #method.query
    async fn query_typed(
        pool: &PgPools,
        query: QueryType,
        params: &[&(dyn ToSql + Sync)],
        is_read_only: bool,
    ) -> Result<Vec<Self::RowType>, SQLError> {
        let raws = Self::query(pool, query, params, is_read_only).await?;
        raws.into_iter()
            .map(|row| {
                let res = Self::parse_type(&row)?;
                Ok(res)
            })
            .collect()
    }

    /// Like [`query_one`], but parse result to a RowType
    ///
    /// [`query_one`]: #method.query_one
    async fn query_one_typed(
        pool: &PgPools,
        query: QueryType,
        params: &[&(dyn ToSql + Sync)],
        is_read_only: bool,
    ) -> Result<Self::RowType, SQLError> {
        let row = Self::query_one(pool, query, params, is_read_only).await?;
        Ok(Self::parse_type(&row)?)
    }

    /// Like [`query_opt`], but parse result to a optional RowType
    ///
    /// [`query_opt`]: #method.query_opt
    async fn query_opt_typed(
        pool: &PgPools,
        query: QueryType,
        params: &[&(dyn ToSql + Sync)],
        is_read_only: bool,
    ) -> Result<Option<Self::RowType>, SQLError> {
        match Self::query_opt(pool, query, params, is_read_only).await? {
            None => Ok(None),
            Some(row) => Ok(Some(Self::parse_type(&row)?)),
        }
    }

    /// Like [`query_raw`], but parse result to a vector of RowType
    ///
    /// [`query_raw`]: #method.query_raw
    async fn query_raw_typed<I, P>(
        pool: &PgPools,
        query: QueryType,
        params: I,
        is_read_only: bool,
    ) -> Result<Vec<Self::RowType>, SQLError>
    where
        P: ToSql,
        I: IntoIterator<Item = P> + Sync + Send,
        I::IntoIter: ExactSizeIterator,
    {
        let mut result: Vec<Self::RowType> = Vec::new();
        let raws = Self::query_raw(pool, query, params, is_read_only).await?;
        pin_mut!(raws);
        while let Some(row) = raws.try_next().await? {
            let res = Self::parse_type(&row)?;
            result.push(res);
        }
        Ok(result)
    }

    /// This function convert an optional vector of string to a list of PostgreSQL fields;
    /// `None` results `*`
    fn field_query_builder(field_list: Option<Vec<&str>>) -> String {
        match field_list {
            None => "*".to_owned(),
            Some(items) => match items.len() {
                0 => "*".to_owned(),
                _ => items.join(", "),
            },
        }
    }

    /// This function converts a vector of Rust `SQLCondition` values to PostgreSQL `WHERE` params
    fn filter_query_builder(filter_list: Option<Vec<SQLCondition<'_>>>, offset: i32) -> String {
        match filter_list {
            None => return "".to_owned(),
            Some(filters) => match filters.len() {
                0 => return "".to_owned(),
                _ => {
                    let mut filter_index = offset;
                    let filter_query: Vec<String> = filters
                        .into_iter()
                        .map(|filter| match filter {
                            SQLCondition::OR | SQLCondition::AND => filter.to_string(),
                            _ => {
                                filter_index += 1;
                                let s = format!("${}", filter_index);
                                filter.to_string().replace("##ID##", &s)
                            }
                        })
                        .collect();
                    return format!(" WHERE {} ", filter_query.join(""));
                }
            },
        }
    }

    /// This function converts a list of optional string to PostgreSQL sorting params (`ORDER BY ...`)
    fn sort_query_builder(sort_list: Option<Vec<&str>>, sort_type: Option<SQLSort>) -> String {
        match sort_list {
            None => "".to_owned(),
            Some(items) => match items.len() {
                0 => return "".to_owned(),
                _ => {
                    let sort_order = match sort_type {
                        None => "ASC".to_owned(),
                        Some(sort) => sort.to_string(),
                    };
                    return format!(" ORDER BY {} {} ", items.join(", "), sort_order);
                }
            },
        }
    }

    /// This function generates `SELECT` query
    fn select_query_builder(
        table_name: Option<&str>,
        field_list: Option<Vec<&str>>,
        filter_list: Option<Vec<SQLCondition<'_>>>,
        sort_list: Option<Vec<&str>>,
        sort_type: Option<SQLSort>,
    ) -> String {
        let table_name = match table_name {
            None => Self::table_name(),
            Some(name) => name,
        };
        let fields = Self::field_query_builder(field_list);
        let filters = Self::filter_query_builder(filter_list, 0);
        let sorts = Self::sort_query_builder(sort_list, sort_type);
        format!(
            "SELECT {} FROM {} {} {}",
            fields, table_name, filters, sorts
        )
    }

    /// Running a `SELECT` query and return a vector of PostgreSQL `Row` type
    async fn select(
        pool: &PgPools,
        table_name: Option<&str>,
        field_list: Option<Vec<&str>>,
        filter_list: Option<Vec<SQLCondition<'_>>>,
        filter_values: &[&(dyn ToSql + Sync)],
        sort_list: Option<Vec<&str>>,
        sort_type: Option<SQLSort>,
    ) -> Result<Vec<Row>, SQLError> {
        let query =
            Self::select_query_builder(table_name, field_list, filter_list, sort_list, sort_type);
        Self::query(pool, QueryType::RAW(query), filter_values, true).await
    }

    /// Like [`select`], but output should be just one row, unless cause error
    ///
    /// [`select`]: #method.select
    async fn select_one(
        pool: &PgPools,
        table_name: Option<&str>,
        field_list: Option<Vec<&str>>,
        filter_list: Option<Vec<SQLCondition<'_>>>,
        filter_values: &[&(dyn ToSql + Sync)],
    ) -> Result<Row, SQLError> {
        let query = Self::select_query_builder(table_name, field_list, filter_list, None, None);
        Self::query_one(pool, QueryType::RAW(query), filter_values, true).await
    }

    /// Like [`select`], but output should be maximum one row or nothing, unless cause error
    ///
    /// [`select`]: #method.select
    async fn select_opt(
        pool: &PgPools,
        table_name: Option<&str>,
        field_list: Option<Vec<&str>>,
        filter_list: Option<Vec<SQLCondition<'_>>>,
        filter_values: &[&(dyn ToSql + Sync)],
    ) -> Result<Option<Row>, SQLError> {
        let query = Self::select_query_builder(table_name, field_list, filter_list, None, None);
        Self::query_opt(pool, QueryType::RAW(query), filter_values, true).await
    }

    /// Like [`select`], but parse output to Rust `RowType` provided in implementation of this trait
    ///
    /// [`select`]: #method.select
    async fn select_typed(
        pool: &PgPools,
        table_name: Option<&str>,
        filter_list: Option<Vec<SQLCondition<'_>>>,
        filter_values: &[&(dyn ToSql + Sync)],
        sort_list: Option<Vec<&str>>,
        sort_type: Option<SQLSort>,
    ) -> Result<Vec<Self::RowType>, SQLError> {
        let raws = Self::select(
            pool,
            table_name,
            None,
            filter_list,
            filter_values,
            sort_list,
            sort_type,
        )
        .await?;
        raws.into_iter()
            .map(|row| {
                let res = Self::parse_type(&row)?;
                Ok(res)
            })
            .collect()
    }

    /// Like [`select_one`], but parse output to `RowType`
    ///
    /// [`select_one`]: #method.select_one
    async fn select_one_typed(
        pool: &PgPools,
        table_name: Option<&str>,
        filter_list: Option<Vec<SQLCondition<'_>>>,
        filter_values: &[&(dyn ToSql + Sync)],
    ) -> Result<Self::RowType, SQLError> {
        let row = Self::select_one(pool, table_name, None, filter_list, filter_values).await?;
        Ok(Self::parse_type(&row)?)
    }

    /// Like [`select_opt`], but parse output to `RowType`
    ///
    /// [`select_opt`]: #method.select_opt
    async fn select_opt_typed(
        pool: &PgPools,
        table_name: Option<&str>,
        filter_list: Option<Vec<SQLCondition<'_>>>,
        filter_values: &[&(dyn ToSql + Sync)],
    ) -> Result<Option<Self::RowType>, SQLError> {
        match Self::select_opt(pool, table_name, None, filter_list, filter_values).await? {
            None => Ok(None),
            Some(row) => Ok(Some(Self::parse_type(&row)?)),
        }
    }

    /// Run a `SELECT` query and return number of rows
    async fn count(
        pool: &PgPools,
        table_name: Option<&str>,
        filter_list: Option<Vec<SQLCondition<'_>>>,
        filter_values: &[&(dyn ToSql + Sync)],
    ) -> Result<u64, SQLError> {
        let query = Self::select_query_builder(table_name, None, filter_list, None, None);
        Self::execute(pool, QueryType::RAW(query), filter_values, true).await
    }

    /// Run a `SELECT` query and return `true` if find any row(s)
    async fn exists(
        pool: &PgPools,
        table_name: Option<&str>,
        filter_list: Option<Vec<SQLCondition<'_>>>,
        filter_values: &[&(dyn ToSql + Sync)],
    ) -> Result<bool, SQLError> {
        Ok(
            match Self::count(pool, table_name, filter_list, filter_values).await? {
                0 => false,
                _ => true,
            },
        )
    }

    /// Run a 'SELECT' query and return `true` if exactly find one row
    async fn exists_one(
        pool: &PgPools,
        table_name: Option<&str>,
        filter_list: Option<Vec<SQLCondition<'_>>>,
        filter_values: &[&(dyn ToSql + Sync)],
    ) -> Result<bool, SQLError> {
        Ok(
            match Self::count(pool, table_name, filter_list, filter_values).await? {
                1 => true,
                _ => false,
            },
        )
    }

    /// Calculate SQL `MIN()` value of generic type `T` using a PostgreSQL `SELECT` query
    async fn min<T>(
        pool: &PgPools,
        table_name: Option<&str>,
        field_name: &str,
        filter_list: Option<Vec<SQLCondition<'_>>>,
        filter_values: &[&(dyn ToSql + Sync)],
    ) -> Result<T, SQLError>
    where
        for<'b> T: FromSql<'b>,
    {
        Ok(Self::select_one(
            pool,
            table_name,
            Some(vec![&format!("MIN({}) as min", field_name)]),
            filter_list,
            filter_values,
        )
        .await?
        .get("min"))
    }

    /// Calculate SQL `MAX()` value of generic type `T` using a PostgreSQL `SELECT` query
    async fn max<T>(
        pool: &PgPools,
        table_name: Option<&str>,
        field_name: &str,
        filter_list: Option<Vec<SQLCondition<'_>>>,
        filter_values: &[&(dyn ToSql + Sync)],
    ) -> Result<T, SQLError>
    where
        for<'b> T: FromSql<'b>,
    {
        Ok(Self::select_one(
            pool,
            table_name,
            Some(vec![&format!("MAX({}) as max", field_name)]),
            filter_list,
            filter_values,
        )
        .await?
        .get("max"))
    }

    /// Calculate current value + `1` of generic integer type `T` using the [`max`] function
    ///
    /// [`max`]: #method.max
    async fn next<T>(
        pool: &PgPools,
        table_name: Option<&str>,
        field_name: &str,
    ) -> Result<T, SQLError>
    where
        for<'b> T: FromSql<'b> + Add<T, Output = T> + Copy + One,
    {
        Ok(Self::max::<T>(pool, table_name, field_name, None, &[]).await? + One::one())
    }

    /// Insert one row to PostgreSQL
    async fn insert(
        pool: &PgPools,
        table_name: Option<&str>,
        field_list: Option<Vec<&str>>,
        values: &[&(dyn ToSql + Sync)],
    ) -> Result<u64, SQLError> {
        let table_name = match table_name {
            None => Self::table_name(),
            Some(name) => name,
        };
        let mut query = format!("INSERT INTO {} ", table_name);
        let param_vec: Vec<String> = (1..values.len() + 1)
            .into_iter()
            .map(|val| format!("${}", val))
            .collect();
        let params = param_vec.join(", ");
        match field_list {
            None => query = format!("{} VALUES ({});", query, params),
            Some(fields) => {
                query = format!("{} ({}) VALUES ({});", query, fields.join(", "), params)
            }
        };
        Self::execute(pool, QueryType::RAW(query), values, false).await
    }

    /// Running `DELETE` query based on provided conditions
    async fn delete(
        pool: &PgPools,
        table_name: Option<&str>,
        filter_list: Option<Vec<SQLCondition<'_>>>,
        filter_values: &[&(dyn ToSql + Sync)],
    ) -> Result<u64, SQLError> {
        let table_name = match table_name {
            None => Self::table_name(),
            Some(name) => name,
        };
        let filters = Self::filter_query_builder(filter_list, 0);
        let query = format!("DELETE FROM {} {}", table_name, filters);
        Self::execute(pool, QueryType::RAW(query), filter_values, false).await
    }

    /// Generating a list of SQL update field based on a vector of string
    fn update_query_builder(update_list: Vec<&str>, offset: i32) -> (i32, String) {
        if update_list.len() == 0 {
            return (0, "".to_owned());
        } else {
            let mut index = offset;
            let list: Vec<String> = update_list
                .into_iter()
                .map(|item| {
                    index += 1;
                    format!("{} = ${}", item, index)
                })
                .collect();
            return (index, list.join(", "));
        }
    }

    /// Running `UPDATE` query based on provided params
    async fn update(
        pool: &PgPools,
        table_name: Option<&str>,
        update_list: Vec<&str>,
        update_values: &[&(dyn ToSql + Sync)],
        filter_list: Option<Vec<SQLCondition<'_>>>,
        filter_values: &[&(dyn ToSql + Sync)],
    ) -> Result<u64, SQLError> {
        let table_name = match table_name {
            None => Self::table_name(),
            Some(name) => name,
        };
        if update_list.len() == 0 {
            return Err("No update field find!".to_owned().into());
        }
        let (offset, lists) = Self::update_query_builder(update_list, 0);
        let filters = Self::filter_query_builder(filter_list, offset);
        let query = format!("UPDATE {} SET {} {}", table_name, lists, filters);
        let params = [update_values, filter_values].concat();
        Self::execute(pool, QueryType::RAW(query), &params, false).await
    }
}
