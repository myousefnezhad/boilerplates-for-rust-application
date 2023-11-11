//! This library provides functions to connect and interact with Redis

use deadpool_redis::{
    redis::{cmd, Cmd, FromRedisValue, RedisError}, //, ToRedisArgs},
    Config,
    Runtime::Tokio1,
};

pub use deadpool_redis::CreatePoolError;
pub use deadpool_redis::Pool as RdPool;

/// This struct provides redis functions
pub struct Redis;

impl Redis {
    /// This function creates Redis pool connection
    pub fn new(url: &str) -> Result<RdPool, CreatePoolError> {
        let config = Config::from_url(url);
        config.create_pool(Some(Tokio1))
    }

    /// This is redis `KEYS` command
    pub async fn keys(
        pool: &RdPool,
        filter_pattern: Option<&str>,
    ) -> Result<Vec<String>, RedisError> {
        let mut client = pool.get().await.unwrap();
        let filter = match filter_pattern {
            None => "*",
            Some(filter) => filter,
        };
        let value: Result<Vec<String>, RedisError> =
            cmd("KEYS").arg(&[&filter]).query_async(&mut client).await;
        value
    }

    /// This is redis `GET` command
    pub async fn get<T>(pool: &RdPool, key: &str) -> Result<T, RedisError>
    where
        T: FromRedisValue,
    {
        let mut client = pool.get().await.unwrap();
        let values: Result<T, RedisError> = cmd("GET").arg(&[&key]).query_async(&mut client).await;
        values
    }

    /// This is redis `MGET` command
    pub async fn mget<T>(pool: &RdPool, keys: Vec<&str>) -> Result<Vec<Vec<T>>, RedisError>
    where
        T: FromRedisValue,
    {
        let mut client = pool.get().await.unwrap();
        let values: Result<Vec<Vec<T>>, RedisError> =
            cmd("MGET").arg(&keys).query_async(&mut client).await;
        values
    }

    /// This is redis `SET` command
    pub async fn set(pool: &RdPool, key: &str, value: &str) -> bool {
        let mut client = pool.get().await.unwrap();
        let res: Result<_, RedisError> = cmd("SET")
            .arg(&[key, value])
            .query_async::<_, ()>(&mut client)
            .await;
        match res {
            Ok(_) => true,
            Err(_) => false,
        }
    }

    /// This is redis `DEL` command
    pub async fn del(pool: &RdPool, key: Vec<&str>) -> bool {
        let mut client = pool.get().await.unwrap();
        let res: Result<_, RedisError> =
            cmd("DEL").arg(&key).query_async::<_, ()>(&mut client).await;
        match res {
            Ok(_) => true,
            Err(_) => false,
        }
    }

    /// This is redis `EXISTS` command
    pub async fn exists(pool: &RdPool, key: &str) -> Result<bool, RedisError> {
        let mut client = pool.get().await.unwrap();
        let res: Result<bool, RedisError> =
            cmd("EXISTS").arg(&[&key]).query_async(&mut client).await;
        res
    }

    /// This is redis `EXPIRE` command
    pub async fn expire(pool: &RdPool, key: &str, time: usize) -> bool {
        let mut client = pool.get().await.unwrap();
        let res: Result<_, RedisError> = Cmd::expire(key, time)
            .query_async::<_, ()>(&mut client)
            .await;
        match res {
            Ok(_) => true,
            Err(_) => false,
        }
    }
}
