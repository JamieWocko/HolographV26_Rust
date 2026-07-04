use anyhow::{Context, Result};
use sqlx::mysql::{MySqlConnectOptions, MySqlPoolOptions, MySqlRow};
use sqlx::{MySql, Pool, Row};

use crate::core::config::DatabaseConfig;

#[derive(Clone)]
pub struct Database {
    pool: Pool<MySql>,
}

impl Database {
    pub async fn connect(config: &DatabaseConfig) -> Result<Self> {
        let password_provided = !config.password.is_empty();
        let options = MySqlConnectOptions::new()
            .host(&config.host)
            .port(config.port)
            .database(&config.name)
            .username(&config.user)
            .password(&config.password)
            .charset("utf8");

        let pool = MySqlPoolOptions::new()
            .max_connections(config.max_connections)
            .connect_with(options)
            .await
            .with_context(|| {
                format!(
                    "failed to connect to MySQL database '{}' at {}:{} for user '{}' using native Rust driver (password_provided: {})",
                    config.name,
                    config.host,
                    config.port,
                    config.user,
                    if password_provided { "yes" } else { "no" }
                )
            })?;

        Ok(Self { pool })
    }

    pub async fn run_query(&self, query: &str) -> Result<()> {
        sqlx::query(query).execute(&self.pool).await?;
        Ok(())
    }

    pub async fn run_read_string(&self, query: &str) -> Result<Option<String>> {
        let row = sqlx::query(query).fetch_optional(&self.pool).await?;
        Ok(row.and_then(|row| cell_to_string(&row, 0)))
    }

    pub async fn run_read_i64(&self, query: &str) -> Result<Option<i64>> {
        let row = sqlx::query(query).fetch_optional(&self.pool).await?;
        Ok(row.and_then(|row| row.try_get::<i64, _>(0).ok()))
    }

    pub async fn run_read_row(&self, query: &str) -> Result<Vec<String>> {
        let Some(row) = sqlx::query(query).fetch_optional(&self.pool).await? else {
            return Ok(Vec::new());
        };

        Ok(row
            .columns()
            .iter()
            .enumerate()
            .map(|(index, _)| cell_to_string(&row, index).unwrap_or_default())
            .collect())
    }

    pub async fn run_read_column_string(&self, query: &str) -> Result<Vec<String>> {
        let rows = sqlx::query(query).fetch_all(&self.pool).await?;
        Ok(rows
            .iter()
            .filter_map(|row| cell_to_string(row, 0))
            .collect())
    }

    pub async fn run_read_column_i64(&self, query: &str) -> Result<Vec<i64>> {
        let rows = sqlx::query(query).fetch_all(&self.pool).await?;
        Ok(rows
            .iter()
            .filter_map(|row| row.try_get::<i64, _>(0).ok())
            .collect())
    }

    pub async fn run_read_table(&self, query: &str) -> Result<Vec<Vec<String>>> {
        let rows = sqlx::query(query).fetch_all(&self.pool).await?;
        Ok(rows
            .into_iter()
            .map(|row| {
                row.columns()
                    .iter()
                    .enumerate()
                    .map(|(index, _)| cell_to_string(&row, index).unwrap_or_default())
                    .collect::<Vec<_>>()
            })
            .collect())
    }

    pub async fn scalar_string(&self, query: &str, fallback: &str) -> Result<String> {
        Ok(self
            .run_read_string(query)
            .await?
            .unwrap_or_else(|| fallback.to_string()))
    }

    pub async fn run_read_unsafe_string(&self, query: &str) -> String {
        self.run_read_string(query)
            .await
            .ok()
            .flatten()
            .unwrap_or_default()
    }

    pub async fn run_read_unsafe_i64(&self, query: &str) -> i64 {
        self.run_read_i64(query)
            .await
            .ok()
            .flatten()
            .unwrap_or_default()
    }

    pub async fn check_exists(&self, query: &str) -> bool {
        sqlx::query(query)
            .fetch_optional(&self.pool)
            .await
            .ok()
            .flatten()
            .is_some()
    }

    pub fn stripslash(value: &str) -> String {
        value.replace('\\', "\\\\").replace('\'', "\\'")
    }

    pub async fn diagnostics(&self) -> Result<DatabaseStats> {
        let users = self
            .run_read_i64("SELECT COUNT(*) FROM users LIMIT 1")
            .await?
            .unwrap_or_default();

        let rooms = self
            .run_read_i64("SELECT COUNT(*) FROM rooms LIMIT 1")
            .await?
            .unwrap_or_default();

        let furniture = self
            .run_read_i64("SELECT COUNT(*) FROM furniture LIMIT 1")
            .await?
            .unwrap_or_default();

        Ok(DatabaseStats {
            users,
            rooms,
            furniture,
        })
    }
}

#[derive(Debug, Clone)]
pub struct DatabaseStats {
    pub users: i64,
    pub rooms: i64,
    pub furniture: i64,
}

fn cell_to_string(row: &MySqlRow, index: usize) -> Option<String> {
    row.try_get::<String, _>(index)
        .ok()
        .or_else(|| {
            row.try_get::<i64, _>(index)
                .ok()
                .map(|value| value.to_string())
        })
        .or_else(|| {
            row.try_get::<u64, _>(index)
                .ok()
                .map(|value| value.to_string())
        })
        .or_else(|| {
            row.try_get::<f64, _>(index)
                .ok()
                .map(|value| value.to_string())
        })
        .or_else(|| {
            row.try_get::<Vec<u8>, _>(index)
                .ok()
                .map(|bytes| String::from_utf8_lossy(&bytes).to_string())
        })
}
