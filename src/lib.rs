use anyhow::{self, Result};
use serde_derive::{Deserialize, Serialize};

pub mod dbinfo;
pub mod inspector;

#[derive(Debug, Copy, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Dialect {
    PostgreSql,
    MySql,
    SQLite,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Options {
    pub dialect: Dialect,
    pub database: String,
    pub default_schema: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct Config {
    pub dialect: Option<Dialect>,
    pub database: String,
    pub default_schema: Option<String>,
    pub paths: Vec<String>,
}

impl Config {
    pub fn to_options(&self) -> Result<Options> {
        let dialect = self
            .dialect
            .ok_or_else(|| anyhow::anyhow!("dialect is required"))?;

        if self.database.is_empty() {
            return Err(anyhow::anyhow!("database is required"));
        }

        let default_schema = self
            .default_schema
            .clone()
            .unwrap_or_else(|| "public".into());

        Ok(Options {
            dialect,
            database: self.database.clone(),
            default_schema,
        })
    }
}
