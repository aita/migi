pub mod dbinfo;
pub mod inspector;

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
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
