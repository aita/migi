use std::collections::HashMap;

use sqlparser::ast::{
    ColumnOptionDef, DataType, Expr, Ident, ObjectName, OnCommit, SqlOption, TableConstraint,
};

use crate::{Dialect, Options};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Dbinfo {
    pub dialect: Dialect,
    pub default_catalog: String,
    pub catalogs: HashMap<String, Catalog>,
}

impl Dbinfo {
    pub fn with_options(options: Options) -> Self {
        let schema = Schema {
            name: options.default_schema.clone(),
            tables: HashMap::new(),
        };
        let catalog = Catalog {
            name: options.database.clone(),
            default_schema: options.default_schema.clone(),
            schemas: HashMap::from([(options.default_schema.clone(), schema)]),
        };
        Self {
            dialect: options.dialect,
            default_catalog: options.database.clone(),
            catalogs: HashMap::from([(options.database.clone(), catalog)]),
        }
    }

    pub fn default_catalog(&mut self) -> &mut Catalog {
        self.catalogs.get_mut(&self.default_catalog).unwrap()
    }

    pub fn add_catalog(&mut self, name: &str, catalog: Catalog) {
        self.catalogs.insert(name.into(), catalog);
    }

    pub fn add_table(&mut self, name: &TableName, table: Table) {
        todo!()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Catalog {
    pub name: String,
    pub default_schema: String,
    pub schemas: HashMap<String, Schema>,
}

impl Catalog {
    pub fn add_schema(&mut self, name: &str, schema: Schema) {
        self.schemas.insert(name.into(), schema);
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Schema {
    pub name: String,
    pub tables: HashMap<String, Table>,
}

impl Schema {
    pub fn add_table(&mut self, name: &str, table: Table) {
        self.tables.insert(name.into(), table);
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
pub struct Table {
    pub name: String,
    pub columns: Vec<Column>,
    pub constraints: Vec<TableConstraint>,
    pub with_options: Vec<SqlOption>, // postgresql with options
    pub without_rowid: bool,          // sqlite without rowid
    pub engine: Option<String>,       // mysql storage engine
    pub comment: Option<String>,
    pub auto_increment_offset: Option<u32>, // mysql auto_increment_offset
    pub default_charset: Option<String>,
    pub collation: Option<String>,
    pub on_commit: Option<OnCommit>,
    pub order_by: Option<Vec<Ident>>,
    pub partition_by: Option<Box<Expr>>,
    pub options: Option<Vec<SqlOption>>,
    pub strict: bool, // sqlite strict tables: https://www.sqlite.org/stricttables.html
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
pub struct Column {
    pub name: String,
    pub data_type: DataType,
    pub collation: Option<ObjectName>,
    pub options: Vec<ColumnOptionDef>,
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
pub struct TableName {
    pub catalog: Option<String>,
    pub schema: Option<String>,
    pub table: String,
}
