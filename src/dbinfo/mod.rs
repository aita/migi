use std::collections::HashMap;

use anyhow::Result;
use sqlparser::ast::{
    ColumnOptionDef, DataType, Expr, Ident, ObjectName, OnCommit, Query, SqlOption, TableConstraint,
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

    fn default_catalog(&self) -> &Catalog {
        self.catalogs.get(&self.default_catalog).unwrap()
    }

    fn default_catalog_mut(&mut self) -> &mut Catalog {
        self.catalogs.get_mut(&self.default_catalog).unwrap()
    }

    pub fn add_catalog(&mut self, name: &str, catalog: Catalog) {
        self.catalogs.insert(name.into(), catalog);
    }

    pub fn add_table(&mut self, name: &TableName, table: Table) -> Result<()> {
        let catalog = if let Some(ref catalog_name) = name.catalog {
            self.get_catalog_mut(catalog_name.value.as_str())?
        } else {
            self.default_catalog_mut()
        };

        let schema = if let Some(ref schema_name) = name.schema {
            catalog.get_schema_mut(schema_name.value.as_str())?
        } else {
            catalog.default_schema_mut()
        };

        schema.add_table(&name.table.value, table);

        Ok(())
    }

    pub fn get_catalog(&self, name: &str) -> Result<&Catalog> {
        self.catalogs
            .get(name)
            .ok_or(anyhow::anyhow!("catalog does not found"))
    }

    pub fn get_catalog_mut(&mut self, name: &str) -> Result<&mut Catalog> {
        self.catalogs
            .get_mut(name)
            .ok_or(anyhow::anyhow!("catalog does not found"))
    }

    pub fn get_table(&self, name: &TableName) -> Result<&Table> {
        let catalog = if let Some(ref catalog_name) = name.catalog {
            self.get_catalog(catalog_name.value.as_str())?
        } else {
            self.default_catalog()
        };

        let schema = if let Some(ref schema_name) = name.schema {
            catalog.get_schema(schema_name.value.as_str())?
        } else {
            catalog.default_schema()
        };

        schema.get_table(name.table.value.as_str())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Catalog {
    pub name: String,
    pub default_schema: String,
    pub schemas: HashMap<String, Schema>,
}

impl Catalog {
    fn default_schema(&self) -> &Schema {
        self.schemas.get(&self.default_schema).unwrap()
    }

    fn default_schema_mut(&mut self) -> &mut Schema {
        self.schemas.get_mut(&self.default_schema).unwrap()
    }

    fn add_schema(&mut self, name: &str, schema: Schema) {
        self.schemas.insert(name.into(), schema);
    }

    pub fn get_schema(&self, name: &str) -> Result<&Schema> {
        self.schemas
            .get(name)
            .ok_or(anyhow::anyhow!("schema does not found"))
    }

    pub fn get_schema_mut(&mut self, name: &str) -> Result<&mut Schema> {
        self.schemas
            .get_mut(name)
            .ok_or(anyhow::anyhow!("schema does not found"))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Schema {
    pub name: String,
    pub tables: HashMap<String, Table>,
}

impl Schema {
    fn add_table(&mut self, name: &str, table: Table) {
        self.tables.insert(name.into(), table);
    }

    pub fn get_table(&self, name: &str) -> Result<&Table> {
        self.tables
            .get(name)
            .ok_or(anyhow::anyhow!("table does not found"))
    }

    pub fn get_table_mut(&mut self, name: &str) -> Result<&mut Table> {
        self.tables
            .get_mut(name)
            .ok_or(anyhow::anyhow!("table does not found"))
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
    pub catalog: Option<Ident>,
    pub schema: Option<Ident>,
    pub table: Ident,
}

pub struct View {
    pub name: String,
    pub materialized: bool,
    pub columns: Vec<ViewColumn>,
    pub query: Box<Query>,
    pub comment: Option<String>,
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
pub struct ViewColumn {
    pub name: String,
    pub data_type: Option<DataType>,
    pub options: Vec<SqlOption>,
}
