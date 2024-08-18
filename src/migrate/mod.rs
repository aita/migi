use std::collections::HashSet;

use anyhow::Result;

use crate::dbinfo::{Catalog, Column, Dbinfo, Schema, Table};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectName(pub Vec<String>);

pub enum MigrationOperation<'a> {
    CreateDatabase { name: String },
    DropDatabase { name: String },
    CreateSchema { name: ObjectName },
    DropSchema { name: ObjectName },
    CreateTable { name: ObjectName, table: &'a Table },
    DropTable { name: ObjectName, table: &'a Table },
    AlterTable(AlterTableOperation),
}

pub enum AlterDatabaseOperation {}

pub enum AlterTableOperation {
    AddColumn,
    DropColumn,
    AlterColumn,
    AddIndex,
    DropIndex,
}

pub struct Migration<'a> {
    pub operations: Vec<MigrationOperation<'a>>,
}

pub struct MigrationGenerator<'a> {
    pub previous: &'a Dbinfo,
    pub current: &'a Dbinfo,
    pub migrations: Migration<'a>,
}

impl<'a> MigrationGenerator<'a> {
    pub fn new(previous: &'a Dbinfo, current: &'a Dbinfo) -> Self {
        Self {
            previous: previous,
            current: current,
            migrations: Migration {
                operations: Vec::new(),
            },
        }
    }

    pub fn generate(mut self) -> Result<Migration<'a>> {
        self.gen_catalogs()?;
        Ok(self.migrations)
    }

    fn gen_catalogs(&mut self) -> Result<()> {
        let previous_catalogs: HashSet<&str> =
            self.previous.catalogs.keys().map(|k| k.as_str()).collect();

        let current_catalogs: HashSet<&str> =
            self.current.catalogs.keys().map(|k| k.as_str()).collect();

        let dropped_catalogs = previous_catalogs.difference(&current_catalogs);
        let created_catalogs = current_catalogs.difference(&previous_catalogs);
        let common_catalogs = previous_catalogs.intersection(&current_catalogs);

        for catalog in dropped_catalogs {
            self.migrations
                .operations
                .push(MigrationOperation::DropDatabase {
                    name: catalog.to_string(),
                });
        }

        for catalog in created_catalogs {
            self.migrations
                .operations
                .push(MigrationOperation::CreateDatabase {
                    name: catalog.to_string(),
                });
        }

        for catalog in common_catalogs {
            let previous_catalog = self.previous.catalogs.get(*catalog).unwrap();
            let current_catalog = self.current.catalogs.get(*catalog).unwrap();

            if previous_catalog.default_schema != current_catalog.default_schema {
                self.gen_schemas(previous_catalog, current_catalog)?;
            }
        }

        Ok(())
    }

    fn gen_schemas(&mut self, previous: &'a Catalog, current: &'a Catalog) -> Result<()> {
        let previous_schemas: HashSet<&str> = previous.schemas.keys().map(|k| k.as_str()).collect();
        let current_schemas: HashSet<&str> = current.schemas.keys().map(|k| k.as_str()).collect();

        let dropped_schemas = previous_schemas.difference(&current_schemas);
        let created_schemas = current_schemas.difference(&previous_schemas);
        let common_schemas = previous_schemas.intersection(&current_schemas);

        for schema in dropped_schemas {
            self.migrations
                .operations
                .push(MigrationOperation::DropSchema {
                    name: ObjectName(vec![current.name.clone(), schema.to_string()]),
                });
        }
        for schema in created_schemas {
            self.migrations
                .operations
                .push(MigrationOperation::CreateSchema {
                    name: ObjectName(vec![current.name.clone(), schema.to_string()]),
                });
        }

        for schema in common_schemas {
            let previous_schema = previous.schemas.get(*schema).unwrap();
            let current_schema = current.schemas.get(*schema).unwrap();

            if previous_schema.tables != current_schema.tables {
                self.gen_tables(&current.name, previous_schema, current_schema)?;
            }
        }

        Ok(())
    }

    fn gen_tables(
        &mut self,
        catalog_name: &str,
        previous: &'a Schema,
        current: &'a Schema,
    ) -> Result<()> {
        let previous_tables: HashSet<&str> = previous.tables.keys().map(|k| k.as_str()).collect();
        let current_tables: HashSet<&str> = current.tables.keys().map(|k| k.as_str()).collect();

        let dropped_tables = previous_tables.difference(&current_tables);
        let created_tables = current_tables.difference(&previous_tables);
        let common_tables = previous_tables.intersection(&current_tables);

        for table in dropped_tables {
            self.migrations
                .operations
                .push(MigrationOperation::DropTable {
                    name: ObjectName(vec![
                        catalog_name.to_string(),
                        current.name.clone(),
                        table.to_string(),
                    ]),
                    table: previous.tables.get(*table).unwrap(),
                });
        }
        for table in created_tables {
            self.migrations
                .operations
                .push(MigrationOperation::CreateTable {
                    name: ObjectName(vec![
                        catalog_name.to_string(),
                        current.name.clone(),
                        table.to_string(),
                    ]),
                    table: current.tables.get(*table).unwrap(),
                });
        }

        for table in common_tables {
            let previous_table = previous.tables.get(*table).unwrap();
            let current_table = current.tables.get(*table).unwrap();

            if previous_table != current_table {
                self.gen_table(
                    ObjectName(vec![catalog_name.to_string(), current.name.clone()]),
                    previous_table,
                    current_table,
                )?;
            }
        }

        Ok(())
    }

    fn gen_table(
        &mut self,
        schema_name: ObjectName,
        previous: &'a Table,
        current: &'a Table,
    ) -> Result<()> {
        let previous_columns: Vec<&str> =
            previous.columns.iter().map(|c| c.name.as_str()).collect();
        let current_columns: Vec<&str> = current.columns.iter().map(|c| c.name.as_str()).collect();

        let columns_diff = diff::slice(&previous_columns, &current_columns);

        // check if we can add columns
        let mut first_added_column = None;
        let mut last_common_column = None;
        for (i, result) in columns_diff.iter().enumerate() {
            match result {
                diff::Result::Left(_) => {}
                diff::Result::Both(_, _) => {
                    last_common_column = Some(i);
                }
                diff::Result::Right(_) => {
                    if first_added_column.is_none() {
                        first_added_column = Some(i);
                    }
                }
            }
        }
        if first_added_column.is_some() && last_common_column.is_some() {
            let first_added_column = first_added_column.unwrap();
            let last_common_column = last_common_column.unwrap();

            if first_added_column < last_common_column {
                anyhow::bail!("migi can't add columns in the middle of a table");
            }
        }

        let table_name = ObjectName(vec![
            schema_name.0[0].clone(),
            schema_name.0[1].clone(),
            current.name.clone(),
        ]);
        let mut i = 0;
        let mut j = 0;
        for result in columns_diff {
            match result {
                diff::Result::Left(_) => {
                    self.gen_drop_column(&table_name, previous, previous.columns.get(i).unwrap())?;
                    i += 1;
                }
                diff::Result::Both(_, _) => {
                    self.gen_alter_column(
                        &table_name,
                        previous,
                        previous.columns.get(i).unwrap(),
                        current,
                        current.columns.get(j).unwrap(),
                    )?;
                    i += 1;
                    j += 1;
                }
                diff::Result::Right(_) => {
                    self.gen_add_column(&table_name, current, current.columns.get(j).unwrap())?;
                    j += 1;
                }
            }
        }

        Ok(())
    }

    fn gen_alter_column(
        &mut self,
        table_name: &ObjectName,
        previous_table: &'a Table,
        previous: &'a Column,
        current_table: &'a Table,
        current: &'a Column,
    ) -> Result<()> {
        todo!()
    }

    fn gen_drop_column(
        &mut self,
        table_name: &ObjectName,
        previous_table: &'a Table,
        previous: &'a Column,
    ) -> Result<()> {
        todo!()
    }

    fn gen_add_column(
        &mut self,
        table_name: &ObjectName,
        current_table: &'a Table,
        current: &'a Column,
    ) -> Result<()> {
        todo!()
    }
}
