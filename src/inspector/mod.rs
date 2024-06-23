use anyhow::Result;
use sqlparser::ast::{ObjectName, Statement};
use sqlparser::dialect::{self, MySqlDialect, PostgreSqlDialect, SQLiteDialect};
use sqlparser::parser::Parser;
use sqlparser::tokenizer::{Location, Token, Tokenizer};

use crate::dbinfo::{Column, Dbinfo, Table, TableName};
use crate::Dialect;

pub struct Inspector<'a> {
    dbinfo: &'a mut Dbinfo,
    filename: Option<String>,
}

impl<'a> Inspector<'a> {
    pub fn new(dbinfo: &'a mut Dbinfo) -> Self {
        Self {
            dbinfo,
            filename: None,
        }
    }

    pub fn inspect(&mut self, sql: &str, filename: &str) -> Result<()> {
        self.filename = Some(filename.to_string());

        let dialect: Box<dyn dialect::Dialect> = match self.dbinfo.dialect {
            Dialect::PostgreSql => Box::new(PostgreSqlDialect {}),
            Dialect::MySql => Box::new(MySqlDialect {}),
            Dialect::SQLite => Box::new(SQLiteDialect {}),
        };

        let tokens = Tokenizer::new(&*dialect, sql).tokenize_with_location()?;

        let mut parser = Parser::new(&*dialect).with_tokens_with_locations(tokens);

        loop {
            let tok = parser.peek_token();
            if tok.token == Token::EOF {
                break;
            }

            let stmt = parser.parse_statement()?;
            self.inspect_stmt(stmt, tok.location)?;
        }

        Ok(())
    }

    fn location(&self, loc: Location) -> String {
        if let Some(filename) = &self.filename {
            format!("{}:{}:{}", filename, loc.line, loc.column)
        } else {
            format!("{}:{}", loc.line, loc.column)
        }
    }

    fn inspect_stmt(&mut self, stmt: Statement, loc: Location) -> Result<()> {
        match stmt {
            Statement::CreateView {
                or_replace,
                materialized,
                name,
                columns,
                query,
                options,
                cluster_by,
                comment,
                with_no_schema_binding,
                if_not_exists,
                temporary,
            } => {
                todo!()
            }
            Statement::CreateTable {
                // or_replace,
                temporary,
                external,
                // global,
                // if_not_exists,
                transient,
                name,
                columns,
                constraints,
                table_properties,
                with_options,
                file_format,
                location,
                query,
                without_rowid,
                like,
                clone,
                engine,
                comment,
                auto_increment_offset,
                default_charset,
                collation,
                on_commit,
                on_cluster,
                order_by,
                partition_by,
                cluster_by,
                options,
                strict,
                ..
            } => {
                if temporary {
                    anyhow::bail!(
                        "{} CREATE TEMPORARY TABLE is not supported",
                        self.location(loc)
                    );
                }
                if external {
                    anyhow::bail!(
                        "{} CREATE EXTERNAL TABLE is not supported",
                        self.location(loc)
                    );
                }
                if transient {
                    anyhow::bail!(
                        "{} CREATE TRANSIENT TABLE is not supported",
                        self.location(loc)
                    );
                }
                if !table_properties.is_empty() {
                    anyhow::bail!(
                        "{} CREATE TABLE ... TBLPROPERTIES is not supported",
                        self.location(loc)
                    );
                }
                if file_format.is_some() {
                    anyhow::bail!(
                        "{} CREATE TABLE ... STORED AS is not supported",
                        self.location(loc)
                    );
                }
                if location.is_some() {
                    anyhow::bail!(
                        "{} CREATE TABLE ... LOCATION is not supported",
                        self.location(loc)
                    );
                }
                if query.is_some() {
                    anyhow::bail!("{} CREATE TABLE AS is not supported", self.location(loc));
                }
                if clone.is_some() {
                    anyhow::bail!(
                        "{} CREATE TABLE ... CLONE is not supported",
                        self.location(loc)
                    );
                }
                if on_cluster.is_some() {
                    anyhow::bail!(
                        "{} CREATE TABLE ... ON CLUSTER is not supported",
                        self.location(loc)
                    );
                }
                if cluster_by.is_some() {
                    anyhow::bail!(
                        "{} CREATE TABLE ... CLUSTER BY is not supported",
                        self.location(loc)
                    );
                }

                let table_name = self.inspect_table_name(name, loc)?;

                let columns = self.inspect_columns(columns, loc)?;

                if like.is_some() {
                    anyhow::bail!(
                        "{} CREATE TABLE ... LIKE is not supported",
                        self.location(loc)
                    );
                }

                let table = Table {
                    name: table_name.table.clone(),
                    columns,
                    constraints,
                    with_options,
                    without_rowid,
                    engine,
                    comment,
                    auto_increment_offset,
                    default_charset,
                    collation,
                    on_commit,
                    order_by,
                    partition_by,
                    options,
                    strict,
                };

                self.dbinfo.add_table(&table_name, table);
            }
            Statement::CreateIndex {
                name,
                table_name,
                using,
                columns,
                unique,
                concurrently,
                if_not_exists,
                include,
                nulls_distinct,
                predicate,
            } => {
                todo!()
            }
            Statement::CreateSchema {
                schema_name,
                if_not_exists,
            } => {
                todo!()
            }
            Statement::CreateDatabase {
                db_name,
                location,
                managed_location,
                if_not_exists,
            } => {
                todo!()
            }
            Statement::AlterTable {
                name,
                if_exists,
                only,
                operations,
                // location,
                ..
            } => {
                todo!()
            }
            Statement::AlterIndex { name, operation } => {
                todo!()
            }
            Statement::AlterView {
                name,
                columns,
                query,
                with_options,
            } => {
                todo!()
            }
            Statement::CreateExtension {
                name,
                if_not_exists,
                cascade,
                schema,
                version,
            } => {
                todo!()
            }
            Statement::CreateFunction {
                or_replace,
                temporary,
                if_not_exists,
                name,
                args,
                return_type,
                function_body,
                behavior,
                called_on_null,
                parallel,
                using,
                language,
                determinism_specifier,
                options,
                remote_connection,
            } => {
                todo!()
            }
            Statement::CreateProcedure {
                or_alter,
                name,
                params,
                body,
            } => {
                todo!()
            }
            _ => {
                println!("Other statement: {:?}", stmt);
            }
        }
        Ok(())
    }

    fn inspect_table_name(&self, name: ObjectName, loc: Location) -> Result<TableName> {
        match name.0.len() {
            1 => Ok(TableName {
                catalog: None,
                schema: None,
                table: name.0[0].value.clone(),
            }),
            2 => match self.dbinfo.dialect {
                Dialect::PostgreSql => Ok(TableName {
                    catalog: None,
                    schema: Some(name.0[0].value.clone()),
                    table: name.0[1].value.clone(),
                }),
                Dialect::MySql => Ok(TableName {
                    catalog: Some(name.0[0].value.clone()),
                    schema: None,
                    table: name.0[1].value.clone(),
                }),
                Dialect::SQLite => {
                    anyhow::bail!("{} invalid table name: {:?}", self.location(loc), name)
                }
            },
            3 => match self.dbinfo.dialect {
                Dialect::PostgreSql => Ok(TableName {
                    catalog: Some(name.0[0].value.clone()),
                    schema: Some(name.0[1].value.clone()),
                    table: name.0[2].value.clone(),
                }),
                Dialect::MySql => {
                    anyhow::bail!("{} invalid table name: {:?}", self.location(loc), name)
                }
                Dialect::SQLite => {
                    anyhow::bail!("{} invalid table name: {:?}", self.location(loc), name)
                }
            },
            _ => anyhow::bail!("{} invalid table name: {:?}", self.location(loc), name),
        }
    }

    fn inspect_columns(
        &self,
        columns: Vec<sqlparser::ast::ColumnDef>,
        loc: Location,
    ) -> Result<Vec<Column>> {
        todo!()
    }
}
