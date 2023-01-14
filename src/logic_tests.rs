use std::str::FromStr;

use sqllogictest::{ColumnType, DBOutput};

use crate::db::Sqlite;

#[derive(thiserror::Error, Debug)]
pub enum LogicTestError {
    #[error("failed select query")]
    FailedSelectQuery,

    #[error("failed execute query")]
    FailedExecuteQuery,
}

enum Query {
    Select,
    Other,
}

impl FromStr for Query {
    type Err = std::convert::Infallible;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_uppercase().as_str() {
            "SELECT" => Ok(Self::Select),
            _ => Ok(Self::Other),
        }
    }
}

impl sqllogictest::DB for Sqlite {
    type Error = LogicTestError;

    fn run(&mut self, sql: &str) -> Result<DBOutput, Self::Error> {
        let query: Query = sql
            .split_whitespace()
            .next()
            .unwrap_or_else(|| panic!("sql is empty: {sql}"))
            .parse()
            .unwrap();

        match query {
            Query::Select => {
                let rows = self.select(sql).map_err(|err| {
                    eprintln!("failed sql: {sql}\n{err}");

                    Self::Error::FailedSelectQuery
                })?;

                let result = DBOutput::Rows {
                    // TODO: How to export types?
                    types: rows.iter().map(|_| ColumnType::Any).collect(),
                    rows: rows
                        .into_iter()
                        .map(|row| {
                            row.into_iter()
                                .map(|record| record.value_to_string())
                                .collect()
                        })
                        .collect(),
                };

                Ok(result)
            }
            Query::Other => {
                self.execute(sql).map_err(|err| {
                    eprintln!("failed sql: {sql}\n{err}");

                    Self::Error::FailedExecuteQuery
                })?;

                Ok(DBOutput::StatementComplete(0))
            }
        }
    }

    fn engine_name(&self) -> &'static str {
        "sqlite"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn select() {
        // invalid slt code
        //
        // hash-threshold 3
        // query I
        // select * from example_basic
        // ----
        // 3 values hashing to b5b44edac84d34d6af3be2a88bfae352
        // # FIXME: the origin data is below.. Why..?
        // # Alice
        // # Bob
        // # Eve

        let storage = Sqlite::new();
        let mut tester = sqllogictest::Runner::new(storage);

        let script = std::fs::read_to_string("./src/slt/basic.slt").unwrap();
        let records = sqllogictest::parse(&script).unwrap();
        for record in records {
            println!("{record}");
            tester.run(record).unwrap();
        }
    }
}
