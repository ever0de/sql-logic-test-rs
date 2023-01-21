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
    fn basic() {
        let storage = Sqlite::new();
        let mut tester = sqllogictest::Runner::new(storage);

        tester.run_file("./src/slt/basic.slt").unwrap();
    }

    #[test]
    fn select() {
        let storage = Sqlite::new();
        let mut tester = sqllogictest::Runner::new(storage);

        let script = std::fs::read_to_string("./sqllogictest/test/select1.test").unwrap();
        let records = sqllogictest::parse(&script).unwrap();

        let mut fail_count = vec![];
        let mut success_count = vec![];
        for record in &records {
            if let sqllogictest::Record::Query {
                expected_results, ..
            } = record
            {
                if expected_results.len() == 1 && expected_results[0].contains("values hashing to")
                {
                    tester.with_hash_threshold(1);
                } else {
                    tester.with_hash_threshold(0);
                }
            }

            match tester.run(record.clone()) {
                Ok(_) => success_count.push(record.to_string()),
                Err(err) => {
                    fail_count.push(err);
                }
            }
        }

        for err in fail_count.iter() {
            println!("failed record: {err}");
        }
        println!(
            r#"{:.2}%, success: {}, fail: {}"#,
            (success_count.len() as f64 / (success_count.len() as f64 + fail_count.len() as f64))
                * 100.0,
            success_count.len(),
            fail_count.len()
        );
    }
}
