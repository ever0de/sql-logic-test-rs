use sqlite::{Connection, State, Statement};

pub mod std_ext;

pub use std_ext::*;

pub struct Sqlite {
    connection: Connection,
}

#[derive(Clone, Debug, Default)]
pub struct Row {
    record: Vec<String>,
}

impl Row {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn push(&mut self, record: String) -> &mut Self {
        self.record.push(record);
        self
    }
}

impl Sqlite {
    const MEMORY_PATH: &str = ":memory:";

    pub fn new() -> Self {
        Self {
            connection: sqlite::open(Self::MEMORY_PATH).unwrap(),
        }
    }

    pub fn prepare<Sql: AsRef<str>>(&self, sql: Sql) -> eyre::Result<Statement> {
        self.connection.prepare(sql).into_eyre()
    }

    pub fn select<Sql: AsRef<str>, Fields: AsRef<[&'static str]>>(
        &self,
        sql: Sql,
        fields: Fields,
    ) -> eyre::Result<Vec<Row>> {
        let mut stmt = self.prepare(sql)?;

        let mut rows = vec![];
        loop {
            if let State::Done = stmt.next()? {
                return Ok(rows);
            }

            let mut row = Row::new();
            for field in fields.as_ref() {
                let record = stmt.read::<String, _>(field.as_ref())?;
                row.push(record);
            }

            rows.push(row)
        }
    }

    pub fn execute<Sql: AsRef<str>>(&self, sql: Sql) -> eyre::Result<()> {
        self.connection.execute(sql).into_eyre()
    }
}

impl Default for Sqlite {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn select() {
        let sqlite = Sqlite::new();
        sqlite
            .execute("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)")
            .unwrap();
        sqlite
            .execute("INSERT INTO test (name) VALUES ('test')")
            .unwrap();

        let rows = sqlite.select("SELECT * FROM test", ["id", "name"]).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].record[0], "1");
        assert_eq!(rows[0].record[1], "test");
    }
}
