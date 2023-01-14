pub use crate::std_ext::*;
use sqlite::{Connection, State, Statement};

pub struct Sqlite {
    connection: Connection,
}

#[derive(Clone, Debug, Default)]
pub struct Row {
    pub record: Vec<Record>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Record {
    pub label: String,
    pub value: String,
}

impl Record {
    pub fn new(label: String, value: String) -> Self {
        Self { label, value }
    }
}

impl Row {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn push(&mut self, record: Record) -> &mut Self {
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
        let sql = sql.as_ref();

        self.connection
            .prepare(sql)
            .wrap_err(format!("failed to prepare statement, sql: {sql}"))
    }

    pub fn select<Sql: AsRef<str>, Fields: AsRef<[&'static str]>>(
        &self,
        sql: Sql,
        fields: Fields,
    ) -> eyre::Result<Vec<Row>> {
        let sql = sql.as_ref();
        let mut stmt = self.prepare(sql)?;

        let mut rows = vec![];
        loop {
            if let State::Done = stmt.next()? {
                return Ok(rows);
            }

            let mut row = Row::new();
            for field in fields.as_ref() {
                let value = stmt
                    .read::<String, _>(field.as_ref())
                    .wrap_err(format!("failed read, field: {field} from sql: {sql}"))?;
                let record = Record::new(field.to_string(), value);
                row.push(record);
            }

            rows.push(row)
        }
    }

    pub fn execute<Sql: AsRef<str>>(&self, sql: Sql) -> eyre::Result<()> {
        let sql = sql.as_ref();

        self.connection
            .execute(sql)
            .wrap_err(format!("failed execute, sql: {sql}"))
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

        let fields = ["id", "name"];
        let rows = sqlite.select("SELECT * FROM test", fields).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].record[0],
            Record::new("id".to_owned(), "1".to_owned())
        );
        assert_eq!(
            rows[0].record[1],
            Record::new("name".to_owned(), "test".to_owned())
        );
    }
}
