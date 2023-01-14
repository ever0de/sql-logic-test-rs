use sqlite::State;

fn main() {
    let connection = sqlite::open(":memory:").unwrap();

    connection.execute("CREATE TABLE Foo (name Text)").unwrap();
    connection
        .execute("INSERT INTO Foo (name) VALUES ('bar')")
        .unwrap();

    let query = "SELECT * FROM Foo";
    let mut statement = connection.prepare(query).unwrap();

    while let Ok(State::Row) = statement.next() {
        println!("name = {}", statement.read::<String, _>("name").unwrap());
    }
}
