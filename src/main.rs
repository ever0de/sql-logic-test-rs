use sql_logic_test::{db::Sqlite, run};

#[tokio::main]
async fn main() {
    let result = run::<Sqlite>("sqllogictest/test/select*.test")
        .await
        .unwrap();

    result.into_iter().for_each(|result| result.display());
}
