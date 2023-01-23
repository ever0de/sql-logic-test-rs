use glob::glob;
use sqllogictest::{AsyncDB, SortMode};
use tracing::{debug, instrument, warn};

pub mod db;
pub mod logic_tests;
pub mod std_ext;

pub async fn run<T: Tester>(glob_path: impl AsRef<str>) -> eyre::Result<Vec<TestResult>> {
    let mut test_result = vec![];

    for entry in glob(glob_path.as_ref())? {
        let path = entry?;

        let mut result = TestResult::new(path.display().to_string());
        let mut tester: sqllogictest::Runner<T> = Tester::tester();

        let records = match sqllogictest::parse_file(&path) {
            Ok(records) => records,
            Err(error) => {
                result.append_parse_error(error);

                continue;
            }
        };

        for record in records {
            let record = match normalize_record(record.clone()) {
                Some(record) => record,
                None => {
                    result.append_skip(record);
                    continue;
                }
            };

            tester.normalize_tester(&record);

            match tester.run_async(record.clone()).await {
                Ok(_) => {
                    result.append_success(record);
                }
                Err(error) => {
                    result.append_test_error(error);
                }
            }
        }

        test_result.push(result);
    }

    Ok(test_result)
}

#[derive(Debug)]
pub struct TestResult {
    pub file_name: String,
    pub success_records: Vec<sqllogictest::Record>,
    pub skip_records: Vec<sqllogictest::Record>,
    pub test_erros: Vec<sqllogictest::TestError>,
    pub parse_errors: Vec<sqllogictest::ParseError>,
}

impl TestResult {
    pub fn new(file_name: String) -> Self {
        Self {
            file_name,
            success_records: vec![],
            skip_records: vec![],
            test_erros: vec![],
            parse_errors: vec![],
        }
    }

    pub fn append_success(&mut self, record: sqllogictest::Record) {
        self.success_records.push(record);
    }
    pub fn append_skip(&mut self, record: sqllogictest::Record) {
        self.skip_records.push(record);
    }
    pub fn append_test_error(&mut self, error: sqllogictest::TestError) {
        self.test_erros.push(error);
    }
    pub fn append_parse_error(&mut self, error: sqllogictest::ParseError) {
        self.parse_errors.push(error);
    }

    pub fn display(&self) {
        println!("file: {}", self.file_name);
        println!("success: {}", self.success_records.len());
        println!("errors: {}", self.test_erros.len());

        let success_percent = (self.success_records.len() as f64
            / (self.success_records.len() + self.test_erros.len() + self.parse_errors.len())
                as f64)
            * 100.0;
        println!(
            r#"{:.2}%, success: {}, fail: {} / skip: {} / parse_fail: {}"#,
            success_percent,
            self.success_records.len(),
            self.test_erros.len(),
            self.skip_records.len(),
            self.parse_errors.len()
        )
    }
}

pub trait Tester: AsyncDB
where
    Self: Sized,
{
    fn new() -> Self;

    fn tester() -> sqllogictest::Runner<Self> {
        let db = Self::new();

        let mut runner = sqllogictest::Runner::new(db);
        runner.with_validator(validator);

        runner
    }
}

impl Tester for db::Sqlite {
    fn new() -> Self {
        Self::default()
    }
}

pub trait Normalizer<D: AsyncDB> {
    fn normalize_tester(&mut self, record: &sqllogictest::Record);
}

impl<D: AsyncDB> Normalizer<D> for sqllogictest::Runner<D> {
    fn normalize_tester(&mut self, record: &sqllogictest::Record) {
        if let sqllogictest::Record::Query {
            expected_results, ..
        } = record
        {
            if expected_results.len() == 1 && expected_results[0].contains("values hashing to") {
                self.with_hash_threshold(1);
            } else {
                self.with_hash_threshold(0);
            }
        }
    }
}

fn normalize_record(record: sqllogictest::Record) -> Option<sqllogictest::Record> {
    match &record {
        sqllogictest::Record::Query {
            sort_mode: Some(SortMode::ValueSort),
            ..
        } => None,
        _ => Some(record),
    }
}

#[instrument]
pub fn validator(actual: &[Vec<String>], expected: &[String]) -> bool {
    if expected.len() == 1 && actual.len() == 1 && expected[0].contains("values hashing to") {
        debug!("{:?} == {expected:?}", actual[0]);

        return actual[0][0] == expected[0];
    }

    let normalized_rows = actual
        .iter()
        .flat_map(|text| text.to_owned())
        .collect::<Vec<_>>();

    if normalized_rows.len() != expected.len() {
        warn!(
            "actual length:{} != expected length: {}",
            normalized_rows.len(),
            expected.len()
        );
        warn!("actual: {:?}\nexpected: {:?}", normalized_rows, expected);

        return false;
    }

    normalized_rows
        .iter()
        .zip(expected.iter())
        .filter(|(actual, expected)| actual != expected)
        .for_each(|(actual, expected)| warn!("{actual} != {expected}"));

    normalized_rows == expected
}
