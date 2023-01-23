use sqllogictest::AsyncDB;
use tracing::{debug, instrument, warn};

pub mod db;
pub mod logic_tests;
pub mod std_ext;

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
