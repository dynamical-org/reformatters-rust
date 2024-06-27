pub mod gfs;
pub mod http;
pub mod object_storage;

use chrono::{DateTime, TimeDelta, Utc};

#[derive(Debug, Clone)]
pub struct AnalysisDataset {
    pub id: &'static str,
    pub name: &'static str,

    pub time_start: DateTime<Utc>,
    pub time_end: Option<DateTime<Utc>>,
    pub time_step: TimeDelta,
    pub time_chunk_size: usize,

    pub longitude_start: f64,
    pub longitude_end: f64,
    pub longitude_step: f64,
    pub longitude_chunk_size: usize,

    pub latitude_start: f64,
    pub latitude_end: f64,
    pub latitude_step: f64,
    pub latitude_chunk_size: usize,

    pub dimension_names: Vec<&'static str>,
    pub data_variable_names: Vec<&'static str>,
}

#[allow(
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::cast_precision_loss
)]
fn num_chunks(size: usize, chunk_size: usize) -> usize {
    ((size as f64) / (chunk_size as f64)).ceil() as usize
}
