pub mod gfs;
pub mod http;

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
