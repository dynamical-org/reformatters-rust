pub mod gfs;

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

#[derive(Debug)]
pub struct AnalysisRunConfig {
    pub dataset: AnalysisDataset,
    pub data_variable_name: String,
    pub time_coordinates: Vec<DateTime<Utc>>,
}

// pub struct DatasetDimension<Coordinate, Frequency> {
//     name: &'static str,
//     coordinate_start: Coordinate,
//     coordinate_end: Coordinate,
//     coordinate_frequency: Frequency,
//     chunk_size: usize,
// }
// pub type DateTimeDimension = DatasetDimension<DateTime<Utc>, TimeDelta>;
// pub type TimeDeltaDimension = DatasetDimension<TimeDelta, TimeDelta>;
// pub type FloatDimension = DatasetDimension<f64, f64>;

// struct SurfaceLonLatAnalysisDataset {
//     id: &'static str,
//     dimensions: (DateTimeDimension, FloatDimension, FloatDimension),
// }
