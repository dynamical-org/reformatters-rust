use crate::AnalysisDataset;
use chrono::{TimeDelta, TimeZone, Utc};
use once_cell::sync::Lazy;

pub static GFS_DATASET: Lazy<AnalysisDataset> = Lazy::new(|| AnalysisDataset {
    id: "noaa-gfs-analysis",
    name: "NOAA GFS Analysis",

    time_start: Utc.with_ymd_and_hms(2021, 1, 1, 0, 0, 0).unwrap(),
    time_end: None,
    time_step: TimeDelta::try_hours(6).unwrap(),
    time_chunk_size: 40,

    longitude_start: -180.,
    longitude_end: 180.,
    longitude_step: 0.25,
    longitude_chunk_size: 360,

    latitude_start: 90.,
    latitude_end: -90.,
    latitude_step: 0.25,
    latitude_chunk_size: 361,

    dimension_names: vec!["time", "longitude", "latitude"],
    data_variable_names: vec![
        "temperature_2m",
        "precipitation_surface",
        "wind_u_10m",
        "wind_v_10m",
    ],
});
