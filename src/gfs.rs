use std::collections::HashMap;

use crate::http::HttpClient;
use crate::AnalysisDataset;

pub use anyhow::{anyhow, Result};
use chrono::{DateTime, TimeDelta, TimeZone, Utc};
use futures::future::join_all;
use futures::TryStreamExt;
use itertools::Itertools;
use ndarray::{s, Array2, Array3, Axis};
use once_cell::sync::Lazy;
use regex::Regex;
use tokio::task::spawn_blocking;

const S3_BUCKET_HOST: &str = "https://noaa-gfs-bdp-pds.s3.amazonaws.com";
const DEST_ROOT_PATH: &str = "aldenks/gfs-dynamical/analysis/v0.1.0.zarr";

/// Dataset config object todos
/// - incorporate element type into object
/// - make dimension objects which contain names, etc
/// - make data variable enum which contains name, dimensions, chunking, units, grib index names, etc
type E = f32; // element type

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

static GRIB_INDEX_VARIABLE_NAMES: Lazy<HashMap<&str, &str>> = Lazy::new(|| {
    HashMap::from([
        ("temperature_2m", "TMP:2 m above ground"),
        ("precipitation_surface", "PRATE:surface"),
        ("wind_u_10m", "UGRD:10 m above ground"),
        ("wind_v_10m", "VGRD:10 m above ground"),
    ])
});

#[derive(Debug, Clone)]
pub struct AnalysisRunConfig {
    pub dataset: AnalysisDataset,
    pub data_variable_name: String,
    pub time_coordinates: Vec<DateTime<Utc>>,
}

#[derive(Debug, Clone)]
pub struct DownloadBatch {
    run_config: AnalysisRunConfig,
    data_variable_name: String,
    time_coordinates: Vec<DateTime<Utc>>,
    time_chunk_index: usize,
}

#[derive(Debug, Clone)]
pub struct DownloadedBatch {
    run_config: AnalysisRunConfig,
    data_variable_name: String,
    time_coordinates: Vec<DateTime<Utc>>,
    time_chunk_index: usize,
    array: Array3<E>,
}

impl AnalysisRunConfig {
    /// # Errors
    ///
    /// Returns `Err` if `self.time_coordinates` is empty.
    pub fn download_batches(&self) -> Result<Vec<DownloadBatch>> {
        if self.time_coordinates.is_empty() {
            return Err(anyhow!("Can't make batches from no timesteps"));
        }
        Ok(self
            .time_coordinates
            .iter()
            .chunks(self.dataset.time_chunk_size)
            .into_iter()
            .enumerate()
            .map(|(time_chunk_index, chunk_time_coordinates)| DownloadBatch {
                run_config: self.clone(),
                data_variable_name: self.data_variable_name.clone(),
                time_coordinates: chunk_time_coordinates.copied().collect(),
                time_chunk_index,
            })
            .collect())
    }
}

impl DownloadBatch {
    async fn process(&self, http_client: HttpClient) -> DownloadedBatch {
        let load_file_futures = self.time_coordinates.iter().map(|init_time| {
            load_file(
                self.data_variable_name.as_str(),
                *init_time,
                http_client.clone(),
            )
        });

        let results = join_all(load_file_futures).await;

        let arrays = results
            .into_iter()
            .map(|r| {
                r.inspect_err(|e| eprintln!("Error getting chunk! {e:?}"))
                    .unwrap_or_else(|_| Array2::from_elem((721, 1440), E::NAN))
            })
            .collect::<Vec<_>>();

        let array_views = arrays
            .iter()
            .map(ndarray::ArrayBase::view)
            .collect::<Vec<_>>();

        let array = ndarray::stack(Axis(0), &array_views).expect("Array shapes must be stackable");

        DownloadedBatch {
            array,
            run_config: self.run_config.clone(),
            data_variable_name: self.data_variable_name.clone(),
            time_coordinates: self.time_coordinates.clone(),
            time_chunk_index: self.time_chunk_index,
        }
    }
}

async fn load_file(
    data_variable_name: &str,
    init_time: DateTime<Utc>,
    http_client: HttpClient,
) -> Result<Array2<E>> {
    let (init_date, init_hour) = (init_time.format("%Y%m%d"), init_time.format("%H"));

    // `atmos` and `wave` directories were added to the path starting 2021-03-23T00Z
    let data_path = if init_time < Utc.with_ymd_and_hms(2021, 3, 23, 0, 0, 0).unwrap() {
        format!("gfs.{init_date}/{init_hour}/gfs.t{init_hour}z.pgrb2.0p25.f000")
    } else {
        format!("gfs.{init_date}/{init_hour}/atmos/gfs.t{init_hour}z.pgrb2.0p25.f000")
    };

    let data_url = format!("{S3_BUCKET_HOST}/{data_path}");
    let index_url = format!("{data_url}.idx");

    let index_contents = http_client
        .get(&index_url)
        .send()
        .await?
        .error_for_status()?
        .text()
        .await?;

    let index_variable_str = GRIB_INDEX_VARIABLE_NAMES
        .get(data_variable_name)
        .unwrap_or_else(|| panic!("missing grib index variable name for {data_variable_name}"));

    let byte_offset_regex = Regex::new(&format!(
        r"\d+:(\d+):.+:{index_variable_str}:.+:\n\d+:(\d+)"
    ))
    .unwrap();

    let (_, [start_offset_str, end_offset_str]) = byte_offset_regex
        .captures(&index_contents)
        .ok_or(anyhow!("Couldn't parse .idx {}", &index_url))?
        .extract::<2>();
    let start_offset: u64 = start_offset_str.parse()?;
    let end_offset: u64 = end_offset_str.parse()?;

    assert!(start_offset < end_offset);

    let response = http_client
        .get(data_url)
        .header("Range", format!("bytes={start_offset}-{end_offset}"))
        .send()
        .await?;

    let _content_length = response
        .content_length()
        .ok_or(anyhow!("s3 response missing content length"))?;

    // Reqwest byte streams implement futures::AsyncRead, tokio's copy wants tokio::AsyncRead, this translates.
    let mut data_stream =
        tokio_util::io::StreamReader::new(response.bytes_stream().map_err(std::io::Error::other));

    let file_path = format!("/tmp/gfs/{data_path}");
    let file_dir = std::path::Path::new(&file_path)
        .parent()
        .expect("file_path must have parent dir");
    tokio::fs::create_dir_all(file_dir).await?;
    let mut file = tokio::fs::File::create(&file_path).await?;

    tokio::io::copy(&mut data_stream, &mut file).await?;

    let array = spawn_blocking(move || -> Result<Array2<E>> {
        let dataset = gdal::Dataset::open(file_path)?;
        let band = dataset.rasterband(1)?;
        let array = band.read_as_array::<E>((0, 0), band.size(), band.size(), None)?;
        Ok(array)
    })
    .await??;

    Ok(array)
}

async fn split_space_chunks(job: Job, time_chunk: Array3<E>) -> Vec<(ChunkIdx3, Array3<E>)> {
    spawn_blocking(move || {
        let [time_size, y_size, x_size] = time_chunk.shape() else {
            panic!("expected 3D array")
        };

        let y_chunk_size = 361;
        let x_chunk_size = 360;

        let y_n_chunks = ((*y_size as f64) / (y_chunk_size as f64)).ceil() as usize;
        let x_n_chunks = ((*x_size as f64) / (x_chunk_size as f64)).ceil() as usize;

        let chunk_i_tuples = (0..y_n_chunks).cartesian_product(0..x_n_chunks);

        chunk_i_tuples
            .map(|(y_chunk_i, x_chunk_i)| {
                let mut chunk = Array3::from_elem([*time_size, y_chunk_size, x_chunk_size], E::NAN);

                let y_start = y_chunk_i * y_chunk_size;
                let y_stop = min(y_start + y_chunk_size, *y_size);
                let x_start = x_chunk_i * x_chunk_size;
                let x_stop = min(x_start + x_chunk_size, *x_size);

                chunk
                    .slice_mut(s![.., ..(y_stop - y_start), ..(x_stop - x_start)])
                    .assign(&time_chunk.slice(s![.., y_start..y_stop, x_start..x_stop]));

                let chunk_idx = [job.i, y_chunk_i, x_chunk_i];
                // println!("Split {:?}", &chunk_idx);
                (chunk_idx, chunk)
            })
            .collect::<Vec<_>>()
    })
    .await
    .unwrap()
}
