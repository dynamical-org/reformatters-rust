use std::mem::size_of_val;
use std::sync::Arc;
use std::{cmp::min, collections::HashMap};

use crate::AnalysisDataset;
use crate::{http::HttpClient, num_chunks};

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
    pub time_coordinates: Arc<Vec<DateTime<Utc>>>, // Arc because this struct is cloned often and this vec can be big
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
    time_chunk_index: usize,
    array: Array3<E>,
}

#[derive(Debug, Clone)]
pub struct ZarrChunkArray {
    run_config: AnalysisRunConfig,
    data_variable_name: String,
    time_chunk_index: usize,
    longitude_chunk_index: usize,
    latitude_chunk_index: usize,
    array: Array3<E>,
}

#[derive(Debug, Clone)]
pub struct ZarrChunkCompressed {
    run_config: AnalysisRunConfig,
    data_variable_name: String,
    time_chunk_index: usize,
    longitude_chunk_index: usize,
    latitude_chunk_index: usize,
    bytes: Vec<u8>,
    uncompressed_length: usize,
    compressed_length: usize,
}

fn get_run_config(
    dataset: &AnalysisDataset,
    data_variable_name: String,
    time_start: DateTime<Utc>,
    time_end: DateTime<Utc>,
) -> Result<AnalysisRunConfig> {
    if !dataset
        .data_variable_names
        .contains(&data_variable_name.as_str())
    {
        return Err(anyhow!("Variable name {data_variable_name} not supported."));
    }

    if time_start < dataset.time_start {
        return Err(anyhow!(
            "Start time {time_start} is before dataset time {}",
            dataset.time_start
        ));
    }

    let now = Utc::now();
    if now < time_end {
        return Err(anyhow!("End time {time_end} is before now ({now})"));
    }

    let time_coordinates = (0..u32::MAX)
        .scan(time_start - dataset.time_step, |time, _| {
            *time += dataset.time_step;
            if *time < time_end {
                Some(*time)
            } else {
                None
            }
        })
        .collect::<Vec<_>>();

    Ok(AnalysisRunConfig {
        dataset: dataset.clone(),
        data_variable_name,
        time_coordinates: Arc::new(time_coordinates),
    })
}

impl AnalysisRunConfig {
    /// # Errors
    ///
    /// Returns `Err` if `self.time_coordinates` is empty.
    pub fn get_download_batches(&self) -> Result<Vec<DownloadBatch>> {
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
            load_variable_from_file(
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
            time_chunk_index: self.time_chunk_index,
        }
    }
}

async fn load_variable_from_file(
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

    // TODO its silly to write to disk just to read the whole thing back into memory right away,
    // figure out correct gdal memory file usage.
    let file_path = format!("/tmp/gfs/{data_path}");
    let file_dir = std::path::Path::new(&file_path)
        .parent()
        .expect("file_path must have parent dir");
    tokio::fs::create_dir_all(file_dir).await?;
    let mut file = tokio::fs::File::create(&file_path).await?;

    // Reqwest byte streams implement futures::AsyncRead, tokio's copy wants tokio::AsyncRead, this translates.
    let mut data_stream =
        tokio_util::io::StreamReader::new(response.bytes_stream().map_err(std::io::Error::other));

    tokio::io::copy(&mut data_stream, &mut file).await?;

    let file_path_move = file_path.clone();
    let array = spawn_blocking(move || -> Result<Array2<E>> {
        let dataset = gdal::Dataset::open(file_path_move)?;
        let band = dataset.rasterband(1)?;
        let array = band.read_as_array::<E>((0, 0), band.size(), band.size(), None)?;
        Ok(array)
    })
    .await??;

    tokio::fs::remove_file(file_path).await?;

    Ok(array)
}

impl DownloadedBatch {
    async fn zarr_array_chunks(self) -> Vec<ZarrChunkArray> {
        spawn_blocking(move || {
            let [time_size, lat_size, lon_size] = self.array.shape() else {
                panic!("expected 3D array")
            };

            let lat_chunk_size = self.run_config.dataset.latitude_chunk_size;
            let lon_chunk_size = self.run_config.dataset.longitude_chunk_size;

            let chunk_i_tuples = (0..num_chunks(*lat_size, lat_chunk_size))
                .cartesian_product(0..num_chunks(*lon_size, lon_chunk_size));

            chunk_i_tuples
                .map(|(lat_chunk_i, lon_chunk_i)| {
                    // create an array the size of a full chunk and fill with nan
                    let mut chunk =
                        Array3::from_elem([*time_size, lat_chunk_size, lon_chunk_size], E::NAN);

                    let lat_start = lat_chunk_i * lat_chunk_size;
                    let lat_stop = min(lat_start + lat_chunk_size, *lat_size);
                    let lon_start = lon_chunk_i * lon_chunk_size;
                    let lon_stop = min(lon_start + lon_chunk_size, *lon_size);

                    // write available data into the correct portion of the chunk array
                    chunk
                        .slice_mut(s![.., ..(lat_stop - lat_start), ..(lon_stop - lon_start)])
                        .assign(&self.array.slice(s![
                            ..,
                            lat_start..lat_stop,
                            lon_start..lon_stop
                        ]));

                    ZarrChunkArray {
                        run_config: self.run_config.clone(),
                        data_variable_name: self.data_variable_name.clone(),
                        time_chunk_index: self.time_chunk_index,
                        longitude_chunk_index: lon_chunk_i,
                        latitude_chunk_index: lat_chunk_i,
                        array: chunk,
                    }
                })
                .collect::<Vec<_>>()
        })
        .await
        .unwrap()
    }
}

impl ZarrChunkArray {
    async fn compress(self) -> ZarrChunkCompressed {
        spawn_blocking(move || {
            let array_slice = self
                .array
                .as_slice()
                .expect("TODO handle non default memory ordering");
            let uncompressed_length = size_of_val(array_slice);
            let element_size = size_of_val(&array_slice[0]);

            let context = blosc::Context::new()
                .compressor(blosc::Compressor::Zstd)
                .unwrap()
                .typesize(Some(element_size))
                .clevel(blosc::Clevel::L5)
                .shuffle(blosc::ShuffleMode::Byte);

            let compressed: Vec<u8> = context.compress(array_slice).into();
            let compressed_length = compressed.len();

            ZarrChunkCompressed {
                run_config: self.run_config,
                data_variable_name: self.data_variable_name,
                time_chunk_index: self.time_chunk_index,
                longitude_chunk_index: self.longitude_chunk_index,
                latitude_chunk_index: self.latitude_chunk_index,
                bytes: compressed,
                uncompressed_length,
                compressed_length,
            }
        })
        .await
        .expect("Could not compress chunk")
    }
}

impl ZarrChunkCompressed {
    async fn upload_chunk(&self, store: ObjStore) -> Result<object_store::PutResult> {
        let variable_name = VARIABLE_LEVEL.replace([':', ' '], "_");
        let chunk_idx_name = chunk_idx.into_iter().join(".");
        let chunk_path = format!("{DEST_ROOT_PATH}/{variable_name}/{chunk_idx_name}");

        let bytes: object_store::PutPayload = data.into();

        let s = Instant::now();

        let mut res = Err(anyhow!("Never attempted to put object"));
        for retry_i in 0..16 {
            match store.put(&chunk_path.clone().into(), bytes.clone()).await {
                Ok(r) => {
                    println!(
                        "Uploaded {:?} in {:.2?} ({:?} mb)",
                        &chunk_idx,
                        s.elapsed(),
                        bytes.content_length() / 10_usize.pow(6)
                    );
                    return Ok(r);
                }
                Err(e) => {
                    println!(
                        "upload err - retry i: {:?}, chunk {:?}",
                        retry_i, &chunk_idx
                    );
                    tokio::time::sleep(Duration::from_secs_f32(
                        8_f32.min(2_f32.powf(retry_i as f32)),
                    ))
                    .await;
                    res = Err(anyhow!(e));
                }
            }
        }
        res
    }
}
