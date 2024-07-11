use std::mem::size_of_val;
use std::sync::Arc;
use std::time::{Duration, Instant};
use std::{cmp::min, collections::HashMap};

use crate::do_upload;
use crate::http;
use crate::num_chunks;
use crate::AnalysisDataset;
use crate::AnalysisRunConfig;
use crate::DataDimension;
use crate::DataVariable;
use crate::{binary_round, output};
pub use anyhow::{anyhow, Result};
use backon::ExponentialBuilder;
use backon::Retryable;
use chrono::{DateTime, TimeDelta, TimeZone, Timelike, Utc};
use futures::future::join_all;
use futures::stream::StreamExt;
use futures::TryStreamExt;
use itertools::Itertools;
use ndarray::{s, Array1, Array2, Array3, Axis};
use once_cell::sync::Lazy;
use output::Storage;
use regex::Regex;
use serde_json::json;
use tokio::task::spawn_blocking;

const S3_BUCKET_HOST: &str = "https://noaa-gfs-bdp-pds.s3.amazonaws.com";

// if using dynamical.s3.source.coop endpoint
const DEST_ROOT_PATH: &str = "analysis-hourly/v0.1.0.zarr";
// if using standard s3 source coop access with session token
// const DEST_ROOT_PATH: &str = "dynamical/gfs/analysis-hourly/v0.1.0.zarr";

/// Dataset config object todos
/// - incorporate element type into object
/// - make dimension objects which contain names, etc
/// - make data variable enum which contains name, dimensions, chunking, units, grib index names, etc
type E = f32; // element type

static HOURLY_AWS_FORECAST_START: Lazy<DateTime<Utc>> =
    Lazy::new(|| Utc.with_ymd_and_hms(2021, 2, 27, 0, 0, 0).unwrap());
const INIT_FREQUENCY_HOURS: i64 = 6;
const EARLY_DATA_FREQUENCY_HOURS: u32 = 3;

pub static GFS_DATASET: Lazy<AnalysisDataset> = Lazy::new(|| AnalysisDataset {
    id: "noaa-gfs-analysis-hourly",
    name: "NOAA GFS analysis, hourly",
    description:
        "Historical weather data from the Global Forecast System (GFS) model operated by NOAA NCEP.",
    url: "https://data.dynamical.org/noaa/gfs/analysis-hourly/latest.zarr",
    spatial_coverage: "Global",
    spatial_resolution: "0.25 degrees (~20km)",
    attribution:
        "NOAA NCEP GFS data processed by dynamical.org from NCAR and NOAA BDP AWS archives.",

    time_start: Utc.with_ymd_and_hms(2015, 1, 15, 0, 0, 0).unwrap(),
    time_end: Utc.with_ymd_and_hms(2024, 7, 1, 0, 0, 0).unwrap(),
    time_step: TimeDelta::try_hours(1).unwrap(),
    time_chunk_size: 160,

    longitude_start: -180.,
    longitude_end: 180.,
    longitude_step: 0.25,
    longitude_chunk_size: 144,

    latitude_start: 90.,
    latitude_end: -90.,
    latitude_step: 0.25,
    latitude_chunk_size: 145,

    data_dimensions: vec![
        DataDimension {
            name: "time",
            long_name: "Time",
            units: "seconds since 1970-01-01 00:00:00",
            dtype: "<i8",
            extra_metadata: HashMap::from([("calendar", "proleptic_gregorian")]),
            statistics_approximate: json!({
                "min": "2015-01-15 00:00:00 UTC",
                "mean": "2019-10-08 11:30:00 UTC",
                "max": "2024-06-30 23:00:00 UTC",
            }),
        },
        DataDimension {
            name: "latitude",
            long_name: "Latitude",
            units: "decimal degrees",
            dtype: "<f8",
            extra_metadata: HashMap::new(),
            statistics_approximate: json!({
                "min": -90.0,
                "mean": 0.0,
                "max": 90.0,
            }),
        },
        DataDimension {
            name: "longitude",
            long_name: "Longitude",
            units: "decimal degrees",
            dtype: "<f8",
            extra_metadata: HashMap::new(),
            statistics_approximate: json!({
                "min": -180.0,
                "mean": -0.125,
                "max": 179.75,
            }),
        },
    ],
    data_variables: vec![
        DataVariable {
            name: "temperature_2m",
            long_name: "Temperature 2 meters above earth surface",
            units: "C",
            dtype: "<f4",
            grib_variable_name: "TMP:2 m above ground",
            statistics_approximate: json!({
                "min": -79.5,
                "mean": 6.041,
                "max": 53.25,
            }),
        },
        DataVariable {
            name: "precipitation_surface",
            long_name: "Precipitation rate at earth surface",
            units: "kg/(m^2 s)",
            dtype: "<f4",
            grib_variable_name: "PRATE:surface",
            statistics_approximate: json!({
                "min": 0.0,
                "mean": 2.911e-05,
                "max": 0.04474,
            }),
        },
        DataVariable {
            name: "wind_u_10m",
            long_name: "Wind speed u-component 10 meters above earth surface",
            units: "m/s",
            dtype: "<f4",
            grib_variable_name: "UGRD:10 m above ground",
            statistics_approximate: json!({
                "min": -96.88,
                "mean": -0.00644,
                "max": 87.5,
            }),
        },
        DataVariable {
            name: "wind_v_10m",
            long_name: "Wind speed v-component 10 meters above earth surface",
            units: "m/s",
            dtype: "<f4",
            grib_variable_name: "VGRD:10 m above ground",
            statistics_approximate: json!({
                "min": -87.88,
                "mean": 0.1571,
                "max": 89.0,
            }),
        },
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

/// # Errors
///
/// Currently never returns an error, but it should report errors
pub async fn reformat(
    data_variable_name: String,
    time_start: DateTime<Utc>,
    time_end: DateTime<Utc>,
    destination: String,
    skip_metadata: bool,
) -> Result<()> {
    let start = Instant::now();

    let run_config = get_run_config(&GFS_DATASET, &data_variable_name, time_start, time_end)?;

    let download_batches = run_config.get_download_batches()?;

    let http_client = http::client()?;

    let object_store = output::get_object_store(&destination)?;

    if !skip_metadata {
        run_config
            .write_zarr_metadata(object_store.clone(), DEST_ROOT_PATH)
            .await?;
        run_config
            .write_dimension_coordinates(object_store.clone(), DEST_ROOT_PATH)
            .await?;
    }

    let results = futures::stream::iter(download_batches)
        .map(|download_batch| download_batch.process(http_client.clone()))
        .buffer_unordered(2)
        .map(DownloadedBatch::zarr_array_chunks)
        .buffer_unordered(30)
        .flat_map(futures::stream::iter)
        .map(ZarrChunkArray::compress)
        .buffer_unordered(30)
        .map(|zarr_chunk_compressed| zarr_chunk_compressed.upload(object_store.clone()))
        .buffer_unordered(30)
        .collect::<Vec<_>>()
        .await;

    print_report(results, start.elapsed());

    Ok(())
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

#[allow(dead_code)] // We report but don't currently do something with all these values
#[derive(Debug, Clone)]
pub struct ZarrChunkUploadInfo {
    run_config: AnalysisRunConfig,
    data_variable_name: String,
    time_chunk_index: usize,
    longitude_chunk_index: usize,
    latitude_chunk_index: usize,
    uncompressed_mb: f64,
    compressed_mb: f64,
    upload_time: Duration,
    e_tag: Option<String>,
    object_version: Option<String>,
}

fn get_run_config(
    dataset: &AnalysisDataset,
    data_variable_name: &str,
    time_start: DateTime<Utc>,
    time_end: DateTime<Utc>,
) -> Result<AnalysisRunConfig> {
    let data_variable = dataset
        .data_variables
        .iter()
        .find(|data_variable| data_variable.name == data_variable_name);

    if data_variable.is_none() {
        return Err(anyhow!("Variable name {data_variable_name} not supported."));
    }

    if time_start < dataset.time_start {
        return Err(anyhow!(
            "Start time {time_start} is before dataset time {}",
            dataset.time_start
        ));
    }

    if dataset.time_end < time_end {
        return Err(anyhow!(
            "End time {time_end} is after dataset time {}",
            dataset.time_end
        ));
    }

    let time_coordinates = (0..u32::MAX)
        .scan(dataset.time_start - dataset.time_step, |time, _| {
            *time += dataset.time_step;
            if *time < dataset.time_end {
                Some(*time)
            } else {
                None
            }
        })
        .collect::<Vec<_>>();

    let latitude_coordinates = (0..u32::MAX)
        .scan(
            dataset.latitude_start + dataset.latitude_step,
            |latitude, _| {
                *latitude -= dataset.latitude_step;
                if *latitude >= dataset.latitude_end {
                    Some(*latitude)
                } else {
                    None
                }
            },
        )
        .collect::<Vec<_>>();

    let longitude_coordinates = (0..u32::MAX)
        .scan(
            dataset.longitude_start - dataset.longitude_step,
            |longitude, _| {
                *longitude += dataset.longitude_step;
                if *longitude < dataset.longitude_end {
                    Some(*longitude)
                } else {
                    None
                }
            },
        )
        .collect::<Vec<_>>();

    Ok(AnalysisRunConfig {
        dataset: dataset.clone(),
        data_variable: data_variable.unwrap().clone(),
        time_coordinates: Arc::new(time_coordinates),
        latitude_coordinates,
        longitude_coordinates,
        time_start,
        time_end,
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
                data_variable_name: self.data_variable.name.to_string(),
                time_coordinates: chunk_time_coordinates.copied().collect(),
                time_chunk_index,
            })
            .filter(|download_batch| {
                let download_batch_start_date = download_batch.time_coordinates.first().unwrap();
                let download_batch_end_date = download_batch.time_coordinates.last().unwrap();
                // If the download_batch_start_date is between the run_config's start and end date
                // or the download_batch_end_date is between the run_config's start and end date
                // then this is a download batch that we want to process
                (download_batch_start_date >= &self.time_start
                    && download_batch_start_date < &self.time_end)
                    || (download_batch_end_date >= &self.time_start
                        && download_batch_end_date < &self.time_end)
            })
            .collect())
    }
}

impl DownloadBatch {
    async fn process(self, http_client: http::Client) -> DownloadedBatch {
        println!(
            "Starting to process {} to {}",
            self.time_coordinates.first().unwrap(),
            self.time_coordinates.last().unwrap()
        );

        let (needs_interpolation, time_coordinates_to_download) =
            self.time_coordinates_to_download();

        // filter time coords to 3 hourly
        // load with filtered time coordinates
        // x = filtered time coords (transformed into seconds since x)
        // q = self.time_coordinates (transformed into seconds since x)
        //

        let load_file_futures =
            time_coordinates_to_download
                .clone()
                .into_iter()
                .map(|time_coordinate| {
                    load_variable_from_file(
                        self.data_variable_name.as_str(),
                        time_coordinate,
                        http_client.clone(),
                    )
                });

        let results = join_all(load_file_futures).await;

        let arrays = results
            .into_iter()
            .map(|r| {
                r.inspect_err(|e| {
                    eprintln!(
                        "Error getting chunk {} - {}! {e:?}",
                        self.time_coordinates.first().unwrap(),
                        self.time_coordinates.last().unwrap()
                    );
                })
                .unwrap_or_else(|_| Array2::from_elem((721, 1440), E::NAN))
            })
            .collect::<Vec<_>>();

        let array = spawn_blocking(move || {
            let array_views = arrays
                .iter()
                .map(ndarray::ArrayBase::view)
                .collect::<Vec<_>>();

            let mut array =
                ndarray::stack(Axis(0), &array_views).expect("Array shapes must be stackable");

            if needs_interpolation {
                let interpolator = ndarray_interp::interp1d::Interp1DBuilder::new(array)
                    .x(to_timestamps(&time_coordinates_to_download))
                    .strategy(ndarray_interp::interp1d::Linear::new())
                    .build()
                    .unwrap();

                array = interpolator
                    .interp_array(&to_timestamps(&self.time_coordinates))
                    .unwrap();
            }

            // improve compression ratio by rounding to keep n mantissa bits, setting the rest to zeros
            array.mapv_inplace(|v| binary_round::round(v, 9));

            array
        })
        .await
        .unwrap();

        DownloadedBatch {
            array,
            run_config: self.run_config.clone(),
            data_variable_name: self.data_variable_name.clone(),
            time_chunk_index: self.time_chunk_index,
        }
    }

    /// Add `EARLY_DATA_FREQUENCY - 1` time coordinates to the start and end of `self.time_coordinates`
    fn time_coordinates_to_download(&self) -> (bool, Vec<DateTime<Utc>>) {
        let first = self.time_coordinates.first().unwrap();
        let last = self.time_coordinates.last().unwrap();
        let time_step_hours: usize = self
            .run_config
            .dataset
            .time_step
            .num_hours()
            .try_into()
            .unwrap();

        if first >= &HOURLY_AWS_FORECAST_START {
            let needs_interpolation = false;
            return (needs_interpolation, self.time_coordinates.clone());
        }

        let mut padded = self.time_coordinates.clone();

        for hour in (1..EARLY_DATA_FREQUENCY_HOURS).step_by(time_step_hours) {
            let coord = *first - TimeDelta::try_hours(hour.into()).unwrap();
            padded.insert(0, coord);
        }

        for hour in (1..EARLY_DATA_FREQUENCY_HOURS).step_by(time_step_hours) {
            let coord = *last + TimeDelta::try_hours(hour.into()).unwrap();
            padded.push(coord);
        }

        let filtered_time_coordinates = padded
            .into_iter()
            .filter(|t| {
                t > &HOURLY_AWS_FORECAST_START || t.hour() % EARLY_DATA_FREQUENCY_HOURS == 0
            })
            .collect_vec();

        let needs_interpolation = true;
        (needs_interpolation, filtered_time_coordinates)
    }
}

fn to_timestamps(times: &[DateTime<Utc>]) -> Array1<E> {
    #[allow(clippy::cast_precision_loss)]
    Array1::from_iter(times.iter().map(|t| t.timestamp() as f32))
}

async fn load_variable_from_file(
    data_variable_name: &str,
    time_coordinate: DateTime<Utc>,
    http_client: http::Client,
) -> Result<Array2<E>> {
    let file_path = if time_coordinate < *HOURLY_AWS_FORECAST_START {
        // Data is 3 hourly in this period
        if time_coordinate.hour() % EARLY_DATA_FREQUENCY_HOURS != 0 {
            return Ok(Array2::from_elem((721, 1440), E::NAN));
        }
        let local_data_dir = std::env::var("LOCAL_DATA_DIR")?;
        // let date_str = time_coordinate.format("%Y%m%d");
        // let lead_time_hour = i64::from(time_coordinate.hour()) % INIT_FREQUENCY_HOURS;
        let lead_time_hour = i64::from(time_coordinate.hour()) % INIT_FREQUENCY_HOURS;
        assert!(lead_time_hour >= 0);
        assert!(lead_time_hour < 6);

        let init_time = time_coordinate
            - TimeDelta::try_hours(lead_time_hour).expect("lead time hours to be within 0 - 5");

        let init_time_str = init_time.format("%Y%m%d%H");
        format!(
            "{local_data_dir}/{data_variable_name}/gfs.0p25.{init_time_str}.f{lead_time_hour:0>3}.grib2"
        )
    } else {
        download_band(data_variable_name, time_coordinate, http_client).await?
    };

    let file_path_move = file_path.clone();
    let array = spawn_blocking(move || -> Result<Array2<E>> {
        let dataset = gdal::Dataset::open(file_path_move)?;
        let band = dataset.rasterband(1)?;
        let array = band.read_as_array::<E>((0, 0), band.size(), band.size(), None)?;
        Ok(array)
    })
    .await??;

    if file_path.starts_with("/tmp/") {
        tokio::fs::remove_file(file_path).await?;
    }

    Ok(array)
}

async fn download_band(
    data_variable_name: &str,
    time_coordinate: DateTime<Utc>,
    http_client: http::Client,
) -> Result<String> {
    let lead_time_hour = i64::from(time_coordinate.hour()) % INIT_FREQUENCY_HOURS;
    assert!(lead_time_hour >= 0);
    assert!(lead_time_hour < 6);

    let init_time = time_coordinate
        - TimeDelta::try_hours(lead_time_hour).expect("lead time hours to be within 0 - 5");

    let (init_date, init_hour) = (init_time.format("%Y%m%d"), init_time.format("%H"));

    // `atmos` and `wave` directories were added to the path starting 2021-03-23T00Z
    let data_path = if init_time < Utc.with_ymd_and_hms(2021, 3, 23, 0, 0, 0).unwrap() {
        format!("gfs.{init_date}/{init_hour}/gfs.t{init_hour}z.pgrb2.0p25.f{lead_time_hour:0>3}")
    } else {
        format!(
            "gfs.{init_date}/{init_hour}/atmos/gfs.t{init_hour}z.pgrb2.0p25.f{lead_time_hour:0>3}"
        )
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

    Ok(file_path)
}

impl DownloadedBatch {
    async fn zarr_array_chunks(self) -> Vec<ZarrChunkArray> {
        spawn_blocking(move || {
            let [time_size, lat_size, lon_size] = self.array.shape() else {
                panic!("expected 3D array")
            };

            let lat_chunk_size = self.run_config.dataset.latitude_chunk_size;
            let lon_chunk_size = self.run_config.dataset.longitude_chunk_size;
            let time_chunk_size = self.run_config.dataset.time_chunk_size;

            let chunk_i_tuples = (0..num_chunks(*lat_size, lat_chunk_size))
                .cartesian_product(0..num_chunks(*lon_size, lon_chunk_size));

            chunk_i_tuples
                .map(|(lat_chunk_i, lon_chunk_i)| {
                    // create an array the size of a full chunk and fill with nan
                    let mut chunk = Array3::from_elem(
                        [time_chunk_size, lat_chunk_size, lon_chunk_size],
                        E::NAN,
                    );

                    let lat_start = lat_chunk_i * lat_chunk_size;
                    let lat_stop = min(lat_start + lat_chunk_size, *lat_size);
                    let lon_start = lon_chunk_i * lon_chunk_size;
                    let lon_stop = min(lon_start + lon_chunk_size, *lon_size);

                    // write available data into the correct portion of the chunk array
                    chunk
                        .slice_mut(s![
                            ..*time_size,
                            ..(lat_stop - lat_start),
                            ..(lon_stop - lon_start)
                        ])
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
                .collect_vec()
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
    async fn upload(self, store: Storage) -> Result<ZarrChunkUploadInfo> {
        let upload_start_time = Instant::now();

        let data_dimension_names: Vec<&str> = self
            .run_config
            .dataset
            .data_dimensions
            .iter()
            .map(|data_dimension| data_dimension.name)
            .collect();
        assert!(data_dimension_names == vec!["time", "latitude", "longitude"]);
        let chunk_index_name = format!(
            "{}.{}.{}",
            self.time_chunk_index, self.latitude_chunk_index, self.longitude_chunk_index
        );
        let chunk_path = format!(
            "{DEST_ROOT_PATH}/{}/{chunk_index_name}",
            self.data_variable_name,
        );

        let bytes: object_store::PutPayload = self.bytes.into();

        let put_result =
            (|| async { do_upload(store.clone(), chunk_path.clone(), bytes.clone()).await })
                .retry(&ExponentialBuilder::default())
                .await
                .inspect_err(|e| println!("Upload error, chunk {chunk_index_name}, {e}"))?;

        let upload_time = upload_start_time.elapsed();

        println!(
            "Uploaded {chunk_index_name} in {upload_time:.2?} ({:?} mb)",
            bytes.content_length() / 10_usize.pow(6)
        );

        #[allow(clippy::cast_precision_loss)]
        Ok(ZarrChunkUploadInfo {
            run_config: self.run_config,
            data_variable_name: self.data_variable_name,
            time_chunk_index: self.time_chunk_index,
            longitude_chunk_index: self.longitude_chunk_index,
            latitude_chunk_index: self.latitude_chunk_index,
            uncompressed_mb: self.uncompressed_length as f64 / 10_f64.powf(6_f64),
            compressed_mb: self.compressed_length as f64 / 10_f64.powf(6_f64),
            upload_time,
            e_tag: put_result.e_tag,
            object_version: put_result.version,
        })
    }
}

#[allow(clippy::cast_precision_loss)]
fn print_report(results: Vec<Result<ZarrChunkUploadInfo>>, elapsed: Duration) {
    let num_chunks = results.len();

    let (uncompressed_mbs, compressed_mbs): (Vec<f64>, Vec<f64>) = results
        .into_iter()
        .filter_map(Result::ok)
        .map(|info| (info.uncompressed_mb, info.compressed_mb))
        .collect();

    let uncompressed_total_mb = uncompressed_mbs.iter().sum::<f64>();
    let compressed_total_mb = compressed_mbs.iter().sum::<f64>();
    let avg_compression_ratio = compressed_total_mb / uncompressed_total_mb;

    let avg_uncompressed_chunk_mb = uncompressed_total_mb / num_chunks as f64;
    let avg_compressed_chunk_mb = compressed_total_mb / num_chunks as f64;

    println!("\nTotals");
    println!(
        "{:.2} GB uncompressed, {:.2} GB compressed",
        uncompressed_total_mb / 1e3,
        compressed_total_mb / 1e3
    );
    println!("{avg_compression_ratio:.2} average compression ratio");

    println!("\nChunks, n = {num_chunks}");
    println!(
        "Average {avg_uncompressed_chunk_mb:.2} MB uncompressed, {avg_compressed_chunk_mb:.2} MB compressed"
    );

    println!("\n{elapsed:.2?} elapsed");
}
