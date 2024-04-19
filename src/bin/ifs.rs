use anyhow::anyhow;
use anyhow::Result;
use chrono::{DateTime, TimeDelta, TimeZone, Utc};
use futures::future::join_all;
use futures::stream;
use futures::{StreamExt, TryStreamExt};
use itertools::Itertools;
use ndarray::{s, Array2, Array3, Axis};
use object_store::aws::AmazonS3Builder;
use object_store::ObjectStore;
use regex::Regex;
use reqwest_middleware::{ClientBuilder, ClientWithMiddleware};
use reqwest_retry::{policies::ExponentialBackoff, RetryTransientMiddleware};
use std::cmp::min;
use std::mem::size_of_val;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;
use tokio::task::spawn_blocking;

type HttpClient = ClientWithMiddleware;
type ObjStore = Arc<dyn ObjectStore>;
type ChunkIdx3 = [usize; 3];

const S3_BUCKET_HOST: &str = "https://ecmwf-forecasts.s3.amazonaws.com";
const DEST_ROOT_PATH: &str = "aldenks/ifs-dynamical/analysis/v0.1.0.zarr";

type E = f32;
const DATASET_SHORT_NAME: &str = "ifs";
const VARIABLE_PARAM_CODE: &str = "tp";
const VARIABLE_NAME: &str = "precipitation_surface";

#[tokio::main]
async fn main() -> Result<()> {
    // console_subscriber::init();

    use std::time::Instant;
    let start = Instant::now();

    let dataset_start_date = Utc.with_ymd_and_hms(2024, 3, 1, 0, 0, 0).unwrap();
    let init_time_chunk_size: TimeDelta = TimeDelta::try_days(10).unwrap();
    let init_freq: TimeDelta = TimeDelta::try_hours(12).unwrap();

    let http_client = http_client()?;
    let output_store = output_store()?;

    let base_buffer = 1;

    let results = stream::iter(0..8) // 46
        .map(|i| {
            let start_date = dataset_start_date + (init_time_chunk_size * i.try_into().unwrap());
            Job {
                i,
                start_date,
                end_date: start_date + init_time_chunk_size,
                init_freq,
            }
        })
        .map(|job| download_and_read_time_chunk(job, http_client.clone()))
        .buffer_unordered(base_buffer)
        .map(|(job, time_chunk)| split_space_chunks(job, time_chunk))
        .buffer_unordered(base_buffer * 8)
        .flat_map(stream::iter)
        .map(|(chunk_idx, array)| spawn_blocking(move || compress_chunk(chunk_idx, array)))
        .buffer_unordered(base_buffer * 8)
        .map(|result| async {
            let (chunk_idx, data, compression_ratio) = result.unwrap().unwrap();
            let data_len = data.len();
            upload_chunk(chunk_idx, data, output_store.clone())
                .await
                .unwrap();
            (compression_ratio, data_len)
        })
        .buffer_unordered(base_buffer * 2)
        .collect::<Vec<_>>()
        .await;

    println!("\n");
    println!("{:?} chunks, {:?} elapsed", results.len(), start.elapsed());

    Ok(())
}

async fn upload_chunk(
    chunk_idx: ChunkIdx3,
    data: Vec<u8>,
    store: ObjStore,
) -> Result<object_store::PutResult> {
    let chunk_idx_name = chunk_idx.into_iter().join(".");
    let chunk_path = format!("{DEST_ROOT_PATH}/{VARIABLE_NAME}/{chunk_idx_name}");

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

fn compress_chunk(chunk_idx: ChunkIdx3, array: Array3<E>) -> Result<(ChunkIdx3, Vec<u8>, f64)> {
    let array_slice = array
        .as_slice()
        .expect("TODO handle non default memory ordering");
    let slice_byte_len = size_of_val(array_slice);
    let element_size = size_of_val(&array_slice[0]);

    // println!("Compressing {:?}", chunk_idx);

    let context = blosc::Context::new()
        .compressor(blosc::Compressor::Zstd)
        .unwrap()
        .typesize(Some(element_size))
        .clevel(blosc::Clevel::L5)
        .shuffle(blosc::ShuffleMode::Byte);

    let compressed: Vec<u8> = context.compress(array_slice).into();

    let compression_ratio = compressed.len() as f64 / slice_byte_len as f64;

    Ok((chunk_idx, compressed, compression_ratio))
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

async fn download_and_read_time_chunk(job: Job, http_client: HttpClient) -> (Job, Array3<E>) {
    let mut futures = vec![];
    let mut init_time = job.start_date;
    while init_time < job.end_date {
        futures.push(load_file(init_time, http_client.clone()));
        init_time += job.init_freq;
    }
    let results = join_all(futures).await;
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

    (job, array)
}

async fn load_file(init_time: DateTime<Utc>, http_client: HttpClient) -> Result<Array2<E>> {
    let (init_date, init_hour) = (init_time.format("%Y%m%d"), init_time.format("%H"));

    let base_data_path = format!(
        "{init_date}/{init_hour}z/{DATASET_SHORT_NAME}/0p25/oper/{init_date}{init_hour}0000-0h-oper-fc"
    );
    let data_path = format!("{base_data_path}.grib2");

    let data_url = format!("{S3_BUCKET_HOST}/{data_path}");
    let index_url = format!("{S3_BUCKET_HOST}/{base_data_path}.index");

    let index_contents = http_client
        .get(&index_url)
        .send()
        .await?
        .error_for_status()?
        .text()
        .await?;

    let byte_offset_regex = Regex::new(&format!(
        r#""levtype": "sfc".*"param": "{VARIABLE_PARAM_CODE}", "_offset": (\d*), "_length": (\d*)"#
    ))
    .unwrap();

    let (_, [start_offset_str, length_str]) = byte_offset_regex
        .captures(&index_contents)
        .ok_or(anyhow!("Couldn't parse .index {}", &index_url))?
        .extract::<2>();
    let start_offset: u64 = start_offset_str.parse()?;
    let length: u64 = length_str.parse()?;
    // let end_offset = start_offset + length - 1; // http range is inclusive of endpoint, subtract 1
    let end_offset = start_offset + length;

    assert!(start_offset < end_offset);

    let response = http_client
        .get(&data_url)
        .header("Range", format!("bytes={start_offset}-{end_offset}"))
        .send()
        .await?;

    // Reqwest byte streams implement futures::AsyncRead, tokio's copy wants tokio::AsyncRead, this translates.
    let mut data_stream =
        tokio_util::io::StreamReader::new(response.bytes_stream().map_err(std::io::Error::other));

    let file_path = format!("/tmp/{DATASET_SHORT_NAME}/{data_path}");
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

#[derive(Default, Debug, Copy, Clone)]
struct Job {
    i: usize,
    start_date: DateTime<Utc>,
    end_date: DateTime<Utc>,
    init_freq: TimeDelta,
}

fn http_client() -> Result<HttpClient> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(45))
        .connect_timeout(Duration::from_secs(5))
        .redirect(reqwest::redirect::Policy::none())
        .https_only(true)
        .build()?;

    let backoff_policy = ExponentialBackoff::builder().build_with_max_retries(32);

    Ok(ClientBuilder::new(client)
        .with(RetryTransientMiddleware::new_with_policy(backoff_policy))
        .build())
}

fn output_store() -> Result<ObjStore> {
    let mut store_builder = AmazonS3Builder::from_env()
        .with_access_key_id(std::env::var("OUTPUT_STORE_ACCESS_KEY_ID")?)
        .with_secret_access_key(std::env::var("OUTPUT_STORE_SECRET_ACCESS_KEY")?);

    if let Ok(bucket) = std::env::var("OUTPUT_STORE_BUCKET") {
        store_builder = store_builder.with_bucket_name(bucket);
    }
    if let Ok(endpoint) = std::env::var("OUTPUT_STORE_ENDPOINT") {
        store_builder = store_builder.with_endpoint(endpoint);
    }
    if let Ok(region) = std::env::var("OUTPUT_STORE_REGION") {
        store_builder = store_builder.with_region(region);
    }

    let store = store_builder.build()?;
    Ok(Arc::new(store))
}
