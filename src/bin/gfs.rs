use anyhow::anyhow;
use anyhow::Result;
use chrono::{DateTime, TimeDelta, TimeZone, Utc};
use clap::Parser;
use futures::stream;
use itertools::Itertools;
use object_store::aws::AmazonS3Builder;
use object_store::ObjectStore;
use reformatters::gfs;
use reformatters::http;
use reformatters::AnalysisDataset;
use reformatters::AnalysisRunConfig;
use std::cmp::min;
use std::mem::size_of_val;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

type ObjStore = Arc<dyn ObjectStore>;
type ChunkIdx3 = [usize; 3];

#[derive(clap::Parser, Debug)]
struct Cli {
    /// Data variable name to ingest
    variable: String,

    /// Earliest timestamp to ingest
    start_time: DateTime<Utc>,

    /// Most recent timestamp to ingest (inclusive)
    end_time: DateTime<Utc>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let start = Instant::now();

    let cli = Cli::try_parse()?;

    let dataset = &gfs::GFS_DATASET;
    let run_config = get_run_config(dataset, cli.variable, cli.start_time, cli.end_time)?;
    let download_batches = run_config.download_batches()?;
    dbg!(download_batches);

    let http_client = http::client?;
    let output_store = output_store()?;

    let base_buffer = 3;

    let results = stream::iter(download_batches)
        .map(|download_batch| {
            process_download_and_read_time_chunk(download_batch, http_client.clone())
        })
        .buffer_unordered(base_buffer)
        .map(|(job, time_chunk)| split_space_chunks(job, time_chunk))
        .buffer_unordered(base_buffer * 8)
        .flat_map(stream::iter)
        .map(|(chunk_idx, array)| spawn_blocking(move || compress_chunk(chunk_idx, &array)))
        .buffer_unordered(base_buffer * 8)
        .map(|result| async {
            let (chunk_idx, data, uncompressed_len, compressed_len) = result.unwrap();
            upload_chunk(chunk_idx, data, output_store.clone())
                .await
                .unwrap();
            (uncompressed_len, compressed_len)
        })
        .buffer_unordered(base_buffer * 2)
        .collect::<Vec<_>>()
        .await;

    print_report(results, start.elapsed());

    Ok(())
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
        .collect();

    Ok(AnalysisRunConfig {
        dataset: dataset.clone(),
        data_variable_name,
        time_coordinates,
    })
}

#[allow(clippy::cast_precision_loss)]
fn print_report(results: Vec<(usize, usize)>, elapsed: Duration) {
    let num_chunks = results.len();

    let (uncompressed_lengths, compressed_lengths): (Vec<usize>, Vec<usize>) =
        results.into_iter().unzip();

    let uncompressed_total_mb = uncompressed_lengths.iter().sum::<usize>() as f64 / 1e6;
    let compressed_total_mb = compressed_lengths.iter().sum::<usize>() as f64 / 1e6;
    let avg_compression_ratio = compressed_total_mb / uncompressed_total_mb;

    let avg_uncompressed_chunk_mb = uncompressed_total_mb / num_chunks as f64;
    let avg_compressed_chunk_mb = compressed_total_mb / num_chunks as f64;

    println!("\nTotals");
    println!("{uncompressed_total_mb} MB uncompressed, {compressed_total_mb} MB compressed");
    println!("{avg_compression_ratio:.2} average compression ratio");

    println!("\nChunks, n = {num_chunks}");
    println!(
        "{avg_uncompressed_chunk_mb} MB uncompressed, {avg_compressed_chunk_mb} MB compressed"
    );

    println!("\n{elapsed:?} elapsed");
}

async fn upload_chunk(
    chunk_idx: ChunkIdx3,
    data: Vec<u8>,
    store: ObjStore,
) -> Result<object_store::PutResult> {
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

fn compress_chunk(chunk_idx: ChunkIdx3, array: &Array3<E>) -> (ChunkIdx3, Vec<u8>, usize, usize) {
    let array_slice = array
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

    (
        chunk_idx,
        compressed,
        uncompressed_length,
        compressed_length,
    )
}

#[derive(Default, Debug, Copy, Clone)]
struct Job {
    i: usize,
    start_date: DateTime<Utc>,
    end_date: DateTime<Utc>,
    init_freq: TimeDelta,
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
