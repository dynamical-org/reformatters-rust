use anyhow::anyhow;
use anyhow::Result;
use chrono::{DateTime, TimeDelta, TimeZone, Utc};
use clap::Parser;
use futures::stream;
use itertools::Itertools;
use reformatters::gfs;
use reformatters::http;
use reformatters::AnalysisDataset;
use reformatters::AnalysisRunConfig;
use std::time::Duration;
use std::time::Instant;

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
