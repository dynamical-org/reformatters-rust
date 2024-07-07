use anyhow::Result;
use chrono::{DateTime, Utc};
use clap::Parser;

#[derive(clap::Parser, Debug)]
struct Cli {
    /// Data variable name to ingest
    variable: String,

    /// Earliest timestamp to ingest
    time_start: DateTime<Utc>,

    /// Most recent timestamp to ingest (inclusive)
    time_end: DateTime<Utc>,

    /// URL string specifying the root of the zarr to create. eg. s3://bucket/path.zarr file:///home/.../path.zarr
    destination: String,

    /// Don't write metadata, just write chunks
    skip_metadata: Option<bool>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::try_parse()?;
    reformatters::gfs::reformat(
        cli.variable,
        cli.time_start,
        cli.time_end,
        cli.destination,
        cli.skip_metadata.unwrap_or(false),
    )
    .await
}
