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

    future_buffer_base_size: Option<usize>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::try_parse()?;
    reformatters::gfs::reformat(
        cli.variable,
        cli.time_start,
        cli.time_end,
        cli.future_buffer_base_size.unwrap_or(3),
    )
    .await
}
