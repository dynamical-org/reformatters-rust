use anyhow::Result;
use chrono::{DateTime, Utc};
use clap::{Parser, Subcommand};

#[derive(clap::Parser, Debug)]
struct Cli {
    #[clap(subcommand)]
    cmd: SubCommand,
}

#[derive(Subcommand, Debug)]
enum SubCommand {
    /// Ingest data for all variables over the date range, using the end of the date
    /// range as the end date of the dataset
    Update {
        /// Earliest timestamp to ingest
        ingest_min_date: DateTime<Utc>,

        /// Most recent timestamp to ingest (inclusive)
        ingest_max_date: DateTime<Utc>,

        /// URL string specifying the root of the zarr to create. eg. s3://bucket/path.zarr file:///home/.../path.zarr
        destination: String,
    },
    /// Backfill data for a specific variable over a date range
    Backfill {
        /// Data variable name to ingest
        variable: String,

        /// Earliest timestamp to ingest
        ingest_min_date: DateTime<Utc>,

        /// Most recent timestamp to ingest (inclusive)
        ingest_max_date: DateTime<Utc>,

        /// Most recent timestamp of the entire dataset
        dataset_max_date: DateTime<Utc>,

        /// URL string specifying the root of the zarr to create. eg. s3://bucket/path.zarr file:///home/.../path.zarr
        destination: String,

        /// Don't write metadata, just write chunks
        skip_metadata: Option<bool>,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::try_parse()?;

    match cli.cmd {
        SubCommand::Backfill {
            variable,
            ingest_min_date,
            ingest_max_date,
            dataset_max_date,
            destination,
            skip_metadata,
        } => {
            reformatters::gfs::reformat(
                variable,
                ingest_min_date,
                ingest_max_date,
                dataset_max_date,
                destination,
                skip_metadata.unwrap_or(false),
            )
            .await
        }
        SubCommand::Update {
            ingest_min_date,
            ingest_max_date,
            destination,
        } => {
            for variable_name in [
                "temperature_2m",
                "precipitation_surface",
                "wind_u_10m",
                "wind_v_10m",
            ] {
                reformatters::gfs::reformat(
                    variable_name.to_string(),
                    ingest_min_date,
                    ingest_max_date,
                    ingest_max_date,
                    destination.clone(),
                    false,
                )
                .await?;
            }
            Ok(())
        }
    }
}
