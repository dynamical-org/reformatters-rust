pub mod gfs;
pub mod http;
pub mod object_storage;
use std::{collections::HashMap, error::Error, fs, io::Write};

use anyhow::Result;
use backon::{ExponentialBuilder, Retryable};
use chrono::{DateTime, TimeDelta, Utc};
use object_storage::{ObjectStore, PutPayload, PutResult};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct DataVariable {
    pub name: &'static str,
    pub long_name: &'static str,
    pub units: &'static str,
    pub grib_variable_name: &'static str,
    pub dtype: &'static str,
    pub value_min: f32,
    pub value_max: f32,
    pub value_mean: f32,
}

#[derive(Debug, Clone)]
pub struct DataDimension {
    pub name: &'static str,
    pub long_name: &'static str,
    pub units: &'static str,
    pub dtype: &'static str,
    pub extra_metadata: HashMap<&'static str, &'static str>,
}

#[derive(Debug, Clone)]
pub struct AnalysisDataset {
    pub id: &'static str,
    pub name: &'static str,
    pub description: &'static str,
    pub url: &'static str,
    pub spatial_coverage: &'static str,
    pub spatial_resolution: &'static str,

    pub time_start: DateTime<Utc>,
    pub time_end: Option<DateTime<Utc>>,
    pub time_step: TimeDelta,
    pub time_chunk_size: usize,

    pub longitude_start: f64,
    pub longitude_end: f64,
    pub longitude_step: f64,
    pub longitude_chunk_size: usize,

    pub latitude_start: f64,
    pub latitude_end: f64,
    pub latitude_step: f64,
    pub latitude_chunk_size: usize,

    pub data_dimensions: Vec<DataDimension>,
    pub data_variables: Vec<DataVariable>,
}

#[derive(Debug, Clone)]
pub struct AnalysisRunConfig {
    pub dataset: AnalysisDataset,
    pub data_variable: DataVariable,
    pub time_coordinates: Arc<Vec<DateTime<Utc>>>, // Arc because this struct is cloned often and this vec can be big
}

const WRITE_METADATA_TO_OBJECT_STORAGE: bool = true;

#[allow(
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::cast_precision_loss
)]
fn num_chunks(size: usize, chunk_size: usize) -> usize {
    ((size as f64) / (chunk_size as f64)).ceil() as usize
}

async fn write_metadata(
    file_path: &str,
    json_value: &Value,
    object_store: ObjectStore,
    dest_root_path: &str,
) -> Result<()> {
    let json_string = serde_json::to_string_pretty(json_value)?;
    if WRITE_METADATA_TO_OBJECT_STORAGE {
        let payload: PutPayload = json_string.into();
        (|| async {
            do_upload(
                object_store.clone(),
                format!("{dest_root_path}/{file_path}"),
                payload.clone(),
            )
            .await
        })
        .retry(&ExponentialBuilder::default())
        .await?;
    } else {
        let bytes = json_string.as_bytes();
        let mut file = fs::File::create(file_path)?;
        file.write_all(bytes)?;
    }

    Ok(())
}

fn to_value<T: Serialize>(value: &T) -> serde_json::Value {
    serde_json::to_value(value).unwrap()
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct ZarrCompressorMetadata {
    blocksize: usize,
    clevel: usize,
    cname: &'static str,
    id: &'static str,
    shuffle: usize,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct ZarrFilterMetadata {}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct ZarrArrayMetadata {
    chunks: Vec<usize>,
    compressor: ZarrCompressorMetadata,
    dtype: &'static str,
    fill_value: Option<&'static str>,
    filters: Option<Vec<ZarrFilterMetadata>>,
    order: &'static str,
    shape: Vec<usize>,
    zarr_format: usize,
}

#[allow(non_snake_case)]
#[derive(Serialize, Deserialize, Debug, Clone)]
struct ZarrAttributeMetadata {
    _ARRAY_DIMENSIONS: Vec<&'static str>,
    long_name: &'static str,
    units: &'static str,
}

async fn write_array_metadata(
    field_name: &str,
    mut zmetadata: HashMap<String, serde_json::Value>,
    zarr_array_json: &serde_json::Value,
    zarr_attribute_json: &serde_json::Value,
    store: ObjectStore,
    dest_root_path: &str,
) -> Result<HashMap<String, serde_json::Value>> {
    zmetadata.insert(format!("{field_name}/.zarray"), to_value(&zarr_array_json));
    zmetadata.insert(
        format!("{field_name}/.zattrs"),
        to_value(&zarr_attribute_json),
    );

    if !WRITE_METADATA_TO_OBJECT_STORAGE {
        fs::create_dir(field_name).expect("Error writing zarr metadata");
    }
    write_metadata(
        &format!("{field_name}/.zarray"),
        zarr_array_json,
        store.clone(),
        dest_root_path,
    )
    .await?;
    write_metadata(
        &format!("{field_name}/.zattrs"),
        zarr_attribute_json,
        store.clone(),
        dest_root_path,
    )
    .await?;

    Ok(zmetadata.clone())
}

impl AnalysisRunConfig {
    #[allow(clippy::too_many_lines)]
    #[allow(clippy::cast_possible_truncation)]
    #[allow(clippy::cast_sign_loss)]
    pub async fn write_zarr_metadata(
        &self,
        store: ObjectStore,
        dest_root_path: &str,
    ) -> Result<()> {
        let data_variable_compressor_metadata = ZarrCompressorMetadata {
            blocksize: 0,
            clevel: 5,
            cname: "zstd",
            id: "blosc",
            shuffle: 1,
        };
        let order = "C";
        let fill_value = "NaN";
        let zarr_format = 2;

        let zgroup = json!({"zarr_format": 2});

        let mut zmetadata: HashMap<String, serde_json::Value> = HashMap::new();

        let time_shape_size = self.time_coordinates.len();
        let latitude_shape_size = ((self.dataset.latitude_start - self.dataset.latitude_end)
            / self.dataset.latitude_step)
            .round() as usize
            + 1;
        let longitude_shape_size = ((self.dataset.longitude_end - self.dataset.longitude_start)
            / self.dataset.longitude_step)
            .round() as usize;

        // Data variable metadata
        for data_variable in self.dataset.data_variables.clone() {
            let zarr_array_metadata = ZarrArrayMetadata {
                chunks: [
                    self.dataset.time_chunk_size,
                    self.dataset.latitude_chunk_size,
                    self.dataset.longitude_chunk_size,
                ]
                .to_vec(),
                compressor: data_variable_compressor_metadata.clone(),
                dtype: data_variable.dtype,
                fill_value: Some(fill_value),
                filters: None,
                order,
                shape: [time_shape_size, latitude_shape_size, longitude_shape_size].to_vec(),
                zarr_format,
            };

            let zarr_attribute_metadata = ZarrAttributeMetadata {
                _ARRAY_DIMENSIONS: ["time", "latitude", "longitude"].to_vec(),
                long_name: data_variable.long_name,
                units: data_variable.units,
            };

            zmetadata = write_array_metadata(
                data_variable.name,
                zmetadata.clone(),
                &to_value(&zarr_array_metadata),
                &to_value(&zarr_attribute_metadata),
                store.clone(),
                dest_root_path,
            )
            .await?;
        }

        let data_dimension_compressor_metadata = ZarrCompressorMetadata {
            blocksize: 0,
            clevel: 5,
            cname: "lz4",
            id: "blosc",
            shuffle: 1,
        };

        let end_date_string = match self.dataset.time_end {
            Some(date) => date.to_string(),
            None => "Present".to_string(),
        };

        for data_dimension in self.dataset.data_dimensions.clone() {
            let shape_size = match data_dimension.name {
                "time" => time_shape_size,
                "latitude" => latitude_shape_size,
                "longitude" => longitude_shape_size,
                &_ => todo!(),
            };

            let fill_value = match data_dimension.name {
                "time" => None,
                &_ => Some("NaN"),
            };

            let zarr_array_metadata = ZarrArrayMetadata {
                chunks: [shape_size].to_vec(),
                compressor: data_dimension_compressor_metadata.clone(),
                dtype: data_dimension.dtype,
                fill_value,
                filters: None,
                order,
                shape: [shape_size].to_vec(),
                zarr_format,
            };

            let zarr_attribute_metadata = ZarrAttributeMetadata {
                _ARRAY_DIMENSIONS: [data_dimension.name].to_vec(),
                long_name: data_dimension.long_name,
                units: data_dimension.units,
            };

            let mut zarr_attribute_metadata_value = to_value(&zarr_attribute_metadata);
            let zarr_attribute_metadata_with_extra_fields =
                zarr_attribute_metadata_value.as_object_mut().unwrap();
            for (key, value) in data_dimension.extra_metadata {
                zarr_attribute_metadata_with_extra_fields.insert(key.to_string(), json!(value));
            }

            let start = match data_dimension.name {
                "longitude" => self.dataset.longitude_start.to_string(),
                "latitude" => self.dataset.latitude_start.to_string(),
                "time" => self.dataset.time_start.to_string(),
                &_ => todo!(),
            };
            let end = match data_dimension.name {
                "longitude" => self.dataset.longitude_end.to_string(),
                "latitude" => self.dataset.latitude_end.to_string(),
                "time" => end_date_string.clone(),
                &_ => todo!(),
            };

            zarr_attribute_metadata_with_extra_fields.extend(HashMap::from([
                ("start".to_string(), to_value(&start)),
                ("end".to_string(), to_value(&end)),
            ]));

            zmetadata = write_array_metadata(
                data_dimension.name,
                zmetadata.clone(),
                &to_value(&zarr_array_metadata),
                &to_value(&zarr_attribute_metadata_with_extra_fields),
                store.clone(),
                dest_root_path,
            )
            .await?;
        }

        let dataset_metadata = HashMap::from([
            ("id".to_string(), to_value(&self.dataset.id)),
            ("name".to_string(), to_value(&self.dataset.name)),
            (
                "description".to_string(),
                to_value(&self.dataset.description),
            ),
            (
                "time_domain".to_string(),
                to_value(&format!(
                    "{} to {}",
                    self.dataset.time_start, end_date_string
                )),
            ),
            (
                "time_resolution".to_string(),
                to_value(&format!("{} hour", self.dataset.time_step.num_hours())),
            ),
            (
                "spatial_domain".to_string(),
                to_value(&self.dataset.spatial_coverage),
            ),
            (
                "spatial_resolution".to_string(),
                to_value(&self.dataset.spatial_resolution),
            ),
        ]);

        zmetadata.insert(".zattrs".to_string(), json!(dataset_metadata));
        zmetadata.insert(".zgroup".to_string(), zgroup.clone());

        write_metadata(
            ".zmetadata",
            &json!({"metadata": to_value(&zmetadata), "zarr_consolidated_format": 1}),
            store.clone(),
            dest_root_path,
        )
        .await?;
        write_metadata(".zattrs", &json!({}), store.clone(), dest_root_path).await?;
        write_metadata(".zgroup", &zgroup, store.clone(), dest_root_path).await?;

        Ok(())
    }
}

pub async fn do_upload(
    store: ObjectStore,
    chunk_path: String,
    bytes: PutPayload,
) -> Result<PutResult> {
    let mut put_result_result = store.put(&chunk_path.into(), bytes).await;

    // A little nonsense to ignore deeply nested error if the ETag isn't
    // present on the response. The put still succeeds in this case.
    if let Err(e) = &put_result_result {
        if let Some(source) = e.source() {
            if let Some(source) = source.source() {
                if source.to_string().starts_with("ETag Header missing") {
                    put_result_result = Ok(PutResult {
                        e_tag: None,
                        version: None,
                    });
                }
            }
        }
    }

    Ok(put_result_result?)
}
