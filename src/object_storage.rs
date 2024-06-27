use anyhow::Result;
use std::sync::Arc;

pub type ObjectStore = Arc<dyn object_store::ObjectStore>;
pub type PutResult = object_store::PutResult;

pub fn output_store() -> Result<ObjectStore> {
    let mut store_builder = object_store::aws::AmazonS3Builder::from_env()
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
