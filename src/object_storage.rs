use anyhow::Result;
use std::sync::Arc;

pub type ObjectStore = Arc<dyn object_store::ObjectStore>;
pub type PutResult = object_store::PutResult;

pub fn output_store() -> Result<ObjectStore> {
    let mut store_builder = object_store::aws::AmazonS3Builder::from_env()
        .with_bucket_name(std::env::var("OUTPUT_STORE_BUCKET")?)
        .with_access_key_id(first_env_var(
            "OUTPUT_STORE_ACCESS_KEY_ID",
            "AWS_ACCESS_KEY_ID",
        )?)
        .with_secret_access_key(first_env_var(
            "OUTPUT_STORE_SECRET_ACCESS_KEY",
            "AWS_SECRET_ACCESS_KEY",
        )?);

    if let Ok(session_token) = first_env_var("OUTPUT_STORE_SESSION_TOKEN", "AWS_SESSION_TOKEN") {
        store_builder = store_builder.with_token(session_token);
    }
    if let Ok(endpoint) = std::env::var("OUTPUT_STORE_ENDPOINT") {
        store_builder = store_builder.with_endpoint(endpoint);
    }
    if let Ok(region) = first_env_var("OUTPUT_STORE_REGION", "AWS_DEFAULT_REGION") {
        store_builder = store_builder.with_region(region);
    }

    let store = store_builder.build()?;
    Ok(Arc::new(store))
}

fn first_env_var(var1: &str, var2: &str) -> Result<String> {
    Ok(std::env::var(var1).or_else(|_| std::env::var(var2))?)
}
