use std::sync::Arc;

use anyhow::{anyhow, bail, Result};
use object_store::{aws::AmazonS3Builder, local::LocalFileSystem};
use url::Url;

pub type ObjectStore = Arc<dyn object_store::ObjectStore>;
pub type PutPayload = object_store::PutPayload;
pub type PutResult = object_store::PutResult;

pub fn get_object_store(dest: String) -> Result<ObjectStore> {
    let url = Url::parse(&dest)?;
    println!("URL: {:?}", url);
    match url.scheme() {
        "file" => {
            if url.host().is_some() {
                bail!("Unsupported file url. Expected no host: {}", url.as_str());
            }
            Ok(Arc::new(LocalFileSystem::new()))
        }
        "s3" => {
            let bucket_name = url.host().ok_or(anyhow!("Invalid bucket_name"))?;
            let store = AmazonS3Builder::from_env()
                .with_bucket_name(bucket_name.to_string())
                .build()?;
            Ok(Arc::new(store))
        }
        "gcs" => {
            bail!("GCS not implemented")
        }
        "azure" => {
            bail!("Azure not implemented")
        }
        _ => bail!("Unsupported url. Try file:/foo, s3://bucket"),
    }
}

// fn first_env_var(var1: &str, var2: &str) -> Result<String> {
//     Ok(std::env::var(var1).or_else(|_| std::env::var(var2))?)
// }
