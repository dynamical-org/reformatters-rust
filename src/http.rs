use anyhow::Result;
use reqwest_middleware::{ClientBuilder, ClientWithMiddleware};
use reqwest_retry::{policies::ExponentialBackoff, RetryTransientMiddleware};
use std::time::Duration;

pub type Client = ClientWithMiddleware;

pub fn client() -> Result<Client> {
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
