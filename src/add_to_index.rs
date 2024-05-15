use elasticsearch::BulkParts;
use serde_json::json;
use serde_json::Value;
use tokio::sync::broadcast;
use tokio::time::{sleep, Duration};

use crate::elastic::create_client;
use crate::elastic::Host;

pub async fn add_to_index(
    es_host: Host,
    index: &str,
    buffer_size: usize,
    timeout: u64,
    mut index_rx: broadcast::Receiver<Value>,
) -> Result<(), Box<dyn std::error::Error>> {

    let mut body: Vec<Value> = Vec::new();

    log::info!("add_to_index Adding records to index: {}", index);
    loop {
        tokio::select! {
            // Wait for a new record or timeout

            result = index_rx.recv() => {
                match result {
                    Ok(record) => {
                        log::debug!("Received record: {:?}", record);

                        body.push(json!({ "index": { "_index": &index} }));
                        body.push(serde_json::to_value(&record)?);

                        log::info!{"body: {:?}", body};

                    },
                    Err(e) => {
                        log::error!("Error receiving record: {}", e);
                        // Handle the error
                    }
                }
            }

            // Timeout after $timeout seconds
            _ = sleep(Duration::from_secs(timeout)) => {
                log::info!("Timeout reached");

                if !body.is_empty() {
                    log::info!("Adding records after timeout reached: {:?}", body);

                    add_records(es_host.clone(), index, body.clone()).await?;
                    body.clear();
                }
            }
        }

        if body.len() > buffer_size {
            log::debug!("Adding records after buffer size reached: {:?}", body);
            add_records(es_host.clone(), index, body.clone()).await?;
            body.clear();
        }
    }
}

async fn add_records(
    es_host: Host,
    index: &str,
    body: Vec<Value>,
) -> Result<Value, Box<dyn std::error::Error>> {
    let client = create_client(es_host.clone())?;

    let body_str: Vec<String> = body.into_iter().map(|v| v.to_string()).collect();

    let response = client
        .bulk(BulkParts::Index(index))
        .body(body_str)
        .send()
        .await?;

    let json_response = response.json::<Value>().await?;

    log::debug!("Response from ES: {:?}", json_response);

    Ok(json_response)
}