use elasticsearch::SearchParts;
use serde_json::{json, Value};
use tokio::sync::mpsc;
use tokio::time::{sleep, Duration};

use crate::elastic::create_client;
use crate::elastic::Host;
use crate::message::Message;

// TODO use json! macro to create the query

pub async fn get_aggs_entries_from_index(
    es_host: Host,
    index: &str,
    page_size: usize,
    agg_sleep: u64,
    tx: mpsc::Sender<Message>,
) -> Result<(), color_eyre::Report> {

    log::info!("################################# Aggs task starting");
    // loop {
        let client = create_client(es_host.clone())?;

        let mut after = String::new();

        let mut hits = 1;

        let timestamp = chrono::Local::now();
        let timestamp_millis = timestamp.timestamp_millis() as f64;

        while hits > 0 {
            hits = 0;

            let json_query = generate_query(page_size, &after)?;

            let value: serde_json::Value = serde_json::from_str(&json_query)?;

            let response = client
                .search(SearchParts::Index(&[index]))
                .body(value)
                .send()
                .await?;

            log::debug!("Response from ES: {:?}", response);

            let response_body = match response.json::<Value>().await {
                Ok(body) => body,
                Err(_) => continue,
            };

            log::debug!("Response body: {:?}", response_body);

            let aggs =
                match response_body["aggregations"]["unique_event_types"]["buckets"].as_array() {
                    Some(aggs) => aggs,
                    None => continue,
                };

            for agg in aggs {
                // let doc_count = agg["doc_count"].as_u64().unwrap();
                let doc_count = match agg["doc_count"].as_u64() {
                    Some(value) => value,
                    None => {
                        log::warn!("doc_count is not a u64 or does not exist");
                        0
                    }
                };

                if doc_count > 1 {
                    let mut agg_clone = agg.clone();

                    agg_clone["timestamp"] = json!({
                        "value": timestamp_millis,
                        "value_as_string": timestamp.to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
                    });

                    let _tx = tx.clone();
                    tokio::spawn(async move {
                        let message = Message::Aggregate {
                            event_type: "Aggregate".to_string(),
                            payload: agg_clone,
                        };

                        log::debug!("Sending message: {:?}", &message);

                        // _tx.send(message).await.unwrap();
                        if let Err(e) = _tx.send(message).await {
                            log::error!("Failed to send message: {}", e);
                        }
                    });
                } // if doc_count > 1
                hits += 1;
            }

            if hits == 0 {
                break;
            }

            after = response_body["aggregations"]["unique_event_types"]["after_key"]["file.parent_path"]
                .clone()
                .to_string();

            // after = match response_body["aggregations"]["unique_event_types"]["after_key"]["file.parent_path"].as_str() {
            //         Some(value) => value,
            //         None => {
            //             log::warn!("after_key is not a string or does not exist");
            //             continue,
            //         },
            //     };
        }

        log::info!("Aggs task sleeping for {} seconds", agg_sleep);
        //sleep for $agg_sleep seconds
        sleep(Duration::from_secs(agg_sleep)).await;
        Ok(())
    // }
}

fn generate_query(page_size: usize, after: &str) -> Result<String, color_eyre::Report> {
    let sources = json!([
        {
          "file.parent_path": {
            "terms": {
              "field": "file.parent_path"
            }
          }
        }
      ]);

    let aggs = json!( {
        "max_timestamp": {
          "max": {
            "field": "@timestamp"
          }
        },
        "file_sizes": {
          "sum": {
            "field": "file.size"
          }
        }
      });

    let mut composite = json!({
        "size": page_size,
        "sources": sources
    });

    // if !after.is_empty() {
    //     let after_value: Value = serde_json::from_str(&after).unwrap();
    //     composite["after"] = after_value;
    // }
    if !after.is_empty() {
        if let Ok(after_value) = serde_json::from_str::<Value>(after) {
            composite["after"] = json!({ "file.parent_path": after_value} );
        } else {
            log::error!("Failed to parse after as JSON");
        }
    }

    let query = json!({
        "size": 0,
        "aggs": {
            "unique_event_types": {
                "composite": composite,
                "aggs": aggs
            }
        }
    })
    .to_string();

    log::debug!("Query: {}", query);

    Ok(query)
}
