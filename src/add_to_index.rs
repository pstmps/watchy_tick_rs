// use serde::Serialize;
use elasticsearch::DeleteByQueryParts;
use serde_json::json;
use serde_json::Value;
use std::collections::HashSet;
use tokio::sync::broadcast;
// use tokio::sync::mpsc;
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
    // let mut file_paths = HashSet::new();
    // let mut records = HashSet::new();

    let mut body: Vec<Value> = Vec::new();

    log::info!("add_to_index Add records to index: {}", index);
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

            // Timeout after 5 seconds
            _ = sleep(Duration::from_secs(timeout)) => {
                log::info!("Timeout reached");
                // if !file_paths.is_empty() || !records.is_empty() {

                //     log::info!("Deleting records after timeout reached: {:?}", file_paths);

                //     flush_records(&mut file_paths, &mut records, &es_host, index).await?;
                // }
            }
        }

        // if file_paths.len() > buffer_size {
        //     log::debug!(
        //         "Adding records after buffer size reached: {:?}",
        //         file_paths
        //     );
        //     flush_records(&mut file_paths, &mut records, &es_host, index).await?;
        // }
    }
    Ok(())
}

// async fn flush_records(
//     file_paths: &mut HashSet<String>,
//     records: &mut HashSet<(String, String)>,
//     es_host: &Host,
//     index: &str,
// ) -> Result<(), Box<dyn std::error::Error>> {
//     let query = generate_query(&*file_paths, &*records)?;
//     log_debug_pretty("Query", &query);
//     let response = delete_records(es_host.clone(), index, query).await?;
//     log_debug_pretty("Response", &response);
//     // clear the file paths and records
//     file_paths.clear();
//     records.clear();
//     Ok(())
// }

// fn log_debug_pretty<T: serde::Serialize>(label: &str, value: &T) {
//     if log::log_enabled!(log::Level::Debug) {
//         if let Ok(value_string) = serde_json::to_string_pretty(value) {
//             log::debug!("{}: {}", label, value_string);
//         } else {
//             log::error!("Failed to serialize {} to a pretty string", label);
//         }
//     }
// }

// fn generate_query(
//     file_paths: &HashSet<String>,
//     records: &HashSet<(String, String)>,
// ) -> Result<Value, Box<dyn std::error::Error>> {
//     let mut file_paths_query = vec![];
//     let mut records_query = vec![];

//     for file_path in file_paths {
//         file_paths_query.push(json!({
//             "term": {
//                 "file.uri": file_path
//             }
//         }));

//         file_paths_query.push(json!({
//             "wildcard": {
//                 "file.uri": {
//                     "value": format!("{}/*", file_path)
//                 }
//             }
//         }));
//     }

//     for (record_id, record_index) in records {
//         records_query.push(json!({
//             "bool": {
//                 "must": [
//                     {
//                         "term": {
//                             "_id": record_id
//                         }
//                     },
//                     {
//                         "term": {
//                             "_index": record_index
//                         }
//                     }
//                 ]
//             }
//         }));
//     }

//     let query = json!({
//         "query": {
//             "bool": {
//                 "should": file_paths_query,
//                 "must_not": records_query
//             }
//         }
//     });

//     Ok(query)
// }

// async fn delete_records(
//     es_host: Host,
//     index: &str,
//     query: Value,
// ) -> Result<Value, Box<dyn std::error::Error>> {
//     let client = create_client(es_host.clone())?;

//     let response = client
//         .delete_by_query(DeleteByQueryParts::Index(&[index]))
//         .body(query)
//         .send()
//         .await?;

//     let json_response = response.json::<Value>().await?;

//     Ok(json_response)
// }
