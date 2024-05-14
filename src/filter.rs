use serde_json::Value;
use tokio::sync::mpsc;

use crate::message::Message;

pub async fn filter_record(
    payload: Value,
    tx: mpsc::Sender<Message>,
) -> Result<(), color_eyre::Report> {

    log::debug!("Filtering record: {:?}", payload);

    let file_sizes = payload
        .get("file_sizes")
        .and_then(|v| v.get("value"))
        .and_then(|v| v.as_f64().map(|f| f as u64))
        .unwrap_or(0);

    if file_sizes > 0 {
        log::debug!("File size is greater than 0, sending to aggregator");
        let message = Message::Add { event_type: "Add".to_string() , payload };
        tx.send(message).await?;
    } else {
        log::debug!("File size is 0, skipping record");
    }

    Ok(())
}