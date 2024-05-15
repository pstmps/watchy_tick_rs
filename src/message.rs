use serde_json::Value;

#[derive(Debug)]
pub enum Message {
    Aggregate { event_type: String, payload: Value },
    // Filter { event_type: String, payload: Value },
    Add { event_type: String, payload: Value },
    // LastRecord { event_type: String, payload: Value },
    // Delete { event_type: String, payload: Value },
}
