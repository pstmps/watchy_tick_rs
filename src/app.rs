use color_eyre::Report;
use serde_json::Value;

use tokio::sync::broadcast;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::sleep;

use crate::aggs::get_aggs_entries_from_index;
use crate::elastic::Host;
use crate::message::Message;
use crate::filter::filter_record;
use crate::add_to_index::add_to_index;

pub struct App {
    pub es_host: Host,
    pub index: String,
    pub new_index: String,
    pub buffers: AppBuffers,
    pub timeouts: AppTimeouts,
}

pub struct AppTimeouts {
    pub index_timeout: u64,
    pub agg_sleep: u64,
    pub run_as_daemon: bool,
}

pub struct AppBuffers {
    pub action_buffer_size: usize,
    pub buffer_size: usize,
    pub page_size: usize,
}

impl App {
    pub fn new(
        es_host: Host,
        index: &str,
        new_index: &str,
        buffers: AppBuffers,
        timeouts: AppTimeouts,
    ) -> Result<Self, Report> {
        Ok(Self {
            es_host,
            index: index.to_string(),
            new_index: new_index.to_string(),
            buffers,
            timeouts,
        })
    }

    pub async fn run(&mut self) -> Result<(), Report> {

        let page_size = self.buffers.page_size;
        let buffer_size = self.buffers.buffer_size;
        let action_buffer_size = self.buffers.action_buffer_size;

        let index_timeout = self.timeouts.index_timeout;
        let agg_sleep = self.timeouts.agg_sleep;
        let run_as_daemon = self.timeouts.run_as_daemon;

        let global_timeout = (index_timeout + agg_sleep) * 2;

        let (event_tx, mut event_rx) = mpsc::channel(action_buffer_size);
        let (index_tx, _index_rx_) = broadcast::channel(action_buffer_size);

        let mut handles = Vec::new();

        let mut agg_handle: Option<tokio::task::JoinHandle<()>> = None;

        let mut index_handle: Option<tokio::task::JoinHandle<()>> = None;

        loop {
            log::debug!("Starting get aggs entries from index task: {}", self.index.as_str());

            // check if aggregation task is runnning, if not, restart it
            if let Some(handle) = &agg_handle {
                if handle.is_finished() {
                    log::debug!("Aggregation task is finished, restarting task");
                    // The task has completed, you can start a new one
                    agg_handle = None;
                }
            }

            if agg_handle.is_none() {
                let _event_tx = event_tx.clone();
                let _index = self.index.clone();
                let _es_host = self.es_host.clone();

                agg_handle = Some(tokio::spawn(async move {
                    loop {
                        match get_aggs_entries_from_index(
                            _es_host.clone(),
                            _index.as_str(),
                            page_size,
                            agg_sleep,
                            _event_tx.clone(),
                        )
                        .await
                        {
                            Ok(_) => {
                                if run_as_daemon{
                                    // if run_as_daemon is true, restart the task after a delay
                                    log::info!("get_aggs_entries_from_index completed, restarting task");
                                    tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                                } else {
                                    // If the function succeeds, break the loop
                                    break;
                                }
                            },
                            Err(e) => {
                                log::error!(
                                    "Failed to start get aggs entries from index task: {}",
                                    e
                                );
                                tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                            }
                        };
                    }
                }));
            }

            //check if index task is running, if not, restart it

            if let Some(handle) = &index_handle {
                if handle.is_finished() {
                    log::debug!("Index task has crashed, restarting ...");
                    // The task has completed, you can start a new one
                    index_handle = None;
                }
            }

            if index_handle.is_none() {
                let mut _index_rx = index_tx.subscribe();
                let _new_index = self.new_index.clone();
                let _es_host = self.es_host.clone();

                index_handle = Some(tokio::spawn(async move {
                    if let Err(e) = add_to_index(
                        _es_host.clone(),
                        _new_index.as_str(),
                        buffer_size,
                        index_timeout,
                        _index_rx,
                    )
                    .await
                    {
                        log::error!("Failed to start delete records from index task: {}", e)
                    }
                }));
            }

            // wait for events in the event channel, if none are received in the last global_timeout seconds, quit or restart the loop
            tokio::select! {

                Some(event) = event_rx.recv() => {
                    let _es_host = self.es_host.clone();
                    let _index = self.index.clone();
                    if let Err(e) = self
                        .process_events(
                            event,
                            &event_tx,
                            &index_tx,
                            &mut handles,
                            _index.as_str(),
                        )
                        .await
                    {
                        log::error!("Failed to process events: {}", e);
                    };
                }

                _ = sleep(tokio::time::Duration::from_secs(global_timeout)) => {
                    if run_as_daemon {
                        log::info!("No events received in the last {} seconds, restarting ... ", global_timeout);
                    } else {
                        log::info!("No events received in the last {} seconds, quitting ... ", global_timeout);
                        std::process::exit(0);
                    }

                }
            }
            // if self.should_quit {
            //     return Ok(());
            // }
            // if self.should_suspend {
            //     return Ok(());
            // }
        }
       
    }

    async fn process_events(
        &mut self,
        event: Message,
        event_tx: &mpsc::Sender<Message>,
        index_tx: &broadcast::Sender<Value>,
        handles: &mut Vec<JoinHandle<()>>,
        index: &str,
    ) -> Result<(), Report> {
        let _event_tx = event_tx.clone();
        match event {
            Message::Aggregate {
                event_type: _event_type,
                payload,
            } => {
                log::debug!(
                    "Aggregate event received: {} with payload: {}",
                    _event_type,
                    payload
                );
                let _payload = payload.clone();
                let _index = index.to_string();
                let lastevent_handle = tokio::spawn(async move {
                    let _ = filter_record(_payload, _event_tx).await;
                });
                handles.push(lastevent_handle);
            }
            Message::Add { event_type, payload } => {
                log::debug!(
                    "Add event received: {} with payload: {}",
                    event_type,
                    payload
                );
                let _index_tx = index_tx.clone();
                let index_handle = tokio::spawn(async move {
                    let _ = _index_tx.send(payload);
                });
                handles.push(index_handle);


            }
            // catchall for debugging
            // _ => {
            //     log::debug!(
            //         "Other event",
  
            //     );

            // }
        }
        Ok(())
    }
}
