use dotenv::dotenv;
use std::env;
use color_eyre::Report;

pub mod aggs;
pub mod app;
pub mod elastic;
pub mod init_logging;
pub mod message;
pub mod filter;
pub mod add_to_index;

use crate::app::{App, AppTimeouts, AppBuffers};
use crate::init_logging::initialize_logging;

async fn tokio_main() -> Result<(), Report> {
    dotenv().ok();

    let log_path = env::var("TICK_LOG_PATH").unwrap_or_else(|_| "log".to_string());

    let log_to_console = env::var("TICK_LOG_CONSOLE")
        .unwrap_or_else(|_| "true".to_string())
        .parse::<bool>()?;

    initialize_logging(&log_path, log_to_console)?;

    let index =
        env::var("TICK_INDEX").unwrap_or_else(|_| ".ds-logs-fim.event-default*".to_string());

    let new_index = env::var("TICK_NEW_INDEX")
        .unwrap_or_else(|_| "fs_state_temp_debug".to_string());

    log::info!("[+] Writing to index: {}", &new_index);

    let action_buffer_size = env::var("TICK_ACTION_BUFFER_SIZE")
        .unwrap_or_else(|_| "1024".to_string())
        .parse::<usize>()?;

    let page_size = env::var("TICK_PAGE_SIZE")
        .unwrap_or_else(|_| "24".to_string())
        .parse::<usize>()?;

    let buffer_size = env::var("TICK_INDEXING_BUFFER")
        .unwrap_or_else(|_| "100".to_string())
        .parse::<usize>()?;

    let _buffers = AppBuffers {
        action_buffer_size,
        buffer_size,
        page_size,
    };

    let index_timeout = env::var("TICK_INDEXING_TIMEOUT")
        .unwrap_or_else(|_| "5".to_string())
        .parse::<u64>()?;

    let agg_sleep = env::var("TICK_AGGREGATION_SLEEP")
        .unwrap_or_else(|_| "20".to_string())
        .parse::<u64>()?;

    let run_as_daemon = env::var("TICK_DAEMON")
        .unwrap_or_else(|_| "false".to_string())
        .parse::<bool>()?;

    let _timeouts = AppTimeouts {
        index_timeout,
        agg_sleep,
        run_as_daemon,
    };

    let es_ip = env::var("ES_IP").ok();
    let es_port = env::var("ES_PORT").ok();

    let cert_path = env::var("CERT_PATH").ok();

    let es_user = env::var("ES_USER").ok();
    let es_password = env::var("ES_PASSWORD").ok();

    let config = elastic::HostConfig {
        user: es_user.clone(),
        password: es_password.clone(),
        host_ip: es_ip.clone(),
        host_port: es_port.map(|p| p.parse::<u16>().unwrap()),
        host_scheme: Some("https".to_string()),
        cert_path,
    };

    let es_host = elastic::Host::new(config);

    // TODO initialize_panic_handler()?;

    let mut app = App::new(
        es_host,
        &index,
        &new_index,
        _buffers,
        _timeouts,
    )?;
    app.run().await?;

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Report> {
    if let Err(e) = tokio_main().await {
        eprintln!("{} error: Something went wrong", env!("CARGO_PKG_NAME"));
        Err(e)
    } else {
        Ok(())
    }
}
