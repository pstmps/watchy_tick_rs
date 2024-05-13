use dotenv::dotenv;
use std::env;

pub mod aggs;
pub mod app;
// pub mod delete_records;
pub mod elastic;
pub mod init_logging;
// pub mod latest;
pub mod message;
// pub mod parse_record;
pub mod filter;
pub mod add_to_index;

use crate::app::App;
use crate::init_logging::initialize_logging;

async fn tokio_main() -> Result<(), Box<dyn std::error::Error>> {
    dotenv().ok();

    let log_path = env::var("CONDENSE_LOG_PATH").unwrap_or_else(|_| "log".to_string());

    let log_to_console = env::var("CONDENSE_LOG_CONSOLE")
        .unwrap_or_else(|_| "true".to_string())
        .parse::<bool>()?;

    initialize_logging(&log_path, log_to_console)?;

    let index =
        env::var("CONDENSE_INDEX").unwrap_or_else(|_| ".ds-logs-fim.event-default*".to_string());

    let new_index = env::var("CONDENSE_NEW_INDEX")
        .unwrap_or_else(|_| "fs_state_temp_debug".to_string());

    log::info!("Index: {}", &new_index);

    let action_buffer_size = env::var("CONDENSE_ACTION_BUFFER_SIZE")
        .unwrap_or_else(|_| "1024".to_string())
        .parse::<usize>()?;

    let page_size = env::var("CONDENSE_PAGE_SIZE")
        .unwrap_or_else(|_| "24".to_string())
        .parse::<usize>()?;

    let buffer_size = env::var("CONDENSE_DELETE_BUFFER")
        .unwrap_or_else(|_| "100".to_string())
        .parse::<usize>()?;

    let del_timeout = env::var("CONDENSE_DELETE_TIMEOUT")
        .unwrap_or_else(|_| "5".to_string())
        .parse::<u64>()?;

    let agg_sleep = env::var("CONDENSE_AGGREGATION_SLEEP")
        .unwrap_or_else(|_| "20".to_string())
        .parse::<u64>()?;

    let run_as_daemon = env::var("CONDENSE_DAEMON")
        .unwrap_or_else(|_| "false".to_string())
        .parse::<bool>()?;

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
        // verify_certs: Some(false),
        // ca_certs: None,
        // ssl_show_warn: Some(true),
    };

    let es_host = elastic::Host::new(config);

    // let client = elastic::create_client(es_host)?;

    // let response = client
    // .cat()
    // .health()
    // .format("json")
    // .send()
    // .await
    // .expect("Failed to send health check request");

    // println!("{:?}", response); 



    // TODO initialize_panic_handler()?;

    let mut app = App::new(
        es_host,
        action_buffer_size,
        &index,
        &new_index,
        page_size,
        buffer_size,
        del_timeout,
        agg_sleep,
        run_as_daemon,
    )?;
    app.run().await?;

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    if let Err(e) = tokio_main().await {
        eprintln!("{} error: Something went wrong", env!("CARGO_PKG_NAME"));
        Err(e)
    } else {
        Ok(())
    }
}
