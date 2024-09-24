use config::{Config, STARLING_CONFIG};
use error::Error;
use fs_engine::FsEngine;
use graph::Graph;
use std::{path::PathBuf, sync::Arc};
use tracing::{info, level_filters::LevelFilter};
use tracing_appender::rolling::{RollingFileAppender, Rotation};
use tracing_subscriber::{
    fmt::writer::MakeWriterExt, layer::SubscriberExt, util::SubscriberInitExt, Layer,
};

mod config;
mod conflict_detector;
mod connection;
mod debouncer;
mod error;
mod fs_engine;
mod graph;
mod node;
mod patch;
mod path_node;
#[cfg(test)]
mod tests;

#[tokio::main]
async fn main() {
    match core().await {
        Ok(()) => (),
        // Any errors we *return* aren't for logging, we'll crash entirely
        Err(e) => {
            eprintln!("error: {}", e);
            std::process::exit(1);
        }
    }
}

async fn core() -> Result<(), Error> {
    let dir = std::env::args()
        .nth(1)
        .map(PathBuf::from)
        .ok_or(Error::NoDir)?;
    // Later functions will panic if this isn't upheld
    if !dir.is_dir() {
        return Err(Error::InvalidDir { path: dir });
    }

    // We need the config to know where to log
    let config = Config::from_dir(&dir)?;
    STARLING_CONFIG.set(config);

    // Set up logging to create a rotating log file for each day
    let file_appender = RollingFileAppender::new(
        Rotation::DAILY,
        STARLING_CONFIG.get().log_directory.as_ref().unwrap(),
        "log",
    );
    // Create a subscriber that writes logs to the file
    let (non_blocking, _guard) = tracing_appender::non_blocking(file_appender);
    // Set the default subscriber to write logs to the non-blocking file appender
    let file_layer = tracing_subscriber::fmt::layer()
        .with_file(true)
        .with_line_number(true)
        .with_thread_ids(true)
        .with_thread_names(true)
        .with_level(true)
        .with_writer(non_blocking);
    // Stdout should only get above warnings
    let stdout_layer = tracing_subscriber::fmt::layer()
        .with_level(true)
        .with_file(false)
        .without_time()
        .compact()
        .with_writer(std::io::stdout)
        .with_filter(LevelFilter::WARN);
    tracing_subscriber::registry()
        .with(stdout_layer)
        .with(file_layer)
        .init();

    // Any errors on each path would be accumulated into each path
    let (graph, initial_writes) = Graph::from_dir(&dir).await;
    let graph = Arc::new(graph);

    // Start up the filesystem processing engine and let it run forever
    let fs_engine = FsEngine::new(graph.clone(), initial_writes);
    let fs_engine_task = fs_engine.run(dir)?;
    info!("about to start filesystem engine");
    fs_engine_task.await;

    // TODO: Set up a server

    Ok(())
}
