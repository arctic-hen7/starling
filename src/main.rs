use config::{Config, STARLING_CONFIG};
use error::Error;
use fs_engine::FsEngine;
use graph::Graph;
use logging::setup_logging;
use server::make_app;
use std::{collections::HashMap, path::PathBuf, sync::Arc};
use tokio::net::TcpListener;
use tracing::info;

mod config;
mod conflict_detector;
mod connection;
mod debouncer;
mod error;
mod fs_engine;
mod graph;
mod logging;
mod node;
mod patch;
mod path_node;
mod server;
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
    // The user will provide a directory as the first argument
    let dir = std::env::args()
        .nth(1)
        .map(PathBuf::from)
        .ok_or(Error::NoDir)?;
    // Later functions will panic if this isn't upheld
    if !dir.is_dir() {
        return Err(Error::InvalidDir { path: dir });
    }

    // Set up configuration and logging (we need config to know where to log)
    STARLING_CONFIG.set(Config::from_dir(&dir)?);
    setup_logging();

    // Any errors on each path would be accumulated into each path, so this can't fail
    let (graph, initial_writes) = Graph::from_dir(&dir, HashMap::new()).await;
    let graph = Arc::new(graph);

    // Start up the filesystem processing engine and let it run forever
    let fs_engine = FsEngine::new(graph.clone(), initial_writes);
    let fs_engine_task = fs_engine.run(dir)?;
    info!("about to start filesystem engine");
    tokio::spawn(fs_engine_task);

    // Set up the server
    let config = STARLING_CONFIG.get();
    let listener = TcpListener::bind((config.host.as_str(), config.port))
        .await
        .map_err(|err| Error::ListenFailed {
            host: config.host.clone(),
            port: config.port,
            err,
        })?;
    info!("about to start server");
    axum::serve(listener, make_app(graph))
        .await
        .map_err(|err| Error::ServeFailed { err })?;

    Ok(())
}
