use crate::config::STARLING_CONFIG;
use tracing::level_filters::LevelFilter;
use tracing_appender::rolling::{RollingFileAppender, Rotation};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter, Layer};

/// Sets up logging across the app. This requires the configuration to have been set up first.
pub fn setup_logging() {
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
    // Stdout should only get above warnings (unless the user configures it otherwise)
    let stdout_layer = tracing_subscriber::fmt::layer()
        .with_level(true)
        .with_file(true)
        .with_line_number(true)
        .without_time()
        .compact()
        .with_writer(std::io::stdout)
        .with_filter(
            EnvFilter::builder()
                .with_default_directive(LevelFilter::WARN.into())
                .with_env_var("STARLING_LOG")
                .from_env_lossy(),
        );
    tracing_subscriber::registry()
        .with(stdout_layer)
        .with(file_layer)
        .init();
}
