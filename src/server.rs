use crate::graph::Graph;
use axum::Router;
use std::sync::Arc;

/// Creates the Axum app for serving over the network, using the given [`Graph`].
pub fn make_app(graph: Arc<Graph>) -> Router {
    Router::new().with_state(graph)
}
