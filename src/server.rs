use crate::{config::STARLING_CONFIG, graph::Graph, node::NodeOptions};
use axum::{
    extract::{Path, State},
    routing::get,
    Json, Router,
};
use orgish::Format;
use serde::Deserialize;
use std::{path::PathBuf, sync::Arc};
use uuid::Uuid;

/// Creates the Axum app for serving over the network, using the given [`Graph`].
pub fn make_app(graph: Arc<Graph>) -> Router {
    Router::new()
        .route(
            "/node/:id",
            get(
                |Path(id): Path<Uuid>,
                 State(graph): State<Arc<Graph>>,
                 Json(opts): Json<NodeOptions>| async move {
                    let node_info = graph.get_node(id, opts).await;
                    Json(node_info)
                },
            ),
        )
        .route(
            "/errors/:path",
            get(
                |Path(path): Path<PathBuf>, State(graph): State<Arc<Graph>>| async move {
                    let errors = graph.errors(&path).await;
                    Json(errors)
                },
            ),
        )
        .route(
            "/nodes",
            get(
                |State(graph): State<Arc<Graph>>, Json(opts): Json<GetNodesOpts>| async move {
                    let nodes = graph.nodes(opts.format).await;
                    Json(nodes)
                },
            ),
        )
        // --- Information about configuration ---
        .route(
            "/info/tags",
            get(|| async {
                let cfg = STARLING_CONFIG.get();
                Json(cfg.tags.clone())
            }),
        )
        .route(
            "/info/link-types",
            get(|| async {
                let cfg = STARLING_CONFIG.get();
                Json(cfg.link_types.clone())
            }),
        )
        .route(
            "/info/default-link-type",
            get(|| async {
                let cfg = STARLING_CONFIG.get();
                Json(cfg.default_link_type.clone())
            }),
        )
        .route(
            "/info/action-keywords",
            get(|| async {
                let cfg = STARLING_CONFIG.get();
                Json(cfg.action_keywords.clone())
            }),
        )
        .with_state(graph)
}

#[derive(Deserialize)]
struct GetNodesOpts {
    format: Format,
}
