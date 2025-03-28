use crate::{config::STARLING_CONFIG, graph::Graph, node::NodeOptions};
use axum::{
    extract::{Path, Query, State},
    response::IntoResponse,
    routing::get,
    Json, Router,
};
use chrono::NaiveDate;
use orgish::Timestamp;
use serde::Deserialize;
use std::{path::PathBuf, sync::Arc};
use uuid::Uuid;

#[derive(Deserialize)]
struct QueryOptions {
    /// If true, the response will be in `bincode`-serialized bytes. This is significantly more
    /// efficient for other Rust programs. Otherwise, JSON will be sent.
    #[serde(default)]
    use_bincode: bool,
}

/// Creates the Axum app for serving over the network, using the given [`Graph`].
pub fn make_app(graph: Arc<Graph>) -> Router {
    let mut router = Router::new()
        .route(
            "/node/:id",
            get(
                |Path(id): Path<Uuid>,
                 Query(QueryOptions { use_bincode }): Query<QueryOptions>,
                 State(graph): State<Arc<Graph>>,
                 Json(opts): Json<NodeOptions>| async move {
                    let node_info = graph.get_node(id, opts).await;
                    if use_bincode {
                        bincode::serialize(&node_info).unwrap().into_response()
                    } else {
                        Json(node_info).into_response()
                    }
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
            "/root-id/:path",
            get(
                |Path(path): Path<PathBuf>, State(graph): State<Arc<Graph>>| async move {
                    let root_id = graph.root_id(&path).await;
                    Json(root_id)
                },
            ),
        )
        .route(
            "/nodes",
            get(
                |State(graph): State<Arc<Graph>>,
                 Query(QueryOptions { use_bincode }): Query<QueryOptions>,
                 Json(opts): Json<NodeOptions>| async move {
                    let nodes = graph.nodes(None, opts).await;
                    if use_bincode {
                        bincode::serialize(&nodes).unwrap().into_response()
                    } else {
                        Json(nodes).into_response()
                    }
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
        // --- Utility methods ---
        .route(
            "/utils/next-timestamp",
            // Returns the next repeat of the given timestamp if there is one.
            get(|Json(ts): Json<Timestamp>| async {
                let next_ts = ts.into_next_repeat().ok();
                Json(next_ts)
            }),
        )
        .route(
            "/utils/next-timestamp/:after",
            get(
                |Path(after): Path<NaiveDate>, Json(ts): Json<Timestamp>| async move {
                    let next_ts = ts.into_next_repeat_after(after).ok();
                    Json(next_ts)
                },
            ),
        );
    // Add index methods
    for index_name in graph.indices.names() {
        let index_name = index_name.clone();
        router = router.route(
            &format!("/index/{}/nodes", index_name),
            get(
                |State(graph): State<Arc<Graph>>,
                 Query(QueryOptions { use_bincode }): Query<QueryOptions>,
                 Json(opts): Json<NodeOptions>| async move {
                    let nodes = graph.nodes(Some(&index_name), opts).await;

                    if use_bincode {
                        bincode::serialize(&nodes).unwrap().into_response()
                    } else {
                        Json(nodes).into_response()
                    }
                },
            ),
        );
    }

    router.with_state(graph)
}
