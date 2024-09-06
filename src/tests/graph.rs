use crate::{
    config::{Config, STARLING_CONFIG},
    graph::*,
    node::{Node, NodeConnection, NodeOptions},
    patch::{GraphPatch, PathPatch},
};
use orgish::Format;
use std::path::PathBuf;
use uuid::Uuid;

/// Trait that allows [&str; _] to be converted to a `HashSet<String>`.
trait IntoHashSet {
    fn into_hs(self) -> std::collections::HashSet<String>;
}
impl<const B: usize> IntoHashSet for [&str; B] {
    fn into_hs(self) -> std::collections::HashSet<String> {
        self.iter().map(|s| s.to_string()).collect()
    }
}
/// Trait that infallibly converts `&str` to `Uuid`.
trait IntoUuid {
    fn uuid(self) -> Uuid;
}
impl IntoUuid for &str {
    fn uuid(self) -> Uuid {
        Uuid::parse_str(self).unwrap()
    }
}

/// Simple macro for creating `HashMap`s.
macro_rules! map {
    {$($key:expr => $value:expr),*} => {
        {
            #[allow(unused_mut)]
            let mut map = std::collections::HashMap::new();
            $(
                map.insert($key, $value);
            )*
            map
        }
    };
}

fn opts_all_conns() -> NodeOptions {
    NodeOptions {
        body: false,
        metadata: false,
        children: false,
        connections: true,
        child_connections: true,
        conn_format: Format::Markdown,
    }
}

// One-stop shop for testing pretty much all fundamental behaviour
#[tokio::test]
async fn should_parse_connections_and_node_data() {
    let mut config = Config::default();
    config.link_types.push("diff".to_string());
    config.link_types.push("other".to_string());
    config.tags.extend(
        ["hello", "world", "foo", "parent_tag", "child_tag"]
            .iter()
            .map(|s| s.to_string()),
    );
    STARLING_CONFIG.set(config);

    // Three basic Markdown files with some links between them
    let file_1 = r#"---
title: File 1
tags:
    - hello
    - world
---
<!--PROPERTIES
ID: 5d93b936-5952-4707-89dd-69ca06c60850
-->

# Node 1 :parent_tag:
<!--PROPERTIES
ID: 5d93b936-5952-4707-89dd-69ca06c60851
-->

Here's a link to [File 2](5d93b936-5952-4707-89dd-69ca06c60855).

## Node 1.1
<!--PROPERTIES
ID: 5d93b936-5952-4707-89dd-69ca06c60852
-->

This is a link to [File 1](5d93b936-5952-4707-89dd-69ca06c60850).

### Node 1.1.1 :child_tag:
<!--PROPERTIES
ID: 5d93b936-5952-4707-89dd-69ca06c60853
-->

Here's a link to a [nonexistent node](6d93b936-5952-4707-89dd-69ca06c60854). But [this](other:5d93b936-5952-4707-89dd-69ca06c60852) one should work, even though it's of a different type. Links like [this](https://example.com) won't be indexed, and ones with invalid types, [like this](thing:5d93b936-5952-4707-89dd-69ca06c60852), will be ignored.

# Node 2
<!--PROPERTIES
ID: 5d93b936-5952-4707-89dd-69ca06c60854
-->

Here's another link to [Node 1.1](5d93b936-5952-4707-89dd-69ca06c60852), using a different type, so it should be amalgamated in the child connections.
"#;
    let file_2 = r#"---
title: File 2
tags:
    - foo
---
<!--PROPERTIES
ID: 5d93b936-5952-4707-89dd-69ca06c60855
-->

We have a few links in the body of the root, like to [Node 1](5d93b936-5952-4707-89dd-69ca06c60851). We'll actually have another link there of a different type too: [Node 1](other:5d93b936-5952-4707-89dd-69ca06c60851).
"#;

    let graph = Graph::new();
    graph
        .process_fs_patch(GraphPatch {
            renames: Vec::new(),
            deletions: Vec::new(),
            creations: vec![
                PathPatch {
                    path: PathBuf::from("file_1.md"),
                    contents_res: Ok(file_1.to_string()),
                },
                PathPatch {
                    path: PathBuf::from("file_2.md"),
                    contents_res: Ok(file_2.to_string()),
                },
            ],
            modifications: Vec::new(),
        })
        .await;

    // Get details about all the nodes
    // File 1
    assert_eq!(
        graph
            .get_node(
                "5d93b936-5952-4707-89dd-69ca06c60850".uuid(),
                opts_all_conns(),
            )
            .await
            .unwrap(),
        Node {
            id: "5d93b936-5952-4707-89dd-69ca06c60850".uuid(),
            title: "File 1".into(),
            tags: ["hello", "world"].into_hs(),
            parent_tags: [].into(),
            children: Vec::new(),
            metadata: None,
            body: None,
            connections: map! {},
            backlinks: map! {
                "5d93b936-5952-4707-89dd-69ca06c60852".uuid() => NodeConnection {
                    title: "Node 1.1".into(),
                    types: ["link"].into_hs()
                }
            },
            child_connections: map! {
                "5d93b936-5952-4707-89dd-69ca06c60855".uuid() => NodeConnection {
                    title: "File 2".into(),
                    types: ["link"].into_hs()
                },
                "5d93b936-5952-4707-89dd-69ca06c60850".uuid() => NodeConnection {
                    title: "File 1".into(),
                    types: ["link"].into_hs()
                },
                "5d93b936-5952-4707-89dd-69ca06c60852".uuid() => NodeConnection {
                    title: "Node 1.1".into(),
                    // Types from connections across nodes get combined
                    types: ["link", "other"].into_hs()
                }
            },
            child_backlinks: map! {
                "5d93b936-5952-4707-89dd-69ca06c60855".uuid() => NodeConnection {
                    title: "File 2".into(),
                    types: ["link", "other"].into_hs()
                },
                "5d93b936-5952-4707-89dd-69ca06c60853".uuid() => NodeConnection {
                    title: "Node 1.1.1".into(),
                    // Invalid type `thing` doesn't get registered
                    types: ["other"].into_hs()
                },
                "5d93b936-5952-4707-89dd-69ca06c60854".uuid() => NodeConnection {
                    title: "Node 2".into(),
                    types: ["link"].into_hs()
                }
            }
        }
    );
    // Node 1
    assert_eq!(
        graph
            .get_node(
                "5d93b936-5952-4707-89dd-69ca06c60851".uuid(),
                opts_all_conns(),
            )
            .await
            .unwrap(),
        Node {
            id: "5d93b936-5952-4707-89dd-69ca06c60851".uuid(),
            title: "Node 1".into(),
            tags: ["parent_tag"].into_hs(),
            parent_tags: ["hello", "world"].into_hs(),
            children: Vec::new(),
            metadata: None,
            body: None,
            connections: map! {
                "5d93b936-5952-4707-89dd-69ca06c60855".uuid() => NodeConnection {
                    title: "File 2".into(),
                    types: ["link"].into_hs()
                }
            },
            backlinks: map! {
                "5d93b936-5952-4707-89dd-69ca06c60855".uuid() => NodeConnection {
                    title: "File 2".into(),
                    types: ["link", "other"].into_hs()
                }
            },
            child_connections: map! {
                "5d93b936-5952-4707-89dd-69ca06c60850".uuid() => NodeConnection {
                    title: "File 1".into(),
                    types: ["link"].into_hs()
                },
                "5d93b936-5952-4707-89dd-69ca06c60852".uuid() => NodeConnection {
                    title: "Node 1.1".into(),
                    types: ["other"].into_hs()
                }
            },
            child_backlinks: map! {
                "5d93b936-5952-4707-89dd-69ca06c60853".uuid() => NodeConnection {
                    title: "Node 1.1.1".into(),
                    // Invalid type `thing` doesn't get registered
                    types: ["other"].into_hs()
                },
                "5d93b936-5952-4707-89dd-69ca06c60854".uuid() => NodeConnection {
                    title: "Node 2".into(),
                    types: ["link"].into_hs()
                }
            }
        }
    );
    // Node 1.1
    assert_eq!(
        graph
            .get_node(
                "5d93b936-5952-4707-89dd-69ca06c60852".uuid(),
                opts_all_conns(),
            )
            .await
            .unwrap(),
        Node {
            id: "5d93b936-5952-4707-89dd-69ca06c60852".uuid(),
            title: "Node 1.1".into(),
            tags: [].into(),
            parent_tags: ["hello", "world", "parent_tag"].into_hs(),
            children: Vec::new(),
            metadata: None,
            body: None,
            connections: map! {
                "5d93b936-5952-4707-89dd-69ca06c60850".uuid() => NodeConnection {
                    title: "File 1".into(),
                    types: ["link"].into_hs()
                }
            },
            backlinks: map! {
                "5d93b936-5952-4707-89dd-69ca06c60853".uuid() => NodeConnection {
                    title: "Node 1.1.1".into(),
                    // Invalid type `thing` doesn't get registered
                    types: ["other"].into_hs()
                },
                "5d93b936-5952-4707-89dd-69ca06c60854".uuid() => NodeConnection {
                    title: "Node 2".into(),
                    types: ["link"].into_hs()
                }
            },
            child_connections: map! {
                "5d93b936-5952-4707-89dd-69ca06c60852".uuid() => NodeConnection {
                    title: "Node 1.1".into(),
                    types: ["other"].into_hs()
                }
            },
            child_backlinks: map! {}
        }
    );
    // Node 1.1.1
    assert_eq!(
        graph
            .get_node(
                "5d93b936-5952-4707-89dd-69ca06c60853".uuid(),
                opts_all_conns(),
            )
            .await
            .unwrap(),
        Node {
            id: "5d93b936-5952-4707-89dd-69ca06c60853".uuid(),
            title: "Node 1.1.1".into(),
            tags: ["child_tag"].into_hs(),
            parent_tags: ["hello", "world", "parent_tag"].into_hs(),
            children: Vec::new(),
            metadata: None,
            body: None,
            connections: map! {
                "5d93b936-5952-4707-89dd-69ca06c60852".uuid() => NodeConnection {
                    title: "Node 1.1".into(),
                    types: ["other"].into_hs()
                }
                // Links with invalid types and UUIDs, along with URL connection, are not
                // registered
            },
            backlinks: map! {},
            child_connections: map! {},
            child_backlinks: map! {}
        }
    );
    // Node 2
    assert_eq!(
        graph
            .get_node(
                "5d93b936-5952-4707-89dd-69ca06c60854".uuid(),
                opts_all_conns(),
            )
            .await
            .unwrap(),
        Node {
            id: "5d93b936-5952-4707-89dd-69ca06c60854".uuid(),
            title: "Node 2".into(),
            tags: [].into_hs(),
            parent_tags: ["hello", "world"].into_hs(),
            children: Vec::new(),
            metadata: None,
            body: None,
            connections: map! {
                "5d93b936-5952-4707-89dd-69ca06c60852".uuid() => NodeConnection {
                    title: "Node 1.1".into(),
                    types: ["link"].into_hs()
                }
            },
            backlinks: map! {},
            child_connections: map! {},
            child_backlinks: map! {}
        }
    );
    // File 2
    assert_eq!(
        graph
            .get_node(
                "5d93b936-5952-4707-89dd-69ca06c60855".uuid(),
                opts_all_conns(),
            )
            .await
            .unwrap(),
        Node {
            id: "5d93b936-5952-4707-89dd-69ca06c60855".uuid(),
            title: "File 2".into(),
            tags: ["foo"].into_hs(),
            parent_tags: [].into(),
            children: Vec::new(),
            metadata: None,
            body: None,
            connections: map! {
                "5d93b936-5952-4707-89dd-69ca06c60851".uuid() => NodeConnection {
                    title: "Node 1".into(),
                    types: ["link", "other"].into_hs()
                }
            },
            backlinks: map! {
                "5d93b936-5952-4707-89dd-69ca06c60851".uuid() => NodeConnection {
                    title: "Node 1".into(),
                    types: ["link"].into_hs()
                }
            },
            child_connections: map! {},
            child_backlinks: map! {}
        }
    );
}
