use crate::{
    config::{Config, STARLING_CONFIG},
    graph::*,
    node::{Node, NodeConnection, NodeMetadata, NodeOptions},
    patch::{GraphPatch, PathPatch},
};
use chrono::NaiveDate;
use orgish::{timestamp::DateTime, Format, Timestamp};
use std::path::PathBuf;
use std::{collections::HashMap, sync::atomic::Ordering};
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

/// Produces options for retrieving a node and all information about its connections.
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

/// Sets up the global configuration (making sure not to do so twice, because otherwise
/// multi-threaded test interleaving can produce nasty deadlocks).
fn setup_config() {
    if !STARLING_CONFIG.setup.load(Ordering::SeqCst) {
        let mut config = Config::default();
        config.link_types.push("diff".to_string());
        config.link_types.push("other".to_string());
        config.tags.extend(
            ["hello", "world", "foo", "parent_tag", "child_tag"]
                .iter()
                .map(|s| s.to_string()),
        );
        STARLING_CONFIG.set(config);
    }
}

static FILE_1: &str = r#"---
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

Here's another link to [Node 1.1](5d93b936-5952-4707-89dd-69ca06c60852), using a different type, so it should be amalgamated in the child connections."#;
static FILE_2: &str = r#"---
title: File 2
tags:
    - foo
---
<!--PROPERTIES
ID: 5d93b936-5952-4707-89dd-69ca06c60855
-->

We have a few links in the body of the root, like to [Node 1](5d93b936-5952-4707-89dd-69ca06c60851). We'll actually have another link there of a different type too: [Node 1](other:5d93b936-5952-4707-89dd-69ca06c60851).

# Node 2.1
This deliberately doesn't have an ID."#;

// This tests creation, self-referencing paths, and basic title rewriting
#[tokio::test]
async fn should_parse_connections_and_node_data() {
    setup_config();

    let graph = Graph::new();
    let writes: HashMap<_, _> = graph
        .process_fs_patch(GraphPatch {
            renames: Vec::new(),
            deletions: Vec::new(),
            creations: vec![
                PathPatch {
                    path: PathBuf::from("file_1.md"),
                    contents_res: Ok(FILE_1.to_string()),
                },
                PathPatch {
                    path: PathBuf::from("file_2.md"),
                    contents_res: Ok(FILE_2.to_string()),
                },
            ],
            modifications: Vec::new(),
        })
        .await
        .into_iter()
        .map(|w| (w.path, w.contents))
        .collect();
    // File 1 should have some titles rewritten and link qualified
    assert_eq!(
        writes.get(&PathBuf::from("file_1.md")).unwrap(),
        &FILE_1
            .replace("[this](other:", "[Node 1.1](other:")
            .replace("](5d", "](link:5d")
            // Invalid ID is still an ID
            .replace("](6d", "](link:6d")
    );
    // File 2 should *start* with the same up to node 2.1
    let file_2_updated = writes.get(&PathBuf::from("file_2.md")).unwrap();
    assert_eq!(
        file_2_updated.split("# Node 2").next().unwrap(),
        FILE_2
            .replace("](5d", "](link:5d")
            .split("# Node 2")
            .next()
            .unwrap()
    );
    // And it should end with something indicating an ID has been added to node 2.1
    assert!(file_2_updated.ends_with("-->\nThis deliberately doesn't have an ID."));
    // We should have the invalid connection registered in file 1
    assert_eq!(
        graph.errors(&PathBuf::from("file_1.md")).await,
        Some(Ok(vec!["6d93b936-5952-4707-89dd-69ca06c60854".uuid()]))
    );
    // And no invalid connections on file 2
    assert_eq!(
        graph.errors(&PathBuf::from("file_2.md")).await,
        Some(Ok(Vec::new()))
    );

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
            path: PathBuf::from("file_1.md"),
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
            path: PathBuf::from("file_1.md"),
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
            path: PathBuf::from("file_1.md"),
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
            path: PathBuf::from("file_1.md"),
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
            path: PathBuf::from("file_1.md"),
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
            path: PathBuf::from("file_2.md"),
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

    // Make sure title rewriting has occurred correctly
    assert_eq!(
        graph
            .get_node(
                "5d93b936-5952-4707-89dd-69ca06c60853".uuid(),
                NodeOptions {
                    body: true,
                    metadata: false,
                    children: false,
                    connections: false,
                    child_connections: false,
                    conn_format: Format::Markdown
                }
            )
            .await
            .unwrap()
            .body,
        // The valid link gets a valid title (only other change is the default link type on the
        // nonexistent one)
        Some(r#"
Here's a link to a [nonexistent node](link:6d93b936-5952-4707-89dd-69ca06c60854). But [Node 1.1](other:5d93b936-5952-4707-89dd-69ca06c60852) one should work, even though it's of a different type. Links like [this](https://example.com) won't be indexed, and ones with invalid types, [like this](thing:5d93b936-5952-4707-89dd-69ca06c60852), will be ignored.
"#.into())
    );
}

#[tokio::test]
async fn title_update_should_trigger_rewrites() {
    setup_config();

    let file_1 = r#"---
title: File 1
---
<!--PROPERTIES
ID: 6097edb8-7a66-45fe-aec3-eb957f511ab0
-->

[File 2](link:6097edb8-7a66-45fe-aec3-eb957f511ab1) is here. And [File 3](link:6097edb8-7a66-45fe-aec3-eb957f511ab2) is here."#;
    let file_2 = r#"---
title: File 2
---
<!--PROPERTIES
ID: 6097edb8-7a66-45fe-aec3-eb957f511ab1
-->

This links to nothing."#;
    let file_3 = r#"---
title: File 3
---
<!--PROPERTIES
ID: 6097edb8-7a66-45fe-aec3-eb957f511ab2
-->

[File 2](link:6097edb8-7a66-45fe-aec3-eb957f511ab1) is here.

# Node 3.1
<!--PROPERTIES
ID: 6097edb8-7a66-45fe-aec3-eb957f511ab3
-->

This is a connection to [File 3](link:6097edb8-7a66-45fe-aec3-eb957f511ab2)."#;

    let graph = Graph::new();
    let writes = graph
        .process_fs_patch(GraphPatch {
            renames: Vec::new(),
            deletions: Vec::new(),
            creations: vec![
                PathPatch {
                    path: PathBuf::from("file_1.md"),
                    contents_res: Ok(file_1.into()),
                },
                PathPatch {
                    path: PathBuf::from("file_2.md"),
                    contents_res: Ok(file_2.into()),
                },
                PathPatch {
                    path: PathBuf::from("file_3.md"),
                    contents_res: Ok(file_3.into()),
                },
            ],
            modifications: Vec::new(),
        })
        .await
        .into_iter()
        .map(|w| (w.path, w.contents))
        .collect::<HashMap<_, _>>();
    // All should be written back to disk on creation to preserve any potentially missing IDs
    assert!(writes.contains_key(&PathBuf::from("file_1.md")));
    assert!(writes.contains_key(&PathBuf::from("file_2.md")));
    assert!(writes.contains_key(&PathBuf::from("file_3.md")));

    let new_file_2 = file_2.replace("title: File 2", "title: File 2+");
    let writes = graph
        .process_fs_patch(GraphPatch {
            renames: Vec::new(),
            deletions: Vec::new(),
            creations: Vec::new(),
            modifications: vec![PathPatch {
                path: PathBuf::from("file_2.md"),
                contents_res: Ok(new_file_2.into()),
            }],
        })
        .await
        .into_iter()
        .map(|w| (w.path, w.contents))
        .collect::<HashMap<_, _>>();

    // We should rewrite the dependent files to update the title, but file 2 itself shouldn't need
    // modification (it's not self-referencing and no connections changed)
    assert_eq!(
        writes.get(&PathBuf::from("file_1.md")).unwrap(),
        &file_1.replace("[File 2]", "[File 2+]")
    );
    assert_eq!(
        writes.get(&PathBuf::from("file_3.md")).unwrap(),
        &file_3.replace("[File 2]", "[File 2+]")
    );
    assert!(!writes.contains_key(&PathBuf::from("file_2.md")));

    // Now try changing file 3: file 1 should change, file 2 should be left alone, and file 3
    // should change because it's self-referencing
    let new_file_3 = file_3
        .replace("title: File 3", "title: File 3+")
        .replace("[File 2]", "[File 2+]");
    let writes = graph
        .process_fs_patch(GraphPatch {
            renames: Vec::new(),
            deletions: Vec::new(),
            creations: Vec::new(),
            modifications: vec![PathPatch {
                path: PathBuf::from("file_3.md"),
                contents_res: Ok(new_file_3.clone()),
            }],
        })
        .await
        .into_iter()
        .map(|w| (w.path, w.contents))
        .collect::<HashMap<_, _>>();
    assert_eq!(
        writes.get(&PathBuf::from("file_1.md")).unwrap(),
        &file_1
            .replace("[File 3]", "[File 3+]")
            .replace("[File 2]", "[File 2+]")
    );
    assert_eq!(
        writes.get(&PathBuf::from("file_3.md")).unwrap(),
        &new_file_3.replace("[File 3]", "[File 3+]")
    );
    assert!(!writes.contains_key(&PathBuf::from("file_2.md")));
}

#[tokio::test]
async fn metadata_should_be_parsed() {
    setup_config();

    let file_1 = r#"---
title: File 1
tags:
    - hello
    - world
---
<!--PROPERTIES
ID: 7097edb8-7a66-45fe-aec3-eb957f511ab0
-->

# TODO [#A] Node 1 :child_tag:
SCHEDULED: <2024-01-01>
<!--PROPERTIES
ID: 7097edb8-7a66-45fe-aec3-eb957f511ab1
LOCATION: Test
-->

This is a test file."#;
    let file_2 = r#"---
title: File 2
---

Here's [Node 1](link:7097edb8-7a66-45fe-aec3-eb957f511ab1)."#;

    let graph = Graph::new();
    graph
        .process_fs_patch(GraphPatch {
            renames: Vec::new(),
            deletions: Vec::new(),
            creations: vec![
                PathPatch {
                    path: PathBuf::from("file_1.md"),
                    contents_res: Ok(file_1.into()),
                },
                PathPatch {
                    path: PathBuf::from("file_2.md"),
                    contents_res: Ok(file_2.into()),
                },
            ],
            modifications: Vec::new(),
        })
        .await;

    let node = graph
        .get_node(
            "7097edb8-7a66-45fe-aec3-eb957f511ab1".uuid(),
            NodeOptions::new(Format::Markdown).metadata(true),
        )
        .await
        .unwrap();
    assert_eq!(
        node.metadata,
        Some(NodeMetadata {
            level: 1,
            priority: Some("A".into()),
            deadline: None,
            scheduled: Some(Timestamp {
                start: DateTime {
                    date: NaiveDate::parse_from_str("2024-01-01", "%Y-%m-%d").unwrap(),
                    time: None
                },
                end: None,
                repeater: None,
                active: true
            }),
            closed: None,
            properties: map! {
                "LOCATION".into() => "Test".into()
            },
            keyword: Some("TODO".into()),
            timestamps: Vec::new()
        })
    );
    assert_eq!(node.tags, ["child_tag"].into_hs());
    assert_eq!(node.parent_tags, ["hello", "world"].into_hs());

    let updated_file_1 = file_1.replace("TODO [#A]", "DONE [#B]");
    let writes = graph
        .process_fs_patch(GraphPatch {
            renames: Vec::new(),
            deletions: Vec::new(),
            creations: Vec::new(),
            modifications: vec![PathPatch {
                path: PathBuf::from("file_1.md"),
                contents_res: Ok(updated_file_1),
            }],
        })
        .await;
    // We haven't modified a title, we've modified metadata in it (so file 2's link remains the
    // same)
    assert!(writes.is_empty());

    let node = graph
        .get_node(
            "7097edb8-7a66-45fe-aec3-eb957f511ab1".uuid(),
            NodeOptions::new(Format::Markdown).metadata(true),
        )
        .await
        .unwrap();
    assert_eq!(
        node.metadata,
        Some(NodeMetadata {
            level: 1,
            priority: Some("B".into()),
            deadline: None,
            scheduled: Some(Timestamp {
                start: DateTime {
                    date: NaiveDate::parse_from_str("2024-01-01", "%Y-%m-%d").unwrap(),
                    time: None
                },
                end: None,
                repeater: None,
                active: true
            }),
            closed: None,
            properties: map! {
                "LOCATION".into() => "Test".into()
            },
            keyword: Some("DONE".into()),
            timestamps: Vec::new()
        })
    );
}

#[tokio::test]
async fn renames_should_work() {
    setup_config();

    let file_1 = r#"---
title: File 1
tags:
    - hello
    - world
---
<!--PROPERTIES
ID: 7097edb8-7a66-45fe-aec3-eb957f511ab0
-->

# TODO [#A] Node 1 :child_tag:
SCHEDULED: <2024-01-01>
<!--PROPERTIES
ID: 7097edb8-7a66-45fe-aec3-eb957f511ab1
LOCATION: Test
-->

This is a test file."#;
    let file_2 = r#"---
title: File 2
---

Here's [Node 1](link:7097edb8-7a66-45fe-aec3-eb957f511ab1)."#;

    let graph = Graph::new();
    graph
        .process_fs_patch(GraphPatch {
            renames: Vec::new(),
            deletions: Vec::new(),
            creations: vec![
                PathPatch {
                    path: PathBuf::from("file_1.md"),
                    contents_res: Ok(file_1.into()),
                },
                PathPatch {
                    path: PathBuf::from("file_2.md"),
                    contents_res: Ok(file_2.into()),
                },
            ],
            modifications: Vec::new(),
        })
        .await;

    let writes = graph
        .process_fs_patch(GraphPatch {
            renames: vec![(PathBuf::from("file_1.md"), PathBuf::from("file_1_new.md"))],
            deletions: Vec::new(),
            creations: Vec::new(),
            modifications: Vec::new(),
        })
        .await;
    // No writes should result from a rename
    assert!(writes.is_empty());
    assert_eq!(
        graph
            .get_node(
                "7097edb8-7a66-45fe-aec3-eb957f511ab1".uuid(),
                NodeOptions::new(Format::Markdown)
            )
            .await
            .unwrap()
            .path,
        PathBuf::from("file_1_new.md")
    );
}

#[tokio::test]
async fn node_deletion_should_remove_backlinks() {
    setup_config();

    let file_1 = r#"---
title: File 1
---
<!--PROPERTIES
ID: 7097edb8-7a66-45fe-aec3-eb957f511ab0
-->

# Node 1
<!--PROPERTIES
ID: 7097edb8-7a66-45fe-aec3-eb957f511ab1
-->

This is a test file. Here's [File 2](link:7097edb8-7a66-45fe-aec3-eb957f511ab2)"#;
    let file_2 = r#"---
title: File 2
---
<!--PROPERTIES
ID: 7097edb8-7a66-45fe-aec3-eb957f511ab2
-->

Here's [Node 1](link:7097edb8-7a66-45fe-aec3-eb957f511ab1)."#;

    let graph = Graph::new();
    graph
        .process_fs_patch(GraphPatch {
            renames: Vec::new(),
            deletions: Vec::new(),
            creations: vec![
                PathPatch {
                    path: PathBuf::from("file_1.md"),
                    contents_res: Ok(file_1.into()),
                },
                PathPatch {
                    path: PathBuf::from("file_2.md"),
                    contents_res: Ok(file_2.into()),
                },
            ],
            modifications: Vec::new(),
        })
        .await;

    // We should initially have a backlink on file 2 coming from node 1
    assert_eq!(
        graph
            .get_node(
                "7097edb8-7a66-45fe-aec3-eb957f511ab2".uuid(),
                NodeOptions::new(Format::Markdown).connections(true)
            )
            .await
            .unwrap()
            .backlinks,
        map! { "7097edb8-7a66-45fe-aec3-eb957f511ab1".uuid() => NodeConnection {
            title: "Node 1".into(),
            types: ["link"].into_hs()
        }}
    );

    let file_1_updated = r#"---
title: File 1
---
<!--PROPERTIES
ID: 7097edb8-7a66-45fe-aec3-eb957f511ab0
-->"#;
    let writes = graph
        .process_fs_patch(GraphPatch {
            renames: Vec::new(),
            deletions: Vec::new(),
            creations: Vec::new(),
            modifications: vec![PathPatch {
                path: PathBuf::from("file_1.md"),
                contents_res: Ok(file_1_updated.into()),
            }],
        })
        .await;
    assert!(writes.is_empty());
    // And now we should have no backlinks on file 2 (node 1's reference is gone)
    assert!(graph
        .get_node(
            "7097edb8-7a66-45fe-aec3-eb957f511ab2".uuid(),
            NodeOptions::new(Format::Markdown).connections(true)
        )
        .await
        .unwrap()
        .backlinks
        .is_empty());
    // And there should be no valid connections in file 2 either
    assert!(graph
        .get_node(
            "7097edb8-7a66-45fe-aec3-eb957f511ab2".uuid(),
            NodeOptions::new(Format::Markdown).connections(true)
        )
        .await
        .unwrap()
        .connections
        .is_empty());
    // But the invalid connection should be there
    assert_eq!(
        graph.errors(&PathBuf::from("file_2.md")).await,
        Some(Ok(vec!["7097edb8-7a66-45fe-aec3-eb957f511ab1".uuid()]))
    );
}

#[tokio::test]
async fn file_deletion_should_work() {
    setup_config();

    let file_1 = r#"---
title: File 1
---
<!--PROPERTIES
ID: 7097edb8-7a66-45fe-aec3-eb957f511ab0
-->

# Node 1
<!--PROPERTIES
ID: 7097edb8-7a66-45fe-aec3-eb957f511ab1
-->

This is a test file. Here's [File 2](link:7097edb8-7a66-45fe-aec3-eb957f511ab2)"#;
    let file_2 = r#"---
title: File 2
---
<!--PROPERTIES
ID: 7097edb8-7a66-45fe-aec3-eb957f511ab2
-->

Here's [Node 1](link:7097edb8-7a66-45fe-aec3-eb957f511ab1)."#;

    let graph = Graph::new();
    graph
        .process_fs_patch(GraphPatch {
            renames: Vec::new(),
            deletions: Vec::new(),
            creations: vec![
                PathPatch {
                    path: PathBuf::from("file_1.md"),
                    contents_res: Ok(file_1.into()),
                },
                PathPatch {
                    path: PathBuf::from("file_2.md"),
                    contents_res: Ok(file_2.into()),
                },
            ],
            modifications: Vec::new(),
        })
        .await;

    // We should initially have a backlink on node 1 coming from file 2
    assert_eq!(
        graph
            .get_node(
                "7097edb8-7a66-45fe-aec3-eb957f511ab1".uuid(),
                NodeOptions::new(Format::Markdown).connections(true)
            )
            .await
            .unwrap()
            .backlinks,
        map! { "7097edb8-7a66-45fe-aec3-eb957f511ab2".uuid() => NodeConnection {
            title: "File 2".into(),
            types: ["link"].into_hs()
        }}
    );

    let writes = graph
        .process_fs_patch(GraphPatch {
            renames: Vec::new(),
            deletions: vec![PathBuf::from("file_2.md")],
            creations: Vec::new(),
            modifications: Vec::new(),
        })
        .await;
    assert!(writes.is_empty());
    // And now we should have no backlinks on node 1
    assert!(graph
        .get_node(
            "7097edb8-7a66-45fe-aec3-eb957f511ab1".uuid(),
            NodeOptions::new(Format::Markdown).connections(true)
        )
        .await
        .unwrap()
        .backlinks
        .is_empty());
    // And no valid connections either
    assert!(graph
        .get_node(
            "7097edb8-7a66-45fe-aec3-eb957f511ab1".uuid(),
            NodeOptions::new(Format::Markdown).connections(true)
        )
        .await
        .unwrap()
        .connections
        .is_empty());
    // But the invalid connection should be there
    assert_eq!(
        graph.errors(&PathBuf::from("file_1.md")).await,
        Some(Ok(vec!["7097edb8-7a66-45fe-aec3-eb957f511ab2".uuid()]))
    );
}

#[tokio::test]
async fn new_node_should_validate_existing_references() {
    setup_config();

    let file_1 = r#"---
title: File 1
---
<!--PROPERTIES
ID: 7097edb8-7a66-45fe-aec3-eb957f511ab0
-->"#;
    let file_2 = r#"---
title: File 2
---
<!--PROPERTIES
ID: 7097edb8-7a66-45fe-aec3-eb957f511ab2
-->

Here's [some node](link:7097edb8-7a66-45fe-aec3-eb957f511ab1)."#;

    let graph = Graph::new();
    graph
        .process_fs_patch(GraphPatch {
            renames: Vec::new(),
            deletions: Vec::new(),
            creations: vec![
                PathPatch {
                    path: PathBuf::from("file_1.md"),
                    contents_res: Ok(file_1.into()),
                },
                PathPatch {
                    path: PathBuf::from("file_2.md"),
                    contents_res: Ok(file_2.into()),
                },
            ],
            modifications: Vec::new(),
        })
        .await;
    // We should have an invalid connection in file 1
    assert_eq!(
        graph.errors(&PathBuf::from("file_2.md")).await,
        Some(Ok(vec!["7097edb8-7a66-45fe-aec3-eb957f511ab1".uuid()]))
    );

    let file_1_updated = r#"---
title: File 1
---
<!--PROPERTIES
ID: 7097edb8-7a66-45fe-aec3-eb957f511ab0
-->

# Node 1
<!--PROPERTIES
ID: 7097edb8-7a66-45fe-aec3-eb957f511ab1
-->

This is a test file. Here's [File 2](link:7097edb8-7a66-45fe-aec3-eb957f511ab2)"#;
    let writes = graph
        .process_fs_patch(GraphPatch {
            renames: Vec::new(),
            deletions: Vec::new(),
            creations: Vec::new(),
            modifications: vec![PathPatch {
                path: PathBuf::from("file_1.md"),
                contents_res: Ok(file_1_updated.into()),
            }],
        })
        .await
        .into_iter()
        .map(|w| (w.path, w.contents))
        .collect::<HashMap<_, _>>();
    // New node and the backlink should be updated, so both files should be written to
    assert_eq!(
        writes.get(&PathBuf::from("file_1.md")).unwrap(),
        &file_1_updated
    );
    assert_eq!(
        writes.get(&PathBuf::from("file_2.md")).unwrap(),
        &file_2.replace("[some node]", "[Node 1]")
    );

    // There should be a valid connection on the new node and a backlink from it on file 2
    assert_eq!(
        graph
            .get_node(
                "7097edb8-7a66-45fe-aec3-eb957f511ab1".uuid(),
                NodeOptions::new(Format::Markdown).connections(true)
            )
            .await
            .unwrap()
            .connections,
        map! { "7097edb8-7a66-45fe-aec3-eb957f511ab2".uuid() => NodeConnection {
            title: "File 2".into(),
            types: ["link"].into_hs()
        }}
    );
    assert_eq!(
        graph
            .get_node(
                "7097edb8-7a66-45fe-aec3-eb957f511ab2".uuid(),
                NodeOptions::new(Format::Markdown).connections(true)
            )
            .await
            .unwrap()
            .backlinks,
        map! { "7097edb8-7a66-45fe-aec3-eb957f511ab1".uuid() => NodeConnection {
            title: "Node 1".into(),
            types: ["link"].into_hs()
        }}
    );
    // And the invalid connections should be gone from file 2
    assert_eq!(
        graph.errors(&PathBuf::from("file_2.md")).await,
        Some(Ok(Vec::new()))
    );
}

#[tokio::test]
async fn changed_connections_should_register() {
    setup_config();

    let file_1 = r#"---
title: File 1
---
<!--PROPERTIES
ID: 7097edb8-7a66-45fe-aec3-eb957f511ab0
-->

# Node 1
<!--PROPERTIES
ID: 7097edb8-7a66-45fe-aec3-eb957f511ab1
-->

This is a test file. Here's [File 2](link:7097edb8-7a66-45fe-aec3-eb957f511ab2)."#;
    let file_2 = r#"---
title: File 2
---
<!--PROPERTIES
ID: 7097edb8-7a66-45fe-aec3-eb957f511ab2
-->

Here's [Node 1](link:7097edb8-7a66-45fe-aec3-eb957f511ab1)."#;

    let graph = Graph::new();
    graph
        .process_fs_patch(GraphPatch {
            renames: Vec::new(),
            deletions: Vec::new(),
            creations: vec![
                PathPatch {
                    path: PathBuf::from("file_1.md"),
                    contents_res: Ok(file_1.into()),
                },
                PathPatch {
                    path: PathBuf::from("file_2.md"),
                    contents_res: Ok(file_2.into()),
                },
            ],
            modifications: Vec::new(),
        })
        .await;
    let file_1_data = graph
        .get_node(
            "7097edb8-7a66-45fe-aec3-eb957f511ab0".uuid(),
            NodeOptions::new(Format::Markdown).connections(true),
        )
        .await
        .unwrap();
    let node_1_data = graph
        .get_node(
            "7097edb8-7a66-45fe-aec3-eb957f511ab1".uuid(),
            NodeOptions::new(Format::Markdown).connections(true),
        )
        .await
        .unwrap();
    let file_2_data = graph
        .get_node(
            "7097edb8-7a66-45fe-aec3-eb957f511ab2".uuid(),
            NodeOptions::new(Format::Markdown).connections(true),
        )
        .await
        .unwrap();

    // No connections/backlinks for the root of file 1
    assert!(file_1_data.connections.is_empty());
    assert!(file_1_data.backlinks.is_empty());
    // Node 1 connects to, and is connected to by, file 2
    assert_eq!(
        node_1_data.connections,
        map! {
            "7097edb8-7a66-45fe-aec3-eb957f511ab2".uuid() => NodeConnection {
                title: "File 2".into(),
                types: ["link"].into_hs()
            }
        }
    );
    assert_eq!(
        node_1_data.backlinks,
        map! {
            "7097edb8-7a66-45fe-aec3-eb957f511ab2".uuid() => NodeConnection {
                title: "File 2".into(),
                types: ["link"].into_hs()
            }
        }
    );
    assert_eq!(
        file_2_data.connections,
        map! {
            "7097edb8-7a66-45fe-aec3-eb957f511ab1".uuid() => NodeConnection {
                title: "Node 1".into(),
                types: ["link"].into_hs()
            }
        }
    );
    assert_eq!(
        file_2_data.backlinks,
        map! {
            "7097edb8-7a66-45fe-aec3-eb957f511ab1".uuid() => NodeConnection {
                title: "Node 1".into(),
                types: ["link"].into_hs()
            }
        }
    );

    let file_1_updated = r#"---
title: File 1
---
<!--PROPERTIES
ID: 7097edb8-7a66-45fe-aec3-eb957f511ab0
-->

# Node 1
<!--PROPERTIES
ID: 7097edb8-7a66-45fe-aec3-eb957f511ab1
-->

This is a test file."#;
    let file_2_updated = r#"---
title: File 2
---
<!--PROPERTIES
ID: 7097edb8-7a66-45fe-aec3-eb957f511ab2
-->

Here's [Node 1](link:7097edb8-7a66-45fe-aec3-eb957f511ab1). And here's [some file](7097edb8-7a66-45fe-aec3-eb957f511ab0)."#;

    let writes = graph
        .process_fs_patch(GraphPatch {
            renames: Vec::new(),
            deletions: Vec::new(),
            creations: Vec::new(),
            modifications: vec![
                PathPatch {
                    path: PathBuf::from("file_1.md"),
                    contents_res: Ok(file_1_updated.into()),
                },
                PathPatch {
                    path: PathBuf::from("file_2.md"),
                    contents_res: Ok(file_2_updated.into()),
                },
            ],
        })
        .await
        .into_iter()
        .map(|w| (w.path, w.contents))
        .collect::<HashMap<_, _>>();
    // We've changed connections in file 2, but only removed one in file 1
    assert!(!writes.contains_key(&PathBuf::from("file_1.md")));
    assert_eq!(
        writes.get(&PathBuf::from("file_2.md")).unwrap(),
        &file_2_updated.replace("[some file](7", "[File 1](link:7")
    );

    let file_1_data = graph
        .get_node(
            "7097edb8-7a66-45fe-aec3-eb957f511ab0".uuid(),
            NodeOptions::new(Format::Markdown).connections(true),
        )
        .await
        .unwrap();
    let node_1_data = graph
        .get_node(
            "7097edb8-7a66-45fe-aec3-eb957f511ab1".uuid(),
            NodeOptions::new(Format::Markdown).connections(true),
        )
        .await
        .unwrap();
    let file_2_data = graph
        .get_node(
            "7097edb8-7a66-45fe-aec3-eb957f511ab2".uuid(),
            NodeOptions::new(Format::Markdown).connections(true),
        )
        .await
        .unwrap();

    // File 1 is now connected to by file 2
    assert!(file_1_data.connections.is_empty());
    assert_eq!(
        file_1_data.backlinks,
        map! {
            "7097edb8-7a66-45fe-aec3-eb957f511ab2".uuid() => NodeConnection {
                title: "File 2".into(),
                types: ["link"].into_hs()
            }
        }
    );
    // Node 1 is connected to by file 2, and now connects nowhere itself
    assert!(node_1_data.connections.is_empty());
    assert_eq!(
        node_1_data.backlinks,
        map! {
            "7097edb8-7a66-45fe-aec3-eb957f511ab2".uuid() => NodeConnection {
                title: "File 2".into(),
                types: ["link"].into_hs()
            }
        }
    );
    assert_eq!(
        file_2_data.connections,
        map! {
            "7097edb8-7a66-45fe-aec3-eb957f511ab0".uuid() => NodeConnection {
                title: "File 1".into(),
                types: ["link"].into_hs()
            },
            "7097edb8-7a66-45fe-aec3-eb957f511ab1".uuid() => NodeConnection {
                title: "Node 1".into(),
                types: ["link"].into_hs()
            }
        }
    );
    assert!(file_2_data.backlinks.is_empty());
}
