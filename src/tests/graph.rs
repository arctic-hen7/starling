use crate::{
    config::{Config, STARLING_CONFIG},
    graph::*,
    patch::{GraphPatch, PathPatch},
};
use std::path::PathBuf;

#[tokio::test]
async fn should_work() {
    let mut config = Config::default();
    config.link_types.push("diff".to_string());
    config.tags.extend(
        ["root_tag", "parent_tag", "child_tag"]
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

# Node 1
<!--PROPERTIES
ID: 5d93b936-5952-4707-89dd-69ca06c60851
-->

Here's a link to [File 2](5d93b936-5952-4707-89dd-69ca06c60855).

## Node 1.1
<!--PROPERTIES
ID: 5d93b936-5952-4707-89dd-69ca06c60852
-->

This is a link to [File 1](5d93b936-5952-4707-89dd-69ca06c60850).

### Node 1.1.1
<!--PROPERTIES
ID: 5d93b936-5952-4707-89dd-69ca06c60853
-->

Here's a link to a [nonexistent node](6d93b936-5952-4707-89dd-69ca06c60854). But [this](other:5d93b936-5952-4707-89dd-69ca06c60852) one should work, even though it's of a different type. Links like [this](https://example.com) won't be indexed, and ones with invalid types, [like this](thing:5d93b936-5952-4707-89dd-69ca06c60852), will be ignored.

# Node 2 [has a link](5d93b936-5952-4707-89dd-69ca06c60853)
<!--PROPERTIES
ID: 5d93b936-5952-4707-89dd-69ca06c60854
-->
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
}
