//! Tests for parsing a single vertex, independently of any others.

use crate::{
    config::{Config, STARLING_CONFIG},
    connection::{Connection, ConnectionTarget},
    vertex::*,
};
use orgish::Format;
use tempfile::tempdir;

#[tokio::test]
async fn should_parse_simple_markdown_vertex() {
    let mut config = Config::default();
    config.link_types.push("diff".to_string());
    config.tags.extend(
        ["root_tag", "parent_tag", "child_tag"]
            .iter()
            .map(|s| s.to_string()),
    );
    STARLING_CONFIG.set(config);

    let domain = tempdir().unwrap();
    let vertex_path = domain.path().join("vertex.md");
    std::fs::write(
        &vertex_path,
        r#"---
title: "A simple vertex"
tags: 
    - root_tag
---

This is strongly related to [Atoms in flux](this_is_another_vertex).
And here's another different link: [Hello](diff:hello).

# Heading 1 [with a link](voila) :parent_tag:

And here are [some links](some_links) in the content!

## Heading 1.1 :child_tag:

There's also [a link](another_link) in here.
But [this link](othertype:test) won't be registered.

        "#,
    )
    .unwrap();

    let connections = vec![
        Connection {
            target: ConnectionTarget::Unknown("this_is_another_vertex".to_string()),
            // The default link tupe in the default config
            ty: "link".to_string(),
            title: "Atoms in flux".to_string(),
        },
        Connection {
            target: ConnectionTarget::Unknown("hello".to_string()),
            ty: "diff".to_string(),
            title: "Hello".to_string(),
        },
        Connection {
            target: ConnectionTarget::Unknown("voila".to_string()),
            ty: "link".to_string(),
            title: "with a link".to_string(),
        },
        Connection {
            target: ConnectionTarget::Unknown("some_links".to_string()),
            ty: "link".to_string(),
            title: "some links".to_string(),
        },
        Connection {
            target: ConnectionTarget::Unknown("another_link".to_string()),
            ty: "link".to_string(),
            title: "a link".to_string(),
        },
    ];

    let vertices = Vertex::many_from_file(&vertex_path, Format::Markdown)
        .await
        .unwrap();
    assert_eq!(vertices.len(), 3);

    // Root vertex
    assert_eq!(vertices[0].title(), "A simple vertex");
    assert_eq!(vertices[0].all_tags().collect::<Vec<_>>(), vec!["root_tag"]);
    assert_eq!(
        vertices[0].connections_out().cloned().collect::<Vec<_>>(),
        connections
    );

    // Heading 1
    assert_eq!(
        vertices[1].title(),
        "A simple vertex/Heading 1 [with a link](voila)"
    );
    assert_eq!(
        vertices[1].all_tags().collect::<Vec<_>>(),
        vec!["parent_tag", "root_tag"]
    );
    assert_eq!(
        vertices[1].connections_out().cloned().collect::<Vec<_>>(),
        connections[2..]
    );

    // Heading 1.1
    assert_eq!(
        vertices[2].title(),
        "A simple vertex/Heading 1 [with a link](voila)/Heading 1.1"
    );
    assert_eq!(
        vertices[2].all_tags().collect::<Vec<_>>(),
        vec!["child_tag", "parent_tag", "root_tag"]
    );
    assert_eq!(
        vertices[2].connections_out().cloned().collect::<Vec<_>>(),
        connections[4..]
    );
}

#[tokio::test]
async fn should_parse_simple_org_vertex() {
    let mut config = Config::default();
    config.link_types.push("diff".to_string());
    config.tags.extend(
        ["root_tag", "parent_tag", "child_tag"]
            .iter()
            .map(|s| s.to_string()),
    );
    STARLING_CONFIG.set(config);

    let domain = tempdir().unwrap();
    let vertex_path = domain.path().join("vertex.org");
    std::fs::write(
        &vertex_path,
        r#"#+title: A simple vertex
#+filetags: :root_tag:

This is strongly related to [[this_is_another_vertex][Atoms in flux]].
And here's another different link: [[diff:hello][Hello]].

* Heading 1 [[voila][with a link]] :parent_tag:

And here are [[some_links][some links]] in the content!

** Heading 1.1 :child_tag:

There's also [[another_link][a link]] in here.
But [[othertype:test][this link]] won't be registered.

        "#,
    )
    .unwrap();

    let connections = vec![
        Connection {
            target: ConnectionTarget::Unknown("this_is_another_vertex".to_string()),
            // The default link tupe in the default config
            ty: "link".to_string(),
            title: "Atoms in flux".to_string(),
        },
        Connection {
            target: ConnectionTarget::Unknown("hello".to_string()),
            ty: "diff".to_string(),
            title: "Hello".to_string(),
        },
        Connection {
            target: ConnectionTarget::Unknown("voila".to_string()),
            ty: "link".to_string(),
            title: "with a link".to_string(),
        },
        Connection {
            target: ConnectionTarget::Unknown("some_links".to_string()),
            ty: "link".to_string(),
            title: "some links".to_string(),
        },
        Connection {
            target: ConnectionTarget::Unknown("another_link".to_string()),
            ty: "link".to_string(),
            title: "a link".to_string(),
        },
    ];

    let vertices = Vertex::many_from_file(&vertex_path, Format::Org)
        .await
        .unwrap();
    assert_eq!(vertices.len(), 3);

    // Root vertex
    assert_eq!(vertices[0].title(), "A simple vertex");
    assert_eq!(vertices[0].all_tags().collect::<Vec<_>>(), vec!["root_tag"]);
    assert_eq!(
        vertices[0].connections_out().cloned().collect::<Vec<_>>(),
        connections
    );

    // Heading 1
    assert_eq!(
        vertices[1].title(),
        "A simple vertex/Heading 1 [[voila][with a link]]"
    );
    assert_eq!(
        vertices[1].all_tags().collect::<Vec<_>>(),
        vec!["parent_tag", "root_tag"]
    );
    assert_eq!(
        vertices[1].connections_out().cloned().collect::<Vec<_>>(),
        connections[2..]
    );

    // Heading 1.1
    assert_eq!(
        vertices[2].title(),
        "A simple vertex/Heading 1 [[voila][with a link]]/Heading 1.1"
    );
    assert_eq!(
        vertices[2].all_tags().collect::<Vec<_>>(),
        vec!["child_tag", "parent_tag", "root_tag"]
    );
    assert_eq!(
        vertices[2].connections_out().cloned().collect::<Vec<_>>(),
        connections[4..]
    );
}
