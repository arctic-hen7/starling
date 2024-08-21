use std::path::PathBuf;

use crate::debouncer::*;

#[test]
fn should_collapse_create_delete() {
    let events = vec![
        Event::Create(PathBuf::from("foo")),
        Event::Rename(PathBuf::from("foo"), PathBuf::from("bar")),
        Event::Delete(PathBuf::from("bar")),
        // This is a deletion of a different file, the old `foo` was renamed (nonsensical in real
        // life, but illustrates the point)
        Event::Delete(PathBuf::from("foo")),
    ];
    let debounced = DebouncedEvents::from_sequential(events);
    assert!(debounced.renames.is_empty());
    assert!(debounced.modifications.is_empty());
    assert!(debounced.creations.is_empty());
    assert_eq!(debounced.deletions, vec![PathBuf::from("foo")]);
}

#[test]
fn should_collapse_renames() {
    let events = vec![
        Event::Create(PathBuf::from("foo")),
        Event::Rename(PathBuf::from("foo"), PathBuf::from("bar")),
        Event::Rename(PathBuf::from("bar"), PathBuf::from("baz")),
        Event::Rename(PathBuf::from("baz"), PathBuf::from("qux")),
    ];
    let debounced = DebouncedEvents::from_sequential(events);
    assert!(debounced.renames.is_empty());
    assert!(debounced.modifications.is_empty());
    assert_eq!(debounced.creations, vec![PathBuf::from("qux")]);
    assert!(debounced.deletions.is_empty());
}

// TODO: More tests
