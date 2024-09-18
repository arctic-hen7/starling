use crate::debouncer::*;
use std::{collections::HashSet, path::PathBuf};

/// A categorised version of [`DebouncedEvents`] for easier testing.
struct DebouncedCategories {
    renames: Vec<(PathBuf, PathBuf)>,
    creations: Vec<PathBuf>,
    deletions: Vec<PathBuf>,
    modifications: Vec<PathBuf>,
}
impl DebouncedCategories {
    fn from_debounced(debounced: DebouncedEvents) -> Self {
        let mut renames = Vec::new();
        let mut creations = Vec::new();
        let mut deletions = Vec::new();
        let mut modifications = Vec::new();
        for (new_path, old_path, event) in debounced.into_iter() {
            // If there's an old path, we have a rename
            if let Some(old_path) = old_path {
                renames.push((old_path, new_path.clone()));
            }

            // If we have an event, push it, using the new path (renames will be actioned first by
            // the caller)
            if let Some(event) = event {
                match event {
                    Event::Delete(_) => deletions.push(new_path),
                    Event::Create(_) => creations.push(new_path),
                    Event::Modify(_) => modifications.push(new_path),
                    Event::Rename(_, _) => unreachable!(),
                }
            }
        }
        Self {
            renames,
            creations,
            deletions,
            modifications,
        }
    }
}

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
    let debounced =
        DebouncedCategories::from_debounced(DebouncedEvents::from_sequential(events.into_iter()));

    assert_eq!(
        debounced.renames,
        vec![(PathBuf::from("foo"), PathBuf::from("bar"))]
    );
    assert!(debounced.modifications.is_empty());
    assert!(debounced.creations.is_empty());
    assert_eq!(
        debounced.deletions.into_iter().collect::<HashSet<_>>(),
        // Both get noted for clarity
        [PathBuf::from("foo"), PathBuf::from("bar")].into()
    );
}

#[test]
fn should_collapse_renames() {
    let events = vec![
        Event::Create(PathBuf::from("foo")),
        Event::Rename(PathBuf::from("foo"), PathBuf::from("bar")),
        Event::Rename(PathBuf::from("bar"), PathBuf::from("baz")),
        Event::Rename(PathBuf::from("baz"), PathBuf::from("qux")),
    ];
    let debounced =
        DebouncedCategories::from_debounced(DebouncedEvents::from_sequential(events.into_iter()));

    assert_eq!(
        debounced.renames,
        vec![(PathBuf::from("foo"), PathBuf::from("qux"))]
    );
    assert!(debounced.modifications.is_empty());
    assert_eq!(debounced.creations, vec![PathBuf::from("qux")]);
    assert!(debounced.deletions.is_empty());
}

#[test]
fn should_handle_rename_with_modify() {
    let events = vec![
        Event::Rename(PathBuf::from("foo"), PathBuf::from("bar")),
        Event::Modify(PathBuf::from("bar")),
        Event::Rename(PathBuf::from("bar"), PathBuf::from("baz")),
    ];
    let debounced =
        DebouncedCategories::from_debounced(DebouncedEvents::from_sequential(events.into_iter()));

    assert_eq!(
        debounced.renames,
        vec![(PathBuf::from("foo"), PathBuf::from("baz"))]
    );
    assert!(debounced.creations.is_empty());
    assert_eq!(debounced.modifications, vec![PathBuf::from("baz")]);
    assert!(debounced.deletions.is_empty());
}

#[test]
fn should_combine_correctly() {
    let events_1 = vec![
        Event::Create(PathBuf::from("foo")),
        Event::Rename(PathBuf::from("foo"), PathBuf::from("bar")),
    ];
    let events_2 = vec![
        Event::Modify(PathBuf::from("bar")),
        Event::Rename(PathBuf::from("bar"), PathBuf::from("baz")),
    ];

    let mut debounced_1 = DebouncedEvents::from_sequential(events_1.into_iter());
    let debounced_2 = DebouncedEvents::from_sequential(events_2.into_iter());
    debounced_1.combine(&debounced_2);
    let debounced = DebouncedCategories::from_debounced(debounced_1);

    // Creation is the critical one, but rename will still be preserved
    assert_eq!(debounced.creations, vec![PathBuf::from("baz")]);
    assert_eq!(
        debounced.renames,
        vec![(PathBuf::from("foo"), PathBuf::from("baz"))]
    );
    assert!(debounced.modifications.is_empty());
    assert!(debounced.deletions.is_empty());
}

// TODO: More tests
