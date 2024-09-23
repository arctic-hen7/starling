use crate::{
    conflict_detector::{Conflict, ConflictDetector, Write, WriteSource},
    debouncer::{DebouncedEvents, Event},
};
use std::path::PathBuf;

/// Creates an out-of-band [`Write`] for the given path with no conflicts or contents for testing.
fn other_write(path: &str) -> Write {
    Write {
        path: PathBuf::from(path),
        contents: String::new(),
        source: WriteSource::Other,
        conflict: Conflict::None,
    }
}

/// Creates an filesystem [`Write`] for the given path with no conflicts or contents for testing.
fn fs_write(path: &str) -> Write {
    Write {
        path: PathBuf::from(path),
        contents: String::new(),
        source: WriteSource::Filesystem,
        conflict: Conflict::None,
    }
}

/// Creates an out-of-band [`Write`] for the given path with no contents, but a simple conflict for
/// testing.
fn other_conflict(path: &str) -> Write {
    Write {
        path: PathBuf::from(path),
        contents: String::new(),
        source: WriteSource::Other,
        conflict: Conflict::Simple,
    }
}

#[test]
fn should_detect_conflicts() {
    let mut cd = ConflictDetector::new();

    let p1 = cd.register_update();

    let p1_writes = vec![other_write("foo"), other_write("bar")];

    cd.add_patch(DebouncedEvents::from_sequential(
        vec![Event::Modify(PathBuf::from("foo"))].into_iter(),
    ));

    // One of the paths p1 wanted to write to conflicted
    assert_eq!(
        cd.detect_conflicts(p1, p1_writes),
        vec![other_conflict("foo"), other_write("bar")]
    );
}

#[test]
fn rename_should_move_write() {
    let mut cd = ConflictDetector::new();

    let p1 = cd.register_update();
    let p1_writes = vec![other_write("foo")];

    cd.add_patch(DebouncedEvents::from_sequential(
        vec![
            Event::Rename(PathBuf::from("foo"), PathBuf::from("bar")),
            Event::Rename(PathBuf::from("bar"), PathBuf::from("baz")),
        ]
        .into_iter(),
    ));

    assert_eq!(cd.detect_conflicts(p1, p1_writes), vec![other_write("baz")]);
}

#[test]
fn deletion_should_drop_write() {
    let mut cd = ConflictDetector::new();

    let p1 = cd.register_update();
    let p1_writes = vec![other_write("foo")];

    cd.add_patch(DebouncedEvents::from_sequential(
        vec![Event::Delete(PathBuf::from("foo"))].into_iter(),
    ));

    assert_eq!(cd.detect_conflicts(p1, p1_writes), Vec::new());
}

#[test]
fn fs_writes_should_be_dropped_on_conflict() {
    let mut cd = ConflictDetector::new();

    let p1 = cd.register_update();
    let p1_writes = vec![other_write("foo"), other_write("unrelated")];

    let patch_1_events = vec![Event::Modify(PathBuf::from("foo"))];
    let patch_1_writes = vec![fs_write("foo"), fs_write("unrelated")];
    let patch_2_events = vec![Event::Modify(PathBuf::from("bar"))];
    let patch_2_writes = vec![fs_write("bar"), fs_write("unrelated")];

    // First patch completes before the update, no conflicts
    let patch_1 = cd.add_patch(DebouncedEvents::from_sequential(patch_1_events.into_iter()));
    assert_eq!(
        cd.detect_conflicts(patch_1, patch_1_writes.clone()),
        patch_1_writes
    );

    let patch_2 = cd.add_patch(DebouncedEvents::from_sequential(patch_2_events.into_iter()));
    // One conflict with the previous patch
    assert_eq!(
        cd.detect_conflicts(p1, p1_writes.clone()),
        vec![other_conflict("foo"), other_write("unrelated")]
    );
    // Patch 2 started, and then p1 completed in the middle, so one corrective write gets dropped
    assert_eq!(
        cd.detect_conflicts(patch_2, patch_2_writes),
        vec![fs_write("bar")]
    );
}

#[test]
fn rolling_conflict_detection_should_work() {
    let mut cd = ConflictDetector::new();

    // Create two patches, the second before the first has completed
    let patch_1_events = vec![Event::Modify(PathBuf::from("foo"))];
    let patch_1 = cd.add_patch(DebouncedEvents::from_sequential(patch_1_events.into_iter()));
    let patch_2_events = vec![
        Event::Rename(PathBuf::from("foo"), PathBuf::from("bar")),
        Event::Delete(PathBuf::from("baz")),
        Event::Modify(PathBuf::from("qux")),
    ];
    let patch_2 = cd.add_patch(DebouncedEvents::from_sequential(patch_2_events.into_iter()));

    // Complete the first patch (conflicts should be dropped and the rename applied)
    let patch_1_writes = vec![fs_write("foo"), fs_write("baz"), fs_write("qux")];
    assert_eq!(
        cd.detect_conflicts(patch_1, patch_1_writes),
        vec![fs_write("bar")]
    );

    // Complete the second patch
    let patch_2_writes = vec![fs_write("bar"), fs_write("qux")];
    assert_eq!(
        cd.detect_conflicts(patch_2, patch_2_writes.clone()),
        patch_2_writes
    );
}

#[test]
fn sequential_patches_should_not_interfere() {
    let mut cd = ConflictDetector::new();

    // Create two patches, the second before the first has completed
    let patch_1_events = vec![Event::Modify(PathBuf::from("foo"))];
    let patch_1 = cd.add_patch(DebouncedEvents::from_sequential(patch_1_events.into_iter()));

    // Complete the first patch
    let patch_1_writes = vec![fs_write("foo"), fs_write("bar")];
    assert_eq!(
        cd.detect_conflicts(patch_1, patch_1_writes.clone()),
        patch_1_writes
    );

    let patch_2_events = vec![Event::Modify(PathBuf::from("bar"))];
    let patch_2 = cd.add_patch(DebouncedEvents::from_sequential(patch_2_events.into_iter()));

    // Complete the second patch
    let patch_2_writes = vec![fs_write("bar"), fs_write("baz")];
    assert_eq!(
        cd.detect_conflicts(patch_2, patch_2_writes.clone()),
        patch_2_writes
    );
}

#[test]
fn rename_then_recreate_should_rename() {
    let mut cd = ConflictDetector::new();

    let p1 = cd.register_update();
    let p1_writes = vec![other_write("foo")];

    cd.add_patch(DebouncedEvents::from_sequential(
        vec![
            Event::Rename(PathBuf::from("foo"), PathBuf::from("bar")),
            Event::Create(PathBuf::from("foo")),
        ]
        .into_iter(),
    ));

    assert_eq!(cd.detect_conflicts(p1, p1_writes), vec![other_write("bar")]);
}

#[test]
fn nightmare_rename_should_be_detected() {
    let mut cd = ConflictDetector::new();

    let p1 = cd.register_update();
    let p1_writes = vec![other_write("foo")];

    cd.add_patch(DebouncedEvents::from_sequential(
        vec![
            Event::Rename(PathBuf::from("foo"), PathBuf::from("bar")),
            Event::Create(PathBuf::from("foo")),
            Event::Rename(PathBuf::from("foo"), PathBuf::from("baz")),
        ]
        .into_iter(),
    ));

    assert_eq!(
        cd.detect_conflicts(p1, p1_writes),
        vec![Write {
            path: PathBuf::from("foo"),
            contents: String::new(),
            source: WriteSource::Other,
            conflict: Conflict::Multi(["bar".into(), "baz".into()].into()),
        }]
    );
}
