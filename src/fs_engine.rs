use crate::{
    config::STARLING_CONFIG,
    conflict_detector::{Conflict, ConflictDetector, Write},
    debouncer::{DebouncedEvents, Event},
    graph::Graph,
    patch::GraphPatch,
};
use crossbeam_queue::SegQueue;
use futures::{future::join_all, Future};
use notify::{
    event::{CreateKind, ModifyKind},
    EventKind as NotifyEvent, RecommendedWatcher, RecursiveMode, Watcher,
};
use std::{collections::HashSet, path::PathBuf, sync::Arc, time::Duration};
use tokio::{select, sync::mpsc};
use tracing::{error, warn};

/// The engine that powers Starling's filesystem interactions. This is responsible for monitoring
/// and debouncing filesystem changes, developing them into patches, and actioning them within the
/// main [`Graph`]. This is also responsible for handling writes, be they from API-triggered
/// updates or from updates made in processing of another update from the filesystem, making it
/// further responsible for bidirectional synchronisation and the prevention of conflicts.
pub struct FsEngine {
    /// The core graph in this Starling instance, shared between this and the server.
    graph: Arc<Graph>,
    /// A conflict detector that stores details about all the events that have occurred since each
    /// filesystem patch. This can be used by out-of-band modifications to declare the point in
    /// time past which they might have conflicts, which can be used to detect conflicts once they
    /// send in their writes.
    conflict_detector: ConflictDetector,
    /// A lock-free queue of writes and the patch numbers they could conflict with. This queue will
    /// be drained when the filesystem is quiet, and the writes will be actioned.
    writes_queue: Arc<SegQueue<(Vec<Write>, u32)>>,
    /// A number of millseconds after which, if there have been no filesystem events, the evnets
    /// received will be actioned.
    debounce_duration: u64,
    watcher: Option<RecommendedWatcher>,
}
impl FsEngine {
    /// Create a new filesystem engine to handle the given graph, which should already have been
    /// instantiated. This also takes some initial corrective writes.
    pub fn new(graph: Arc<Graph>, writes: Vec<Write>) -> Self {
        // Create our conflict detector and register the initial writes as an update (even though
        // the probability of conflicts is near zero at application start)
        let mut conflict_detector = ConflictDetector::new();
        let patch_idx = conflict_detector.register_update();
        let writes_queue = SegQueue::new();
        writes_queue.push((writes, patch_idx));

        Self {
            graph,
            debounce_duration: STARLING_CONFIG.get().debounce_duration,
            conflict_detector,
            writes_queue: Arc::new(writes_queue),
            watcher: None,
        }
    }
    /// Start the filesystem engine, monitoring the filesystem for changes and updating the graph
    /// accordingly. The future this returns will run forever, and should be spawned on its own
    /// task.
    ///
    /// This takes the same directory as the graph started on.
    pub fn run(mut self, dir: PathBuf) -> Result<impl Future<Output = ()> + Send, notify::Error> {
        assert!(dir.is_dir());

        let (tx, mut rx) = mpsc::unbounded_channel();
        let mut watcher =
            notify::recommended_watcher(move |ev: Result<notify::Event, notify::Error>| {
                if let Ok(ev) = ev {
                    if ev.need_rescan() {
                        // The watcher backend missed something, we need to rescan *everything*
                        let _ = tx.send(None);
                    }

                    // If we couldn't send over the channel, the main engine has gone down, and so
                    // will we imminently
                    let _ = match ev.kind {
                        NotifyEvent::Create(create_kind) => match create_kind {
                            // If we're told this is a folder, ignore it
                            CreateKind::Folder => Ok(()),
                            // But if it's definitely a file, or if we're unsure, let the path
                            // patch system handle it
                            _ => tx.send(Some(Event::Create(ev.paths[0].clone()))),
                        },
                        NotifyEvent::Modify(modify_kind) => match modify_kind {
                            ModifyKind::Data(_) | ModifyKind::Any | ModifyKind::Other => {
                                tx.send(Some(Event::Modify(ev.paths[0].clone())))
                            }
                            // We don't need to do anything for a metadata change
                            ModifyKind::Metadata(_) => Ok(()),
                            // We technically don't know if both paths will be present if the
                            // notifier hasn't stitched them together, but we'll find out!
                            ModifyKind::Name(_) => tx.send(Some(Event::Rename(
                                ev.paths[0].clone(),
                                ev.paths[1].clone(),
                            ))),
                        },
                        NotifyEvent::Remove(_) => tx.send(Some(Event::Delete(ev.paths[0].clone()))),

                        // Non-modifying accesses don't concern us
                        NotifyEvent::Access(_) => Ok(()),
                        // We can't really do anything with these...
                        NotifyEvent::Any | NotifyEvent::Other => Ok(()),
                    };
                }
            })
            .unwrap();
        // If watching the directory fails, we'll error before the future so the user can handle
        // this immediately
        watcher.watch(&dir, RecursiveMode::Recursive)?;

        Ok(async move {
            self.watcher = Some(watcher);

            // This will hold an `AbortHandle` for the task that develops an I/O-resolved patch to
            // send to the graph
            let mut patch_task = None;
            // Set of paths we've just written to do make sure we don't detect our own
            // modifications to them (infinite loops)
            let mut self_writes = HashSet::new();

            let mut debounced_events = DebouncedEvents::new();
            loop {
                select! {
                    _ = tokio::time::sleep(Duration::from_millis(self.debounce_duration)) => {
                        // The timer elapsed before we received another event, let's check if
                        // there's already a patch task running (if so, it wasn't cancelled by a
                        // new event, so we can't have any new events, so we should do nothing)
                        // and, if there isn't, start one.
                        //
                        // We check only `None`, not `Some(handle) if handle.is_finished()` because
                        // if the previous task finishes, and we have a handle, then there hasn't
                        // been an intermediate event to set it back to `None`, so there's no point
                        // in starting another patch processor.
                        if patch_task.is_none() {
                            // Record that a new patch is starting for the conflict detector
                            let patch_idx = self.conflict_detector.add_patch(debounced_events.clone());

                            let debounced_events_clone = debounced_events.clone();
                            let graph = self.graph.clone();
                            let writes_queue = self.writes_queue.clone();
                            patch_task = Some(tokio::spawn(async move {
                                let patch = GraphPatch::from_events(debounced_events_clone).await;

                                // Hand off the graph processing to another task (it's *not*
                                // cancel-safe, and there's no need to cancel it, many of these can
                                // run simultaneously)
                                tokio::spawn(async move {
                                    let writes = graph.process_fs_patch(patch).await;
                                    writes_queue.push((writes, patch_idx));
                                });
                            }));
                        }

                        // We're in a lull of filesystem events, let's see if previous patches have
                        // resolved, or if any out-of-band modifications have sent writes through.
                        // We'll do this by draining the lock-free queue of writes we have, where
                        // each one has an associated index for a patch it depends on (i.e. it
                        // could conflict with any events in or after that patch). Conveniently, we
                        // have a `ConflictDetector` that can handle all of this for us! Note that
                        // it doesn't matter if the patch we depend on hasn't happened yet, because
                        // we'll have all the events that have occurred up until *now* and we'll
                        // write these in a moment, so there won't be any more conflicts we can do
                        // anything about.
                        let mut write_futs = Vec::new();
                        while let Some((writes, patch_idx)) = self.writes_queue.pop() {
                            let updated_writes =
                                self.conflict_detector.detect_conflicts(patch_idx, writes);
                            for write in updated_writes {
                                match write.conflict {
                                    Conflict::None => {
                                        write_futs.push(
                                            tokio::fs::write(write.path.clone(), write.contents)
                                        );
                                        // Record that we've written to this path
                                        self_writes.insert(write.path);
                                    },
                                    Conflict::Simple => {
                                        // The modification in `write.contents` conflicts with the
                                        // state on the disk
                                        eprintln!("[ERROR]: Conflict in '{:?}'", write.path);
                                    }
                                    Conflict::Multi(paths) => {
                                        // The path we want to write to was renamed, recreated, and
                                        // renamed to somewhere else at least once, meaning we
                                        // don't know where to send our modification
                                        eprintln!(
                                            "[ERROR]: Conflict with write to '{:?}', could go to any of {:?}",
                                            write.path,
                                            paths
                                        );
                                    }
                                }
                            }
                        }
                        // Action all those writes (yes, a conflict could occur during this, but
                        // there's nothing we can possibly do about that)
                        join_all(write_futs).await;
                    },
                    res = rx.recv() => {
                        // Receiving an event means any partly or fully developed I/O patches have
                        // to be cancelled; we'll take account of the new modifications first.
                        // Previous events are saved in `debounced_events`.
                        if let Some(patch_task) = patch_task.take() {
                            if patch_task.is_finished() {
                                // The timer doesn't care if it sees a finished patch developed,
                                // that tells it there hasn't been another event. We're the only
                                // ones who can really observe this, and we should ensure we aren't
                                // accumulating pointlessly on already-handled events.
                                debounced_events = DebouncedEvents::new();
                            } else {
                                // We've aborted *and* set the handle to `None`, meaning that's a
                                // reliable signal
                                patch_task.abort();
                            }
                        }

                        if let Some(event_opt) = res {
                            if let Some(event) = event_opt {
                                // Debounce in real time because it's fast and ensures we have a
                                // map of paths to events. Be sure *not* to record this if this was
                                // a path we just wrote to though, to prevent infinite loops.
                                if self_writes.remove(event.path()) {
                                    // On modification (what we expect), block the event; otherwise
                                    // allow the event through (but we really should have seen a
                                    // modification first, so a bit weird)
                                    match event {
                                        Event::Modify(_) => continue,
                                        _ => warn!(
                                            "saw non-modification on self-write"
                                        )
                                    }
                                }
                                debounced_events.push(event);
                            } else {
                                // We need to rescan everything
                                todo!()
                            }
                        } else {
                            // The file notifying thread has gone down, which shouldn't happen
                            // without our go-ahead, so this is a critical error and we should
                            // terminate
                            error!("file notifier thread went down unexpectedly");
                            break;
                        }
                    },
                };
            }
        })
    }
}
