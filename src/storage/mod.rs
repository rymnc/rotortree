mod error;
mod recovery;
mod token;
mod wal;

use std::{
    io::Write,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{
            AtomicBool,
            AtomicUsize,
            Ordering,
        },
    },
    time::Duration,
};

use arc_swap::ArcSwap;
use parking_lot::Mutex;

use crate::{
    Hash,
    Hasher,
    LeanIMT,
    tree::{
        TreeInner,
        TreeSnapshot,
    },
};

pub use error::{
    RotorTreeError,
    StorageError,
};
pub use token::DurabilityToken;

use recovery::RecoveryResult;
use token::DurabilityTracker;

/// Configuration for opening a `RotorTree`
pub struct RotorTreeConfig {
    /// Directory path where the WAL file will be stored
    pub path: PathBuf,
    /// Controls when WAL entries are fsynced to disk
    pub flush_policy: FlushPolicy,
}

/// Controls when buffered WAL entries are fsynced to disk
pub enum FlushPolicy {
    /// Fsync on a periodic interval (default: 10ms)
    Interval(Duration),
    /// Fsync after every N buffered entries
    BatchSize(usize),
    /// Caller controls flushing via `flush()`
    Manual,
}

impl Default for FlushPolicy {
    fn default() -> Self {
        Self::Interval(Duration::from_millis(10))
    }
}

/// Internal mutable state
struct DurableState<const N: usize, const MAX_DEPTH: usize> {
    inner: TreeInner<N, MAX_DEPTH>,
    buffer: Vec<u8>,
    next_seq: u64,
}

/// Shared state
struct Shared<H: Hasher, const N: usize, const MAX_DEPTH: usize> {
    hasher: H,
    state: Mutex<DurableState<N, MAX_DEPTH>>,
    wal_file: Mutex<std::fs::File>,
    snapshot: ArcSwap<TreeSnapshot<N, MAX_DEPTH>>,
    durability: DurabilityTracker,
    closed: AtomicBool,
    flush_failed: AtomicBool,
    flush_error: Mutex<Option<Arc<std::io::Error>>>,
    entry_count: AtomicUsize,
}

impl<H: Hasher, const N: usize, const MAX_DEPTH: usize> Shared<H, N, MAX_DEPTH> {
    fn flush_inner(&self) -> Result<(), StorageError> {
        let wal_file = self.wal_file.lock();
        let (buf, last_seq) = {
            let mut state = self.state.lock();
            if state.buffer.is_empty() {
                return Ok(());
            }
            let buf = std::mem::take(&mut state.buffer);
            let last_seq = state.next_seq.saturating_sub(1);
            self.entry_count.store(0, Ordering::Relaxed);
            (buf, last_seq)
        };

        let result = (&*wal_file)
            .write_all(&buf)
            .and_then(|()| wal_file.sync_data());

        match result {
            Ok(()) => {
                // empty buf
                let mut state = self.state.lock();
                if state.buffer.is_empty() {
                    let mut returned = buf;
                    returned.clear();
                    state.buffer = returned;
                }
                self.durability.mark_flushed(last_seq);
                Ok(())
            }
            Err(e) => {
                // put back into buf
                let mut state = self.state.lock();
                if state.buffer.is_empty() {
                    state.buffer = buf;
                } else {
                    let mut combined = buf;
                    combined.extend_from_slice(&state.buffer);
                    state.buffer = combined;
                }
                let err = Arc::new(e);
                *self.flush_error.lock() = Some(Arc::clone(&err));
                self.flush_failed.store(true, Ordering::Relaxed);
                Err(StorageError::FlushFailed(err))
            }
        }
    }

    fn check_closed(&self) -> Result<(), StorageError> {
        if self.closed.load(Ordering::Acquire) {
            return Err(StorageError::Closed);
        }
        if self.flush_failed.load(Ordering::Relaxed) {
            let err = self.flush_error.lock().clone().unwrap_or_else(|| {
                Arc::new(std::io::Error::other("unknown flush error"))
            });
            return Err(StorageError::FlushFailed(err));
        }
        Ok(())
    }
}

/// Handle for the background flush thread
struct FlushHandle {
    handle: std::thread::JoinHandle<()>,
    shutdown: Arc<(Mutex<bool>, parking_lot::Condvar)>,
}

/// Wraps the core tree with persistence via wal
/// insertions go to an in-memory wal buffer, but the state is available for computing on immediately
/// whenever flush() is called, configured via `FlushPolicy`, the buffer is written to the disk
pub struct RotorTree<H: Hasher, const N: usize, const MAX_DEPTH: usize> {
    shared: Arc<Shared<H, N, MAX_DEPTH>>,
    flush_handle: Option<FlushHandle>,
}

impl<H: Hasher, const N: usize, const MAX_DEPTH: usize> RotorTree<H, N, MAX_DEPTH> {
    const _ASSERT_N: () = assert!(N >= 2, "branching factor must be at least 2");
    const _ASSERT_DEPTH: () = assert!(MAX_DEPTH >= 1, "max depth must be at least 1");

    /// Open or create a RotorTree at the given path
    ///
    /// If a pre-existing wal is found, replays it to recover the tree state
    pub fn open(hasher: H, config: RotorTreeConfig) -> Result<Self, RotorTreeError> {
        let () = Self::_ASSERT_N;
        let () = Self::_ASSERT_DEPTH;

        std::fs::create_dir_all(&config.path)?;

        let wal_path = config.path.join("wal");
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&wal_path)?;

        use fs2::FileExt;
        file.try_lock_exclusive()
            .map_err(|_| StorageError::FileLocked)?;

        if file.metadata()?.len() == 0
            && let Some(parent) = wal_path.parent()
        {
            let dir = std::fs::File::open(parent)?;
            dir.sync_all()?;
        }

        let mut file = file;
        let RecoveryResult { inner, next_seq } =
            recovery::recover::<H, _, N, MAX_DEPTH>(&mut file, &hasher)?;

        let snap = inner.snapshot();
        let durability = DurabilityTracker::new();

        if next_seq > 0 {
            durability.mark_flushed(next_seq.checked_sub(1).expect("next_seq > 0; qed"));
        }

        let state = DurableState {
            inner,
            buffer: Vec::new(),
            next_seq,
        };

        let shared = Arc::new(Shared {
            hasher,
            state: Mutex::new(state),
            wal_file: Mutex::new(file),
            snapshot: ArcSwap::from_pointee(snap),
            durability,
            closed: AtomicBool::new(false),
            flush_failed: AtomicBool::new(false),
            flush_error: Mutex::new(None),
            entry_count: AtomicUsize::new(0),
        });

        let flush_handle = start_flush_thread(&shared, &config.flush_policy)?;

        Ok(Self {
            shared,
            flush_handle,
        })
    }

    /// Insert a single leaf
    pub fn insert(&self, leaf: Hash) -> Result<(Hash, DurabilityToken), RotorTreeError> {
        self.shared.check_closed()?;

        let (root, token) = {
            let mut state = self.shared.state.lock();
            let root = LeanIMT::<H, N, MAX_DEPTH>::_insert(
                &mut state.inner,
                &self.shared.hasher,
                leaf,
            )?;
            let seq = state.next_seq;
            wal::serialize_entry(&mut state.buffer, seq, wal::WalPayload::Single(leaf));
            state.next_seq = state.next_seq.checked_add(1).expect("seq overflow; qed");
            self.shared.entry_count.fetch_add(1, Ordering::Relaxed);
            let snap = state.inner.snapshot();
            self.shared.snapshot.store(Arc::new(snap));
            let token = self.shared.durability.token(seq);
            (root, token)
        };

        Ok((root, token))
    }

    /// Insert multiple leaves in a batch
    pub fn insert_many(
        &self,
        leaves: &[Hash],
    ) -> Result<(Hash, DurabilityToken), RotorTreeError> {
        self.shared.check_closed()?;

        let (root, token) = {
            let mut state = self.shared.state.lock();
            let root = LeanIMT::<H, N, MAX_DEPTH>::_insert_many(
                &mut state.inner,
                &self.shared.hasher,
                leaves,
            )?;
            let seq = state.next_seq;
            wal::serialize_entry(
                &mut state.buffer,
                seq,
                wal::WalPayload::Batch(wal::NewCow::Borrowed(leaves)),
            );
            state.next_seq = state.next_seq.checked_add(1).expect("seq overflow; qed");
            self.shared.entry_count.fetch_add(1, Ordering::Relaxed);
            let snap = state.inner.snapshot();
            self.shared.snapshot.store(Arc::new(snap));
            let token = self.shared.durability.token(seq);
            (root, token)
        };

        Ok((root, token))
    }

    /// Insert a single leaf and block until it is durable (fsynced)
    pub fn insert_durable(&self, leaf: Hash) -> Result<Hash, RotorTreeError> {
        let (root, token) = self.insert(leaf)?;
        self.flush().map_err(RotorTreeError::Storage)?;
        token.wait();
        Ok(root)
    }

    /// The current Merkle root, or `None` if the tree is empty
    pub fn root(&self) -> Option<Hash> {
        self.shared.snapshot.load().root
    }

    /// Number of leaves in the tree
    pub fn size(&self) -> u64 {
        self.shared.snapshot.load().size
    }

    /// Current depth (hash layers above the leaf level)
    pub fn depth(&self) -> usize {
        self.shared.snapshot.load().depth
    }

    /// Get a lock-free snapshot for proof generation
    pub fn snapshot(&self) -> Arc<TreeSnapshot<N, MAX_DEPTH>> {
        self.shared.snapshot.load_full()
    }

    /// Flush all buffered WAL entries to disk
    pub fn flush(&self) -> Result<(), StorageError> {
        self.shared.check_closed()?;
        self.shared.flush_inner()
    }

    /// Close the tree
    pub fn close(mut self) -> Result<(), StorageError> {
        self.close_inner()
    }

    fn close_inner(&mut self) -> Result<(), StorageError> {
        if self.shared.closed.swap(true, Ordering::AcqRel) {
            return Ok(());
        }

        if let Some(handle) = self.flush_handle.take() {
            {
                let (lock, cvar) = &*handle.shutdown;
                let mut shutdown = lock.lock();
                *shutdown = true;
                cvar.notify_one();
            }
            let _ = handle.handle.join();
        }

        self.shared.flush_inner()?;

        {
            let file = self.shared.wal_file.lock();
            let _ = fs2::FileExt::unlock(&*file);
        }

        Ok(())
    }
}

fn start_flush_thread<H: Hasher, const N: usize, const MAX_DEPTH: usize>(
    shared: &Arc<Shared<H, N, MAX_DEPTH>>,
    policy: &FlushPolicy,
) -> Result<Option<FlushHandle>, RotorTreeError> {
    match policy {
        FlushPolicy::Manual => Ok(None),
        FlushPolicy::Interval(duration) => {
            let duration = *duration;
            let shared = Arc::clone(shared);
            spawn_flush_thread(move |shutdown| {
                let (lock, cvar) = &*shutdown;
                loop {
                    let mut stop = lock.lock();
                    cvar.wait_for(&mut stop, duration);
                    if *stop {
                        break;
                    }
                    drop(stop);
                    if let Err(e) = shared.flush_inner() {
                        eprintln!("rotortree: background flush error: {e}");
                    }
                }
            })
        }
        FlushPolicy::BatchSize(threshold) => {
            let threshold = *threshold;
            let shared = Arc::clone(shared);
            spawn_flush_thread(move |shutdown| {
                let (lock, cvar) = &*shutdown;
                let check_interval = Duration::from_millis(1);
                loop {
                    let mut stop = lock.lock();
                    cvar.wait_for(&mut stop, check_interval);
                    if *stop {
                        break;
                    }
                    drop(stop);
                    if shared.entry_count.load(Ordering::Relaxed) >= threshold
                        && let Err(e) = shared.flush_inner()
                    {
                        eprintln!("rotortree: background flush error: {e}");
                    }
                }
            })
        }
    }
}

fn spawn_flush_thread<F>(body: F) -> Result<Option<FlushHandle>, RotorTreeError>
where
    F: FnOnce(Arc<(Mutex<bool>, parking_lot::Condvar)>) + Send + 'static,
{
    let shutdown = Arc::new((Mutex::new(false), parking_lot::Condvar::new()));
    let shutdown_clone = Arc::clone(&shutdown);

    let handle = std::thread::Builder::new()
        .name("rotortree-flush".to_string())
        .spawn(move || {
            body(shutdown_clone);
        })
        .map_err(|e| RotorTreeError::Storage(StorageError::Io(e)))?;

    Ok(Some(FlushHandle { handle, shutdown }))
}

impl<H: Hasher, const N: usize, const MAX_DEPTH: usize> Drop
    for RotorTree<H, N, MAX_DEPTH>
{
    fn drop(&mut self) {
        if self.shared.closed.load(Ordering::Acquire) {
            return;
        }
        if std::thread::panicking() {
            self.shared.closed.store(true, Ordering::Release);
            if let Some(handle) = self.flush_handle.take() {
                let (lock, cvar) = &*handle.shutdown;
                let mut shutdown = lock.lock();
                *shutdown = true;
                cvar.notify_one();
                drop(shutdown);
                let _ = handle.handle.join();
            }
            if let Some(file) = self.shared.wal_file.try_lock() {
                let _ = fs2::FileExt::unlock(&*file);
            }
            return;
        }
        if let Err(e) = self.close_inner() {
            eprintln!("rotortree: error during drop: {e}");
        }
    }
}
