#[cfg(not(feature = "std"))]
use alloc::{
    boxed::Box,
    sync::Arc,
    vec::Vec,
};
#[cfg(feature = "std")]
use std::{
    sync::Arc,
    vec::Vec,
};

use crate::{
    Hash,
    Hasher,
    TreeError,
};

/// Number of hashes per chunk for structural sharing.
pub(crate) const CHUNK_SIZE: usize = 128;

/// Number of chunks per immutable segment
const CHUNKS_PER_SEGMENT: usize = 256;

/// Number of parents per rayon task
#[cfg(feature = "parallel")]
const PAR_CHUNK_SIZE: usize = 64;

#[cfg(feature = "parallel")]
pub(crate) fn parallel_threshold() -> usize {
    static THRESHOLD: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
    *THRESHOLD.get_or_init(|| {
        std::env::var("ROTORTREE_PARALLEL_THRESHOLD")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(1024)
    })
}

#[cfg(not(feature = "storage"))]
#[derive(Clone)]
pub(crate) struct Chunk(Arc<[Hash; CHUNK_SIZE]>);

#[cfg(not(feature = "storage"))]
impl Chunk {
    #[inline(always)]
    pub(crate) fn as_slice(&self) -> &[Hash; CHUNK_SIZE] {
        &self.0
    }

    #[inline(always)]
    fn make_mut(&mut self) -> &mut [Hash; CHUNK_SIZE] {
        Arc::make_mut(&mut self.0)
    }

    fn new_memory(data: [Hash; CHUNK_SIZE]) -> Self {
        Self(Arc::new(data))
    }

    #[cfg(test)]
    fn ptr_eq(a: &Self, b: &Self) -> bool {
        Arc::ptr_eq(&a.0, &b.0)
    }
}

#[cfg(feature = "storage")]
#[derive(Clone)]
pub(crate) struct Chunk(ChunkInner);

#[cfg(feature = "storage")]
#[derive(Clone)]
enum ChunkInner {
    Memory(Arc<[Hash; CHUNK_SIZE]>),
    Mapped {
        region: Arc<crate::storage::data::MmapRegion>,
        offset: usize,
    },
}

#[cfg(feature = "storage")]
impl Chunk {
    #[inline(always)]
    pub(crate) fn as_slice(&self) -> &[Hash; CHUNK_SIZE] {
        match &self.0 {
            ChunkInner::Memory(arc) => arc,
            ChunkInner::Mapped { region, offset } => {
                // SAFETY: offset validated at construction
                unsafe { &*(region.as_ptr().add(*offset).cast::<[Hash; CHUNK_SIZE]>()) }
            }
        }
    }

    #[inline(always)]
    fn make_mut(&mut self) -> &mut [Hash; CHUNK_SIZE] {
        if matches!(&self.0, ChunkInner::Mapped { .. }) {
            let data = *self.as_slice();
            self.0 = ChunkInner::Memory(Arc::new(data));
        }
        match &mut self.0 {
            ChunkInner::Memory(arc) => Arc::make_mut(arc),
            ChunkInner::Mapped { .. } => unreachable!(),
        }
    }

    fn new_memory(data: [Hash; CHUNK_SIZE]) -> Self {
        Self(ChunkInner::Memory(Arc::new(data)))
    }

    pub(crate) fn new_mapped(
        region: Arc<crate::storage::data::MmapRegion>,
        offset: usize,
    ) -> Self {
        const CHUNK_BYTE_SIZE: usize = CHUNK_SIZE * 32;
        assert!(
            offset + CHUNK_BYTE_SIZE <= region.valid_len(),
            "Chunk::new_mapped: offset {offset} + {CHUNK_BYTE_SIZE} exceeds valid_len {}",
            region.valid_len()
        );
        Self(ChunkInner::Mapped { region, offset })
    }

    #[cfg(test)]
    fn ptr_eq(a: &Self, b: &Self) -> bool {
        match (&a.0, &b.0) {
            (ChunkInner::Memory(a), ChunkInner::Memory(b)) => Arc::ptr_eq(a, b),
            _ => false,
        }
    }
}

/// Returns the number of hash layers above the leaf level.
///
/// - `size <= 1` → 0 (root IS the leaf or tree is empty)
/// - `size == N` → 1
/// - `size == N^k` → k
#[allow(clippy::arithmetic_side_effects)]
#[inline(always)]
fn ceil_log_n(size: u64, n: usize) -> usize {
    if size <= 1 {
        return 0;
    }
    if n.is_power_of_two() {
        let k = n.trailing_zeros();
        let bits = u64::BITS - (size - 1).leading_zeros();
        return bits.div_ceil(k) as usize;
    }
    (size - 1).ilog(n as u64) as usize + 1
}

/// Convert `u64` to `usize`, returning `CapacityExceeded` on
/// failure (relevant on 32-bit platforms).
fn u64_to_usize(val: u64) -> Result<usize, TreeError> {
    usize::try_from(val).map_err(|_| TreeError::CapacityExceeded)
}

/// A single level of the tree stored as segmented chunks plus a
/// fixed-size tail buffer.
#[derive(Clone)]
pub(crate) struct ChunkedLevel {
    /// Immutable segments of committed chunks, shared with snapshots.
    segments: Vec<Arc<[Chunk; CHUNKS_PER_SEGMENT]>>,
    /// Mutable buffer of committed chunks not yet frozen into a segment.
    /// At most `CHUNKS_PER_SEGMENT - 1` items.
    pending: Vec<Chunk>,
    /// Fixed-size tail buffer (partially filled).
    tail: [Hash; CHUNK_SIZE],
    /// Number of valid entries in `tail`.
    tail_len: usize,
    /// Total number of hashes in this level.
    len: usize,
}

impl ChunkedLevel {
    fn new() -> Self {
        Self {
            segments: Vec::new(),
            pending: Vec::new(),
            tail: [[0u8; 32]; CHUNK_SIZE],
            tail_len: 0,
            len: 0,
        }
    }

    /// Total number of committed chunks (segments + pending).
    pub(crate) fn chunk_count(&self) -> usize {
        self.segments.len() * CHUNKS_PER_SEGMENT + self.pending.len()
    }

    /// Resolve a chunk index to a slice reference.
    #[inline(always)]
    fn chunk_slice(&self, chunk_idx: usize) -> &[Hash; CHUNK_SIZE] {
        let committed = self.segments.len() * CHUNKS_PER_SEGMENT;
        if chunk_idx < committed {
            let seg_idx = chunk_idx / CHUNKS_PER_SEGMENT;
            let seg_off = chunk_idx % CHUNKS_PER_SEGMENT;
            self.segments[seg_idx][seg_off].as_slice()
        } else {
            self.pending[chunk_idx - committed].as_slice()
        }
    }

    /// Read a hash at the given index.
    fn get(&self, index: usize) -> Result<Hash, TreeError> {
        let chunk_idx = index / CHUNK_SIZE;
        let offset = index % CHUNK_SIZE;
        if chunk_idx < self.chunk_count() {
            Ok(self.chunk_slice(chunk_idx)[offset])
        } else {
            Ok(self.tail[offset])
        }
    }

    /// Copy a contiguous group of hashes into `out`.
    /// Fast path when the group falls within a single chunk or tail.
    fn get_group(&self, start: usize, count: usize, out: &mut [Hash]) {
        let chunk_idx = start / CHUNK_SIZE;
        let offset = start % CHUNK_SIZE;
        if offset + count <= CHUNK_SIZE {
            let src = if chunk_idx < self.chunk_count() {
                &self.chunk_slice(chunk_idx)[offset..offset + count]
            } else {
                &self.tail[offset..offset + count]
            };
            out[..count].copy_from_slice(src);
        } else {
            for (i, item) in out.iter_mut().enumerate().take(count) {
                *item = self.get(start + i).expect("checked prev; qed");
            }
        }
    }

    /// Write a hash at the given index
    fn set(&mut self, index: usize, value: Hash) -> Result<(), TreeError> {
        while self.len <= index {
            self.push([0u8; 32])?;
        }
        let chunk_idx = index / CHUNK_SIZE;
        let offset = index % CHUNK_SIZE;
        let committed = self.segments.len() * CHUNKS_PER_SEGMENT;
        if chunk_idx < committed {
            let seg_idx = chunk_idx / CHUNKS_PER_SEGMENT;
            let seg_off = chunk_idx % CHUNKS_PER_SEGMENT;
            Arc::make_mut(&mut self.segments[seg_idx])[seg_off].make_mut()[offset] =
                value;
        } else if chunk_idx - committed < self.pending.len() {
            self.pending[chunk_idx - committed].make_mut()[offset] = value;
        } else {
            self.tail[offset] = value;
        }
        Ok(())
    }

    /// Append a hash. Promotes the tail when it reaches
    /// `CHUNK_SIZE`.
    fn push(&mut self, value: Hash) -> Result<(), TreeError> {
        self.tail[self.tail_len] = value;
        self.tail_len = self.tail_len.checked_add(1).ok_or(TreeError::MathError)?;
        self.len = self.len.checked_add(1).ok_or(TreeError::MathError)?;
        if self.tail_len == CHUNK_SIZE {
            self.promote_tail();
        }
        Ok(())
    }

    fn extend(&mut self, values: &[Hash]) -> Result<(), TreeError> {
        if values.is_empty() {
            return Ok(());
        }
        let new_len = self
            .len
            .checked_add(values.len())
            .ok_or(TreeError::MathError)?;
        let mut remaining = values;

        // fill current tail
        if self.tail_len > 0 {
            let space = CHUNK_SIZE - self.tail_len;
            let to_copy = space.min(remaining.len());
            self.tail[self.tail_len..self.tail_len + to_copy]
                .copy_from_slice(&remaining[..to_copy]);
            self.tail_len += to_copy;
            remaining = &remaining[to_copy..];
            if self.tail_len == CHUNK_SIZE {
                self.promote_tail();
            }
        }

        // dont use tail for full chunks
        let full_chunks = remaining.len() / CHUNK_SIZE;
        if full_chunks > 0 {
            for i in 0..full_chunks {
                let start = i * CHUNK_SIZE;
                let chunk: [Hash; CHUNK_SIZE] = remaining[start..start + CHUNK_SIZE]
                    .try_into()
                    .expect("slice len == CHUNK_SIZE; qed");
                self.push_chunk(Chunk::new_memory(chunk));
            }
            remaining = &remaining[full_chunks * CHUNK_SIZE..];
        }

        // tail remainder
        if !remaining.is_empty() {
            self.tail[..remaining.len()].copy_from_slice(remaining);
            self.tail_len = remaining.len();
        }

        self.len = new_len;
        Ok(())
    }

    fn ensure_len(&mut self, target: usize) -> Result<(), TreeError> {
        if self.len >= target {
            return Ok(());
        }
        let needed = target - self.len;

        let tail_space = CHUNK_SIZE - self.tail_len;
        let fill_tail = tail_space.min(needed);
        self.tail_len += fill_tail;
        let mut filled = fill_tail;
        if self.tail_len == CHUNK_SIZE {
            self.promote_tail();
        }

        let remaining = needed - filled;
        let full_chunks = remaining / CHUNK_SIZE;
        if full_chunks > 0 {
            for _ in 0..full_chunks {
                self.push_chunk(Chunk::new_memory([[0u8; 32]; CHUNK_SIZE]));
            }
            filled += full_chunks * CHUNK_SIZE;
        }

        let leftover = needed - filled;
        self.tail_len += leftover;

        self.len = target;
        Ok(())
    }

    /// Promote the full tail into a chunk, freezing pending if full
    fn promote_tail(&mut self) {
        debug_assert_eq!(self.tail_len, CHUNK_SIZE);
        self.push_chunk(Chunk::new_memory(self.tail));
        self.tail = [[0u8; 32]; CHUNK_SIZE];
        self.tail_len = 0;
    }

    /// Push a chunk to pending, freezing into a segment when full
    fn push_chunk(&mut self, chunk: Chunk) {
        self.pending.push(chunk);
        if self.pending.len() == CHUNKS_PER_SEGMENT {
            self.freeze_pending();
        }
    }

    /// Freeze the full pending buffer into an immutable segment
    fn freeze_pending(&mut self) {
        debug_assert_eq!(self.pending.len(), CHUNKS_PER_SEGMENT);
        let pending = core::mem::take(&mut self.pending);
        let boxed_arr: Box<[Chunk; CHUNKS_PER_SEGMENT]> = pending
            .into_boxed_slice()
            .try_into()
            .unwrap_or_else(|_| unreachable!()); // qed
        self.segments.push(Arc::from(boxed_arr));
    }

    /// Collect chunks from index `already` onward
    #[cfg(feature = "storage")]
    pub(crate) fn chunks_since(&self, already: usize) -> Vec<Chunk> {
        let total = self.chunk_count();
        if already >= total {
            return Vec::new();
        }
        let committed = self.segments.len() * CHUNKS_PER_SEGMENT;
        let mut result = Vec::with_capacity(total - already);

        // Collect from segments
        if already < committed {
            let start_seg = already / CHUNKS_PER_SEGMENT;
            let start_off = already % CHUNKS_PER_SEGMENT;
            for (seg_i, segment) in self.segments.iter().enumerate().skip(start_seg) {
                let from = if seg_i == start_seg { start_off } else { 0 };
                for chunk in &segment[from..] {
                    result.push(chunk.clone());
                }
            }
        }

        // Collect from pending
        let pending_start = already.saturating_sub(committed);
        if pending_start < self.pending.len() {
            for chunk in &self.pending[pending_start..] {
                result.push(chunk.clone());
            }
        }

        result
    }

    /// Remap the first `count` chunks to mmap-backed chunks
    #[cfg(feature = "storage")]
    pub(crate) fn remap_chunks(
        &mut self,
        count: usize,
        region: &Arc<crate::storage::data::MmapRegion>,
    ) {
        use crate::storage::checkpoint::CHUNK_BYTE_SIZE;

        let total = self.chunk_count();
        let remap_count = count.min(total);
        if remap_count == 0 {
            return;
        }

        let committed = self.segments.len() * CHUNKS_PER_SEGMENT;
        let mut unmapped: Vec<Chunk> =
            Vec::with_capacity(total.saturating_sub(remap_count));
        for chunk_idx in remap_count..total {
            if chunk_idx < committed {
                let seg_idx = chunk_idx / CHUNKS_PER_SEGMENT;
                let seg_off = chunk_idx % CHUNKS_PER_SEGMENT;
                unmapped.push(self.segments[seg_idx][seg_off].clone());
            } else {
                unmapped.push(self.pending[chunk_idx - committed].clone());
            }
        }

        self.segments.clear();
        self.pending.clear();

        for chunk_idx in 0..remap_count {
            let offset = chunk_idx * CHUNK_BYTE_SIZE;
            self.push_chunk(Chunk::new_mapped(Arc::clone(region), offset));
        }
        for chunk in unmapped {
            self.push_chunk(chunk);
        }
    }

    /// Access the tail buffer
    #[cfg(feature = "storage")]
    pub(crate) fn tail_data(&self) -> &[Hash; CHUNK_SIZE] {
        &self.tail
    }

    #[cfg(test)]
    fn get_chunk(&self, idx: usize) -> &Chunk {
        let committed = self.segments.len() * CHUNKS_PER_SEGMENT;
        if idx < committed {
            &self.segments[idx / CHUNKS_PER_SEGMENT][idx % CHUNKS_PER_SEGMENT]
        } else {
            &self.pending[idx - committed]
        }
    }

    /// Create a snapshot level
    fn snapshot(&self) -> SnapshotLevel {
        SnapshotLevel {
            segments: self.segments.clone(),
            pending: self.pending.clone(),
            tail: self.tail,
            len: self.len,
        }
    }
}

/// Immutable view of a single tree level for proof generation.
pub(crate) struct SnapshotLevel {
    segments: Vec<Arc<[Chunk; CHUNKS_PER_SEGMENT]>>,
    pending: Vec<Chunk>,
    tail: [Hash; CHUNK_SIZE],
    len: usize,
}

impl SnapshotLevel {
    const EMPTY: Self = Self {
        segments: Vec::new(),
        pending: Vec::new(),
        tail: [[0u8; 32]; CHUNK_SIZE],
        len: 0,
    };

    /// Total number of committed chunks (segments + pending).
    fn chunk_count(&self) -> usize {
        self.segments.len() * CHUNKS_PER_SEGMENT + self.pending.len()
    }

    /// Resolve a chunk index to a slice reference.
    #[inline(always)]
    fn chunk_slice(&self, chunk_idx: usize) -> &[Hash; CHUNK_SIZE] {
        let committed = self.segments.len() * CHUNKS_PER_SEGMENT;
        if chunk_idx < committed {
            let seg_idx = chunk_idx / CHUNKS_PER_SEGMENT;
            let seg_off = chunk_idx % CHUNKS_PER_SEGMENT;
            self.segments[seg_idx][seg_off].as_slice()
        } else {
            self.pending[chunk_idx - committed].as_slice()
        }
    }

    pub(crate) fn get(&self, index: usize) -> Result<Hash, TreeError> {
        let chunk_idx = index / CHUNK_SIZE;
        let offset = index % CHUNK_SIZE;
        if chunk_idx < self.chunk_count() {
            Ok(self.chunk_slice(chunk_idx)[offset])
        } else {
            Ok(self.tail[offset])
        }
    }

    pub(crate) fn get_group(&self, start: usize, count: usize, out: &mut [Hash]) {
        let chunk_idx = start / CHUNK_SIZE;
        let offset = start % CHUNK_SIZE;
        if offset + count <= CHUNK_SIZE {
            let src = if chunk_idx < self.chunk_count() {
                &self.chunk_slice(chunk_idx)[offset..offset + count]
            } else {
                &self.tail[offset..offset + count]
            };
            out[..count].copy_from_slice(src);
        } else {
            for (i, item) in out.iter_mut().enumerate().take(count) {
                *item = self.get(start + i).expect("checked prev; qed");
            }
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.len
    }

    /// Get a reference to a chunk by index (test helper).
    #[cfg(test)]
    fn get_chunk(&self, idx: usize) -> &Chunk {
        let committed = self.segments.len() * CHUNKS_PER_SEGMENT;
        if idx < committed {
            &self.segments[idx / CHUNKS_PER_SEGMENT][idx % CHUNKS_PER_SEGMENT]
        } else {
            &self.pending[idx - committed]
        }
    }
}

/// Immutable snapshot of the tree for lock-free reads and proof
/// generation.
pub struct TreeSnapshot<const N: usize, const MAX_DEPTH: usize> {
    pub(crate) levels: [SnapshotLevel; MAX_DEPTH],
    pub(crate) root: Option<Hash>,
    pub(crate) size: u64,
    pub(crate) depth: usize,
}

impl<const N: usize, const MAX_DEPTH: usize> TreeSnapshot<N, MAX_DEPTH> {
    /// The Merkle root, or `None` if the tree is empty.
    pub fn root(&self) -> Option<Hash> {
        self.root
    }

    /// Number of leaves in the snapshot.
    pub fn size(&self) -> u64 {
        self.size
    }

    /// Current depth (hash layers above the leaf level).
    pub fn depth(&self) -> usize {
        self.depth
    }
}

/// Mutable tree state.
pub(crate) struct TreeInner<const N: usize, const MAX_DEPTH: usize> {
    /// Levels 0..depth-1
    pub(crate) levels: [ChunkedLevel; MAX_DEPTH],
    /// The root hash
    pub(crate) root: Option<Hash>,
    pub(crate) size: u64,
    pub(crate) depth: usize,
}

impl<const N: usize, const MAX_DEPTH: usize> TreeInner<N, MAX_DEPTH> {
    pub(crate) fn new() -> Self {
        Self {
            levels: core::array::from_fn(|_| ChunkedLevel::new()),
            root: None,
            size: 0,
            depth: 0,
        }
    }

    /// Replace a level's contents from checkpoint data.
    /// Partitions `chunks` into segments and pending.
    #[cfg(feature = "storage")]
    pub(crate) fn set_level_from_parts(
        &mut self,
        level_idx: usize,
        chunks: Vec<Chunk>,
        tail: [Hash; CHUNK_SIZE],
        tail_len: usize,
        len: usize,
    ) {
        let full_segments = chunks.len() / CHUNKS_PER_SEGMENT;
        let mut segments = Vec::with_capacity(full_segments);
        let mut drain = chunks.into_iter();
        for _ in 0..full_segments {
            let seg: Vec<Chunk> = drain.by_ref().take(CHUNKS_PER_SEGMENT).collect();
            let boxed: Box<[Chunk; CHUNKS_PER_SEGMENT]> = seg
                .into_boxed_slice()
                .try_into()
                .unwrap_or_else(|_| unreachable!());
            segments.push(Arc::from(boxed));
        }
        let pending: Vec<Chunk> = drain.collect();

        self.levels[level_idx] = ChunkedLevel {
            segments,
            pending,
            tail,
            tail_len,
            len,
        };
    }

    /// Recompute the root hash from level 0 data bottom-up
    #[cfg(feature = "storage")]
    pub(crate) fn recompute_root<H: Hasher>(&self, hasher: &H) -> Option<Hash> {
        if self.size == 0 {
            return None;
        }
        if self.size == 1 {
            return self.levels[0].get(0).ok();
        }

        let depth = self.depth;
        let level0_len = self.levels[0].len;
        let mut current: Vec<Hash> = Vec::with_capacity(level0_len);
        for i in 0..level0_len {
            current.push(self.levels[0].get(i).ok()?);
        }

        for _level in 0..depth {
            let len = current.len();
            let num_parents = len.div_ceil(N);

            #[cfg(feature = "parallel")]
            let parents = {
                use rayon::prelude::*;
                if num_parents >= parallel_threshold() {
                    let mut buf = vec![[0u8; 32]; num_parents];
                    buf.par_chunks_mut(PAR_CHUNK_SIZE).enumerate().for_each(
                        |(ci, chunk)| {
                            let base = ci * PAR_CHUNK_SIZE;
                            for (i, slot) in chunk.iter_mut().enumerate() {
                                *slot =
                                    Self::_hash_group(&current, base + i, len, hasher);
                            }
                        },
                    );
                    buf
                } else {
                    (0..num_parents)
                        .map(|parent_idx| {
                            Self::_hash_group(&current, parent_idx, len, hasher)
                        })
                        .collect()
                }
            };

            #[cfg(not(feature = "parallel"))]
            let parents: Vec<Hash> = (0..num_parents)
                .map(|parent_idx| Self::_hash_group(&current, parent_idx, len, hasher))
                .collect();

            current = parents;
        }

        current.first().copied()
    }

    #[cfg(feature = "storage")]
    fn _hash_group<H: Hasher>(
        current: &[Hash],
        parent_idx: usize,
        len: usize,
        hasher: &H,
    ) -> Hash {
        let start = parent_idx * N;
        let end = core::cmp::min(start + N, len);
        let count = end - start;
        if count == 1 {
            current[start]
        } else {
            let mut children = [[0u8; 32]; N];
            children[..count].copy_from_slice(&current[start..end]);
            hasher.hash_children(&children[..count])
        }
    }

    pub(crate) fn snapshot(&self) -> TreeSnapshot<N, MAX_DEPTH> {
        let mut levels = [const { SnapshotLevel::EMPTY }; MAX_DEPTH];
        let snap_count = core::cmp::min(self.depth.saturating_add(1), MAX_DEPTH);
        for (dst, src) in levels.iter_mut().zip(self.levels.iter()).take(snap_count) {
            *dst = src.snapshot();
        }
        TreeSnapshot {
            levels,
            root: self.root,
            size: self.size,
            depth: self.depth,
        }
    }
}

/// An N-ary Lean Incremental Merkle Tree.
///
/// # Type Parameters
///
/// - `H`: Hash function ([`Hasher`])
/// - `N`: Branching factor (compile-time, must be >= 2)
/// - `MAX_DEPTH`: Maximum tree depth (must be >= 1)
pub struct LeanIMT<H: Hasher, const N: usize, const MAX_DEPTH: usize> {
    hasher: H,
    #[cfg(not(feature = "concurrent"))]
    #[cfg_attr(docsrs, doc(cfg(not(feature = "concurrent"))))]
    inner: TreeInner<N, MAX_DEPTH>,
    #[cfg(feature = "concurrent")]
    #[cfg_attr(docsrs, doc(cfg(feature = "concurrent")))]
    inner: parking_lot::Mutex<TreeInner<N, MAX_DEPTH>>,
    #[cfg(feature = "concurrent")]
    #[cfg_attr(docsrs, doc(cfg(feature = "concurrent")))]
    snapshot: arc_swap::ArcSwap<TreeSnapshot<N, MAX_DEPTH>>,
}

impl<H: Hasher, const N: usize, const MAX_DEPTH: usize> LeanIMT<H, N, MAX_DEPTH> {
    const _ASSERT_N: () = assert!(N >= 2, "branching factor must be at least 2");
    const _ASSERT_DEPTH: () = assert!(MAX_DEPTH >= 1, "max depth must be at least 1");

    /// Create a new empty tree.
    #[cfg(not(feature = "concurrent"))]
    pub fn new(hasher: H) -> Self {
        let () = Self::_ASSERT_N;
        let () = Self::_ASSERT_DEPTH;
        Self {
            hasher,
            inner: TreeInner::new(),
        }
    }

    /// Create a new empty tree
    #[cfg(feature = "concurrent")]
    #[cfg_attr(docsrs, doc(cfg(feature = "concurrent")))]
    pub fn new(hasher: H) -> Self {
        let () = Self::_ASSERT_N;
        let () = Self::_ASSERT_DEPTH;
        let inner = TreeInner::new();
        let snap = inner.snapshot();
        Self {
            hasher,
            inner: parking_lot::Mutex::new(inner),
            snapshot: arc_swap::ArcSwap::from_pointee(snap),
        }
    }

    /// Insert a single leaf. Returns the new Merkle root.
    #[cfg(not(feature = "concurrent"))]
    pub fn insert(&mut self, leaf: Hash) -> Result<Hash, TreeError> {
        Self::_insert(&mut self.inner, &self.hasher, leaf)
    }

    /// Insert a single leaf. Returns the new Merkle root.
    #[cfg(feature = "concurrent")]
    #[cfg_attr(docsrs, doc(cfg(feature = "concurrent")))]
    pub fn insert(&self, leaf: Hash) -> Result<Hash, TreeError> {
        let mut inner = self.inner.lock();
        let root = Self::_insert(&mut inner, &self.hasher, leaf)?;
        let snap = inner.snapshot();
        self.snapshot.store(Arc::new(snap));
        Ok(root)
    }

    /// Insert multiple leaves in a batch. Returns the new root.
    #[cfg(not(feature = "concurrent"))]
    pub fn insert_many(&mut self, leaves: &[Hash]) -> Result<Hash, TreeError> {
        Self::_insert_many(&mut self.inner, &self.hasher, leaves)
    }

    /// Insert multiple leaves in a batch. Returns the new root.
    #[cfg(feature = "concurrent")]
    #[cfg_attr(docsrs, doc(cfg(feature = "concurrent")))]
    pub fn insert_many(&self, leaves: &[Hash]) -> Result<Hash, TreeError> {
        let mut inner = self.inner.lock();
        let root = Self::_insert_many(&mut inner, &self.hasher, leaves)?;
        let snap = inner.snapshot();
        self.snapshot.store(Arc::new(snap));
        Ok(root)
    }

    /// The current Merkle root, or `None` if the tree is empty.
    #[cfg(not(feature = "concurrent"))]
    pub fn root(&self) -> Option<Hash> {
        self.inner.root
    }

    /// The current Merkle root, or `None` if the tree is empty.
    #[cfg(feature = "concurrent")]
    #[cfg_attr(docsrs, doc(cfg(feature = "concurrent")))]
    pub fn root(&self) -> Option<Hash> {
        self.snapshot.load().root
    }

    /// Number of leaves in the tree.
    #[cfg(not(feature = "concurrent"))]
    pub fn size(&self) -> u64 {
        self.inner.size
    }

    /// Number of leaves in the tree.
    #[cfg(feature = "concurrent")]
    #[cfg_attr(docsrs, doc(cfg(feature = "concurrent")))]
    pub fn size(&self) -> u64 {
        self.snapshot.load().size
    }

    /// Current depth (hash layers above the leaf level).
    #[cfg(not(feature = "concurrent"))]
    pub fn depth(&self) -> usize {
        self.inner.depth
    }

    /// Current depth (hash layers above the leaf level).
    #[cfg(feature = "concurrent")]
    #[cfg_attr(docsrs, doc(cfg(feature = "concurrent")))]
    pub fn depth(&self) -> usize {
        self.snapshot.load().depth
    }

    /// Create an immutable snapshot for proof generation.
    #[cfg(not(feature = "concurrent"))]
    pub fn snapshot(&self) -> TreeSnapshot<N, MAX_DEPTH> {
        self.inner.snapshot()
    }

    /// Create an immutable snapshot for proof generation.
    ///
    /// Returns an `Arc` for lock-free sharing across threads.
    #[cfg(feature = "concurrent")]
    #[cfg_attr(docsrs, doc(cfg(feature = "concurrent")))]
    pub fn snapshot(&self) -> Arc<TreeSnapshot<N, MAX_DEPTH>> {
        self.snapshot.load_full()
    }

    pub(crate) fn _insert(
        inner: &mut TreeInner<N, MAX_DEPTH>,
        hasher: &H,
        leaf: Hash,
    ) -> Result<Hash, TreeError> {
        let new_size = inner
            .size
            .checked_add(1)
            .ok_or(TreeError::CapacityExceeded)?;
        let depth = ceil_log_n(new_size, N);
        if depth > MAX_DEPTH {
            return Err(TreeError::MaxDepthExceeded {
                max_depth: MAX_DEPTH,
            });
        }
        let index = u64_to_usize(inner.size)?;

        let mut node = leaf;
        let mut idx = index;
        for level in 0..depth {
            inner.levels[level].set(idx, node)?;

            let child_pos = idx % N;
            if child_pos != 0 {
                let group_start = idx - child_pos;
                let count = child_pos + 1;
                let mut children = [[0u8; 32]; N];
                if child_pos > 0 {
                    inner.levels[level].get_group(group_start, child_pos, &mut children);
                }
                children[child_pos] = node;
                node = hasher.hash_children(&children[..count]);
            }
            idx /= N;
        }

        if depth < MAX_DEPTH {
            inner.levels[depth].set(0, node)?;
        }
        inner.root = Some(node);
        inner.size = new_size;
        inner.depth = depth;
        Ok(node)
    }

    /// Compute the parent hash for a group at `parent_idx`
    /// within a single level.
    fn _compute_parent(
        child_level: &ChunkedLevel,
        parent_idx: usize,
        level_len: usize,
        hasher: &H,
    ) -> Result<Hash, TreeError> {
        let group_start = parent_idx * N;
        let group_end = core::cmp::min(group_start + N, level_len);
        let count = group_end - group_start;
        if count == 1 {
            child_level.get(group_start)
        } else {
            let mut children = [[0u8; 32]; N];
            child_level.get_group(group_start, count, &mut children);
            Ok(hasher.hash_children(&children[..count]))
        }
    }

    /// Sequential inner loop for one level of `_insert_many`.
    #[allow(clippy::too_many_arguments)]
    fn _insert_many_level_seq(
        levels: &mut [ChunkedLevel],
        level: usize,
        start_parent: usize,
        num_parents: usize,
        level_len: usize,
        is_root_level: bool,
        hasher: &H,
        root: &mut Hash,
    ) -> Result<(), TreeError> {
        for parent_idx in start_parent..num_parents {
            let parent =
                Self::_compute_parent(&levels[level], parent_idx, level_len, hasher)?;
            let next_level = level + 1;
            if next_level < levels.len() {
                levels[next_level].set(parent_idx, parent)?;
            }
            if is_root_level {
                *root = parent;
            }
        }
        Ok(())
    }

    pub(crate) fn _insert_many(
        inner: &mut TreeInner<N, MAX_DEPTH>,
        hasher: &H,
        leaves: &[Hash],
    ) -> Result<Hash, TreeError> {
        if leaves.is_empty() {
            return Err(TreeError::EmptyBatch);
        }

        let batch_len = u64::try_from(leaves.len()).unwrap_or(u64::MAX);
        let new_size = inner
            .size
            .checked_add(batch_len)
            .ok_or(TreeError::CapacityExceeded)?;
        let depth = ceil_log_n(new_size, N);
        if depth > MAX_DEPTH {
            return Err(TreeError::MaxDepthExceeded {
                max_depth: MAX_DEPTH,
            });
        }

        inner.levels[0].extend(leaves)?;

        // allocate upfront
        {
            let mut level_len = inner.levels[0].len;
            for level in 0..depth {
                let num_parents = level_len.div_ceil(N);
                if level + 1 < MAX_DEPTH {
                    inner.levels[level + 1].ensure_len(num_parents)?;
                }
                level_len = num_parents;
            }
        }

        let old_size_usize = u64_to_usize(inner.size)?;
        let mut start_parent = old_size_usize / N;

        let mut root = if depth == 0 {
            inner.levels[0].get(0)?
        } else {
            [0u8; 32]
        };

        #[cfg(feature = "parallel")]
        let mut par_buf: Vec<Hash> = Vec::new();

        for level in 0..depth {
            let level_len = inner.levels[level].len;
            let num_parents = level_len.div_ceil(N);
            let is_root_level = level + 1 == depth;

            #[cfg(feature = "parallel")]
            {
                let work = num_parents - start_parent;
                if work >= parallel_threshold() {
                    use rayon::prelude::*;

                    let split_at = level + 1;
                    let (child_levels, parent_levels) =
                        inner.levels.split_at_mut(split_at);
                    let child_level = &child_levels[level];

                    par_buf.clear();
                    par_buf.resize(work, [0u8; 32]);

                    par_buf.par_chunks_mut(PAR_CHUNK_SIZE).enumerate().for_each(
                        |(ci, chunk)| {
                            let base = start_parent + ci * PAR_CHUNK_SIZE;
                            for (i, slot) in chunk.iter_mut().enumerate() {
                                *slot = Self::_compute_parent(
                                    child_level,
                                    base + i,
                                    level_len,
                                    hasher,
                                )
                                .expect("ensure_len guarantees valid indices");
                            }
                        },
                    );

                    let parent_level = &mut parent_levels[0];
                    for (i, &parent) in par_buf.iter().enumerate() {
                        let parent_idx = start_parent + i;
                        if split_at < MAX_DEPTH {
                            parent_level.set(parent_idx, parent)?;
                        }
                        if is_root_level {
                            root = parent;
                        }
                    }
                } else {
                    Self::_insert_many_level_seq(
                        &mut inner.levels,
                        level,
                        start_parent,
                        num_parents,
                        level_len,
                        is_root_level,
                        hasher,
                        &mut root,
                    )?;
                }
            }

            #[cfg(not(feature = "parallel"))]
            Self::_insert_many_level_seq(
                &mut inner.levels,
                level,
                start_parent,
                num_parents,
                level_len,
                is_root_level,
                hasher,
                &mut root,
            )?;

            start_parent /= N;
        }

        inner.root = Some(root);
        inner.size = new_size;
        inner.depth = depth;
        Ok(root)
    }
}

#[cfg(test)]
#[cfg_attr(feature = "concurrent", allow(unused_mut))]
mod tests {
    #[cfg(not(feature = "std"))]
    use alloc::vec::Vec;
    #[cfg(feature = "std")]
    use std::vec::Vec;

    use super::*;

    #[derive(Clone)]
    struct XorHasher;

    impl crate::Hasher for XorHasher {
        fn hash_children(&self, children: &[Hash]) -> Hash {
            let mut result = [0u8; 32];
            for child in children {
                for (r, c) in result.iter_mut().zip(child.iter()) {
                    *r ^= c;
                }
            }
            result
        }
    }

    fn leaf(n: u8) -> Hash {
        let mut h = [0u8; 32];
        h[0] = n;
        h
    }

    #[test]
    fn ceil_log_n_empty() {
        assert_eq!(ceil_log_n(0, 2), 0);
    }

    #[test]
    fn ceil_log_n_one() {
        assert_eq!(ceil_log_n(1, 2), 0);
    }

    #[test]
    fn ceil_log_n_binary_full() {
        assert_eq!(ceil_log_n(4, 2), 2);
    }

    #[test]
    fn ceil_log_n_binary_partial() {
        assert_eq!(ceil_log_n(3, 2), 2);
    }

    #[test]
    fn ceil_log_n_ternary() {
        assert_eq!(ceil_log_n(4, 3), 2);
    }

    #[test]
    fn ceil_log_n_ternary_exact() {
        assert_eq!(ceil_log_n(9, 3), 2);
    }

    #[test]
    fn ceil_log_n_quaternary() {
        assert_eq!(ceil_log_n(16, 4), 2);
        assert_eq!(ceil_log_n(17, 4), 3);
    }

    #[test]
    fn ceil_log_n_large_n() {
        assert_eq!(ceil_log_n(256, 16), 2);
        assert_eq!(ceil_log_n(257, 16), 3);
    }

    #[test]
    fn chunked_level_push_and_get() {
        let mut level = ChunkedLevel::new();
        for i in 0u8..10 {
            level.push(leaf(i)).unwrap();
        }
        assert_eq!(level.len, 10);
        for i in 0u8..10 {
            assert_eq!(level.get(i as usize).unwrap(), leaf(i));
        }
    }

    #[test]
    fn chunked_level_promotes_at_chunk_size() {
        let mut level = ChunkedLevel::new();
        for i in 0..CHUNK_SIZE {
            level.push(leaf(i as u8)).unwrap();
        }
        assert_eq!(level.chunk_count(), 1);
        assert_eq!(level.tail_len, 0);
        assert_eq!(level.len, CHUNK_SIZE);

        // One more goes into the new tail.
        level.push(leaf(0xFF)).unwrap();
        assert_eq!(level.chunk_count(), 1);
        assert_eq!(level.tail_len, 1);
        assert_eq!(level.len, CHUNK_SIZE + 1);
    }

    #[test]
    fn chunked_level_snapshot_shares_arcs() {
        let mut level = ChunkedLevel::new();
        for i in 0..CHUNK_SIZE + 5 {
            level.push(leaf(i as u8)).unwrap();
        }
        let snap = level.snapshot();
        assert_eq!(snap.len(), level.len);
        // The completed chunk Arc is shared.
        assert!(Chunk::ptr_eq(level.get_chunk(0), snap.get_chunk(0)));
        // Data matches.
        for i in 0..level.len {
            assert_eq!(level.get(i).unwrap(), snap.get(i).unwrap());
        }
    }

    #[test]
    fn empty_tree() {
        let tree = LeanIMT::<XorHasher, 2, 32>::new(XorHasher);
        assert_eq!(tree.root(), None);
        assert_eq!(tree.size(), 0);
        assert_eq!(tree.depth(), 0);
    }

    #[test]
    fn insert_single_leaf_binary() {
        let mut tree = LeanIMT::<XorHasher, 2, 32>::new(XorHasher);
        let l = leaf(1);
        let root = tree.insert(l).unwrap();
        assert_eq!(root, l); // single leaf = root (lifted)
        assert_eq!(tree.size(), 1);
        assert_eq!(tree.depth(), 0);
    }

    #[test]
    fn insert_two_leaves_binary() {
        let hasher = XorHasher;
        let mut tree = LeanIMT::<XorHasher, 2, 32>::new(hasher.clone());
        let l0 = leaf(1);
        let l1 = leaf(2);
        tree.insert(l0).unwrap();
        let root = tree.insert(l1).unwrap();

        let expected = hasher.hash_children(&[l0, l1]);
        assert_eq!(root, expected);
        assert_eq!(tree.size(), 2);
        assert_eq!(tree.depth(), 1);
    }

    #[test]
    fn insert_three_leaves_binary() {
        let h = XorHasher;
        let mut tree = LeanIMT::<XorHasher, 2, 32>::new(h.clone());
        let l0 = leaf(1);
        let l1 = leaf(2);
        let l2 = leaf(3);
        tree.insert(l0).unwrap();
        tree.insert(l1).unwrap();
        let root = tree.insert(l2).unwrap();

        // Level 0: [l0, l1, l2]
        // Level 1: [H(l0,l1), l2_lifted]
        // Level 2: [H(H(l0,l1), l2)]
        let h01 = h.hash_children(&[l0, l1]);
        let expected = h.hash_children(&[h01, l2]);
        assert_eq!(root, expected);
        assert_eq!(tree.depth(), 2);
    }

    #[test]
    fn insert_four_leaves_binary() {
        let h = XorHasher;
        let mut tree = LeanIMT::<XorHasher, 2, 32>::new(h.clone());
        let leaves: Vec<Hash> = (1..=4).map(leaf).collect();
        for &l in &leaves {
            tree.insert(l).unwrap();
        }

        // Level 0: [l0, l1, l2, l3]
        // Level 1: [H(l0,l1), H(l2,l3)]
        // Level 2: [H(H(l0,l1), H(l2,l3))]
        let h01 = h.hash_children(&[leaves[0], leaves[1]]);
        let h23 = h.hash_children(&[leaves[2], leaves[3]]);
        let expected = h.hash_children(&[h01, h23]);
        assert_eq!(tree.root(), Some(expected));
        assert_eq!(tree.depth(), 2);
    }

    #[test]
    fn insert_four_leaves_ternary() {
        let h = XorHasher;
        let mut tree = LeanIMT::<XorHasher, 3, 32>::new(h.clone());
        let leaves: Vec<Hash> = (1..=4).map(leaf).collect();
        for &l in &leaves {
            tree.insert(l).unwrap();
        }

        // Level 0: [l0, l1, l2, l3]
        // Level 1: [H(l0,l1,l2), l3_lifted]
        // Level 2: [H(H(l0,l1,l2), l3)]
        let h012 = h.hash_children(&[leaves[0], leaves[1], leaves[2]]);
        let expected = h.hash_children(&[h012, leaves[3]]);
        assert_eq!(tree.root(), Some(expected));
        assert_eq!(tree.depth(), 2);
    }

    #[test]
    fn insert_two_leaves_ternary() {
        let h = XorHasher;
        let mut tree = LeanIMT::<XorHasher, 3, 32>::new(h.clone());
        let l0 = leaf(1);
        let l1 = leaf(2);
        tree.insert(l0).unwrap();
        let root = tree.insert(l1).unwrap();

        let expected = h.hash_children(&[l0, l1]);
        assert_eq!(root, expected);
        assert_eq!(tree.depth(), 1);
    }

    #[test]
    fn insert_five_leaves_quaternary() {
        let h = XorHasher;
        let mut tree = LeanIMT::<XorHasher, 4, 32>::new(h.clone());
        let leaves: Vec<Hash> = (1..=5).map(leaf).collect();
        for &l in &leaves {
            tree.insert(l).unwrap();
        }

        // Level 0: [l0..l4]
        // Level 1: [H(l0,l1,l2,l3), l4_lifted]
        // Level 2: [H(H(l0,l1,l2,l3), l4)]
        let h0123 = h.hash_children(&[leaves[0], leaves[1], leaves[2], leaves[3]]);
        let expected = h.hash_children(&[h0123, leaves[4]]);
        assert_eq!(tree.root(), Some(expected));
        assert_eq!(tree.depth(), 2);
    }

    #[test]
    fn insert_many_matches_sequential_binary() {
        let h = XorHasher;
        let leaves: Vec<Hash> = (1..=7).map(leaf).collect();

        let mut seq = LeanIMT::<XorHasher, 2, 32>::new(h.clone());
        for &l in &leaves {
            seq.insert(l).unwrap();
        }

        let mut batch = LeanIMT::<XorHasher, 2, 32>::new(h.clone());
        batch.insert_many(&leaves).unwrap();

        assert_eq!(seq.root(), batch.root());
        assert_eq!(seq.size(), batch.size());
    }

    #[test]
    fn insert_many_matches_sequential_ternary() {
        let h = XorHasher;
        let leaves: Vec<Hash> = (1..=10).map(leaf).collect();

        let mut seq = LeanIMT::<XorHasher, 3, 32>::new(h.clone());
        for &l in &leaves {
            seq.insert(l).unwrap();
        }

        let mut batch = LeanIMT::<XorHasher, 3, 32>::new(h.clone());
        batch.insert_many(&leaves).unwrap();

        assert_eq!(seq.root(), batch.root());
    }

    #[test]
    fn insert_many_incremental() {
        let h = XorHasher;
        let leaves: Vec<Hash> = (1..=10).map(leaf).collect();

        let mut seq = LeanIMT::<XorHasher, 2, 32>::new(h.clone());
        for &l in &leaves {
            seq.insert(l).unwrap();
        }

        let mut mixed = LeanIMT::<XorHasher, 2, 32>::new(h.clone());
        for &l in &leaves[..3] {
            mixed.insert(l).unwrap();
        }
        mixed.insert_many(&leaves[3..]).unwrap();

        assert_eq!(seq.root(), mixed.root());
    }

    #[test]
    fn insert_many_empty_batch_error() {
        let mut tree = LeanIMT::<XorHasher, 2, 32>::new(XorHasher);
        assert_eq!(tree.insert_many(&[]), Err(TreeError::EmptyBatch));
    }

    #[test]
    fn insert_many_chunk_boundary() {
        let leaves: Vec<Hash> = (0..CHUNK_SIZE)
            .map(|i| {
                let mut h = [0u8; 32];
                let bytes = (i as u64).to_le_bytes();
                h[..8].copy_from_slice(&bytes);
                h
            })
            .collect();

        let mut seq = LeanIMT::<XorHasher, 2, 32>::new(XorHasher);
        for &l in &leaves {
            seq.insert(l).unwrap();
        }

        let mut batch = LeanIMT::<XorHasher, 2, 32>::new(XorHasher);
        batch.insert_many(&leaves).unwrap();

        assert_eq!(seq.root(), batch.root());
        assert_eq!(seq.size(), batch.size());
        assert_eq!(seq.size(), CHUNK_SIZE as u64);
    }

    #[test]
    fn max_depth_exceeded() {
        let mut tree = LeanIMT::<XorHasher, 2, 1>::new(XorHasher);
        let l = [0u8; 32];
        tree.insert(l).unwrap(); // size=1, depth=0
        tree.insert(l).unwrap(); // size=2, depth=1
        let err = tree.insert(l).unwrap_err();
        assert_eq!(err, TreeError::MaxDepthExceeded { max_depth: 1 });
    }

    #[cfg(feature = "blake3")]
    mod blake3_tests {
        use super::*;
        use crate::Blake3Hasher;

        fn blake3_leaf(n: u8) -> Hash {
            *::blake3::hash(&[n]).as_bytes()
        }

        #[test]
        fn binary_four_leaves_known_vector() {
            let h = Blake3Hasher;
            let mut tree = LeanIMT::<Blake3Hasher, 2, 32>::new(h);

            let l0 = blake3_leaf(0);
            let l1 = blake3_leaf(1);
            let l2 = blake3_leaf(2);
            let l3 = blake3_leaf(3);

            let r1 = tree.insert(l0).unwrap();
            assert_eq!(r1, l0);

            let r2 = tree.insert(l1).unwrap();
            let h01 = h.hash_children(&[l0, l1]);
            assert_eq!(r2, h01);

            let r3 = tree.insert(l2).unwrap();
            let expected3 = h.hash_children(&[h01, l2]);
            assert_eq!(r3, expected3);

            let r4 = tree.insert(l3).unwrap();
            let h23 = h.hash_children(&[l2, l3]);
            let expected4 = h.hash_children(&[h01, h23]);
            assert_eq!(r4, expected4);
        }

        #[test]
        fn ternary_four_leaves_known_vector() {
            let h = Blake3Hasher;
            let mut tree = LeanIMT::<Blake3Hasher, 3, 32>::new(h);

            let l0 = blake3_leaf(0);
            let l1 = blake3_leaf(1);
            let l2 = blake3_leaf(2);
            let l3 = blake3_leaf(3);

            tree.insert(l0).unwrap();

            let r2 = tree.insert(l1).unwrap();
            assert_eq!(r2, h.hash_children(&[l0, l1]));

            let r3 = tree.insert(l2).unwrap();
            assert_eq!(r3, h.hash_children(&[l0, l1, l2]));

            let r4 = tree.insert(l3).unwrap();
            let h012 = h.hash_children(&[l0, l1, l2]);
            assert_eq!(r4, h.hash_children(&[h012, l3]));
        }

        #[test]
        fn quaternary_five_leaves_known_vector() {
            let h = Blake3Hasher;
            let mut tree = LeanIMT::<Blake3Hasher, 4, 32>::new(h);

            let leaves: Vec<Hash> = (0..5).map(blake3_leaf).collect();
            for &l in &leaves {
                tree.insert(l).unwrap();
            }

            let h0123 = h.hash_children(&[leaves[0], leaves[1], leaves[2], leaves[3]]);
            let expected = h.hash_children(&[h0123, leaves[4]]);
            assert_eq!(tree.root(), Some(expected));
        }
    }
}

#[cfg(all(test, feature = "concurrent"))]
mod concurrent_tests {
    use std::{
        sync::Arc,
        thread,
        vec::Vec,
    };

    use super::*;

    #[derive(Clone)]
    struct XorHasher;

    impl crate::Hasher for XorHasher {
        fn hash_children(&self, children: &[Hash]) -> Hash {
            let mut result = [0u8; 32];
            for child in children {
                for (r, c) in result.iter_mut().zip(child.iter()) {
                    *r ^= c;
                }
            }
            result
        }
    }

    fn leaf(n: u8) -> Hash {
        let mut h = [0u8; 32];
        h[0] = n;
        h
    }

    #[test]
    fn concurrent_insert_single_thread() {
        let tree = LeanIMT::<XorHasher, 2, 32>::new(XorHasher);
        for i in 1..=10u8 {
            tree.insert(leaf(i)).unwrap();
        }
        assert_eq!(tree.size(), 10);

        let snap = tree.snapshot();
        for i in 0..10u64 {
            let proof = snap.generate_proof(i).unwrap();
            assert!(proof.verify(&XorHasher).unwrap());
        }
    }

    #[test]
    fn concurrent_multi_thread_insert() {
        let tree = Arc::new(LeanIMT::<XorHasher, 2, 32>::new(XorHasher));
        let num_threads = 4;
        let leaves_per_thread = 50;

        let handles: Vec<_> = (0..num_threads)
            .map(|t| {
                let tree = Arc::clone(&tree);
                thread::spawn(move || {
                    let base = (t * leaves_per_thread) as u8;
                    for i in 0..leaves_per_thread as u8 {
                        let mut l = [0u8; 32];
                        l[0] = base.wrapping_add(i);
                        l[1] = t as u8;
                        tree.insert(l).unwrap();
                    }
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }

        let total = num_threads * leaves_per_thread;
        assert_eq!(tree.size(), total as u64);

        let snap = tree.snapshot();
        for i in 0..total as u64 {
            let proof = snap.generate_proof(i).unwrap();
            assert!(proof.verify(&XorHasher).unwrap());
        }
    }

    #[test]
    fn concurrent_reader_writer() {
        let tree = Arc::new(LeanIMT::<XorHasher, 2, 32>::new(XorHasher));
        let num_inserts = 200u64;
        let num_readers = 3;

        let writer_tree = Arc::clone(&tree);
        let writer = thread::spawn(move || {
            for i in 0..num_inserts {
                let mut l = [0u8; 32];
                l[0] = i as u8;
                l[1] = (i >> 8) as u8;
                writer_tree.insert(l).unwrap();
            }
        });

        let readers: Vec<_> = (0..num_readers)
            .map(|_| {
                let tree = Arc::clone(&tree);
                thread::spawn(move || {
                    for _ in 0..100 {
                        let snap = tree.snapshot();
                        let size = snap.size();
                        if size == 0 {
                            continue;
                        }
                        for i in 0..size {
                            let proof = snap.generate_proof(i).unwrap();
                            assert!(proof.verify(&XorHasher).unwrap());
                        }
                    }
                })
            })
            .collect();

        writer.join().unwrap();
        for r in readers {
            r.join().unwrap();
        }

        assert_eq!(tree.size(), num_inserts);
    }

    #[test]
    fn snapshot_isolation() {
        let tree = LeanIMT::<XorHasher, 2, 32>::new(XorHasher);
        for i in 1..=5u8 {
            tree.insert(leaf(i)).unwrap();
        }

        let snap = tree.snapshot();
        let snap_root = snap.root();
        let snap_size = snap.size();
        let snap_depth = snap.depth();

        // Insert more after taking the snapshot.
        for i in 6..=10u8 {
            tree.insert(leaf(i)).unwrap();
        }

        // The snapshot must be unchanged.
        assert_eq!(snap.root(), snap_root);
        assert_eq!(snap.size(), snap_size);
        assert_eq!(snap.depth(), snap_depth);

        // But the tree itself has advanced.
        assert_eq!(tree.size(), 10);
        assert_ne!(tree.root(), snap_root);
    }

    #[test]
    fn concurrent_insert_many() {
        let tree = Arc::new(LeanIMT::<XorHasher, 2, 32>::new(XorHasher));
        let num_threads = 4;
        let batch_size = 25;

        let handles: Vec<_> = (0..num_threads)
            .map(|t| {
                let tree = Arc::clone(&tree);
                thread::spawn(move || {
                    let batch: Vec<Hash> = (0..batch_size)
                        .map(|i| {
                            let mut l = [0u8; 32];
                            l[0] = i as u8;
                            l[1] = t as u8;
                            l
                        })
                        .collect();
                    tree.insert_many(&batch).unwrap();
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }

        let total = (num_threads * batch_size) as u64;
        assert_eq!(tree.size(), total);

        let snap = tree.snapshot();
        for i in 0..total {
            let proof = snap.generate_proof(i).unwrap();
            assert!(proof.verify(&XorHasher).unwrap());
        }
    }
}

#[cfg(all(test, feature = "parallel"))]
#[cfg_attr(feature = "concurrent", allow(unused_mut))]
mod parallel_tests {
    use std::vec::Vec;

    use super::*;

    #[derive(Clone)]
    struct XorHasher;

    impl crate::Hasher for XorHasher {
        fn hash_children(&self, children: &[Hash]) -> Hash {
            let mut result = [0u8; 32];
            for child in children {
                for (r, c) in result.iter_mut().zip(child.iter()) {
                    *r ^= c;
                }
            }
            result
        }
    }

    fn make_leaves(count: usize) -> Vec<Hash> {
        (0..count)
            .map(|i| {
                let mut h = [0u8; 32];
                let bytes = (i as u64).to_le_bytes();
                h[..8].copy_from_slice(&bytes);
                h
            })
            .collect()
    }

    #[test]
    fn parallel_insert_many_matches_sequential_binary() {
        let leaves = make_leaves(1000);

        let mut seq = LeanIMT::<XorHasher, 2, 32>::new(XorHasher);
        for &l in &leaves {
            seq.insert(l).unwrap();
        }

        let mut batch = LeanIMT::<XorHasher, 2, 32>::new(XorHasher);
        batch.insert_many(&leaves).unwrap();

        assert_eq!(seq.root(), batch.root());
        assert_eq!(seq.size(), batch.size());
    }

    #[test]
    fn parallel_insert_many_matches_sequential_ternary() {
        let leaves = make_leaves(1000);

        let mut seq = LeanIMT::<XorHasher, 3, 32>::new(XorHasher);
        for &l in &leaves {
            seq.insert(l).unwrap();
        }

        let mut batch = LeanIMT::<XorHasher, 3, 32>::new(XorHasher);
        batch.insert_many(&leaves).unwrap();

        assert_eq!(seq.root(), batch.root());
    }

    #[test]
    fn parallel_insert_many_matches_sequential_quaternary() {
        let leaves = make_leaves(1000);

        let mut seq = LeanIMT::<XorHasher, 4, 32>::new(XorHasher);
        for &l in &leaves {
            seq.insert(l).unwrap();
        }

        let mut batch = LeanIMT::<XorHasher, 4, 32>::new(XorHasher);
        batch.insert_many(&leaves).unwrap();

        assert_eq!(seq.root(), batch.root());
    }

    #[test]
    fn parallel_large_batch_proofs() {
        let leaves = make_leaves(2000);

        let mut tree = LeanIMT::<XorHasher, 2, 32>::new(XorHasher);
        tree.insert_many(&leaves).unwrap();

        let snap = tree.snapshot();
        for i in 0..2000u64 {
            let proof = snap.generate_proof(i).unwrap();
            assert!(proof.verify(&XorHasher).unwrap());
        }
    }

    #[test]
    fn parallel_insert_many_incremental() {
        let leaves = make_leaves(1000);

        let mut seq = LeanIMT::<XorHasher, 2, 32>::new(XorHasher);
        for &l in &leaves {
            seq.insert(l).unwrap();
        }

        let mut mixed = LeanIMT::<XorHasher, 2, 32>::new(XorHasher);
        for &l in &leaves[..100] {
            mixed.insert(l).unwrap();
        }
        mixed.insert_many(&leaves[100..]).unwrap();

        assert_eq!(seq.root(), mixed.root());
    }
}
