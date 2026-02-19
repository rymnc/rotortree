use std::{
    fs,
    io::{
        self,
        Seek,
        Write,
    },
    path::{
        Path,
        PathBuf,
    },
    sync::Arc,
};

use crate::{
    Hash,
    tree::CHUNK_SIZE,
};

use super::{
    data::MmapRegion,
    error::StorageError,
    frame,
};

const HEADER_MAGIC: [u8; 4] = *b"RTRD";
const META_MAGIC: [u8; 4] = *b"RTMD";

pub(crate) const CHUNK_BYTE_SIZE: usize = CHUNK_SIZE * 32;

/// Controls when checkpoints are triggered
pub enum CheckpointPolicy {
    /// Caller calls `checkpoint()` explicitly
    Manual,
    /// Auto-checkpoint after every N WAL entries
    EveryNEntries(u64),
    /// Auto-checkpoint when in-memory chunks exceed N bytes
    MemoryThreshold(usize),
    /// Checkpoint only on graceful close
    OnClose,
}

impl Default for CheckpointPolicy {
    fn default() -> Self {
        Self::Manual
    }
}

/// Controls which tree levels are kept in memory vs mmap'd
pub struct TieringConfig {
    /// Levels below this value have their committed chunks mmap'd after checkpoint.
    /// Set to `usize::MAX` to never mmap (default: all in memory)
    pub pin_above_level: usize,
}

impl Default for TieringConfig {
    fn default() -> Self {
        Self {
            pin_above_level: usize::MAX,
        }
    }
}

/// Versioned header written once at data directory creation
#[derive(Debug, Clone, wincode::SchemaWrite, wincode::SchemaRead)]
enum HeaderFrame {
    V1 {
        magic: [u8; 4],
        n: u32,
        max_depth: u32,
        chunk_size: u32,
    },
}

/// Versioned checkpoint metadata (atomically written at each checkpoint)
#[derive(Debug, Clone, wincode::SchemaWrite, wincode::SchemaRead)]
enum MetaFrame {
    V1 {
        magic: [u8; 4],
        n: u32,
        max_depth: u32,
        last_wal_seq: u64,
        leaf_count: u64,
        depth: u32,
        root_hash: Hash,
    },
}

/// Deserialized header.bin contents
#[derive(Debug, Clone, Copy)]
pub(crate) struct HeaderData {
    pub(crate) n: u32,
    pub(crate) max_depth: u32,
    pub(crate) chunk_size: u32,
}

/// Deserialized checkpoint.meta contents
#[doc(hidden)]
#[derive(Debug, Clone, Copy)]
pub struct CheckpointMeta {
    pub n: u32,
    pub max_depth: u32,
    pub last_wal_seq: u64,
    pub leaf_count: u64,
    pub depth: u32,
    pub root_hash: Hash,
}

/// Write `header.bin`
pub(crate) fn write_header(data_dir: &Path, n: u32, max_depth: u32) -> io::Result<()> {
    #[allow(clippy::cast_possible_truncation)]
    let header = HeaderFrame::V1 {
        magic: HEADER_MAGIC,
        n,
        max_depth,
        chunk_size: CHUNK_SIZE as u32,
    };
    let buf = frame::serialize_frame(&header);
    let path = data_dir.join("header.bin");
    let mut file = fs::File::create(&path)?;
    file.write_all(&buf)?;
    file.sync_all()?;
    Ok(())
}

/// Read and validate `header.bin`. Returns `None` if missing or corrupt
pub(crate) fn read_header(data_dir: &Path) -> Result<Option<HeaderData>, StorageError> {
    let path = data_dir.join("header.bin");
    let header: HeaderFrame = match frame::read_frame_file(&path)? {
        Some(h) => h,
        None => return Ok(None),
    };

    let HeaderFrame::V1 {
        magic,
        n,
        max_depth,
        chunk_size,
    } = header;

    if magic != HEADER_MAGIC {
        return Ok(None);
    }

    Ok(Some(HeaderData {
        n,
        max_depth,
        chunk_size,
    }))
}

/// Write checkpoint metadata atomically
pub(crate) fn write_meta(data_dir: &Path, meta: &CheckpointMeta) -> io::Result<()> {
    let frame = MetaFrame::V1 {
        magic: META_MAGIC,
        n: meta.n,
        max_depth: meta.max_depth,
        last_wal_seq: meta.last_wal_seq,
        leaf_count: meta.leaf_count,
        depth: meta.depth,
        root_hash: meta.root_hash,
    };
    let buf = frame::serialize_frame(&frame);
    atomic_write(&data_dir.join("checkpoint.meta"), &buf)
}

/// Read and validate checkpoint metadata. Returns `None` if missing or corrupt
pub(crate) fn read_meta(data_dir: &Path) -> Result<Option<CheckpointMeta>, StorageError> {
    let path = data_dir.join("checkpoint.meta");
    let meta_frame: MetaFrame = match frame::read_frame_file(&path)? {
        Some(m) => m,
        None => return Ok(None),
    };

    let MetaFrame::V1 {
        magic,
        n,
        max_depth,
        last_wal_seq,
        leaf_count,
        depth,
        root_hash,
    } = meta_frame;

    if magic != META_MAGIC {
        return Ok(None);
    }

    Ok(Some(CheckpointMeta {
        n,
        max_depth,
        last_wal_seq,
        leaf_count,
        depth,
        root_hash,
    }))
}

/// Write all level tails atomically (tmp -> fsync -> rename).
pub(crate) fn write_tails(
    data_dir: &Path,
    tails: &[[Hash; CHUNK_SIZE]],
    max_depth: usize,
) -> io::Result<()> {
    let total_size = max_depth * CHUNK_BYTE_SIZE;
    let mut buf = vec![0u8; total_size];

    for (i, tail) in tails.iter().enumerate() {
        let base = i * CHUNK_BYTE_SIZE;
        let tail_bytes = tail.len() * 32;
        // SAFETY: tail is &[Hash] where Hash is [u8; 32], so this is a flat byte view
        let src =
            unsafe { std::slice::from_raw_parts(tail.as_ptr() as *const u8, tail_bytes) };
        buf[base..base + tail_bytes].copy_from_slice(src);
    }

    atomic_write(&data_dir.join("tails.bin"), &buf)
}

/// Read all tails from disk. Returns `None` if the file is missing or wrong size
pub(crate) fn read_tails(
    data_dir: &Path,
    max_depth: usize,
) -> io::Result<Option<Vec<[Hash; CHUNK_SIZE]>>> {
    let path = data_dir.join("tails.bin");
    let data = match fs::read(&path) {
        Ok(d) => d,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e),
    };
    let expected = max_depth * CHUNK_BYTE_SIZE;
    if data.len() != expected {
        return Ok(None);
    }

    let mut tails = Vec::with_capacity(max_depth);
    for i in 0..max_depth {
        let base = i * CHUNK_BYTE_SIZE;
        let chunk = &data[base..base + CHUNK_BYTE_SIZE];
        // SAFETY: CHUNK_BYTE_SIZE == CHUNK_SIZE * 32, so chunk is exactly [[u8; 32]; CHUNK_SIZE]
        let tail: [[u8; 32]; CHUNK_SIZE] =
            unsafe { chunk.as_ptr().cast::<[[u8; 32]; CHUNK_SIZE]>().read() };
        tails.push(tail);
    }

    Ok(Some(tails))
}

pub(crate) fn level_file_path(data_dir: &Path, level_idx: usize) -> PathBuf {
    data_dir.join(format!("level_{level_idx}.dat"))
}

pub(crate) fn append_chunks_to_level<'a>(
    data_dir: &Path,
    level_idx: usize,
    from_chunk: usize,
    chunks: impl Iterator<Item = &'a [Hash; CHUNK_SIZE]>,
) -> io::Result<fs::File> {
    let path = level_file_path(data_dir, level_idx);
    let mut file = fs::OpenOptions::new()
        .create(true)
        .write(true)
        .read(true)
        .truncate(false)
        .open(&path)?;

    let offset = (from_chunk as u64) * (CHUNK_BYTE_SIZE as u64);
    file.seek(io::SeekFrom::Start(offset))?;

    for chunk in chunks {
        file.write_all(chunk.as_flattened())?;
    }
    Ok(file)
}

pub(crate) fn mmap_level_file(
    data_dir: &Path,
    level_idx: usize,
    valid_chunks: usize,
) -> io::Result<Option<Arc<MmapRegion>>> {
    if valid_chunks == 0 {
        return Ok(None);
    }
    let path = level_file_path(data_dir, level_idx);
    let file = match fs::File::open(&path) {
        Ok(f) => f,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e),
    };
    let file_len = usize::try_from(file.metadata()?.len()).unwrap_or(usize::MAX);
    let valid_len = valid_chunks * CHUNK_BYTE_SIZE;
    if file_len < valid_len {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "level_{level_idx}.dat too small: {file_len} bytes < {valid_len} expected"
            ),
        ));
    }

    let mmap = unsafe { memmap2::MmapOptions::new().len(file_len).map_copy(&file)? };

    Ok(Some(Arc::new(MmapRegion::new(mmap, valid_len))))
}

pub(crate) fn atomic_write(path: &Path, data: &[u8]) -> io::Result<()> {
    let mut s = path.as_os_str().to_owned();
    s.push(".tmp");
    let tmp = PathBuf::from(s);
    {
        let mut file = fs::File::create(&tmp)?;
        file.write_all(data)?;
        file.sync_all()?;
    }
    fs::rename(&tmp, path)?;
    if let Some(parent) = path.parent() {
        let dir = fs::File::open(parent)?;
        dir.sync_all()?;
    }
    Ok(())
}

#[doc(hidden)]
pub fn write_test_meta(data_dir: &Path, meta: &CheckpointMeta) -> io::Result<()> {
    write_meta(data_dir, meta)
}
