//! Low-level I/O utilities for reading and writing segment files.

use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};

use crate::error::{StorageError, StorageResult};

/// Atomically write data to a file using a temporary file and rename.
/// This ensures that if a crash occurs during the write, the original file
/// remains intact (or doesn't exist yet).
fn atomic_write_sync(path: &Path, write_fn: impl FnOnce(&mut BufWriter<File>) -> std::io::Result<()>) -> StorageResult<()> {
    // Ensure parent directory exists
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .map_err(|e| StorageError::WriteFailed(format!("Failed to create directory: {}", e)))?;
    }

    let temp_path = get_temp_path(path);

    // Write to temporary file
    let file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&temp_path)
        .map_err(|e| StorageError::WriteFailed(format!("Failed to create temp file: {}", e)))?;

    let mut writer = BufWriter::new(file);
    write_fn(&mut writer)?;
    writer.flush()?;

    // Sync to disk before rename
    writer.get_ref().sync_all()
        .map_err(|e| StorageError::WriteFailed(format!("Failed to sync temp file: {}", e)))?;

    // Atomic rename
    fs::rename(&temp_path, path)
        .map_err(|e| StorageError::WriteFailed(format!("Failed to rename temp file: {}", e)))?;

    Ok(())
}

/// Get the temporary file path for atomic writes.
/// Uses process ID and a counter to ensure uniqueness even with concurrent writes.
fn get_temp_path(path: &Path) -> PathBuf {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);

    let mut temp_path = path.to_path_buf();
    let file_name = path.file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("temp");
    let unique_id = COUNTER.fetch_add(1, Ordering::Relaxed);
    let pid = std::process::id();
    temp_path.set_file_name(format!(".{}.{}.{}.tmp", file_name, pid, unique_id));
    temp_path
}

use dashcore::{
    block::Header as BlockHeader,
    consensus::{encode, Decodable, Encodable},
    hash_types::FilterHeader,
    BlockHash,
};
use dashcore_hashes::Hash;

/// Load headers from file.
pub(super) async fn load_headers_from_file(path: &Path) -> StorageResult<Vec<BlockHeader>> {
    tokio::task::spawn_blocking({
        let path = path.to_path_buf();
        move || {
            let file = File::open(&path)?;
            let mut reader = BufReader::new(file);
            let mut headers = Vec::new();

            loop {
                match BlockHeader::consensus_decode(&mut reader) {
                    Ok(header) => headers.push(header),
                    Err(encode::Error::Io(ref e))
                        if e.kind() == std::io::ErrorKind::UnexpectedEof =>
                    {
                        break
                    }
                    Err(e) => {
                        return Err(StorageError::ReadFailed(format!(
                            "Failed to decode header: {}",
                            e
                        )))
                    }
                }
            }

            Ok(headers)
        }
    })
    .await
    .map_err(|e| StorageError::ReadFailed(format!("Task join error: {}", e)))?
}

/// Load filter headers from file.
pub(super) async fn load_filter_headers_from_file(path: &Path) -> StorageResult<Vec<FilterHeader>> {
    tokio::task::spawn_blocking({
        let path = path.to_path_buf();
        move || {
            let file = File::open(&path)?;
            let mut reader = BufReader::new(file);
            let mut headers = Vec::new();

            loop {
                match FilterHeader::consensus_decode(&mut reader) {
                    Ok(header) => headers.push(header),
                    Err(encode::Error::Io(ref e))
                        if e.kind() == std::io::ErrorKind::UnexpectedEof =>
                    {
                        break
                    }
                    Err(e) => {
                        return Err(StorageError::ReadFailed(format!(
                            "Failed to decode filter header: {}",
                            e
                        )))
                    }
                }
            }

            Ok(headers)
        }
    })
    .await
    .map_err(|e| StorageError::ReadFailed(format!("Task join error: {}", e)))?
}

/// Load index from file.
pub(super) async fn load_index_from_file(path: &Path) -> StorageResult<HashMap<BlockHash, u32>> {
    tokio::task::spawn_blocking({
        let path = path.to_path_buf();
        move || {
            let content = fs::read(&path)?;
            bincode::deserialize(&content).map_err(|e| {
                StorageError::ReadFailed(format!("Failed to deserialize index: {}", e))
            })
        }
    })
    .await
    .map_err(|e| StorageError::ReadFailed(format!("Task join error: {}", e)))?
}

/// Save a segment of headers to disk.
pub(super) async fn save_segment_to_disk(
    path: &Path,
    headers: &[BlockHeader],
) -> StorageResult<()> {
    tokio::task::spawn_blocking({
        let path = path.to_path_buf();
        let headers = headers.to_vec();
        move || {
            let file = OpenOptions::new().create(true).write(true).truncate(true).open(&path)?;
            let mut writer = BufWriter::new(file);

            // Only save actual headers, not sentinel headers
            for header in headers {
                // Skip sentinel headers (used for padding)
                if header.version.to_consensus() == i32::MAX
                    && header.time == u32::MAX
                    && header.nonce == u32::MAX
                    && header.prev_blockhash == BlockHash::from_byte_array([0xFF; 32])
                {
                    continue;
                }
                header.consensus_encode(&mut writer).map_err(|e| {
                    StorageError::WriteFailed(format!("Failed to encode header: {}", e))
                })?;
            }

            writer.flush()?;
            Ok(())
        }
    })
    .await
    .map_err(|e| StorageError::WriteFailed(format!("Task join error: {}", e)))?
}

/// Save a segment of filter headers to disk atomically.
/// Uses temp file + rename pattern to ensure crash resilience.
pub(super) async fn save_filter_segment_to_disk(
    path: &Path,
    filter_headers: &[FilterHeader],
) -> StorageResult<()> {
    tokio::task::spawn_blocking({
        let path = path.to_path_buf();
        let filter_headers = filter_headers.to_vec();
        move || {
            atomic_write_sync(&path, |writer| {
                for header in &filter_headers {
                    header.consensus_encode(writer).map_err(|e| {
                        std::io::Error::new(std::io::ErrorKind::Other, format!("Failed to encode filter header: {}", e))
                    })?;
                }
                Ok(())
            })
        }
    })
    .await
    .map_err(|e| StorageError::WriteFailed(format!("Task join error: {}", e)))?
}

/// Save index to disk.
pub(super) async fn save_index_to_disk(
    path: &Path,
    index: &HashMap<BlockHash, u32>,
) -> StorageResult<()> {
    tokio::task::spawn_blocking({
        let path = path.to_path_buf();
        let index = index.clone();
        move || {
            let data = bincode::serialize(&index).map_err(|e| {
                StorageError::WriteFailed(format!("Failed to serialize index: {}", e))
            })?;
            fs::write(&path, data)?;
            Ok(())
        }
    })
    .await
    .map_err(|e| StorageError::WriteFailed(format!("Task join error: {}", e)))?
}

// Combined filter data segment magic bytes: "FDS2" (Filter Data Segment v2 - combined format)
const FILTER_DATA_SEGMENT_MAGIC: [u8; 4] = [0x46, 0x44, 0x53, 0x32];
const FILTER_DATA_SEGMENT_VERSION: u16 = 1;

use super::segments::FilterDataIndexEntry;

/// Load filter data segment from the combined segment file.
/// - Header (12 bytes): magic (4) + version (2) + count (2) + data_offset (4)
/// - Index entries (12 bytes each): offset (8) + length (4)
/// - Data section: raw filter bytes
///
/// Returns (index_entries, data_offset) where:
/// - index_entries have RELATIVE offsets (relative to data section start)
/// - data_offset is where the data section starts in the file
pub(super) async fn load_filter_data_index(path: &Path) -> StorageResult<(Vec<FilterDataIndexEntry>, u64)> {
    use std::io::Read;

    tokio::task::spawn_blocking({
        let path = path.to_path_buf();
        move || {
            let file = File::open(&path)?;
            let mut reader = BufReader::new(file);

            // Read and validate magic bytes
            let mut magic = [0u8; 4];
            reader.read_exact(&mut magic)?;

            if magic != FILTER_DATA_SEGMENT_MAGIC {
                return Err(StorageError::ReadFailed(
                    "Invalid filter data segment magic bytes".to_string(),
                ));
            }

            let mut version_bytes = [0u8; 2];
            reader.read_exact(&mut version_bytes)?;
            let version = u16::from_le_bytes(version_bytes);
            if version != FILTER_DATA_SEGMENT_VERSION {
                return Err(StorageError::ReadFailed(format!(
                    "Unsupported filter data segment version: {}",
                    version
                )));
            }

            let mut count_bytes = [0u8; 2];
            reader.read_exact(&mut count_bytes)?;
            let count = u16::from_le_bytes(count_bytes) as usize;

            // Read data offset (where data section starts)
            let mut data_offset_bytes = [0u8; 4];
            reader.read_exact(&mut data_offset_bytes)?;
            let data_offset = u32::from_le_bytes(data_offset_bytes) as u64;

            // Read entries and convert to relative offsets
            let mut entries = Vec::with_capacity(count);
            for _ in 0..count {
                let mut offset_bytes = [0u8; 8];
                let mut length_bytes = [0u8; 4];
                reader.read_exact(&mut offset_bytes)?;
                reader.read_exact(&mut length_bytes)?;

                let absolute_offset = u64::from_le_bytes(offset_bytes);
                // Convert to relative offset (relative to data section)
                let relative_offset = absolute_offset.saturating_sub(data_offset);

                entries.push(FilterDataIndexEntry {
                    offset: relative_offset,
                    length: u32::from_le_bytes(length_bytes),
                });
            }

            Ok((entries, data_offset))
        }
    })
    .await
    .map_err(|e| StorageError::ReadFailed(format!("Task join error: {}", e)))?
}

/// Load a single filter from the combined segment file.
/// - relative_offset: offset relative to data section start
/// - data_offset: where data section starts in the file
/// The actual seek position is relative_offset + data_offset.
pub(super) async fn load_filter_data_at_offset(
    path: &Path,
    relative_offset: u64,
    data_offset: u64,
    length: u32,
) -> StorageResult<Vec<u8>> {
    use std::io::{Read, Seek, SeekFrom};

    tokio::task::spawn_blocking({
        let path = path.to_path_buf();
        move || {
            let mut file = File::open(&path)?;
            let absolute_offset = relative_offset + data_offset;
            file.seek(SeekFrom::Start(absolute_offset))?;

            let mut data = vec![0u8; length as usize];
            file.read_exact(&mut data)?;

            Ok(data)
        }
    })
    .await
    .map_err(|e| StorageError::ReadFailed(format!("Task join error: {}", e)))?
}

/// Save filter data segment as a single combined file atomically.
/// Combined format ensures index and data are always consistent.
/// Format:
/// - Header (12 bytes): magic (4) + version (2) + count (2) + data_offset (4)
/// - Index entries (12 bytes each): offset (8) + length (4)
/// - Data section: raw filter bytes
pub(super) async fn save_filter_data_segment(
    segment_path: &Path,
    index: &[FilterDataIndexEntry],
    data: &[u8],
) -> StorageResult<()> {
    tokio::task::spawn_blocking({
        let segment_path = segment_path.to_path_buf();
        let index = index.to_vec();
        let data = data.to_vec();
        move || {
            // Calculate data section offset
            // Header: 12 bytes, Index: 12 bytes per entry
            let header_size = 12u32;
            let index_size = (index.len() as u32) * 12;
            let data_offset = header_size + index_size;

            // Write combined file atomically
            atomic_write_sync(&segment_path, |writer| {
                // Write header
                writer.write_all(&FILTER_DATA_SEGMENT_MAGIC)?;
                writer.write_all(&FILTER_DATA_SEGMENT_VERSION.to_le_bytes())?;
                writer.write_all(&(index.len() as u16).to_le_bytes())?;
                writer.write_all(&data_offset.to_le_bytes())?;

                // Write index entries
                // Adjust offsets to be relative to file start (add data_offset)
                for entry in &index {
                    let absolute_offset = entry.offset + data_offset as u64;
                    writer.write_all(&absolute_offset.to_le_bytes())?;
                    writer.write_all(&entry.length.to_le_bytes())?;
                }

                // Write data section
                writer.write_all(&data)?;

                Ok(())
            })?;

            Ok(())
        }
    })
    .await
    .map_err(|e| StorageError::WriteFailed(format!("Task join error: {}", e)))?
}
