//! WAL segment decode (read-only).
//!
//! A NexusDB WAL segment is a bare concatenation of records — no file header,
//! no magic number, no frame header. The format is:
//!
//! ```text
//! [payload_len: u32 LE][crc32: u32 LE][payload]
//! ```
//!
//! Where `payload` is:
//!
//! ```text
//! [op: u8][db_len: u16 LE][db bytes][tbl_len: u16 LE][tbl bytes]
//! [key_len: u32 LE][key bytes][val_len: u32 LE][val bytes]
//! ```
//!
//! - `op` = 1 (PUT) or 2 (DELETE)
//! - CRC32 is IEEE polynomial (0xEDB88320), init = 0xFFFF_FFFF, final XOR = 0xFFFF_FFFF
//! - Torn writes: if `payload_len` extends beyond available data, or CRC fails,
//!   decoding stops at that point. All records before the first corrupted record
//!   are guaranteed valid.
//!
//! Cross-platform: all integer decoding is explicit LE. No `unsafe`. No OS
//! dependencies.

use std::io::Read;
use std::path::Path;

use crate::dir::ShardDir;

/// A single decoded WAL record.
#[derive(Debug, PartialEq, Eq)]
pub struct WalRecord {
    pub db: String,
    pub table: String,
    pub pkey: Vec<u8>,
    /// `Some(value)` = PUT, `None` = DELETE.
    pub value: Option<Vec<u8>>,
}

const OP_PUT: u8 = 1;
const OP_DEL: u8 = 2;

/// CRC32 (IEEE) with a 256-entry lookup table.
///
/// Matches the engine's implementation exactly (polynomial 0xEDB88320,
/// init 0xFFFF_FFFF, final XOR 0xFFFF_FFFF).
pub fn crc32(data: &[u8]) -> u32 {
    static TABLE: std::sync::OnceLock<[u32; 256]> = std::sync::OnceLock::new();
    let table = TABLE.get_or_init(|| {
        let mut t = [0u32; 256];
        for (i, e) in t.iter_mut().enumerate() {
            let mut c = i as u32;
            for _ in 0..8 {
                c = if c & 1 == 1 {
                    0xEDB8_8320 ^ (c >> 1)
                } else {
                    c >> 1
                };
            }
            *e = c;
        }
        t
    });
    let mut crc = 0xFFFF_FFFFu32;
    for &b in data {
        crc = table[((crc ^ b as u32) & 0xFF) as usize] ^ (crc >> 8);
    }
    !crc
}

/// Decode all intact records from a WAL byte slice.
///
/// Torn writes are silently truncated: if the last record's `payload_len`
/// extends beyond the available data, or its CRC32 check fails, we stop
/// and return the records decoded so far.
pub fn decode_records(data: &[u8]) -> Vec<WalRecord> {
    let mut out = Vec::new();
    let mut pos = 0usize;
    loop {
        if pos + 8 > data.len() {
            break;
        }
        let len = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap()) as usize;
        let crc = u32::from_le_bytes(data[pos + 4..pos + 8].try_into().unwrap());
        let Some(payload) = data.get(pos + 8..pos + 8 + len) else {
            break; // length past end = torn tail
        };
        if crc32(payload) != crc {
            break; // CRC mismatch = torn (subsequent data is untrustworthy)
        }
        if let Some(rec) = decode_payload(payload) {
            out.push(rec);
        } else {
            break; // malformed payload despite CRC passing — defensive stop
        }
        pos += 8 + len;
    }
    out
}

/// Decode a single WAL payload.
///
/// Returns `None` if the op code is unknown or the payload is structurally
/// malformed (bounds check failure).
pub fn decode_payload(p: &[u8]) -> Option<WalRecord> {
    let mut pos = 0usize;
    let op = *p.first()?;
    pos += 1;
    let db_len = u16::from_le_bytes(p.get(pos..pos + 2)?.try_into().ok()?) as usize;
    pos += 2;
    let db = String::from_utf8(p.get(pos..pos + db_len)?.to_vec()).ok()?;
    pos += db_len;
    let tbl_len = u16::from_le_bytes(p.get(pos..pos + 2)?.try_into().ok()?) as usize;
    pos += 2;
    let table = String::from_utf8(p.get(pos..pos + tbl_len)?.to_vec()).ok()?;
    pos += tbl_len;
    let key_len = u32::from_le_bytes(p.get(pos..pos + 4)?.try_into().ok()?) as usize;
    pos += 4;
    let pkey = p.get(pos..pos + key_len)?.to_vec();
    pos += key_len;
    let val_len = u32::from_le_bytes(p.get(pos..pos + 4)?.try_into().ok()?) as usize;
    pos += 4;
    let val = p.get(pos..pos + val_len)?.to_vec();
    match op {
        OP_PUT => Some(WalRecord {
            db,
            table,
            pkey,
            value: Some(val),
        }),
        OP_DEL => Some(WalRecord {
            db,
            table,
            pkey,
            value: None,
        }),
        _ => None,
    }
}

/// Read a WAL segment file and decode its records.
///
/// Returns `Ok(records)` on success, or `Err` if the file cannot be read.
pub fn read_wal_file(path: &Path) -> std::io::Result<Vec<WalRecord>> {
    let mut f = std::fs::File::open(path)?;
    let mut buf = Vec::new();
    f.read_to_end(&mut buf)?;
    Ok(decode_records(&buf))
}

/// Collect all WAL segments from a shard, sorted by sequence number.
pub fn list_wal(shard: &ShardDir) -> Vec<&crate::dir::WalSegment> {
    let mut segs: Vec<_> = shard.wal_segments.iter().collect();
    segs.sort_by_key(|w| w.seq);
    segs
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper: encode a single WAL record (matching the engine's format).
    fn encode_record(buf: &mut Vec<u8>, db: &str, table: &str, pkey: &[u8], value: Option<&[u8]>) {
        let val = value.unwrap_or(&[]);
        let payload_len = 1 + 2 + db.len() + 2 + table.len() + 4 + pkey.len() + 4 + val.len();
        buf.extend_from_slice(&(payload_len as u32).to_le_bytes());
        let crc_pos = buf.len();
        buf.extend_from_slice(&[0u8; 4]); // CRC placeholder
        let p0 = buf.len();
        buf.push(if value.is_some() { OP_PUT } else { OP_DEL });
        buf.extend_from_slice(&(db.len() as u16).to_le_bytes());
        buf.extend_from_slice(db.as_bytes());
        buf.extend_from_slice(&(table.len() as u16).to_le_bytes());
        buf.extend_from_slice(table.as_bytes());
        buf.extend_from_slice(&(pkey.len() as u32).to_le_bytes());
        buf.extend_from_slice(pkey);
        buf.extend_from_slice(&(val.len() as u32).to_le_bytes());
        buf.extend_from_slice(val);
        let crc = crc32(&buf[p0..]);
        buf[crc_pos..crc_pos + 4].copy_from_slice(&crc.to_le_bytes());
    }

    #[test]
    fn crc32_matches_known_value() {
        // Empty string CRC32 = 0x00000000
        assert_eq!(crc32(b""), 0x0000_0000);
        // "hello" CRC32 = 0x3610A686 (standard CRC-32)
        assert_eq!(crc32(b"hello"), 0x3610_A686);
        // "world" CRC32 = 0x3A771143 (standard CRC-32)
        assert_eq!(crc32(b"world"), 0x3A77_1143);
    }

    #[test]
    fn decode_single_put_record() {
        let mut buf = Vec::new();
        encode_record(&mut buf, "db1", "t1", b"key-a", Some(b"val-a"));
        let recs = decode_records(&buf);
        assert_eq!(recs.len(), 1);
        assert_eq!(recs[0].db, "db1");
        assert_eq!(recs[0].table, "t1");
        assert_eq!(recs[0].pkey, b"key-a");
        assert_eq!(recs[0].value.as_deref(), Some(b"val-a".as_ref()));
    }

    #[test]
    fn decode_single_delete_record() {
        let mut buf = Vec::new();
        encode_record(&mut buf, "db2", "t2", b"key-b", None);
        let recs = decode_records(&buf);
        assert_eq!(recs.len(), 1);
        assert_eq!(recs[0].db, "db2");
        assert_eq!(recs[0].table, "t2");
        assert_eq!(recs[0].pkey, b"key-b");
        assert_eq!(recs[0].value, None);
    }

    #[test]
    fn decode_multiple_records() {
        let mut buf = Vec::new();
        encode_record(&mut buf, "db1", "t1", b"key-a", Some(b"val-a"));
        encode_record(&mut buf, "db2", "t2", b"key-b", None);
        encode_record(&mut buf, "db3", "t3", b"key-c", Some(b"val-c"));
        let recs = decode_records(&buf);
        assert_eq!(recs.len(), 3);
        assert_eq!(recs[0].pkey, b"key-a");
        assert_eq!(recs[1].pkey, b"key-b");
        assert_eq!(recs[2].pkey, b"key-c");
        assert!(recs[0].value.is_some());
        assert!(recs[1].value.is_none());
        assert!(recs[2].value.is_some());
    }

    #[test]
    fn torn_tail_truncates() {
        let mut buf = Vec::new();
        encode_record(&mut buf, "d", "t", b"k1", Some(b"v1"));
        let good_len = buf.len();
        encode_record(&mut buf, "d", "t", b"k2", Some(b"v2"));
        // Truncate the second record midway
        buf.truncate(good_len + 5);
        let recs = decode_records(&buf);
        assert_eq!(recs.len(), 1);
        assert_eq!(recs[0].pkey, b"k1");
    }

    #[test]
    fn crc_failure_stops_decoding() {
        let mut buf = Vec::new();
        encode_record(&mut buf, "d", "t", b"k1", Some(b"v1"));
        let p = buf.len();
        encode_record(&mut buf, "d", "t", b"k2", Some(b"v2"));
        // Corrupt a byte in the second payload
        buf[p + 10] ^= 0xFF;
        let recs = decode_records(&buf);
        assert_eq!(recs.len(), 1);
        assert_eq!(recs[0].pkey, b"k1");
    }

    #[test]
    fn empty_data_returns_empty() {
        let recs = decode_records(b"");
        assert!(recs.is_empty());
    }

    #[test]
    fn partial_header_stops() {
        // Only 4 bytes — not enough for the 8-byte header
        let recs = decode_records(b"\x00\x01\x00\x00");
        assert!(recs.is_empty());
    }
}