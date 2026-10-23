//! ⭐ Meta log (实验分支, 2026-10-23): page.mate 的追加式增量日志.
//!
//! ## 目标
//! 把 meta 稳态写从"每周期重写脏子区 (8KB/子区)"改成"追加变更条目
//! (~20B/槽)": 大 keyspace 随机覆写下, 原地写每周期成本 = 脏子区数 × 8KB,
//! 与改了几个槽无关; 追加日志成本 = 变更槽数 × 20B, 与 keyspace 无关.
//!
//! ## 格式
//! - 段文件 `page.metlog.{seq:06}` (同 page.mate 目录), 定长 4MB, 顺序追加
//! - 段头 16B: magic u64 + version u32 + reserved u32
//! - 记录 20B: vpid u64 (LE) + pid 8B (LE) + crc32 u32 (payload 16B)
//! - 绝对赋值 ⇒ 重放幂等; 同一 vpid 多次出现时后写覆盖先写
//!
//! ## 恢复
//! `page.mate` (base) + 全部剩余段按 seq 重放. 已合并段在 mate fsync 后删除,
//! 所以剩余段 = 未合并尾部. 尾段 torn tail 静默截断 (fsync 未完成的批次本来
//! 就没确认); 非尾段损坏 = `InvalidData` 硬错误 (真实完整性问题).
//!
//! ## 与 mate 的关系
//! 合并完成前 `mate ⊆ log 前缀`; 热路径只写日志, 未进日志的变更仅在内存缓冲
//! + WAL 中 (WAL 段保留规则由 `MetaCache::has_unflushed` 覆盖).

use std::fs::{File, OpenOptions};
use std::io::{self, Read};
use std::path::{Path, PathBuf};

use crate::file_at::FileAt;
use crate::types::PidLocation;
use crate::wal::crc32;

/// 单段最大字节数 (4MB).
pub const SEGMENT_SIZE: u64 = 4 << 20;
/// 记录大小: vpid 8B + pid 8B + crc 4B.
pub const RECORD_SIZE: usize = 20;
/// 段头大小.
pub const HEADER_SIZE: u64 = 16;
/// 段头 magic ("NXMETALG").
const MAGIC: u64 = 0x4E58_4D45_5441_4C47;
const VERSION: u32 = 1;

/// 段文件路径.
pub fn segment_path(dir: &Path, seq: u64) -> PathBuf {
    dir.join(format!("page.metlog.{seq:06}"))
}

/// 新段头字节.
pub fn header_bytes() -> [u8; HEADER_SIZE as usize] {
    let mut h = [0u8; HEADER_SIZE as usize];
    h[0..8].copy_from_slice(&MAGIC.to_le_bytes());
    h[8..12].copy_from_slice(&VERSION.to_le_bytes());
    h
}

/// 编码一条记录 (20B) 到 `out`.
pub fn encode_record(out: &mut Vec<u8>, vpid: u64, pid: PidLocation) {
    let start = out.len();
    out.extend_from_slice(&vpid.to_le_bytes());
    out.extend_from_slice(&pid.to_bytes());
    let crc = crc32(&out[start..start + 16]);
    out.extend_from_slice(&crc.to_le_bytes());
}

/// 解析记录, 返回 (记录数, 有效字节数); 遇第一条不完整/CRC 错误记录即停.
pub fn decode_into(bytes: &[u8], mut apply: impl FnMut(u64, PidLocation)) -> (usize, usize) {
    let mut off = 0usize;
    let mut n = 0usize;
    while off + RECORD_SIZE <= bytes.len() {
        let payload = &bytes[off..off + 16];
        let crc = u32::from_le_bytes(bytes[off + 16..off + 20].try_into().expect("4B crc"));
        if crc32(payload) != crc {
            break;
        }
        let vpid = u64::from_le_bytes(payload[0..8].try_into().expect("8B vpid"));
        let pid = PidLocation::from_bytes(payload[8..16].try_into().expect("8B pid"));
        apply(vpid, pid);
        off += RECORD_SIZE;
        n += 1;
    }
    (n, off)
}

/// 列出目录内全部段 (seq 升序). 目录不存在 → 空.
pub fn list_segments(dir: &Path) -> io::Result<Vec<(u64, PathBuf)>> {
    let mut out = Vec::new();
    let rd = match std::fs::read_dir(dir) {
        Ok(rd) => rd,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(out),
        Err(e) => return Err(e),
    };
    for ent in rd {
        let ent = ent?;
        let name = ent.file_name();
        let Some(name) = name.to_str() else { continue };
        let Some(seq) = name
            .strip_prefix("page.metlog.")
            .and_then(|s| s.parse::<u64>().ok())
        else {
            continue;
        };
        out.push((seq, ent.path()));
    }
    out.sort_by_key(|(s, _)| *s);
    Ok(out)
}

/// 重放结果元信息.
#[derive(Debug)]
pub struct ReplayInfo {
    pub segments: Vec<(u64, PathBuf)>,
    pub max_seq: u64,
    pub total_bytes: u64,
    pub records: usize,
}

/// 按 seq 顺序重放全部段, 逐条回调 `apply`.
///
/// 尾段 torn tail 静默丢弃 (未 fsync 确认); 非尾段损坏返回 `InvalidData`.
pub fn replay_into(dir: &Path, mut apply: impl FnMut(u64, PidLocation)) -> io::Result<ReplayInfo> {
    let segments = list_segments(dir)?;
    let max_seq = segments.last().map(|(s, _)| *s).unwrap_or(0);
    let mut total_bytes = 0u64;
    let mut records = 0usize;
    for (i, (seq, path)) in segments.iter().enumerate() {
        let is_last = i + 1 == segments.len();
        let mut f = File::open(path)?;
        let mut bytes = Vec::new();
        f.read_to_end(&mut bytes)?;
        total_bytes += bytes.len() as u64;
        if bytes.len() < HEADER_SIZE as usize {
            // 首次 append 崩溃: 只有半写段头 → 视为空段
            if is_last {
                continue;
            }
            return Err(corrupt(*seq, "short header"));
        }
        let magic = u64::from_le_bytes(bytes[0..8].try_into().expect("8B magic"));
        let ver = u32::from_le_bytes(bytes[8..12].try_into().expect("4B version"));
        if magic != MAGIC || ver != VERSION {
            if is_last {
                continue;
            }
            return Err(corrupt(*seq, "bad header"));
        }
        let (n, valid) = decode_into(&bytes[HEADER_SIZE as usize..], &mut apply);
        records += n;
        if HEADER_SIZE as usize + valid < bytes.len() && !is_last {
            return Err(corrupt(*seq, "torn record before last segment"));
        }
    }
    Ok(ReplayInfo {
        segments,
        max_seq,
        total_bytes,
        records,
    })
}

fn corrupt(seq: u64, what: &str) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        format!("meta log segment {seq} corrupt: {what}"),
    )
}

/// 同步追加一批字节 (offset 由调用方按段布局算好) + fsync.
pub fn append_sync(path: &Path, offset: u64, bytes: &[u8]) -> io::Result<()> {
    let f = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(path)?;
    f.write_all_at(bytes, offset)?;
    f.sync_all()
}

/// 截断段到指定长度 (追加失败回滚用).
pub fn truncate_sync(path: &Path, len: u64) -> io::Result<()> {
    let f = OpenOptions::new().write(true).open(path)?;
    f.set_len(len)?;
    f.sync_all()
}

/// 删除一组段文件 (失败仅忽略 — 下轮合并重试; 重放幂等).
pub fn delete_segments(paths: &[PathBuf]) {
    for p in paths {
        let _ = std::fs::remove_file(p);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::PID_ALIVE;
    use std::io::Write;

    fn pid(n: u64) -> PidLocation {
        PidLocation {
            file_id: (n & 0xFFFF) as u32,
            chunk_idx: ((n >> 16) & 0xFF) as u8,
            page_idx: ((n >> 24) & 0x3F) as u16,
            flags: PID_ALIVE,
        }
    }

    #[test]
    fn encode_decode_roundtrip() {
        let mut bytes = Vec::new();
        for i in 0..100u64 {
            encode_record(&mut bytes, i * 7, pid(i));
        }
        assert_eq!(bytes.len(), 100 * RECORD_SIZE);
        let mut got = Vec::new();
        let (n, valid) = decode_into(&bytes, |v, p| got.push((v, p)));
        assert_eq!(n, 100);
        assert_eq!(valid, bytes.len());
        for (i, (v, p)) in got.iter().enumerate() {
            assert_eq!(*v, i as u64 * 7);
            assert_eq!(*p, pid(i as u64));
        }
    }

    #[test]
    fn decode_stops_at_torn_record() {
        let mut bytes = Vec::new();
        for i in 0..10u64 {
            encode_record(&mut bytes, i, pid(i));
        }
        // 尾部半条记录
        bytes.extend_from_slice(&[0xAB; 7]);
        let mut n = 0;
        let (cnt, valid) = decode_into(&bytes, |_, _| n += 1);
        assert_eq!(cnt, 10);
        assert_eq!(n, 10);
        assert_eq!(valid, 10 * RECORD_SIZE);

        // CRC 损坏: 第 5 条起丢弃
        let mut bytes = Vec::new();
        for i in 0..10u64 {
            encode_record(&mut bytes, i, pid(i));
        }
        bytes[5 * RECORD_SIZE + 3] ^= 0xFF;
        let (cnt, valid) = decode_into(&bytes, |_, _| {});
        assert_eq!(cnt, 5);
        assert_eq!(valid, 5 * RECORD_SIZE);
    }

    #[test]
    fn replay_across_segments_and_torn_tail() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path();

        // 段 1: 2 条; 段 2: 3 条 + 半条
        let mut b1 = header_bytes().to_vec();
        encode_record(&mut b1, 1, pid(11));
        encode_record(&mut b1, 2, pid(12));
        append_sync(&segment_path(dir, 1), 0, &b1).unwrap();

        let mut b2 = header_bytes().to_vec();
        for i in 3..6u64 {
            encode_record(&mut b2, i, pid(10 + i));
        }
        b2.extend_from_slice(&[0xCD; 9]);
        append_sync(&segment_path(dir, 2), 0, &b2).unwrap();

        let mut got = Vec::new();
        let info = replay_into(dir, |v, p| got.push((v, p))).unwrap();
        assert_eq!(info.segments.len(), 2);
        assert_eq!(info.max_seq, 2);
        assert_eq!(info.records, 5);
        assert_eq!(
            got.iter().map(|(v, _)| *v).collect::<Vec<_>>(),
            vec![1, 2, 3, 4, 5]
        );
        assert_eq!(got[4].1, pid(15));
    }

    #[test]
    fn non_last_segment_corruption_is_hard_error() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path();
        let mut b1 = header_bytes().to_vec();
        encode_record(&mut b1, 1, pid(1));
        b1.extend_from_slice(&[0xEE; 5]); // 段 1 torn, 但后面还有段 2
        append_sync(&segment_path(dir, 1), 0, &b1).unwrap();
        let mut b2 = header_bytes().to_vec();
        encode_record(&mut b2, 2, pid(2));
        append_sync(&segment_path(dir, 2), 0, &b2).unwrap();

        let err = replay_into(dir, |_, _| {}).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }

    #[test]
    fn bad_last_header_is_ignored() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path();
        let mut f = File::create(segment_path(dir, 1)).unwrap();
        f.write_all(&[0u8; 5]).unwrap();
        drop(f);
        let info = replay_into(dir, |_, _| {}).unwrap();
        assert_eq!(info.records, 0);
        assert_eq!(info.segments.len(), 1);
    }

    #[test]
    fn list_segments_ignores_foreign_files() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path();
        File::create(dir.join("page.mate")).unwrap();
        File::create(dir.join("page.metlog.000003")).unwrap();
        File::create(dir.join("page.metlog.000001")).unwrap();
        File::create(dir.join("page.metlog.bad")).unwrap();
        let segs = list_segments(dir).unwrap();
        assert_eq!(segs.iter().map(|(s, _)| *s).collect::<Vec<_>>(), vec![1, 3]);
    }
}
