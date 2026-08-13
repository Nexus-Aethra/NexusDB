//! ⭐ WAL (F60): per-shard 预写日志 — 填补周期刷盘 (256 写/10s) 的丢失窗口.
//!
//! ## 设计
//! - **插入点**: `put_physical` / `put_physical_many` / `delete_physical`
//!   成功路径 append — 全部写路径 (String KV / SQL row / Redis 复合) 的唯一
//!   收敛点; 非幂等 RMW (INCR/APPEND/..) 在 shard 层已算成结果态才落 KV,
//!   故记录 (db, table, pkey, value/del) 按序重放天然幂等 (last-writer-wins).
//! - **段文件**: `{block_root}/shard_{N}.wal.{seq:06}` — cur 段 append,
//!   刷盘快照触发时 seal (cur 进 sealed 列表, 开新段), meta flush 完成后删
//!   sealed 段 (其覆盖的记录已由 chunk+meta 持久化). crash 时现存全部段重放.
//! - **三档**:
//!   - `Off`: 不建 WalWriter, 零开销
//!   - `Periodic` (默认): append 进内存 buf 即返回, shard 主循环每 1s
//!     flush+fsync (丢失窗口 10s → ~1s)
//!   - `Strict`: 回复客户端前 flush+fsync (组提交: 一轮 drain 的多个写共享
//!     一次 fsync) — reply 到达 ⇒ 已持久化, crash 零丢失
//! - **torn write**: 记录带 len + crc32, 尾部损坏记录即截断点, 静默丢弃其后
//!   (periodic 档丢尾属于窗口语义; strict 档回复前已 fsync, 已回复记录必完整)

use std::io::{self, Write};
use std::path::{Path, PathBuf};

/// WAL 持久化档位.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum WalMode {
    /// 无 WAL (兼容基准, 丢失窗口 = 刷盘周期 10s).
    Off,
    /// 每秒 fsync (默认): 窗口 ~1s, 性能基本无感.
    #[default]
    Periodic,
    /// 每批回复前 fsync + 组提交: crash 零丢失 (用户可选最高级别).
    Strict,
}

impl WalMode {
    /// 配置字符串解析 ("off"/"periodic"/"strict", 大小写不敏感).
    pub fn parse(s: &str) -> Option<Self> {
        match s.to_ascii_lowercase().as_str() {
            "off" => Some(Self::Off),
            "periodic" | "" => Some(Self::Periodic),
            "strict" => Some(Self::Strict),
            _ => None,
        }
    }
}

/// 单条重放记录.
#[derive(Debug, PartialEq, Eq)]
pub struct WalRecord {
    pub db: String,
    pub table: String,
    pub pkey: Vec<u8>,
    /// Some = put, None = delete.
    pub value: Option<Vec<u8>>,
}

const OP_PUT: u8 = 1;
const OP_DEL: u8 = 2;

/// CRC32 (IEEE) 查表实现 (零依赖; ⭐ v2/F62 pub — OCC 读集指纹复用).
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

/// 记录编码进 buf: [u32 len][u32 crc][payload], payload =
/// [op][u16 db_len][db][u16 tbl_len][tbl][u32 key_len][key][u32 val_len][val].
fn encode_record(buf: &mut Vec<u8>, db: &str, table: &str, pkey: &[u8], value: Option<&[u8]>) {
    let val = value.unwrap_or(&[]);
    let payload_len = 1 + 2 + db.len() + 2 + table.len() + 4 + pkey.len() + 4 + val.len();
    buf.reserve(8 + payload_len);
    buf.extend_from_slice(&(payload_len as u32).to_le_bytes());
    let crc_pos = buf.len();
    buf.extend_from_slice(&[0u8; 4]); // crc 占位
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

/// 从字节流解码全部完好记录 (torn tail 静默截断).
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
            break; // 长度越界 = torn tail
        };
        if crc32(payload) != crc {
            break; // 校验失败 = torn (其后不可信)
        }
        if let Some(rec) = decode_payload(payload) {
            out.push(rec);
        } else {
            break; // payload 结构坏 (理论上 crc 已挡, 防御)
        }
        pos += 8 + len;
    }
    out
}

fn decode_payload(p: &[u8]) -> Option<WalRecord> {
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

/// per-shard WAL 写入器 (Off 档不构造).
pub struct WalWriter {
    dir: PathBuf,
    shard_id: u32,
    mode: WalMode,
    /// 当前段文件 (总是存在).
    cur: std::fs::File,
    cur_seq: u64,
    /// 待 append 的记录 buf (flush 时 write_all 进 cur).
    buf: Vec<u8>,
    /// 自上次 fsync 后有过写盘 (buf 之外还需 fsync 的量).
    dirty_since_sync: bool,
    /// 已 seal 待删段 (对应刷盘在途快照; meta flush 完成后删除).
    sealed: Vec<PathBuf>,
    /// io_uring 后端时 fsync 走异步 SQE (不阻塞 shard 线程).
    use_uring: bool,
    last_sync: std::time::Instant,
}

impl WalWriter {
    fn seg_path(dir: &Path, shard_id: u32, seq: u64) -> PathBuf {
        dir.join(format!("shard_{shard_id}.wal.{seq:06}"))
    }

    /// 列出现存段 (升序) — 重放顺序.
    pub fn existing_segments(dir: &Path, shard_id: u32) -> Vec<PathBuf> {
        let prefix = format!("shard_{shard_id}.wal.");
        let mut segs: Vec<(u64, PathBuf)> = std::fs::read_dir(dir)
            .into_iter()
            .flatten()
            .flatten()
            .filter_map(|e| {
                let name = e.file_name().into_string().ok()?;
                let seq: u64 = name.strip_prefix(&prefix)?.parse().ok()?;
                Some((seq, e.path()))
            })
            .collect();
        segs.sort();
        segs.into_iter().map(|(_, p)| p).collect()
    }

    /// 打开 (不重放 — 重放由 engine 层读 `existing_segments` 后调
    /// `purge_replayed`); 新 cur 段号 = 现存最大 + 1.
    pub fn open(dir: &Path, shard_id: u32, mode: WalMode, use_uring: bool) -> io::Result<Self> {
        std::fs::create_dir_all(dir)?;
        let next_seq = Self::existing_segments(dir, shard_id)
            .last()
            .and_then(|p| p.extension()?.to_str()?.parse::<u64>().ok())
            .map_or(1, |s| s + 1);
        let cur = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(Self::seg_path(dir, shard_id, next_seq))?;
        Ok(Self {
            dir: dir.to_path_buf(),
            shard_id,
            mode,
            cur,
            cur_seq: next_seq,
            buf: Vec::with_capacity(64 * 1024),
            dirty_since_sync: false,
            sealed: Vec::new(),
            use_uring,
            last_sync: std::time::Instant::now(),
        })
    }

    pub fn mode(&self) -> WalMode {
        self.mode
    }

    /// 记录一次 put (结果态).
    pub fn append_put(&mut self, db: &str, table: &str, pkey: &[u8], value: &[u8]) {
        encode_record(&mut self.buf, db, table, pkey, Some(value));
    }

    /// 记录一次 delete.
    pub fn append_del(&mut self, db: &str, table: &str, pkey: &[u8]) {
        encode_record(&mut self.buf, db, table, pkey, None);
    }

    /// 有未持久化内容 (buf 或已写未 sync).
    pub fn needs_sync(&self) -> bool {
        !self.buf.is_empty() || self.dirty_since_sync
    }

    /// Periodic 档: 距上次 sync 是否已达周期.
    pub fn periodic_due(&self, period: std::time::Duration) -> bool {
        self.needs_sync() && self.last_sync.elapsed() >= period
    }

    /// buf 落盘 + fsync (strict 回复前 / periodic 每秒 / seal 前).
    /// io_uring 后端 fsync 为异步 SQE (协程挂起不阻塞 shard 线程).
    pub async fn flush_and_sync(&mut self) -> io::Result<()> {
        if !self.buf.is_empty() {
            self.cur.write_all(&self.buf)?;
            self.buf.clear();
            self.dirty_since_sync = true;
        }
        if self.dirty_since_sync {
            #[cfg(target_os = "linux")]
            if self.use_uring {
                use std::os::fd::AsRawFd;
                scheduler::io_ops::fsync(self.cur.as_raw_fd()).await?;
            } else {
                self.cur.sync_data()?;
            }
            #[cfg(not(target_os = "linux"))]
            {
                // Windows MVP 始终使用 StdFs；配置层不会允许 io_uring。
                self.cur.sync_data()?;
            }
            self.dirty_since_sync = false;
        }
        self.last_sync = std::time::Instant::now();
        Ok(())
    }

    /// 刷盘快照触发时刻调用 (**先 seal 后快照的同轮内**, 无并发写间隙):
    /// cur 段结束进 sealed 列表, 开新段. buf 随 seal 落入旧段 (不强制 fsync —
    /// periodic 档尾部未 sync 属窗口语义, strict 档此刻 buf 必已同步).
    pub fn seal(&mut self) -> io::Result<()> {
        if !self.buf.is_empty() {
            self.cur.write_all(&self.buf)?;
            self.buf.clear();
            self.dirty_since_sync = true;
        }
        self.sealed
            .push(Self::seg_path(&self.dir, self.shard_id, self.cur_seq));
        self.cur_seq += 1;
        self.cur = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(Self::seg_path(&self.dir, self.shard_id, self.cur_seq))?;
        self.dirty_since_sync = false; // 新段无未 sync 内容 (旧段丢弃在即)
        Ok(())
    }

    /// meta flush 完成后调用: sealed 段覆盖的记录已由 chunk+meta 持久化, 删除.
    pub fn drop_sealed(&mut self) {
        for p in self.sealed.drain(..) {
            let _ = std::fs::remove_file(&p);
        }
    }

    /// 正常关闭时调用: 全量已落盘, 删除全部段 (含 cur, 重启免重放).
    pub fn purge_all(&mut self) {
        self.buf.clear();
        self.drop_sealed();
        let _ = std::fs::remove_file(Self::seg_path(&self.dir, self.shard_id, self.cur_seq));
    }

    /// 重放完成后调用: 删除给定重放源段文件 (重放产物由后续正常刷盘持久化).
    pub fn purge_replayed(paths: &[PathBuf]) {
        for p in paths {
            let _ = std::fs::remove_file(p);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn record_roundtrip() {
        let mut buf = Vec::new();
        encode_record(&mut buf, "db1", "t1", b"key-a", Some(b"val-a"));
        encode_record(&mut buf, "db2", "t2", b"key-b", None);
        let recs = decode_records(&buf);
        assert_eq!(recs.len(), 2);
        assert_eq!(recs[0].db, "db1");
        assert_eq!(recs[0].value.as_deref(), Some(b"val-a".as_ref()));
        assert_eq!(recs[1].table, "t2");
        assert_eq!(recs[1].value, None);
    }

    #[test]
    fn torn_tail_truncates() {
        let mut buf = Vec::new();
        encode_record(&mut buf, "d", "t", b"k1", Some(b"v1"));
        let good_len = buf.len();
        encode_record(&mut buf, "d", "t", b"k2", Some(b"v2"));
        // 尾部截一半 = torn write
        buf.truncate(good_len + 5);
        let recs = decode_records(&buf);
        assert_eq!(recs.len(), 1);
        assert_eq!(recs[0].pkey, b"k1");
        // crc 破坏第二条
        let mut buf2 = Vec::new();
        encode_record(&mut buf2, "d", "t", b"k1", Some(b"v1"));
        let p = buf2.len();
        encode_record(&mut buf2, "d", "t", b"k2", Some(b"v2"));
        buf2[p + 10] ^= 0xFF;
        assert_eq!(decode_records(&buf2).len(), 1);
    }

    #[test]
    fn seal_and_segments() {
        let tmp = tempfile::tempdir().unwrap();
        let mut w = WalWriter::open(tmp.path(), 0, WalMode::Periodic, false).unwrap();
        w.append_put("d", "t", b"k1", b"v1");
        w.seal().unwrap(); // k1 落入段 1
        w.append_put("d", "t", b"k2", b"v2");
        // buf 未 flush 时 crash 模拟: 段 2 空, 段 1 含 k1
        let segs = WalWriter::existing_segments(tmp.path(), 0);
        assert_eq!(segs.len(), 2);
        let recs = decode_records(&std::fs::read(&segs[0]).unwrap());
        assert_eq!(recs.len(), 1);
        assert_eq!(recs[0].pkey, b"k1");
        // drop_sealed 只删 sealed 段
        w.drop_sealed();
        let segs = WalWriter::existing_segments(tmp.path(), 0);
        assert_eq!(segs.len(), 1);
        // 重开: 新段号递增, 不覆盖旧段
        drop(w);
        let w2 = WalWriter::open(tmp.path(), 0, WalMode::Periodic, false).unwrap();
        assert_eq!(WalWriter::existing_segments(tmp.path(), 0).len(), 2);
        drop(w2);
    }

    #[test]
    fn mode_parse() {
        assert_eq!(WalMode::parse("off"), Some(WalMode::Off));
        assert_eq!(WalMode::parse("Periodic"), Some(WalMode::Periodic));
        assert_eq!(WalMode::parse("STRICT"), Some(WalMode::Strict));
        assert_eq!(WalMode::parse("bogus"), None);
    }
}
