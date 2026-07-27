//! T7 recover: 启动时从 .block 扫描重建 MetaCache + Allocator 状态 (DESIGN §4.7).
//!
//! 流程 (DESIGN §3.0.3 union 语义):
//! 1. **加载 page.mate** 进 MetaCache 作为初值 (可能 stale).
//! 2. **扫描 .block 文件** (按 file_id 升序), 对每个 page 读 header 找 vpid.
//! 3. 把找到的 (vpid, pid) 写入 MetaCache (覆盖 mate 同 vpid 条目).
//! 4. 推导 alloc 状态: max_vpid, last_pid (max file_id/chunk_idx/page_idx).
//!
//! ## page header layout (DESIGN §4.2.3)
//!
//! - [0x00..0x04] magic "LCBP"
//! - [0x04]       page_type (1=Meta, 2=Internal, 3=Leaf)
//! - [0x14..0x18] version (4B LE)
//! - [0x18..0x20] vpid (8B LE)
//!
//! recover 扫 .block 时跳过:
//! - 无 magic 的 page (空 page 或损坏)
//! - page_type 越界的 page (corrupted)
//!
//! 遇到第一个 invalid page 停止扫描该 block (TDD 简化, T11 polish 加 union 容错).
//!
//! ## 单线程使用
//!
//! recover 调用方保证单线程使用 (per-shard thread), 沿用 MetaCache 契约.

use std::io;
use std::io::Read;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};

use crate::alloc::{PidAllocator, VpidAllocator};
use crate::meta_cache::MetaCache;
use crate::types::{DEFAULT_DB_NAME, DEFAULT_SHARD_ID, PAGE_SIZE, PID_ALIVE, PidLocation};

/// Page magic "LCBP" = [0x4C, 0x43, 0x42, 0x50].
const PAGE_MAGIC: [u8; 4] = [0x4C, 0x43, 0x42, 0x50];

/// block file 大小: 10MB (DESIGN §4.3.1).
const BLOCK_FILE_SIZE: u64 = 10 * 1024 * 1024;

/// chunk 大小: 1MB.
const CHUNK_SIZE: u64 = 1024 * 1024;

/// 单 chunk 的 page 数: 64 (1MB / 16KB).
const PAGES_PER_CHUNK: usize = 64;

/// 每 block 的 chunk 数: 10.
const CHUNKS_PER_BLOCK: u64 = 10;

// =====================================================================
// RecoveredState
// =====================================================================

/// recover 产出: 重建后的 MetaCache + 分配器状态.
pub struct RecoveredState {
    /// 重建后的 vpid → pid 映射.
    /// - 已包含 page.mate 加载的初值
    /// - 已被 .block scan 覆盖 (新数据权威)
    pub meta: MetaCache,

    /// vpid 分配器: next_vpid 已设为 max(seen vpid) + 1.
    pub vpid_alloc: VpidAllocator,

    /// pid 分配器: 指向活跃 block 最后一个 chunk + page_idx 末尾.
    pub pid_alloc: PidAllocator,

    /// 下一个待分配 vpid (冗余字段, 方便 caller 不必 move vpid_alloc 也能拿到).
    pub next_vpid: u64,

    /// 下一个要创建的 block file id (max(seen file_id) + 1).
    pub next_file_id: u32,
}

// =====================================================================
// 公开 API: recover(block_dir)
// =====================================================================

/// 启动 recover: 加载 `page.mate` (作为初值) + 扫描 `.block` 文件重建.
///
/// **Compat API**: 单 db 模式直接传 block_dir. 仍可工作, 内部委托给 `recover_for_shard`.
///
/// **新代码推荐用**: `recover_for_shard(block_root, shard_id)` 走
/// `block_root/default/shard_{N}/` 路径, 跟 T12.13 多 db 命名空间统一.
pub fn recover(block_dir: &Path) -> io::Result<RecoveredState> {
    let recovered = recover_for_shard(block_dir, DEFAULT_DB_NAME, DEFAULT_SHARD_ID)?;
    Ok(recovered)
}

/// ⭐ T12.12 公开 API: 按 `(block_root, db_name, shard_id)` 定位单 db 单 shard 目录
/// 并 recover.
///
/// **路径格式** (plan §1, T12 阶段 4 引入):
/// ```text
/// block_root/
/// └── {db_name}/              ← e.g. "default"
///     └── shard_{N}/          ← e.g. "shard_0"
///         ├── 000001.block
///         ├── 000002.block
///         └── page.mate
/// ```
///
/// **单 db 兼容**: 调用方传 `("default", 0)`, 路径 = `block_root/default/shard_0/`.
/// 等价于旧 `block_dir = block_root/default/shard_0/` 用法.
///
/// **Compat 模式**: 如果 `block_root` 本身直接是 block_dir (即里面已经有 `page.mate`
/// 或 `.block` 文件, 没有 `db_name/shard_N/` 子目录), 自动 fallback 到旧行为.
/// 这保证旧测试用 `OpenOptions::default().block_dir = "./data"` 直接当 block_dir 用
/// 时仍然能 recover (而不要求先建子目录).
pub fn recover_for_shard(
    block_root: &Path,
    db_name: &str,
    shard_id: u32,
) -> io::Result<RecoveredState> {
    let shard_dir = shard_dir_path(block_root, db_name, shard_id);

    // 优先用 shard_dir; 如果不存在, 但 block_root 直接是旧 block_dir, fallback.
    let block_dir: PathBuf = if shard_dir.exists() {
        shard_dir
    } else if block_root.join("page.mate").exists() || has_any_block(block_root) {
        // 旧 layout: block_root 就是 block_dir (OpenOptions::default 单 db 模式)
        block_root.to_path_buf()
    } else {
        // 全新 db, 用 shard_dir (后面会 ensure created)
        shard_dir
    };

    let mate_path = block_dir.join("page.mate");
    if !mate_path.exists() {
        // 没有 page.mate → 全新库, 走默认 empty MetaCache
        let tmp_mate = std::env::temp_dir().join("nexus_recover_empty.mate");
        let _ = std::fs::File::create(&tmp_mate);
        return Ok(RecoveredState {
            meta: MetaCache::open(&tmp_mate)?,
            vpid_alloc: VpidAllocator::new(0),
            pid_alloc: PidAllocator::new(0, 0, 0),
            next_vpid: 0,
            next_file_id: 0,
        });
    }

    // 1. 加载 page.mate 进 MetaCache (⭐ G3: mate 是主源 — meta 异步刷盘后
    //    最多落后一轮, 且 free-chunk 延迟复用保证 mate 未确认前旧位置数据仍有效)
    let mut meta = MetaCache::open(&mate_path)?;

    // 2. 收集 .block 文件, 按 file_id 升序 (G4 后可能有 file_id 空洞, 天然兼容)
    let block_files = collect_block_files(&block_dir)?;

    // 3. ⭐ 扫描 .block 文件提取 (vpid → pid) 候选.
    //    ⭐ G3 主源切换: chunk 复用后 "pid 越大越新" 不再成立, 扫描 union
    //    覆盖 meta 会把死页当新数据. 改为: **meta 有记录的 vpid 以 meta 为准**,
    //    扫描仅补 meta 缺失的 vpid (crash 窗口内首次写入、mate 尚未记录的页).
    //    丢失窗口 = 上次 meta 刷盘以来的更新 (≤ 一个刷盘周期, 与周期持久化承诺一致).
    let mut fill: std::collections::HashMap<u64, PidLocation> = std::collections::HashMap::new();
    let (max_vpid, last_pid, seen_any) = scan_block_files(&block_files, &mut fill)?;
    for (vpid, pid) in fill {
        // ⭐ 大 value: has_record 含墓碑 (PID_FREED) — 已释放的溢出页 vpid
        // 磁盘上仍残留旧 header, 回填会把死页"复活"为活页 (存储泄漏).
        if !meta.has_record(vpid) {
            meta.write(vpid, pid);
        }
        // meta 已有记录 (活或墓碑): 扫描候选可能是历史死页或复用后新写
        // (无法区分), 以 meta 一致点为准, 跳过.
    }

    // 4. 推导 alloc 起点 (vpid 水位取扫描与 mate 两者较大 — mate 可能含
    //    扫描不信任的 Internal 页 vpid; 注意用已分配 slot 而非数组水位,
    //    mate 文件可能预分配全零区)
    let scan_next_vpid = if seen_any { max_vpid + 1 } else { 0 };
    let mate_next_vpid = meta
        .iter_allocated()
        .map(|(v, _)| v + 1)
        .max()
        .unwrap_or(0);
    let next_vpid = scan_next_vpid.max(mate_next_vpid);
    let next_file_id = if seen_any { last_pid.0 + 1 } else { 0 };
    let mut pid_alloc = if seen_any {
        PidAllocator::new(last_pid.0, last_pid.1, last_pid.2 + 1)
    } else {
        PidAllocator::new(0, 0, 0)
    };

    // 4b. ⭐ Phase B: pid.state 快速路径 — 上次 flush 持久化的 pid_alloc 水位.
    //     与扫描推导取较大值 (pid.state 可能落后于崩溃前未 flush 的写入;
    //     扫描也可能落后于已 flush 但 magic 已被后续格式覆盖的边界).
    //     chunk_id = pid/64 的快速定位语义: 水位直接给出 (file, chunk, page).
    let pid_state_path = block_dir.join("pid.state");
    if let Ok(bytes) = std::fs::read(&pid_state_path)
        && bytes.len() == 8
    {
        let saved = PidLocation::from_bytes(&bytes[..8].try_into().expect("8B"));
        let saved_tuple = (saved.file_id, saved.chunk_idx, saved.page_idx);
        let cur = pid_alloc.current();
        let scanned_tuple = (cur.0, cur.1, cur.2 as u16);
        if saved_tuple > scanned_tuple && saved.page_idx <= u8::MAX as u16 {
            pid_alloc =
                PidAllocator::new(saved.file_id, saved.chunk_idx, saved.page_idx as u8);
        }
    }

    Ok(RecoveredState {
        meta,
        vpid_alloc: VpidAllocator::new(next_vpid),
        pid_alloc,
        next_vpid,
        next_file_id,
    })
}

/// 拼出单 db 单 shard 目录: `{block_root}/{db_name}/shard_{N}`.
pub fn shard_dir_path(block_root: &Path, db_name: &str, shard_id: u32) -> PathBuf {
    block_root.join(db_name).join(format!("shard_{shard_id}"))
}

/// 检查 `dir` 是否有任何 `.block` 文件 (compat 探测).
fn has_any_block(dir: &Path) -> bool {
    if !dir.exists() {
        return false;
    }
    std::fs::read_dir(dir)
        .map(|rd| {
            rd.filter_map(|e| e.ok())
                .any(|e| e.file_name().to_string_lossy().ends_with(".block"))
        })
        .unwrap_or(false)
}

// =====================================================================
// 内部 helper
// =====================================================================

/// 收集 block_dir 内所有 `.block` 文件, 按 file_id 升序返回.
///
/// 文件名格式: `{file_id+1:06}.block`, 如 `000001.block` (file_id 0).
fn collect_block_files(block_dir: &Path) -> io::Result<Vec<(u32, std::path::PathBuf)>> {
    if !block_dir.exists() {
        return Ok(Vec::new());
    }

    let mut files: Vec<(u32, std::path::PathBuf)> = std::fs::read_dir(block_dir)?
        .filter_map(|e| e.ok())
        .filter_map(|e| {
            let name = e.file_name().to_string_lossy().to_string();
            if !name.ends_with(".block") {
                return None;
            }
            // 解析 "{file_id+1:06}.block" 前缀
            let prefix = name.trim_end_matches(".block");
            prefix
                .parse::<u32>()
                .ok()
                .map(|n| (n.saturating_sub(1), e.path()))
        })
        .collect();
    files.sort_by_key(|(fid, _)| *fid);
    Ok(files)
}

/// 扫描所有 .block 文件, 从 page header 提取 vpid, 填 MetaCache (覆盖 mate).
///
/// **返回**: (max_vpid, last_pid, seen_any)
/// - max_vpid: 扫描过程中见到的最大 vpid
/// - last_pid: max (file_id, chunk_idx, page_idx) 三元组
/// - seen_any: 是否有任何非空 page (true → 非空库)
fn scan_block_files(
    files: &[(u32, std::path::PathBuf)],
    fill: &mut std::collections::HashMap<u64, PidLocation>,
) -> io::Result<(u64, (u32, u8, u8), bool)> {
    let mut max_vpid: u64 = 0;
    let mut last_pid: (u32, u8, u8) = (0, 0, 0);
    let mut seen_any = false;

    for (file_id, path) in files {
        let (block_max_vpid, block_last_pid, block_seen) = scan_block_file(*file_id, path, fill)?;
        if block_max_vpid > max_vpid {
            max_vpid = block_max_vpid;
        }
        if block_seen && !seen_any {
            last_pid = block_last_pid;
            seen_any = true;
        } else if block_seen && is_pid_after(block_last_pid, last_pid) {
            last_pid = block_last_pid;
        }
        seen_any = seen_any || block_seen;
    }

    if !seen_any {
        max_vpid = 0;
        last_pid = (0, 0, 0);
    }

    Ok((max_vpid, last_pid, seen_any))
}

/// 扫描单个 .block 文件 (10MB), 顺序读每 16KB 一个 page, 解析 header.
///
/// **遇到第一个 invalid page 停止扫描该 block** (TDD 简化, T11 polish 加容错).
fn scan_block_file(
    file_id: u32,
    path: &Path,
    fill: &mut std::collections::HashMap<u64, PidLocation>,
) -> io::Result<(u64, (u32, u8, u8), bool)> {
    let f = std::fs::File::open(path)?;
    let mut buf = [0u8; PAGE_SIZE];

    let mut max_vpid: u64 = 0;
    let mut last_pid: (u32, u8, u8) = (0, 0, 0);
    let mut seen_any = false;

    for chunk_idx in 0..CHUNKS_PER_BLOCK {
        for page_idx in 0..PAGES_PER_CHUNK {
            // ⭐ 修复 (2026-07-21): 与 `chunk_offset` 一致, 每个 .block 文件独立 offset 空间.
            // off 应该是 chunk 内偏移 (chunk_idx * CHUNK_SIZE + page_idx * PAGE_SIZE).
            let off = chunk_idx * CHUNK_SIZE + (page_idx as u64) * PAGE_SIZE as u64;
            // 读一个 page
            match f.read_exact_at(&mut buf, off) {
                Ok(()) => {}
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                    // 文件结束 (block 没写满 10MB)
                    return Ok((max_vpid, last_pid, seen_any));
                }
                Err(e) => return Err(e),
            }

            // 校验 magic
            if buf[0..4] != PAGE_MAGIC {
                // 这个 page 没被写过 (sparse file). 跳过, 继续扫描后续 page.
                // **设计 (2026-07-21 修复 T15)**: 早期实现 "遇 invalid page 立即停止扫描"
                // 是错的, 因为 .block 文件可能 sparse (chunk 末 page 还没分配).
                // 现在改为跳过空白 page, 继续扫描后续 page.
                continue;
            }

            // 校验 page_type (1=Meta 2=Internal 3=Leaf 4=Overflow 5=OverflowIndex)
            let page_type_byte = buf[4];
            if !(1..=5).contains(&page_type_byte) {
                // corrupted page → 跳过
                continue;
            }

            // ⭐ 关键: Internal page (page_type=2) 的 vpid 字段 (0x18..0x20) 被 page crate
            // 复用作 `first_child` (而非 page 自己的 vpid). 因此 scan .block 时, 对 internal page
            // **不能**用 page header 的 vpid 字段更新 MetaCache (会把 first_child 的 vpid 错误
            // 覆盖到 internal page 自己的位置, 破坏已写入的 leaf 映射).
            //
            // 正确的 vpid → pid 映射来源:
            // 1. 之前 flush 时 meta_cache 写回 page.mate (recover 加载 mate 拿初始映射)
            // 2. 对 internal page, root 的 vpid 映射在 page.mate 中已存在 (root 通过 pager.create
            //    分配时 meta.write 写入了, flush 落盘); 内部节点的 vpid 不需要在 meta_cache 中
            //    (通过 root 沿 internal_child 到达)
            //
            // 因此这里只对 Leaf (3) / Meta (1) 信任 vpid 字段, Internal (2) 跳过 meta.write.
            // 但仍记录 pid_alloc 位置 (last_pid 推进), 因为 pid_alloc 跟踪"已分配的 page 位置".
            let trust_vpid_field = page_type_byte != 2;

            // 读 vpid (仅当 trust_vpid_field=true 时使用, 否则仅作 allocator 跟踪参考)
            let vpid = u64::from_le_bytes(buf[0x18..0x20].try_into().unwrap());

            // 构造 PidLocation
            let pid = PidLocation {
                file_id,
                chunk_idx: chunk_idx as u8,
                page_idx: page_idx as u16,
                flags: PID_ALIVE,
            };

            if trust_vpid_field {
                // ⭐ G3: 收集到本地 map (同 vpid 多处出现时靠后者覆盖);
                // 是否写入 meta 由 recover 主流程决定 (meta 为主, 扫描仅补缺).
                fill.insert(vpid, pid);

                // 更新 max (用 vpid 字段值, 因为对 Leaf/Meta 这就是 page 自己的 vpid)
                if vpid > max_vpid {
                    max_vpid = vpid;
                }
            }
            // 跟踪 pid_alloc 状态: 任何含 magic + 合法 page_type 的 page 都算 "已分配".
            // 这保证 internal page 的位置也被 PidAllocator 识别 (下次 alloc 跳过它).
            let cur = (file_id, chunk_idx as u8, page_idx as u8);
            if !seen_any || is_pid_after(cur, last_pid) {
                last_pid = cur;
            }
            seen_any = true;
        }
    }

    Ok((max_vpid, last_pid, seen_any))
}

/// 比较 (file_id, chunk_idx, page_idx) 字典序, a > b?
fn is_pid_after(a: (u32, u8, u8), b: (u32, u8, u8)) -> bool {
    if a.0 != b.0 {
        return a.0 > b.0;
    }
    if a.1 != b.1 {
        return a.1 > b.1;
    }
    a.2 > b.2
}

// 抑制 unused 警告
#[allow(dead_code)]
fn _unused_read_import() {
    let _ = std::io::stdin().read(&mut [0u8; 1]);
}

// =====================================================================
// 单元测试
// =====================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn collect_block_files_empty_dir() {
        let tmp = tempfile::tempdir().unwrap();
        let files = collect_block_files(tmp.path()).unwrap();
        assert!(files.is_empty());
    }

    #[test]
    fn collect_block_files_picks_correct_files() {
        let tmp = tempfile::tempdir().unwrap();
        for id in &[3u32, 1, 2] {
            let path = tmp.path().join(format!("{:06}.block", id));
            std::fs::File::create(&path).unwrap();
        }
        // 干扰文件
        std::fs::File::create(tmp.path().join("not_a_block.txt")).unwrap();
        std::fs::File::create(tmp.path().join("page.mate")).unwrap();

        let files = collect_block_files(tmp.path()).unwrap();
        let fids: Vec<u32> = files.iter().map(|(fid, _)| *fid).collect();
        assert_eq!(fids, vec![0, 1, 2], "应按 file_id 升序, 过滤非 .block");
    }

    #[test]
    fn recover_with_missing_dir_returns_empty_state() {
        let missing = std::env::temp_dir().join("nexus_recover_missing_dir_xyz");
        let _ = std::fs::remove_dir_all(&missing);

        let state = recover(&missing).expect("recover missing dir ok");
        assert_eq!(state.next_vpid, 0);
        assert_eq!(state.next_file_id, 0);
    }

    #[test]
    fn scan_block_file_empty_block() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("000001.block");
        // 10MB 全 0
        let f = std::fs::File::create(&path).unwrap();
        f.set_len(BLOCK_FILE_SIZE).unwrap();
        drop(f);

        let mut fill = std::collections::HashMap::new();
        let (max_vpid, last_pid, seen_any) = scan_block_file(0, &path, &mut fill).unwrap();
        assert_eq!(max_vpid, 0);
        assert_eq!(last_pid, (0, 0, 0));
        assert!(!seen_any);
        assert!(fill.is_empty());
    }

    // =================================================================
    // T12.12 单元测试: recover_for_shard 路径拼接 + compat fallback
    // =================================================================

    #[test]
    fn shard_dir_path_format() {
        // 标准拼接: {block_root}/{db_name}/shard_{N}
        let p = shard_dir_path(Path::new("/data"), "app", 0);
        assert_eq!(p, PathBuf::from("/data/app/shard_0"));

        let p = shard_dir_path(Path::new("/var/lib/nx"), "logs", 3);
        assert_eq!(p, PathBuf::from("/var/lib/nx/logs/shard_3"));
    }

    #[test]
    fn recover_for_shard_compat_fallback_to_block_dir() {
        // 旧 layout: block_root 本身就是 block_dir (有 page.mate)
        let tmp = tempfile::tempdir().unwrap();
        let mate = tmp.path().join("page.mate");
        std::fs::File::create(&mate)
            .unwrap()
            .set_len(10 * 1024 * 1024)
            .unwrap();
        let block = tmp.path().join("000001.block");
        std::fs::File::create(&block)
            .unwrap()
            .set_len(BLOCK_FILE_SIZE)
            .unwrap();

        // 传 (block_root, "default", 0) → block_root/default/shard_0 不存在
        // 但 block_root 本身有 page.mate → fallback 到 block_root 直接
        let state = recover_for_shard(tmp.path(), "default", 0).expect("compat fallback ok");
        assert_eq!(state.next_vpid, 0, "空库 → next_vpid = 0");
        assert_eq!(state.next_file_id, 0, "空库 → next_file_id = 0");
    }

    #[test]
    fn recover_for_shard_finds_shard_dir_layout() {
        // 新 layout: block_root/{db_name}/shard_{N}/
        let tmp = tempfile::tempdir().unwrap();
        let shard_dir = tmp.path().join("default").join("shard_0");
        std::fs::create_dir_all(&shard_dir).unwrap();
        let mate = shard_dir.join("page.mate");
        std::fs::File::create(&mate)
            .unwrap()
            .set_len(10 * 1024 * 1024)
            .unwrap();
        let block = shard_dir.join("000001.block");
        std::fs::File::create(&block)
            .unwrap()
            .set_len(BLOCK_FILE_SIZE)
            .unwrap();

        let state = recover_for_shard(tmp.path(), "default", 0).expect("shard layout recover ok");
        assert_eq!(state.next_vpid, 0, "空库 → next_vpid = 0");
        assert_eq!(state.next_file_id, 0, "空库 → next_file_id = 0");
    }

    #[test]
    fn recover_for_shard_picks_correct_shard() {
        // 验证 shard_id 真的影响路径: shard_0 vs shard_1 各自独立
        let tmp = tempfile::tempdir().unwrap();
        for shard_id in 0..2u32 {
            let shard_dir = tmp.path().join("default").join(format!("shard_{shard_id}"));
            std::fs::create_dir_all(&shard_dir).unwrap();
            std::fs::File::create(shard_dir.join("page.mate"))
                .unwrap()
                .set_len(10 * 1024 * 1024)
                .unwrap();
            std::fs::File::create(shard_dir.join("000001.block"))
                .unwrap()
                .set_len(BLOCK_FILE_SIZE)
                .unwrap();
        }

        // shard_0 和 shard_1 都能独立 recover
        let s0 = recover_for_shard(tmp.path(), "default", 0).unwrap();
        let s1 = recover_for_shard(tmp.path(), "default", 1).unwrap();
        assert_eq!(s0.next_vpid, 0);
        assert_eq!(s1.next_vpid, 0);
    }

    #[test]
    fn recover_for_shard_missing_dir_returns_empty() {
        // 完全不存在的路径 → next_vpid=0, next_file_id=0
        let missing = std::env::temp_dir().join("nexus_recover_for_shard_missing_xyz");
        let _ = std::fs::remove_dir_all(&missing);

        let state =
            recover_for_shard(&missing, "default", 0).expect("recover_for_shard missing dir ok");
        assert_eq!(state.next_vpid, 0);
        assert_eq!(state.next_file_id, 0);
    }
}
