//! ⭐ meta log (实验) 存储层测试: 缓冲去重 / 追加回滚 / 合并 / 崩溃重放 / 迁移.

use std::path::{Path, PathBuf};

use storage::PidLocation;
use storage::meta_cache::{META_LOG_BATCH_ENTRIES, MetaCache, MetaLogBatch, MetaMergeBatch};

fn pid(n: u64) -> PidLocation {
    PidLocation {
        file_id: (n & 0xFFFF) as u32,
        chunk_idx: ((n >> 16) & 0xFF) as u8,
        page_idx: ((n >> 24) & 0x3F) as u16,
        flags: storage::PID_ALIVE,
    }
}

fn mate_path(dir: &Path) -> PathBuf {
    dir.join("page.mate")
}

fn segments(dir: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    let Ok(rd) = std::fs::read_dir(dir) else {
        return out;
    };
    for ent in rd.flatten() {
        let name = ent.file_name();
        if name.to_str().is_some_and(|s| s.starts_with("page.metlog.")) {
            out.push(ent.path());
        }
    }
    out.sort();
    out
}

/// 模拟写盘协程: 追加日志批 + fsync, 再收割成功.
fn commit_log(meta: &mut MetaCache, b: MetaLogBatch) {
    storage::meta_log::append_sync(&b.path, b.offset, &b.bytes).unwrap();
    let done = b.done_info();
    meta.complete_log_append(done, true);
}

/// 模拟写盘协程: mate 子区写 + fsync.
fn apply_mate(mb: &MetaMergeBatch) {
    use storage::file_at::FileAt;
    let f = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(&mb.mate_path)
        .unwrap();
    for it in &mb.items {
        f.write_all_at(&it.bytes, it.offset()).unwrap();
    }
    f.sync_all().unwrap();
}

/// 模拟合并协程: (可选) 日志追加 → mate 子区写 → 收割成功.
fn commit_merge(meta: &mut MetaCache, mb: MetaMergeBatch) {
    if let Some(b) = &mb.log {
        storage::meta_log::append_sync(&b.path, b.offset, &b.bytes).unwrap();
    }
    apply_mate(&mb);
    let done = mb.done_info();
    meta.complete_merge(done, true, true);
}

/// 缓冲内按 vpid 去重: 同一 vpid 多轮改写 → 只出一条记录 (取最新值).
#[test]
fn buffer_dedups_same_vpid_across_rounds() {
    let tmp = tempfile::tempdir().unwrap();
    let mut meta = MetaCache::open_with_log(&mate_path(tmp.path()), true).unwrap();
    assert!(meta.log_enabled());

    for v in 1..=5u64 {
        meta.write(7, pid(v));
    }
    meta.write(8, pid(99));
    assert_eq!(meta.pending_len(), 2, "同 vpid 多轮写只占一个缓冲位");

    let b = meta.take_log_batch(0).expect("batch");
    assert_eq!(b.offset, 0);
    assert!(b.new_segment);
    assert_eq!(b.entries, vec![7, 8]);
    // 新段 = 16B header + 2 × 20B 记录
    assert_eq!(b.bytes.len(), 16 + 2 * 20);
    assert_eq!(meta.pending_len(), 0);
    assert!(meta.log_busy());

    commit_log(&mut meta, b);
    assert!(!meta.log_busy());
    assert!(!meta.has_unflushed(), "已 append 进日志 = 持久");
}

/// append 失败: 回滚段位置 + pending 重排队, 重试数据不丢.
#[test]
fn append_failure_requeues_and_rolls_back() {
    let tmp = tempfile::tempdir().unwrap();
    let mut meta = MetaCache::open_with_log(&mate_path(tmp.path()), true).unwrap();

    meta.write(3, pid(1));
    meta.write(4, pid(2));
    let b = meta.take_log_batch(0).unwrap();
    assert!(b.new_segment);
    let done = b.done_info();
    meta.complete_log_append(done, false);
    assert_eq!(meta.pending_len(), 2, "失败条目重排队");
    assert!(segments(tmp.path()).is_empty(), "半写新段已删除");
    assert!(!meta.log_busy());

    // 重试: 仍从 offset 0 开新段, 字节一致.
    let b2 = meta.take_log_batch(0).unwrap();
    assert_eq!(b2.offset, 0);
    assert!(b2.new_segment);
    commit_log(&mut meta, b2);
    assert_eq!(segments(tmp.path()).len(), 1);
}

/// 已有段追加失败: 截断半写尾部, 重试写在同一 offset.
#[test]
fn append_failure_truncates_existing_segment() {
    let tmp = tempfile::tempdir().unwrap();
    let mut meta = MetaCache::open_with_log(&mate_path(tmp.path()), true).unwrap();

    meta.write(1, pid(1));
    let b = meta.take_log_batch(0).unwrap();
    let first_len = b.bytes.len() as u64;
    commit_log(&mut meta, b);

    meta.write(2, pid(2));
    let b2 = meta.take_log_batch(0).unwrap();
    assert!(!b2.new_segment, "追加到同段");
    assert_eq!(b2.offset, first_len);
    meta.complete_log_append(b2.done_info(), false);

    let seg = segments(tmp.path())[0].clone();
    assert_eq!(
        std::fs::metadata(&seg).unwrap().len(),
        first_len,
        "失败尾部已截断"
    );
    assert_eq!(meta.pending_len(), 1);

    let b3 = meta.take_log_batch(0).unwrap();
    assert_eq!(b3.offset, first_len, "重试偏移不变");
    commit_log(&mut meta, b3);
    assert_eq!(std::fs::metadata(&seg).unwrap().len(), first_len + 20);
}

/// 崩溃重放: 只 append 未合并 → 重新 open 能看到日志尾巴 (log 开/关都可见).
#[test]
fn reopen_replays_unmerged_log() {
    let tmp = tempfile::tempdir().unwrap();
    {
        let mut meta = MetaCache::open_with_log(&mate_path(tmp.path()), true).unwrap();
        meta.write(10, pid(100));
        meta.write(11, pid(101));
        let b = meta.take_log_batch(0).unwrap();
        commit_log(&mut meta, b);
        assert_eq!(segments(tmp.path()).len(), 1);
        // 模拟崩溃: 不合并、不 flush, 直接 drop
    }

    // 功能关闭路径: 重放后立即合并进 mate 并删段
    {
        let mut meta = MetaCache::open(&mate_path(tmp.path())).unwrap();
        assert!(!meta.log_enabled(), "未启用时回到原地写模式");
        assert_eq!(meta.read(10), Some(pid(100)));
        assert_eq!(meta.read(11), Some(pid(101)));
    }
    assert!(segments(tmp.path()).is_empty(), "迁移后日志段已删除");

    // mate 已成为权威: 再开一次仍可见
    let mut meta = MetaCache::open(&mate_path(tmp.path())).unwrap();
    assert_eq!(meta.read(10), Some(pid(100)));
}

/// 合并: 脏子区写进 mate + 删段; 合并后重开可见.
#[test]
fn merge_makes_mate_authoritative_and_deletes_segments() {
    let tmp = tempfile::tempdir().unwrap();
    let mut meta = MetaCache::open_with_log(&mate_path(tmp.path()), true).unwrap();
    for v in 0..3u64 {
        meta.write(v, pid(v + 10));
    }
    let b = meta.take_log_batch(0).unwrap();
    commit_log(&mut meta, b);
    assert_eq!(meta.dirty_count(), 1, "append 标记了子区脏");

    let mb = meta.take_merge_batch(mate_path(tmp.path()), 0).unwrap();
    assert!(mb.log.is_none(), "pending 已空, 纯 mate 合并");
    assert_eq!(mb.items.len(), 1);
    assert_eq!(mb.segments.len(), 1);
    commit_merge(&mut meta, mb);

    assert!(segments(tmp.path()).is_empty(), "合并成功删段");
    assert_eq!(meta.dirty_count(), 0);
    assert!(!meta.log_busy());

    // 重开 (legacy) 看到 mate 中的值
    let mut reopened = MetaCache::open(&mate_path(tmp.path())).unwrap();
    for v in 0..3u64 {
        assert_eq!(reopened.read(v), Some(pid(v + 10)));
    }
}

/// 合并先把 pending 追加进日志再写 mate: 快照包含最新值.
#[test]
fn merge_drains_pending_before_snapshot() {
    let tmp = tempfile::tempdir().unwrap();
    let mut meta = MetaCache::open_with_log(&mate_path(tmp.path()), true).unwrap();

    meta.write(5, pid(1));
    let b = meta.take_log_batch(0).unwrap();
    commit_log(&mut meta, b);
    // 同一 vpid 又写一次 (pending, 尚未 append)
    meta.write(5, pid(2));
    meta.write(6, pid(3));

    let mb = meta.take_merge_batch(mate_path(tmp.path()), 0).unwrap();
    assert!(mb.log.is_some(), "合并必须带上 pending 追加");
    commit_merge(&mut meta, mb);

    let mut reopened = MetaCache::open(&mate_path(tmp.path())).unwrap();
    assert_eq!(reopened.read(5), Some(pid(2)), "mate 是最新值");
    assert_eq!(reopened.read(6), Some(pid(3)));
    assert!(segments(tmp.path()).is_empty());
}

/// 合并失败 (mate 写失败): 段保留, 脏位重标, 下轮可重试.
#[test]
fn merge_failure_keeps_segments_and_retries() {
    let tmp = tempfile::tempdir().unwrap();
    let mut meta = MetaCache::open_with_log(&mate_path(tmp.path()), true).unwrap();
    meta.write(1, pid(7));
    let b = meta.take_log_batch(0).unwrap();
    commit_log(&mut meta, b);

    let mb = meta.take_merge_batch(mate_path(tmp.path()), 0).unwrap();
    let done = mb.done_info();
    meta.complete_merge(done, true, false); // mate 写失败 (不写 mate)
    assert_eq!(segments(tmp.path()).len(), 1, "失败保留段");
    assert_eq!(meta.dirty_count(), 1, "脏位重标");
    assert!(!meta.log_busy());

    let mb2 = meta.take_merge_batch(mate_path(tmp.path()), 0).unwrap();
    commit_merge(&mut meta, mb2);
    assert!(segments(tmp.path()).is_empty());
    let mut reopened = MetaCache::open(&mate_path(tmp.path())).unwrap();
    assert_eq!(reopened.read(1), Some(pid(7)));
}

/// 同步 flush (close 契约): pending + 全部脏区落 mate, 段清空, 可重开.
#[test]
fn flush_dirty_is_authoritative_in_log_mode() {
    let tmp = tempfile::tempdir().unwrap();
    let mut meta = MetaCache::open_with_log(&mate_path(tmp.path()), true).unwrap();
    meta.write(2, pid(20));
    meta.write(3, pid(21));
    meta.flush_dirty().unwrap();

    assert!(segments(tmp.path()).is_empty(), "flush 后无残留段");
    assert!(!meta.has_unflushed());
    let mut reopened = MetaCache::open(&mate_path(tmp.path())).unwrap();
    assert_eq!(reopened.read(2), Some(pid(20)));
    assert_eq!(reopened.read(3), Some(pid(21)));
}

/// flush 前置条件: 有在飞 append 时拒绝 (防同段并发写).
#[test]
fn flush_dirty_refuses_with_inflight_append() {
    let tmp = tempfile::tempdir().unwrap();
    let mut meta = MetaCache::open_with_log(&mate_path(tmp.path()), true).unwrap();
    meta.write(1, pid(1));
    let _b = meta.take_log_batch(0).unwrap();
    let err = meta.flush_dirty().unwrap_err();
    assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
}

/// 延迟释放前缀: 队列全取走才可 promote; 有截断/剩余时保守返回 0.
#[test]
fn promote_prefix_requires_full_queue_drain() {
    let tmp = tempfile::tempdir().unwrap();
    let mut meta = MetaCache::open_with_log(&mate_path(tmp.path()), true).unwrap();
    meta.write(1, pid(1));
    meta.write(2, pid(2));

    let b = meta.take_log_batch(7).unwrap();
    assert_eq!(b.promote_prefix, 7, "队列全取走 → 本批覆盖前缀");
    commit_log(&mut meta, b);

    // 超过单批上限: pending 剩余 → 前缀保守为 0
    for v in 0..(META_LOG_BATCH_ENTRIES as u64 + 100) {
        meta.write(v, pid(v + 1));
    }
    let b2 = meta.take_log_batch(9).unwrap();
    assert_eq!(b2.promote_prefix, 0, "队列未取完 → 不 promote");
    commit_log(&mut meta, b2);
}
