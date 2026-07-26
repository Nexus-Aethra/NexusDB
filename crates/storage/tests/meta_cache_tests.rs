//! MetaCache v3 测试: 全量平坦数组 + dirty window + flush.
//!
//! 设计 (2026-07-27):
//! - open 整读 page.mate 进平坦数组 (全量缓存, 无 miss 路径)
//! - write 懒扩容 + 按 1MB window (128K slot) 粒度标脏
//! - flush_dirty 整 window write_at + 单次 sync_all (同步全量路径)
//! - take/complete 快照状态机由 meta_cache.rs 单测覆盖, 这里覆盖持久化行为

use std::io::{Read, Seek, Write};
use std::os::unix::fs::FileExt;

use storage::{PID_ALIVE, SLOTS_PER_WINDOW};
use storage::test_support::{MetaCache, PidLocation};

mod common;

use common::run_async;

/// Make a page.mate file with valid PidLocation entries (flags=PID_ALIVE).
fn make_mate_file(tmp: &tempfile::TempDir, total_vpids: u64) -> std::path::PathBuf {
    let path = tmp.path().join("page.mate");
    let mut f = std::fs::File::create(&path).unwrap();
    for i in 0..total_vpids {
        let pid_bytes: [u8; 8] = [
            (i & 0xFF) as u8,
            ((i >> 8) & 0xFF) as u8,
            0, 0,
            0,
            (i & 0xFF) as u8,
            0,
            PID_ALIVE,
        ];
        f.write_all(&pid_bytes).unwrap();
    }
    f.sync_all().unwrap();
    path
}

/// Make a zero-filled page.mate file. Used by tests that verify unallocated slot behavior.
fn make_mate_file_zeros(tmp: &tempfile::TempDir, total_vpids: u64) -> std::path::PathBuf {
    let path = tmp.path().join("page.mate");
    let mut f = std::fs::File::create(&path).unwrap();
    let total_bytes = (total_vpids as usize) * 8;
    f.write_all(&vec![0u8; total_bytes]).unwrap();
    f.sync_all().unwrap();
    path
}

#[test]
fn open_loads_full_mate_into_flat_array() {
    // v3: open 整读 page.mate, len = 文件 slot 数 (全量缓存).
    let tmp = tempfile::tempdir().unwrap();
    let path = make_mate_file(&tmp, 1024);
    let cache = MetaCache::open(&path).unwrap();

    assert_eq!(cache.len(), 1024, "v3 open 整读全量 slot");
    assert_eq!(cache.dirty_count(), 0, "open 后无 dirty window");
}

#[test]
fn read_unallocated_returns_none() {
    run_async(async move {
        let tmp = tempfile::tempdir().unwrap();
        let _path = make_mate_file_zeros(&tmp, 1024);
        let mut cache = MetaCache::open(&_path).unwrap();
        assert!(cache.read(0).is_none());
        assert!(cache.read(1000).is_none(), "越界 read → None");
    });
}

#[test]
fn write_then_read_roundtrip_marks_dirty() {
    run_async(async move {
        let tmp = tempfile::tempdir().unwrap();
        let path = make_mate_file(&tmp, 1024);
        let mut cache = MetaCache::open(&path).unwrap();

        let pid = PidLocation::from_bytes(&[0u8, 0, 0, 0, 0, 0, 7, PID_ALIVE]);
        cache.write(0, pid);
        assert_eq!(cache.read(0).expect("written vpid should be visible"), pid);

        // v3: dirty_count = dirty window 数
        assert_eq!(cache.dirty_count(), 1, "write vpid 0 marks window 0 dirty");
        assert!(cache.contains(0));
    });
}

#[test]
fn read_far_vpid_from_mate_no_miss_path() {
    run_async(async move {
        // v3: 全量缓存, 远端 vpid 直接内存索引 (无 pread miss 路径).
        let tmp = tempfile::tempdir().unwrap();
        let path = make_mate_file(&tmp, 1024 * 1024);

        {
            let f = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
            let pid_bytes = PidLocation::from_bytes(&[1, 0, 0, 0, 2, 3, 0, PID_ALIVE]).to_bytes();
            f.write_at(&pid_bytes, 200_000 * 8).unwrap();
            f.sync_all().unwrap();
        }

        let mut cache = MetaCache::open(&path).unwrap();
        let pid = cache.read(200_000).expect("vpid 200_000 loaded at open");
        assert_eq!(pid.file_id(), 1);
        assert_eq!(pid.chunk_idx(), 2);
        assert_eq!(pid.page_idx(), 3);
        assert_eq!(pid.flags(), PID_ALIVE);
        assert!(cache.contains(200_000));
        assert_eq!(cache.len(), 1024 * 1024, "全量载入");
    });
}

#[test]
fn flush_dirty_pwrites_to_mate_and_clears_dirty() {
    run_async(async move {
        let tmp = tempfile::tempdir().unwrap();
        let path = make_mate_file(&tmp, 1024 * 1024);
        let mut cache = MetaCache::open(&path).unwrap();

        cache.write(
            0,
            PidLocation::from_bytes(&[0, 0, 0, 0, 0, 0, 0, PID_ALIVE]),
        );
        assert_eq!(cache.dirty_count(), 1, "write 应标 dirty window");

        cache.flush_dirty().expect("flush_dirty ok");
        assert_eq!(cache.dirty_count(), 0, "flush 必须清 dirty");

        let mut f = std::fs::File::open(&path).unwrap();
        let mut buf = [0u8; 8];
        f.read_exact(&mut buf).unwrap();
        assert_eq!(buf, [0, 0, 0, 0, 0, 0, 0, PID_ALIVE], "flush 必须写回 mate");
    });
}

#[test]
fn write_to_far_vpid_lazy_grows() {
    run_async(async move {
        // v3: write 越界懒扩容, 中间空洞为全零 (read None).
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("page.mate");
        std::fs::File::create(&path).unwrap();
        let mut cache = MetaCache::open(&path).unwrap();

        let pid = PidLocation::from_bytes(&[0, 0, 0, 0, 0, 5, 0, PID_ALIVE]);
        cache.write(640_000, pid);
        assert_eq!(cache.read(640_000).unwrap(), pid);
        assert_eq!(cache.len(), 640_001, "懒扩容到 vpid+1");
        assert!(cache.read(300_000).is_none(), "空洞 slot → None");
        // 640_000 / 131072 = window 4; 只有该 window dirty
        assert_eq!(cache.dirty_count(), 1, "只标写到的 window");
    });
}

#[test]
fn flush_writes_all_dirty_windows() {
    run_async(async move {
        // 跨 window 的两个 dirty slot, flush 都写回.
        let tmp = tempfile::tempdir().unwrap();
        let path = make_mate_file(&tmp, 1024 * 1024);
        let mut cache = MetaCache::open(&path).unwrap();

        cache.write(
            0,
            PidLocation::from_bytes(&[0, 0, 0, 0, 0, 0, 5, PID_ALIVE]),
        );
        cache.write(
            640_000,
            PidLocation::from_bytes(&[0, 0, 0, 0, 0, 5, 7, PID_ALIVE]),
        );
        assert_eq!(cache.dirty_count(), 2, "两个不同 window dirty");

        cache.flush_dirty().unwrap();

        let mut f = std::fs::File::open(&path).unwrap();
        let mut buf = [0u8; 8];

        f.read_exact(&mut buf).unwrap();
        assert_eq!(buf[6], 5);

        f.seek(std::io::SeekFrom::Start(640_000 * 8)).unwrap();
        let mut buf2 = [0u8; 8];
        f.read_exact(&mut buf2).unwrap();
        assert_eq!(buf2[6], 7);
    });
}

#[test]
fn pids_in_mate_roundtrip_after_open() {
    run_async(async move {
        let tmp = tempfile::tempdir().unwrap();
        let path = make_mate_file(&tmp, 1024);

        {
            let f = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
            for vpid in 0u64..4 {
                let pid = PidLocation::from_bytes(&[
                    vpid as u8,
                    0,
                    0,
                    0,
                    vpid as u8,
                    (vpid * 2) as u8,
                    (vpid * 3) as u8,
                    PID_ALIVE,
                ]);
                let bytes = pid.to_bytes();
                f.write_at(&bytes, vpid * 8).unwrap();
            }
            f.sync_all().unwrap();
        }

        let mut cache = MetaCache::open(&path).unwrap();
        for vpid in 0u64..4 {
            let pid = cache.read(vpid).expect("vpid from mate should load");
            let expected = PidLocation::from_bytes(&[
                vpid as u8,
                0,
                0,
                0,
                vpid as u8,
                (vpid * 2) as u8,
                (vpid * 3) as u8,
                PID_ALIVE,
            ]);
            assert_eq!(pid, expected);
        }
    });
}

#[test]
fn write_same_vpid_overwrites() {
    run_async(async move {
        let tmp = tempfile::tempdir().unwrap();
        let path = make_mate_file(&tmp, 1024);
        let mut cache = MetaCache::open(&path).unwrap();

        let pid1 = PidLocation::from_bytes(&[1, 0, 0, 0, 1, 0, 0, PID_ALIVE]);
        let pid2 = PidLocation::from_bytes(&[2, 0, 0, 0, 2, 0, 0, PID_ALIVE]);

        cache.write(5, pid1);
        assert_eq!(cache.read(5).unwrap(), pid1);

        cache.write(5, pid2);
        assert_eq!(
            cache.read(5).unwrap(),
            pid2,
            "second write should overwrite"
        );
        assert_eq!(cache.dirty_count(), 1, "同 window 多次 write 只算 1 dirty");
    });
}

#[test]
fn read_after_flush_returns_same_data() {
    run_async(async move {
        let tmp = tempfile::tempdir().unwrap();
        let path = make_mate_file(&tmp, 1024);
        let mut cache = MetaCache::open(&path).unwrap();

        let pid = PidLocation::from_bytes(&[9, 9, 9, 9, 9, 9, 9, PID_ALIVE]);
        cache.write(42, pid);
        cache.flush_dirty().unwrap();

        drop(cache);
        let mut cache2 = MetaCache::open(&path).unwrap();
        assert_eq!(
            cache2.read(42).unwrap(),
            pid,
            "data must survive flush + reopen"
        );
    });
}

#[test]
fn flush_with_no_dirty_windows_is_noop() {
    let tmp = tempfile::tempdir().unwrap();
    let path = make_mate_file_zeros(&tmp, 1024);
    let mut cache = MetaCache::open(&path).unwrap();

    cache.flush_dirty().unwrap();

    let size = std::fs::metadata(&path).unwrap().len();
    assert_eq!(size, 1024 * 8);

    let mut f = std::fs::File::open(&path).unwrap();
    let mut buf = [0u8; 8];
    f.read_exact(&mut buf).unwrap();
    assert_eq!(
        buf, [0u8; 8],
        "mate should remain all zeros after empty flush"
    );
}

#[test]
fn read_unallocated_vpid_returns_none() {
    run_async(async move {
        let tmp = tempfile::tempdir().unwrap();
        let path = make_mate_file_zeros(&tmp, 1024);
        let mut cache = MetaCache::open(&path).unwrap();

        assert!(cache.read(0).is_none());
        assert!(cache.read(100).is_none());
        assert!(cache.read(999_999).is_none());

        let pid = PidLocation::from_bytes(&[1, 0, 0, 0, 0, 0, 0, PID_ALIVE]);
        cache.write(100, pid);
        assert_eq!(cache.read(100).unwrap(), pid);
    });
}

#[test]
fn meta_cache_open_empty_mate_file_does_not_panic() {
    run_async(async move {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("empty.mate");
        std::fs::File::create(&path).unwrap();

        let mut cache = MetaCache::open(&path).unwrap();
        assert!(cache.read(0).is_none());
        cache.write(
            0,
            PidLocation::from_bytes(&[0, 0, 0, 0, 0, 0, 0, PID_ALIVE]),
        );
        assert!(cache.read(0).is_some());
    });
}

#[test]
fn many_overwrites_keeps_dirty_flag() {
    run_async(async move {
        // 连续写同一 vpid 100 次, dirty_count 仍 = 1 (window 粒度不重复计).
        let tmp = tempfile::tempdir().unwrap();
        let path = make_mate_file(&tmp, 1024);
        let mut cache = MetaCache::open(&path).unwrap();
        for i in 0..100 {
            let pid = PidLocation::from_bytes(&[i, 0, 0, 0, 0, 0, 0, PID_ALIVE]);
            cache.write(0, pid);
        }
        assert_eq!(cache.dirty_count(), 1, "同 window 多次 write 只算 1 dirty");
        let pid = PidLocation::from_bytes(&[99, 0, 0, 0, 0, 0, 0, PID_ALIVE]);
        assert_eq!(cache.read(0).unwrap(), pid, "最终值应是第 99 次 write");
    });
}

#[test]
fn dirty_count_decreases_after_flush_dirty() {
    run_async(async move {
        // 3 个写落在 3 个不同 window → dirty_count = 3, flush 清零.
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("page.mate");
        std::fs::File::create(&path).unwrap();
        let mut cache = MetaCache::open(&path).unwrap();

        let spw = SLOTS_PER_WINDOW as u64;
        cache.write(
            0,
            PidLocation::from_bytes(&[1, 0, 0, 0, 0, 0, 0, PID_ALIVE]),
        );
        cache.write(
            spw,
            PidLocation::from_bytes(&[2, 0, 0, 0, 0, 0, 0, PID_ALIVE]),
        );
        cache.write(
            spw * 2,
            PidLocation::from_bytes(&[3, 0, 0, 0, 0, 0, 0, PID_ALIVE]),
        );
        assert_eq!(cache.dirty_count(), 3);

        cache.flush_dirty().unwrap();
        assert_eq!(cache.dirty_count(), 0, "flush 后 dirty_count 清零");

        // 三个 window 的数据都持久化了
        let f = std::fs::File::open(&path).unwrap();
        for (i, vpid) in [0u64, spw, spw * 2].iter().enumerate() {
            let mut buf = [0u8; 8];
            f.read_exact_at(&mut buf, vpid * 8).unwrap();
            assert_eq!(buf[0], (i + 1) as u8, "window {i} 数据写回");
        }
    });
}

#[test]
fn flush_then_reopen_survives() {
    run_async(async move {
        let tmp = tempfile::tempdir().unwrap();
        let path = make_mate_file(&tmp, 1024);

        let mut cache = MetaCache::open(&path).unwrap();
        let pid = PidLocation::from_bytes(&[7, 7, 7, 7, 7, 7, 7, PID_ALIVE]);
        cache.write(42, pid);
        cache.flush_dirty().unwrap();
        drop(cache);

        let mut cache2 = MetaCache::open(&path).unwrap();
        let got = cache2.read(42).expect("mate 应有 vpid 42 数据");
        assert_eq!(got, pid, "reopen 应读到之前写入的 pid");
    });
}

// =====================================================================
// db-aware 签名兼容: v3 db 参数忽略 (vpid 单空间, 与持久化 off=vpid×8 一致)
// =====================================================================

#[test]
fn db_aware_api_ignores_db_single_vpid_space() {
    // v3 语义: db-aware API 与 vpid-only API 完全等价 (Pager 已按 db 目录隔离).
    let tmp = tempfile::tempdir().unwrap();
    let path = make_mate_file(&tmp, 1024);
    let mut cache = MetaCache::open(&path).unwrap();

    let pid_a = PidLocation::from_bytes(&[1, 0, 0, 0, 0, 0, 0, PID_ALIVE]);
    let pid_b = PidLocation::from_bytes(&[2, 0, 0, 0, 0, 0, 0, PID_ALIVE]);
    cache.write_db(0, 100, pid_a);
    cache.write_db(1, 100, pid_b);

    // 同一 vpid 空间: 后写覆盖 (与持久化语义一致, v2 的内存独立是与磁盘不一致的假象)
    assert_eq!(cache.read_db(0, 100).unwrap(), pid_b);
    assert_eq!(cache.read_db(1, 100).unwrap(), pid_b);
    assert_eq!(cache.read(100).unwrap(), pid_b);
    assert!(cache.contains_db(7, 100), "db 参数忽略");
}

#[test]
fn db_aware_compat_api_default_is_db_zero() {
    run_async(async move {
        let tmp = tempfile::tempdir().unwrap();
        let path = make_mate_file(&tmp, 1024);
        let mut cache = MetaCache::open(&path).unwrap();

        let pid = PidLocation::from_bytes(&[1, 0, 0, 0, 0, 0, 0, PID_ALIVE]);
        cache.write(7, pid);

        let via_compat = cache.read(7);
        let via_db_aware = cache.read_db(0, 7);
        assert_eq!(via_compat, via_db_aware);
        assert_eq!(via_compat, Some(pid));
    });
}
