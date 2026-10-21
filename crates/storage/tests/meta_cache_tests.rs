//! MetaCache v3 测试: 全量平坦数组 + dirty window + flush.
//!
//! 设计 (2026-07-27):
//! - open 整读 page.mate 进平坦数组 (全量缓存, 无 miss 路径)
//! - write 懒扩容 + 按 1MB window (128K slot) 粒度标脏
//! - flush_dirty 整 window write_at + 单次 sync_all (同步全量路径)
//! - take/complete 快照状态机由 meta_cache.rs 单测覆盖, 这里覆盖持久化行为

use std::io::{Read, Seek, Write};
use std::os::unix::fs::FileExt;

use storage::test_support::{MetaCache, PidLocation};
use storage::{PID_ALIVE, SLOTS_PER_WINDOW};

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
            0,
            0,
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

/// ⭐ 审计 P2 (2026-10-17): `flush_dirty` 的"无 in-flight 快照"前置条件从
/// debug_assert 升级为运行时硬错误 — release 下也不再静默乱序覆盖.
#[test]
fn flush_dirty_rejects_inflight_snapshots() {
    let path =
        std::env::temp_dir().join(format!("nexus_meta_inflight_{}.mate", std::process::id()));
    let _ = std::fs::remove_file(&path);
    let mut cache = MetaCache::open(&path).unwrap();
    cache.write(
        1,
        storage::PidLocation {
            file_id: 0,
            chunk_idx: 0,
            page_idx: 1,
            flags: storage::PID_ALIVE,
        },
    );
    // 取快照 → window 0 进入 in-flight (模拟异步刷盘协程还握着旧快照)
    let snaps = cache.take_dirty_subrange_snapshots();
    assert!(!snaps.is_empty(), "应有 in-flight 快照");

    let r = cache.flush_dirty();
    assert!(
        r.is_err(),
        "有 in-flight 快照时 flush_dirty 必须报错 (旧实现 release 下静默覆盖)"
    );
    let _ = std::fs::remove_file(&path);
}

fn varied_pid(i: u64) -> storage::PidLocation {
    storage::PidLocation {
        file_id: (i % 7) as u32,
        chunk_idx: (i % 251) as u8,
        page_idx: (i % 65536) as u16,
        // 混入 flags=0 未分配槽, 覆盖零值布局
        flags: if i.is_multiple_of(13) {
            0
        } else {
            storage::PID_ALIVE
        },
    }
}

/// O1 必败回归: window 快照序列化应走 memcpy（LE 目标）, 且字节与逐槽
/// to_bytes 标量参考逐字节相同 (布局假设若被破坏则红).
///
/// 速度断言取**自校准相对值**（memcpy ≥ 3× 标量）而非绝对毫秒, 排除机器噪声.
#[test]
fn window_snapshot_is_memcpy_fast_and_byte_exact() {
    let path = std::env::temp_dir().join(format!("nexus_meta_memcpy_{}.mate", std::process::id()));
    let _ = std::fs::remove_file(&path);
    let mut cache = MetaCache::open(&path).unwrap();

    const WINDOWS: u64 = 4;
    const SPW: u64 = storage::meta_cache::SLOTS_PER_WINDOW as u64;
    for i in 0..WINDOWS * SPW {
        cache.write(i, varied_pid(i));
    }

    let t0 = std::time::Instant::now();
    let snaps = cache.take_dirty_subrange_snapshots();
    let prod = t0.elapsed();
    assert_eq!(
        snaps.len() as u64,
        WINDOWS,
        "应有 {WINDOWS} 个子区批 (满窗=1 合并批)"
    );

    // 标量参考 (旧实现语义 = 磁盘格式): 逐槽 to_bytes 拼接
    let slots: Vec<storage::PidLocation> = (0..WINDOWS * SPW).map(varied_pid).collect();
    let t1 = std::time::Instant::now();
    let mut ref_bytes: Vec<Vec<u8>> = Vec::with_capacity(WINDOWS as usize);
    for w in 0..WINDOWS {
        let s = (w * SPW) as usize;
        let e = ((w + 1) * SPW) as usize;
        let mut v = Vec::with_capacity((e - s) * 8);
        for slot in &slots[s..e] {
            v.extend_from_slice(&slot.to_bytes());
        }
        ref_bytes.push(v);
    }
    let scalar = t1.elapsed();

    for it in &snaps {
        assert_eq!(
            &it.bytes, &ref_bytes[it.window as usize],
            "window {} 快照字节必须与标量参考逐字节相同",
            it.window
        );
    }
    for it in &snaps {
        cache.complete_subrange_flush(it.window, it.sub_start, it.sub_len, true);
    }

    let ratio = scalar.as_secs_f64() / prod.as_secs_f64().max(1e-9);
    println!("[window_snapshot] memcpy {prod:?} vs scalar {scalar:?} — {ratio:.1}x (要求 ≥3x)");
    assert!(
        prod <= scalar / 3,
        "memcpy 路径应至少比逐槽标量快 3x: memcpy {prod:?} vs scalar {scalar:?}"
    );
    let _ = std::fs::remove_file(&path);
}

// ===== O2 子窗脏粒度 (8KB) — 行为级必败回归 =====

use storage::meta_cache::MetaWindowItem;
use storage::meta_cache::SUB_SLOTS;

fn fill_window(cache: &mut MetaCache, w: u64) {
    let spw = storage::meta_cache::SLOTS_PER_WINDOW as u64;
    for i in (w * spw)..((w + 1) * spw) {
        cache.write(i, varied_pid(i));
    }
}

/// 标量参考: window 内 [sub_start, sub_start+sub_len) 子区的槽字节.
/// 注意: 必须复刻测试里对 3 个 hot 槽的覆写 (sub 5/6/9 各覆写第 0/1/2 槽).
fn scalar_subrange_bytes(w: u64, sub_start: usize, sub_len: usize) -> Vec<u8> {
    let spw = storage::meta_cache::SLOTS_PER_WINDOW;
    let s = w as usize * spw + sub_start * SUB_SLOTS;
    let e = s + sub_len * SUB_SLOTS;
    let slot_at = |vpid: usize| -> storage::PidLocation {
        if vpid == 5 * SUB_SLOTS {
            varied_pid(77_000)
        } else if vpid == 6 * SUB_SLOTS + 1 {
            varied_pid(77_001)
        } else if vpid == 9 * SUB_SLOTS + 2 {
            varied_pid(77_002)
        } else {
            varied_pid(vpid as u64)
        }
    };
    (s..e).flat_map(|vpid| slot_at(vpid).to_bytes()).collect()
}

/// O2 必败: 满窗刷完后单槽写 → 快照应只含 1 个子区 (≤8KB).
/// 旧全窗实现给出整窗 1MB — 用 (window, sub_start, sub_len) 结构断言.
#[test]
fn subrange_snapshot_isolates_hot_slot() {
    let path = std::env::temp_dir().join(format!("nexus_meta_sub_{}.mate", std::process::id()));
    let _ = std::fs::remove_file(&path);
    let mut cache = MetaCache::open(&path).unwrap();

    fill_window(&mut cache, 0);
    cache.flush_dirty().unwrap(); // 满窗先整体刷完, 清干净

    let sub = 7usize;
    let vpid = (sub * SUB_SLOTS) as u64;
    cache.write(vpid, varied_pid(999_999));
    let items: Vec<MetaWindowItem> = cache.take_dirty_subrange_snapshots();
    assert_eq!(items.len(), 1, "单槽写只应产生 1 个子区快照");
    assert_eq!(items[0].window, 0);
    assert_eq!(items[0].sub_start as usize, sub);
    assert_eq!(items[0].sub_len, 1, "只覆盖 1 个子区");
    assert_eq!(
        items[0].bytes.len(),
        SUB_SLOTS * 8,
        "快照应为 8KB (1 子区), 而非整窗 1MB"
    );
    for it in &items {
        cache.complete_subrange_flush(it.window, it.sub_start, it.sub_len, true);
    }
    let _ = std::fs::remove_file(&path);
}

/// O2: 多子区散写 → 快照按连续脏区合并; 字节与标量参考逐字节一致.
#[test]
fn subrange_snapshot_coalesces_and_byte_exact() {
    let path = std::env::temp_dir().join(format!("nexus_meta_sub2_{}.mate", std::process::id()));
    let _ = std::fs::remove_file(&path);
    let mut cache = MetaCache::open(&path).unwrap();

    fill_window(&mut cache, 0);
    cache.flush_dirty().unwrap();
    for (i, &sub) in [5usize, 6, 9].iter().enumerate() {
        cache.write((sub * SUB_SLOTS + i) as u64, varied_pid(77_000 + i as u64));
    }
    let items: Vec<MetaWindowItem> = cache.take_dirty_subrange_snapshots();
    assert_eq!(items.len(), 2, "5,6 相邻合并; 9 单独");
    assert_eq!((items[0].sub_start, items[0].sub_len), (5, 2));
    assert_eq!((items[1].sub_start, items[1].sub_len), (9, 1));
    assert_eq!(items[0].bytes.len(), 2 * SUB_SLOTS * 8);
    let expect = scalar_subrange_bytes(0, 5, 2);
    assert_eq!(items[0].bytes, expect, "item 字节须与标量参考一致");
    for it in &items {
        cache.complete_subrange_flush(it.window, it.sub_start, it.sub_len, true);
    }
    let _ = std::fs::remove_file(&path);
}

/// O2: in-flight 期间同子区重写 → dirty 存活下轮重取; 失败重试同理.
#[test]
fn subrange_inflight_retake_and_retry() {
    let path = std::env::temp_dir().join(format!("nexus_meta_sub3_{}.mate", std::process::id()));
    let _ = std::fs::remove_file(&path);
    let mut cache = MetaCache::open(&path).unwrap();

    fill_window(&mut cache, 0);
    cache.flush_dirty().unwrap();

    // 子区 3 写入 → 取快照 (3 在飞)
    cache.write(3 * SUB_SLOTS as u64, varied_pid(1));
    let items = cache.take_dirty_subrange_snapshots();
    assert_eq!(items.len(), 1);
    assert_eq!(items[0].sub_start, 3);

    // 在飞期间写子区 3 与新子区 9
    cache.write(3 * SUB_SLOTS as u64, varied_pid(2));
    cache.write(9 * SUB_SLOTS as u64, varied_pid(3));
    let more = cache.take_dirty_subrange_snapshots();
    assert_eq!(more.len(), 1, "3 在飞跳过; 只有 9 可取");
    assert_eq!(more[0].sub_start, 9);

    // 失败重试: 9 快照失败 → 9 重新脏
    cache.complete_subrange_flush(0, 9, 1, false);
    let retry = cache.take_dirty_subrange_snapshots();
    assert_eq!(retry.len(), 1, "失败快照的子区应重取");
    assert_eq!(retry[0].sub_start, 9);
    cache.complete_subrange_flush(0, 9, 1, true);

    // 3 在飞完成, 清 in-flight (期间重写的值留下 dirty)
    cache.complete_subrange_flush(0, 3, 1, true);
    let last = cache.take_dirty_subrange_snapshots();
    assert_eq!(last.len(), 1, "在飞期间对子区 3 的重写应保留脏位");
    assert_eq!(last[0].sub_start, 3);
    cache.complete_subrange_flush(0, 3, 1, true);
    assert!(
        cache.take_dirty_subrange_snapshots().is_empty(),
        "全部完成后无剩余脏子区"
    );
    let _ = std::fs::remove_file(&path);
}
