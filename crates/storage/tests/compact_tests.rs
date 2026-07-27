//! ⭐ G2: chunk compact 集成测试 (死槽填充 + CAS 提交 + 延迟释放).
//!
//! 直接驱动三阶段状态机 (start_compact → analyze_compact_read →
//! complete_compact), IO 段在测试内 await 执行 (StdFs 后端).
//! 生产路径由 manager.rs drive_async_flush b3/收割段接线, 语义相同.

use std::io::Write;

use storage::alloc::{PidAllocator, VpidAllocator};
use storage::chunk_lru::ChunkList;
use storage::chunk_writer::{ChunkWriter, NowChunks};
use storage::pager::{PageWriteBatch, Pager};
use storage::{MetaCache, PAGE_SIZE};

mod common;

use common::run_async;


/// 构造带合法 page header 的测试页 (magic "LCBP" + page_type=3;
/// vpid 字段由 submit 自动覆盖). compact 判活依赖 header 自描述,
/// 与生产 B+ 树页 (page crate 构造) 同约定.
fn make_page(fill: u8) -> Box<[u8; PAGE_SIZE]> {
    let mut p = Box::new([fill; PAGE_SIZE]);
    p[0..4].copy_from_slice(b"LCBP");
    p[4] = 3; // Leaf
    p
}

fn setup_pager(tmp: &tempfile::TempDir) -> Pager {
    let mate = tmp.path().join("page.mate");
    std::fs::File::create(&mate)
        .unwrap()
        .write_all(&vec![0u8; 1024 * 1024])
        .unwrap();
    let meta = MetaCache::open(&mate).unwrap();
    let block_path = tmp.path().join("000001.block");
    let f = std::fs::File::create(&block_path).unwrap();
    f.set_len(16 * 1024 * 1024).unwrap();
    Pager::new(
        tmp.path().to_path_buf(),
        meta,
        VpidAllocator::new(0),
        PidAllocator::new(0, 0, 1),
        ChunkList::new(32),
        NowChunks::new(),
        ChunkWriter::new(&block_path).unwrap(),
    )
}

/// 造场景: 创建 n 页 → flush 落盘 → COW 更新 stale 集合让旧 chunk 稀疏.
/// 返回创建的 vpid 列表.
async fn fill_and_sparsify(pager: &mut Pager, total: usize, keep_alive: &[u64]) -> Vec<u64> {
    let mut vpids = Vec::with_capacity(total);
    for i in 0..total {
        let page = make_page((i % 251) as u8 + 1);
        vpids.push(pager.create(page).await.unwrap());
    }
    pager.flush().await.unwrap(); // 全部落盘, nowchunks 清空 → 后续更新走 COW

    // COW 更新除 keep_alive 外的全部 vpid → 旧 chunk 只剩 keep_alive 活页
    for &vpid in &vpids {
        if keep_alive.contains(&vpid) {
            continue;
        }
        let mut b = PageWriteBatch::new();
        b.add(vpid, make_page(0xEE));
        b.submit(pager).await.unwrap();
    }
    pager.flush().await.unwrap(); // 新写落盘
    vpids
}

/// 死槽填充 roundtrip: 两个稀疏 chunk compact 后, 活页数据可读且 src 进延迟释放.
#[test]
fn compact_fill_dead_slots_roundtrip() {
    run_async(async move {
        let tmp = tempfile::tempdir().unwrap();
        let mut pager = setup_pager(&tmp);

        // 192 页跨 chunk (0,0)/(0,1)/(0,2)+; 让 chunk1 的 vpid 70 和
        // chunk2 的 vpid 130 存活, 其余 COW 走
        let keep = [70u64, 130u64];
        let _vpids = fill_and_sparsify(&mut pager, 192, &keep).await;

        assert!(
            pager.liveness().live_pages(storage::PageKey { file_id: 0, chunk_idx: 1 }) > 0,
            "chunk1 应有残余活页"
        );

        // 阶段 1: 选 victim (排除 active/chunk0)
        let rj = pager.start_compact().expect("应能选出 victim");
        assert!(pager.compact_inflight());
        let (dst, src) = (rj.dst, rj.src);
        assert_ne!(dst, src);
        assert_ne!(src.chunk_idx, 0, "META chunk 不可为 src");

        // 阶段 1 IO: 读 dst+src chunk (测试内直接 await)
        let dst_bytes = rj.io.read_page_chunk(&rj.dir, dst).await.unwrap();
        let src_bytes = rj.io.read_page_chunk(&rj.dir, src).await.unwrap();

        // 阶段 2: 判活 + 组装写作业
        let wj = pager
            .analyze_compact_read(dst, src, false, Ok((dst_bytes, src_bytes)))
            .expect("有活页应产出写作业");
        assert!(!wj.moves.is_empty());
        let moved_vpids: Vec<u64> = wj.moves.iter().map(|(v, _, _)| *v).collect();

        // 阶段 2 IO: 写 dst 死槽
        let items: Vec<(u8, &[u8])> = wj.items.iter().map(|(p, d)| (*p, d.as_slice())).collect();
        wj.io.write_pages_batch(&wj.dir, wj.dst, &items).await.unwrap();
        drop(items);

        // 阶段 3: CAS 提交
        pager.complete_compact(wj.dst, wj.src, wj.moves, Ok(()));
        assert!(!pager.compact_inflight());
        assert_eq!(
            pager.liveness().live_pages(src),
            0,
            "src 活页全部迁走"
        );
        assert_eq!(pager.liveness().pending_free_count(), 1, "src 入延迟释放");

        // 迁移后的页可读且内容正确 (走 disk: chunk_list 已 invalidate)
        for vpid in moved_vpids {
            let page = pager.read(vpid).await.unwrap();
            assert_ne!(page[0x30], 0, "迁移页数据区内容非零");
        }

        // meta 确认落盘后 promote → free 可复用
        pager.flush().await.unwrap(); // 同步路径刷 meta
        // 同步 flush 不走 complete_meta_flush; 手动模拟异步确认点语义:
        // take_meta_flush_batch 后无 dirty → 但同步 flush 已清 dirty,
        // promote 由异步路径触发. 这里直接验证 pending 仍在 (未确认不复用).
        assert_eq!(pager.liveness().pending_free_count(), 1);
    });
}

/// CAS 竞态: compact IO 完成后、提交前, 用户 COW 覆盖 src 活页 →
/// 提交跳过该页, 读到用户新值 (不回滚).
#[test]
fn compact_cas_skips_concurrently_updated_page() {
    run_async(async move {
        let tmp = tempfile::tempdir().unwrap();
        let mut pager = setup_pager(&tmp);

        let keep = [70u64, 75u64, 130u64];
        let _ = fill_and_sparsify(&mut pager, 192, &keep).await;

        let rj = pager.start_compact().expect("victims");
        let (dst, src) = (rj.dst, rj.src);
        let dst_bytes = rj.io.read_page_chunk(&rj.dir, dst).await.unwrap();
        let src_bytes = rj.io.read_page_chunk(&rj.dir, src).await.unwrap();
        let wj = pager
            .analyze_compact_read(dst, src, false, Ok((dst_bytes, src_bytes)))
            .expect("write job");

        // 挑一个被搬运的 vpid, 在写盘期间被用户 COW 更新
        let raced_vpid = wj.moves[0].0;
        let mut b = PageWriteBatch::new();
        b.add(raced_vpid, make_page(0xAB));
        b.submit(&mut pager).await.unwrap();

        // 写盘 + 提交
        let items: Vec<(u8, &[u8])> = wj.items.iter().map(|(p, d)| (*p, d.as_slice())).collect();
        wj.io.write_pages_batch(&wj.dir, wj.dst, &items).await.unwrap();
        drop(items);
        pager.complete_compact(wj.dst, wj.src, wj.moves, Ok(()));

        // 竞态页读到用户新值 (CAS 跳过, 未被 compact 回滚).
        // 注: [0..0x28] 是 header 区, 断言用数据区偏移.
        let page = pager.read(raced_vpid).await.unwrap();
        assert_eq!(page[0x30], 0xAB, "并发 COW 写不能被 compact 回滚");
    });
}

/// 写失败: 丢弃 job, 无 meta 变更, 数据完好, 可下轮重试.
#[test]
fn compact_write_failure_is_side_effect_free() {
    run_async(async move {
        let tmp = tempfile::tempdir().unwrap();
        let mut pager = setup_pager(&tmp);

        let keep = [70u64, 130u64];
        let _ = fill_and_sparsify(&mut pager, 192, &keep).await;

        let rj = pager.start_compact().expect("victims");
        let (dst, src) = (rj.dst, rj.src);
        let dst_bytes = rj.io.read_page_chunk(&rj.dir, dst).await.unwrap();
        let src_bytes = rj.io.read_page_chunk(&rj.dir, src).await.unwrap();
        let wj = pager
            .analyze_compact_read(dst, src, false, Ok((dst_bytes, src_bytes)))
            .expect("write job");
        let moved: Vec<u64> = wj.moves.iter().map(|(v, _, _)| *v).collect();
        let live_before = pager.liveness().live_pages(src);

        // 模拟写失败
        pager.complete_compact(
            wj.dst,
            wj.src,
            wj.moves,
            Err(std::io::Error::other("simulated")),
        );
        assert!(!pager.compact_inflight());
        assert_eq!(
            pager.liveness().live_pages(src),
            live_before,
            "失败不改 liveness"
        );
        assert_eq!(pager.liveness().pending_free_count(), 0, "失败不释放");
        for vpid in moved {
            let page = pager.read(vpid).await.unwrap();
            assert_ne!(page[0x30], 0xEE, "原数据完好");
        }
        // 可重试 (重置节流窗口后)
        pager.reset_compact_throttle();
        assert!(pager.start_compact().is_some(), "下轮可重新发起");
    });
}

/// ⭐ G4: block 全死 → 收割 → meta 确认 → unlink 文件; reopen 兼容 file_id 空洞.
///
/// 注: file0 含 MetaPage 固定位 (vpid 0 = META_VPID 永活在 (0,0,0)),
/// 永不可回收 — 因此验证 file1 的回收.
#[test]
fn dead_block_is_unlinked_and_reopen_survives() {
    run_async(async move {
        let tmp = tempfile::tempdir().unwrap();
        let mut pager = setup_pager(&tmp);

        // 写 1400 页: file0 (vpid 0..638) + file1 (vpid 639..1278) + file2 溢出
        let mut vpids = Vec::new();
        for i in 0..1400usize {
            let page = make_page((i % 251) as u8 + 1);
            vpids.push(pager.create(page).await.unwrap());
        }
        pager.flush().await.unwrap();

        // COW 更新 file1 区间的全部 vpid (实测 vpid 640..=1279 落在 file1)
        // → file1 页全死 (新位置在 file2+)
        for &vpid in &vpids[640..1280] {
            let mut b = PageWriteBatch::new();
            b.add(vpid, make_page(0xEE));
            b.submit(&mut pager).await.unwrap();
        }
        pager.flush().await.unwrap();
        assert!(
            pager.liveness().block_fully_free(1),
            "file1 应全死 (无活页)"
        );

        // 收割 dead chunk (start_compact 顺带; victims 可能 None 无所谓)
        let _ = pager.start_compact();
        assert!(pager.liveness().pending_free_count() > 0, "dead chunk 入延迟释放");

        // meta 确认链路 → promote → maybe_drop_free_blocks
        if let Some(mb) = pager.take_meta_flush_batch() {
            let witems: Vec<(u32, &[u8])> =
                mb.windows.iter().map(|(w, b)| (*w, b.as_slice())).collect();
            mb.io.write_mate_windows(&mb.mate_path, &witems).await.unwrap();
            drop(witems);
            for (w, _) in &mb.windows {
                pager.complete_meta_flush(*w, Ok(()));
            }
        } else {
            // 无脏 window (flush 已同步刷过): 手动走确认点语义 —
            // 再写一页触发 dirty 再确认
            let mut b = PageWriteBatch::new();
            b.add(vpids[0], make_page(0xCC));
            b.submit(&mut pager).await.unwrap();
            pager.flush().await.unwrap();
            let mb = pager.take_meta_flush_batch();
            if let Some(mb) = mb {
                let witems: Vec<(u32, &[u8])> =
                    mb.windows.iter().map(|(w, b)| (*w, b.as_slice())).collect();
                mb.io.write_mate_windows(&mb.mate_path, &witems).await.unwrap();
                drop(witems);
                for (w, _) in &mb.windows {
                    pager.complete_meta_flush(*w, Ok(()));
                }
            }
        }

        let block1 = tmp.path().join("000002.block");
        assert!(!block1.exists(), "全死 block 文件应被 unlink");

        // reopen: file_id 空洞 (只剩 000002.block+), 数据完整
        pager.flush().await.unwrap();
        drop(pager);
        let mut state = storage::test_support::recover(tmp.path()).expect("recover");
        for &v in &vpids {
            assert!(state.meta.read(v).is_some(), "vpid {v} reopen 后不丢");
        }
    });
}

/// 读失败: 无副作用, inflight 清除.
#[test]
fn compact_read_failure_clears_inflight() {
    run_async(async move {
        let tmp = tempfile::tempdir().unwrap();
        let mut pager = setup_pager(&tmp);

        let keep = [70u64, 130u64];
        let _ = fill_and_sparsify(&mut pager, 192, &keep).await;

        let rj = pager.start_compact().expect("victims");
        assert!(pager.compact_inflight());
        let r = pager.analyze_compact_read(
            rj.dst,
            rj.src,
            false,
            Err(std::io::Error::other("simulated read failure")),
        );
        assert!(r.is_none());
        assert!(!pager.compact_inflight(), "读失败清 inflight");
    });
}

/// ⭐ G3: 延迟释放 → meta 确认 → promote → free chunk 被复用,
/// 新写落在被释放的 chunk 位置, reopen 后数据完整.
#[test]
fn compact_freed_chunk_is_reused_and_survives_reopen() {
    run_async(async move {
        let tmp = tempfile::tempdir().unwrap();
        let mut pager = setup_pager(&tmp);

        let keep = [70u64, 130u64];
        let vpids = fill_and_sparsify(&mut pager, 192, &keep).await;

        // 完整 compact 流程
        let rj = pager.start_compact().expect("victims");
        let (dst, src) = (rj.dst, rj.src);
        let dst_bytes = rj.io.read_page_chunk(&rj.dir, dst).await.unwrap();
        let src_bytes = rj.io.read_page_chunk(&rj.dir, src).await.unwrap();
        let wj = pager
            .analyze_compact_read(dst, src, false, Ok((dst_bytes, src_bytes)))
            .expect("write job");
        let items: Vec<(u8, &[u8])> = wj.items.iter().map(|(p, d)| (*p, d.as_slice())).collect();
        wj.io.write_pages_batch(&wj.dir, wj.dst, &items).await.unwrap();
        drop(items);
        pager.complete_compact(wj.dst, wj.src, wj.moves, Ok(()));
        assert_eq!(pager.liveness().pending_free_count(), 1);

        // 模拟异步 meta 确认链路: 取 window 快照 → 写盘 → 逐 window 确认
        let mb = pager.take_meta_flush_batch().expect("meta 批 (compact 后 due)");
        let witems: Vec<(u32, &[u8])> = mb.windows.iter().map(|(w, b)| (*w, b.as_slice())).collect();
        mb.io.write_mate_windows(&mb.mate_path, &witems).await.unwrap();
        drop(witems);
        for (w, _) in &mb.windows {
            pager.complete_meta_flush(*w, Ok(()));
        }
        assert_eq!(pager.liveness().pending_free_count(), 0, "确认后 promote");
        assert_eq!(pager.liveness().free_count(), 1, "src 可复用");

        // 新写袞满当前 active chunk 触发 rotate → 应复用 freed src chunk
        let mut new_vpids = Vec::new();
        for i in 0..128 {
            let page = make_page((i % 200) as u8 + 1);
            new_vpids.push(pager.create(page).await.unwrap());
        }
        assert_eq!(pager.liveness().free_count(), 0, "free chunk 已被复用");
        pager.flush().await.unwrap();

        // reopen: recover 重建 (mate 为主源), 新旧数据均可读
        drop(pager);
        let mut state = storage::test_support::recover(tmp.path()).expect("recover");
        for &v in keep.iter().chain(new_vpids.iter()) {
            assert!(
                state.meta.read(v).is_some(),
                "vpid {v} 在 reopen 后仍有映射"
            );
        }
        // 旧活页不丢 (非 keep 的页被 COW 过, 也都应在)
        for &v in &vpids {
            assert!(state.meta.read(v).is_some(), "vpid {v} 不丢");
        }
    });
}

/// ⭐ B-drain: 半空 block 主动排空 — 中等活度 chunk (live >= 32, 普通 compact
/// 阈值永不选) 被逐轮迁出 (每轮一个 chunk, 状态机分片; 含 fresh bump dst
/// 兑底路径), 最终 block 全死 → unlink 回收.
#[test]
fn block_drain_evicts_mid_live_chunks_and_unlinks() {
    run_async(async move {
        let tmp = tempfile::tempdir().unwrap();
        let mut pager = setup_pager(&tmp);

        // 1400 页: file0 (vpid 0..=639 区) / file1 (vpid 640..=1279) / file2+
        let mut vpids = Vec::new();
        for i in 0..1400usize {
            let page = make_page((i % 251) as u8 + 1);
            vpids.push(pager.create(page).await.unwrap());
        }
        pager.flush().await.unwrap();

        // file1 保留两个中等活度 chunk: chunk3 前 40 页 + chunk7 前 40 页
        // (live=40 >= 32, 普通 compact 阈值永不选它们为 victim)
        let keep: Vec<u64> = (832..872u64).chain(1088..1128u64).collect();
        for &vpid in &vpids[640..1280] {
            if keep.contains(&vpid) {
                continue;
            }
            let mut b = PageWriteBatch::new();
            b.add(vpid, make_page(0xEE));
            b.submit(&mut pager).await.unwrap();
        }
        pager.flush().await.unwrap();

        let c3 = storage::PageKey { file_id: 1, chunk_idx: 3 };
        let c7 = storage::PageKey { file_id: 1, chunk_idx: 7 };
        assert_eq!(pager.liveness().live_pages(c3), 40);
        assert_eq!(pager.liveness().live_pages(c7), 40);
        assert!(!pager.liveness().block_fully_free(1), "file1 半空非全死");

        // 进入排空模式 (自动路径由 complete_compact 验收触发, 测试显式请求)
        pager.request_block_drain(1);

        // 状态机分片驱动: 每轮迁移一个 chunk, 直到 target 达成
        for _round in 0..8 {
            pager.reset_compact_throttle();
            let Some(rj) = pager.start_compact() else {
                break; // target 清除 (block 全死) 且无普通 victim
            };
            if rj.src.file_id != 1 {
                // drain 达成后落到普通 pick 的作业: 直接放弃本轮
                pager.analyze_compact_read(rj.dst, rj.src, rj.dst_fresh, Err(std::io::Error::other("skip")));
                break;
            }
            // 协程 IO 段 (fresh dst 传全零, 与 manager 分支一致)
            let dst_bytes = if rj.dst_fresh {
                vec![0u8; storage::CHUNK_SIZE]
            } else {
                rj.io.read_page_chunk(&rj.dir, rj.dst).await.unwrap()
            };
            let src_bytes = rj.io.read_page_chunk(&rj.dir, rj.src).await.unwrap();
            let Some(wj) =
                pager.analyze_compact_read(rj.dst, rj.src, rj.dst_fresh, Ok((dst_bytes, src_bytes)))
            else {
                continue; // src 全死分支 (已入延迟释放)
            };
            wj.execute().await.unwrap(); // fresh dst 整 chunk 写 / 常规死槽批写
            pager.complete_compact(wj.dst, wj.src, wj.moves, Ok(()));
        }

        assert!(
            pager.liveness().block_fully_free(1),
            "排空后 file1 应全死"
        );

        // meta 确认 → promote → unlink file1
        if let Some(mb) = pager.take_meta_flush_batch() {
            let witems: Vec<(u32, &[u8])> =
                mb.windows.iter().map(|(w, b)| (*w, b.as_slice())).collect();
            mb.io.write_mate_windows(&mb.mate_path, &witems).await.unwrap();
            drop(witems);
            for (w, _) in &mb.windows {
                pager.complete_meta_flush(*w, Ok(()));
            }
        }
        assert!(
            !tmp.path().join("000002.block").exists(),
            "排空后的 block 文件应被 unlink"
        );

        // 迁出的中等活度页数据完整可读
        for &v in &keep {
            let page = pager.read(v).await.unwrap();
            assert_ne!(page[0x30], 0xEE, "keep 页 {v} 内容不是被覆盖版本");
            assert_ne!(page[0x30], 0, "keep 页 {v} 数据区非零");
        }

        // reopen 完整性
        pager.flush().await.unwrap();
        drop(pager);
        let mut state = storage::test_support::recover(tmp.path()).expect("recover");
        for &v in &vpids {
            assert!(state.meta.read(v).is_some(), "vpid {v} reopen 后不丢");
        }
    });
}
