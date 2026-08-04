// ⭐ 解耦 2026-08: shard 线程主循环 + 异步落盘驱动 (从 manager.rs 拆出).
// 职责: 每 shard 独立线程的消息处理 (admin + KV ops), 异步落盘 (drive/drain).
use crate::error::{ShardError, ShardResult};
use crate::exec_cmds::*;
use crate::manager::{block_on_io, FlushDone, FlushDoneSlot, ReplySink};
use crate::request::{BatchOp, BatchResult, ShardErrorKind, ShardId, ShardReply, ShardRequest, ShardResponse};
use crate::router::Router;
use std::cell::RefCell;
use std::rc::Rc;
use std::sync::{Arc, Mutex as StdMutex};
use storage::{OpenOptions, StorageEngine};

pub(crate) fn drive_async_flush(
    engine: &std::rc::Rc<std::cell::RefCell<Option<StorageEngine>>>,
    rt: &scheduler::SchedHandle,
    flush_done: &FlushDoneSlot,
) {
    // ⭐ DIAG: NLOG_NO_FLUSH=1 禁用异步落盘 (定位数据丢失根因)
    if std::env::var("NLOG_NO_FLUSH").is_ok_and(|v| v == "1") {
        return;
    }
    let round_start = std::time::Instant::now();
    {
        let mut e_borrow = engine.borrow_mut();
        if let Some(e) = e_borrow.as_mut() {
            // a. 收割上轮完成的落盘 (data: 成功入 chunk_list, 失败回 pending;
            //    meta: 清 in-flight, 全部确认后 persist pid.state)
            for done in flush_done.borrow_mut().drain(..) {
                let cor_start = std::time::Instant::now();
                match done {
                    FlushDone::Data(key, result) => {
                        if let Err(err) = e.pager_mut().complete_flush(key, result) {
                            nlog::error!("shard", "chunk flush failed (requeued): {err}");
                        }
                    }
                    FlushDone::Meta(window_idx, result) => {
                        if let Err(ref err) = result {
                            nlog::error!("shard", "meta window {window_idx} flush failed (will retry): {err}");
                        }
                        e.pager_mut().complete_meta_flush(window_idx, result);
                        // ⭐ WAL (F60): meta 全部持久化 → sealed 段可删
                        e.wal_drop_sealed_if_meta_flushed();
                    }
                    // ⭐ G2 阶段 2 (同步): meta 判活 → 组装写作业 → 低优先级协程写盘
                    FlushDone::CompactRead(dst, src, dst_fresh, read_result) => {
                        if let Some(wj) =
                            e.pager_mut().analyze_compact_read(dst, src, dst_fresh, read_result)
                        {
                            let done = flush_done.clone();
                            scheduler::spawn_on_low(
                                rt,
                                Box::pin(async move {
                                    // fresh dst 整 chunk 写 / 常规 dst 死槽批写
                                    let r = wj.execute().await;
                                    done.borrow_mut().push(FlushDone::CompactWrite(
                                        wj.dst, wj.src, wj.moves, r,
                                    ));
                                }),
                            );
                        }
                    }
                    // ⭐ G2 阶段 3 (同步): CAS 提交 (防回滚并发 COW 写)
                    FlushDone::CompactWrite(dst, src, moves, result) => {
                        if let Err(ref err) = result {
                            nlog::warn!("shard", "compact write failed (will retry): {err}");
                        }
                        e.pager_mut().complete_compact(dst, src, moves, result);
                    }
                }
                if crate::PROBE.is_enabled() {
                    crate::PROBE
                        .sync_write_coroutine_ns
                        .record(cor_start.elapsed().as_nanos() as u64);
                }
            }
            // b. 提交新作业: 同步入队 + spawn 协程 (SQE 在首次 poll 时提交)
            // Phase C: 按 file 成批, 每批 write ×N + fsync ×1 (长尾对症)
            let batches = e.pager_mut().take_flush_batches();
            if crate::PROBE.is_enabled() && !batches.is_empty() {
                let inflight = e.pager_mut().flush_backlog();
                crate::PROBE
                    .in_flight_peak
                    .fetch_max(inflight as u64, std::sync::atomic::Ordering::Relaxed);
            }
            for batch in batches {
                let done = flush_done.clone();
                scheduler::spawn_on(
                    rt,
                    Box::pin(async move {
                        let items: Vec<(storage::PageKey, &[u8])> = batch
                            .items
                            .iter()
                            .map(|(k, b)| (*k, b.as_slice()))
                            .collect();
                        let r = batch.io.write_chunks_batch(&batch.dir, &items).await;
                        drop(items);
                        // 逐 key push 完成槽 (io::Error 不可 Clone, 用 msg 重建)
                        let mut slot = done.borrow_mut();
                        match r {
                            Ok(()) => {
                                for (key, _) in &batch.items {
                                    slot.push(FlushDone::Data(*key, Ok(())));
                                }
                            }
                            Err(err) => {
                                let msg = err.to_string();
                                for (key, _) in &batch.items {
                                    slot.push(FlushDone::Data(
                                        *key,
                                        Err(std::io::Error::other(msg.clone())),
                                    ));
                                }
                            }
                        }
                    }),
                );
            }
            // b2. ⭐ Phase M3: meta window 异步刷盘 (data backlog 排空后才取得到批,
            // data→meta 顺序不变; fsync 在协程里, 主循环零阻塞)
            if let Some(mb) = e.pager_mut().take_meta_flush_batch() {
                let done = flush_done.clone();
                scheduler::spawn_on(
                    rt,
                    Box::pin(async move {
                        let items: Vec<(u32, &[u8])> = mb
                            .windows
                            .iter()
                            .map(|(w, b)| (*w, b.as_slice()))
                            .collect();
                        let r = mb.io.write_mate_windows(&mb.mate_path, &items).await;
                        drop(items);
                        let mut slot = done.borrow_mut();
                        match r {
                            Ok(()) => {
                                for (w, _) in &mb.windows {
                                    slot.push(FlushDone::Meta(*w, Ok(())));
                                }
                            }
                            Err(err) => {
                                let msg = err.to_string();
                                for (w, _) in &mb.windows {
                                    slot.push(FlushDone::Meta(
                                        *w,
                                        Err(std::io::Error::other(msg.clone())),
                                    ));
                                }
                            }
                        }
                    }),
                );
            }
            // b3. ⭐ G2: 空闲段发起 chunk compact (低优先级协程读 dst+src 字节;
            // 判活用 header 候选 + meta 点查, 零全扫).
            // 触发条件: data backlog == 0; start_compact 内部节流 + 至多 1 个在飞.
            if e.pager_mut().flush_backlog() == 0
                && let Some(rj) = e.pager_mut().start_compact()
            {
                let done = flush_done.clone();
                scheduler::spawn_on_low(
                    rt,
                    Box::pin(async move {
                        // ⭐ B-drain: fresh dst (全新 bump chunk) 磁盘无内容,
                        // 跳过读直接传全零 (analyze 判 64 槽全死槽)
                        let dst_r = if rj.dst_fresh {
                            Ok(vec![0u8; storage::CHUNK_SIZE])
                        } else {
                            rj.io.read_page_chunk(&rj.dir, rj.dst).await
                        };
                        let r = match dst_r {
                            Ok(dst_bytes) => rj
                                .io
                                .read_page_chunk(&rj.dir, rj.src)
                                .await
                                .map(|src_bytes| (dst_bytes, src_bytes)),
                            Err(e) => Err(e),
                        };
                        done.borrow_mut().push(FlushDone::CompactRead(
                            rj.dst, rj.src, rj.dst_fresh, r,
                        ));
                    }),
                );
            }
            // c. 周期/计数刷盘 (内部守卫: 有 in-flight/pending 时自动推迟)
            let pf_start = std::time::Instant::now();
            let pf = block_on_io(e.pager_mut().maybe_periodic_flush());
            // ⭐ WAL (F60): 刷盘快照已入队 → 同轮内 seal 当前段 (无并发写间隙;
            // 段覆盖记录 ⊆ 快照内容, meta 全部落盘后删)
            if matches!(pf, Ok(true)) {
                e.wal_seal();
            }
            // ⭐ WAL (F60): periodic 档每 1s 落盘+fsync (丢失窗口 10s → ~1s)
            if let Err(err) = block_on_io(e.wal_periodic_tick()) {
                nlog::error!("shard", "WAL periodic sync failed: {err}");
            }
            if crate::PROBE.is_enabled() {
                crate::PROBE
                    .block_on_io_ns
                    .record(pf_start.elapsed().as_nanos() as u64);
            }
        }
    }
    // d. 推进落盘协程 (提交 SQE / 收割 CQE / 完成时 push 完成槽)
    let di_start = std::time::Instant::now();
    rt.clone().drive_until_idle(256);
    if crate::PROBE.is_enabled() {
        crate::PROBE
            .drive_until_idle_ns
            .record(di_start.elapsed().as_nanos() as u64);
        crate::PROBE
            .drive_round_ns
            .record(round_start.elapsed().as_nanos() as u64);
    }
}

/// ⭐ 排空异步落盘 backlog (flush 请求/shutdown 前调用, 保证 flush() 契约).
pub(crate) fn drain_async_flush(
    engine: &std::rc::Rc<std::cell::RefCell<Option<StorageEngine>>>,
    rt: &scheduler::SchedHandle,
    flush_done: &FlushDoneSlot,
) {
    loop {
        drive_async_flush(engine, rt, flush_done);
        let drained = {
            let mut e_borrow = engine.borrow_mut();
            match e_borrow.as_mut() {
                // ⭐ Phase M3: 含 meta backlog (due/dirty/in-flight) 才算排空
                Some(e) => e.pager_mut().total_async_backlog() == 0,
                None => true,
            }
        };
        if drained && flush_done.borrow().is_empty() {
            break;
        }
        rt.clone().drive_until_idle(1000);
    }
}

/// shard 线程主函数. ⭐ 同时处理 ShardRequest (admin) 和 ShardTask (KV ops).
pub(crate) fn shard_thread_main(
    shard_id: usize,
    storage_opts: OpenOptions,
    _router: Arc<dyn Router>,
    inbox: crate::inbox::SharedInbox,
    task_inbox: crate::task_inbox::SharedTaskInbox,
    reply_sink: Arc<StdMutex<Option<Arc<dyn ReplySink>>>>,
    reply_bus_set: Arc<crate::task_reply_bus::ReplyBusSet>,
) {
    use std::cell::RefCell;
    use std::rc::Rc;

    // T18c: 支持 SQPOLL (sqpoll_ms > 0 时启用内核线程轮询)
    let sqpoll_ms = storage_opts.io_config.sqpoll_ms;
    let scheduler = if sqpoll_ms > 0 {
        scheduler::Scheduler::new_with_sqpoll(sqpoll_ms)
    } else {
        scheduler::Scheduler::new()
    };
    let rt = scheduler::SchedHandle::new(scheduler);
    rt.set_current();

    let engine: Rc<RefCell<Option<StorageEngine>>> = Rc::new(RefCell::new(None));

    let engine_init = engine.clone();
    let init_result: Rc<RefCell<Option<Result<(), storage::StorageError>>>> =
        Rc::new(RefCell::new(None));
    let init_result_clone = init_result.clone();

    let init_fut = Box::pin(async move {
        let result = StorageEngine::open(storage_opts).await;
        match result {
            Ok(e) => {
                *engine_init.borrow_mut() = Some(e);
                *init_result_clone.borrow_mut() = Some(Ok(()));
            }
            Err(e) => {
                *init_result_clone.borrow_mut() = Some(Err(e));
            }
        }
    });
    scheduler::spawn_on(&rt, init_fut);

    while init_result.borrow().is_none() {
        rt.clone().drive_until_idle(1000);
    }
    if init_result.borrow().as_ref().unwrap().is_err() {
        let err = init_result.borrow().as_ref().unwrap().as_ref().err().map(|e| format!("{e:?}"));
        nlog::error!("shard", "shard-{shard_id} engine init failed: {err:?}, exiting");
        return;
    }
    drop(init_result);
    nlog::info!("shard", "shard-{shard_id} engine ready");

    // ⭐ 探针启用: NLOG_PROBE=1 时 dump_all() 可输出各阶段 histogram.
    if std::env::var("NLOG_PROBE").ok().as_deref() == Some("1") {
        crate::PROBE.enable();
        nlog::info!("shard", "probes enabled (NLOG_PROBE=1)");
    }

    // ⭐ 异步落盘完成槽
    let flush_done: FlushDoneSlot = Rc::new(RefCell::new(Vec::new()));

    // ⭐ 主循环: 同时 poll 两个 inbox (ShardRequest + ShardTask)
    //
    // ⭐ 方向 1 优化 (2026-07-24): 慢路径从 yield 自旋改为 poll() 真阻塞双 eventfd.
    // 前提是 drain() 已修复丢唤醒竞态 (先重置 pending 再 pop),
    // 否则睡眠后可能永久错过通知. 10ms timeout 兑底驱动周期刷盘.
    const SPIN_ROUNDS_BEFORE_PARK: u32 = 1024;

    loop {
        // spin poll 两个 inbox, 任一有数据就退出 spin
        let mut spins = 0u32;
        let (batch, tasks) = loop {
            let b = inbox.drain();
            let t = task_inbox.drain();
            if !b.is_empty() || !t.is_empty() {
                break (b, t);
            }
            spins += 1;
            if spins >= SPIN_ROUNDS_BEFORE_PARK {
                // 慢速路径: poll() 阻塞等两个 eventfd (零 CPU, 精确唤醒).
                // timeout 10ms: 周期性醒来驱动自动持久化检查.
                let mut fds = [
                    libc::pollfd {
                        fd: inbox.eventfd(),
                        events: libc::POLLIN,
                        revents: 0,
                    },
                    libc::pollfd {
                        fd: task_inbox.eventfd(),
                        events: libc::POLLIN,
                        revents: 0,
                    },
                ];
                unsafe {
                    libc::poll(fds.as_mut_ptr(), 2, 10);
                }
                // 消耗 eventfd 计数 (仅在 POLLIN 时读; eventfd 是 blocking 模式,
                // 计数为 0 时读会阻塞)
                if fds[0].revents & libc::POLLIN != 0 {
                    let mut v: u64 = 0;
                    unsafe {
                        libc::read(inbox.eventfd(), &mut v as *mut u64 as *mut libc::c_void, 8);
                    }
                }
                if fds[1].revents & libc::POLLIN != 0 {
                    let mut v: u64 = 0;
                    unsafe {
                        libc::read(
                            task_inbox.eventfd(),
                            &mut v as *mut u64 as *mut libc::c_void,
                            8,
                        );
                    }
                }
                let b = inbox.drain();
                let t = task_inbox.drain();
                if !b.is_empty() || !t.is_empty() {
                    break (b, t);
                }
                // timeout 醒来无数据: 驱动异步落盘 + 周期刷盘后继续睡
                drive_async_flush(&engine, &rt, &flush_done);
                spins = 0;
                continue;
            }
            for _ in 0..4 {
                std::hint::spin_loop();
            }
        };

        rt.clone().drive_until_idle(0);

        let mut should_shutdown = false;
        for req in batch {
            match req {
                ShardRequest::Shutdown { reply } => {
                    let _ = reply.send(Ok(ShardReply::ShutdownOk));
                    should_shutdown = true;
                }
                ShardRequest::Flush { reply } => {
                    // ⭐ flush 契约: 先排空异步落盘 backlog (避免同 key 并发写)
                    drain_async_flush(&engine, &rt, &flush_done);
                    let mut e_borrow = engine.borrow_mut();
                    if let Some(e) = e_borrow.as_mut() {
                        let r = block_on_io(e.flush());
                        let _ = reply.send(match r {
                            Ok(()) => Ok(ShardReply::FlushOk),
                            Err(err) => Err(ShardErrorKind::from_storage_display(&err)),
                        });
                    } else {
                        let _ = reply.send(Err(ShardErrorKind::StorageError(
                            "engine not init".into(),
                        )));
                    }
                }
                ShardRequest::Batch { ops, req_id, reply } => {
                    let mut e_borrow = engine.borrow_mut();
                    if let Some(e) = e_borrow.as_mut() {
                        let mut results = Vec::with_capacity(ops.len());
                        for op in ops {
                            // ⭐ T1: 惰性建表 (已存在 = registry 纯内存查表)
                            {
                                let (db, table, _) = op.locator();
                                if let Err(err) = block_on_io(e.ensure_table(db, table)) {
                                    results.push(BatchResult::Error(err.to_string()));
                                    continue;
                                }
                            }
                            let r = match op {
                                // ⭐ 事务批 (管理面 Batch 兼容臂; 热路径走 ShardTask)
                                BatchOp::TxnApply { ops, read_set } => exec_txn_apply(e, ops, read_set),
                                // ⭐ M3-2: 行数估计 (只读, 表不存在=0)
                                BatchOp::EstimateRowCount { db, table } => {
                                    BatchResult::RowCount(e.estimate_row_count(&db, &table).unwrap_or(0))
                                }
                                // ⭐ M3-4: distinct 估计 (只读)
                                BatchOp::EstimateDistinct { db, table, iids } => {
                                    BatchResult::DistinctCounts(
                                        iids.iter()
                                            .map(|iid| e.estimate_distinct(&db, &table, *iid).unwrap_or(0))
                                            .collect(),
                                    )
                                }
                                // ⭐ M3-5: min/max 估计 (只读)
                                BatchOp::EstimateRanges { db, table, iids } => {
                                    BatchResult::RangeBounds(
                                        iids.iter()
                                            .map(|iid| {
                                                e.estimate_range(&db, &table, *iid)
                                                    .map(|(lo, hi)| (Some(lo), Some(hi)))
                                                    .unwrap_or((None, None))
                                            })
                                            .collect(),
                                    )
                                }
                                // ⭐ F65: 占坑 op (管理面兼容; 热路径走 ShardTask → exec_task_op)
                                op @ (BatchOp::ReserveUnique { .. }
                                | BatchOp::StealUnique { .. }
                                | BatchOp::ConfirmUnique { .. }
                                | BatchOp::ReleaseUnique { .. }
                                | BatchOp::CatalogDump { .. }) => exec_task_op(e, op),
                                BatchOp::Put { db, table, key, val } => {
                                    match block_on_io(e.table_put(&db, &table, &key, &val)) {
                                        Ok(_) => BatchResult::PutOk,
                                        Err(err) => BatchResult::Error(err.to_string()),
                                    }
                                }
                                BatchOp::Get { db, table, key } => {
                                    // ⭐ Phase H: 类型感知 (hash key → WRONGTYPE)
                                    match block_on_io(e.table_get_typed(&db, &table, &key)) {
                                        Ok(v) => BatchResult::GetValue(v),
                                        Err(err) => BatchResult::Error(err.to_string()),
                                    }
                                }
                                BatchOp::Delete { db, table, key } => {
                                    // ⭐ Phase H: 类型感知 (顺带清 hash 全部行/孤儿行)
                                    match block_on_io(e.key_delete_any(&db, &table, &key)) {
                                        Ok(b) => BatchResult::DeleteExisted(b),
                                        Err(err) => BatchResult::Error(err.to_string()),
                                    }
                                }
                                BatchOp::MultiGet { db, table, keys } => {
                                    let refs: Vec<&[u8]> =
                                        keys.iter().map(|k| k.as_slice()).collect();
                                    match block_on_io(e.table_get_many(&db, &table, &refs)) {
                                        Ok(vs) => BatchResult::Values(vs),
                                        Err(err) => BatchResult::Error(err.to_string()),
                                    }
                                }
                                BatchOp::MultiPut { db, table, pairs } => {
                                    match block_on_io(e.table_put_many(&db, &table, &pairs)) {
                                        Ok(_) => BatchResult::MultiPutOk,
                                        Err(err) => BatchResult::Error(err.to_string()),
                                    }
                                }
                                BatchOp::MultiPutNx { db, table, pairs } => {
                                    exec_multiputnx(e, &db, &table, &pairs)
                                }
                                BatchOp::Incr { db, table, key, delta } => {
                                    exec_incr(e, &db, &table, &key, delta)
                                }
                                BatchOp::IncrFloat { db, table, key, delta } => {
                                    exec_incr_float(e, &db, &table, &key, delta)
                                }
                                BatchOp::Append { db, table, key, suffix } => {
                                    exec_append(e, &db, &table, &key, &suffix)
                                }
                                BatchOp::SetNx { db, table, key, val } => {
                                    exec_setnx(e, &db, &table, &key, &val)
                                }
                                BatchOp::GetDel { db, table, key } => {
                                    exec_getdel(e, &db, &table, &key)
                                }
                                BatchOp::GetSet { db, table, key, val } => {
                                    exec_getset(e, &db, &table, &key, &val)
                                }
                                BatchOp::SetRange { db, table, key, offset, data } => {
                                    exec_setrange(e, &db, &table, &key, offset, &data)
                                }
                                BatchOp::HSet { db, table, key, pairs } => {
                                    match block_on_io(e.hash_set(&db, &table, &key, &pairs)) {
                                        Ok(n) => BatchResult::Integer(n),
                                        Err(err) => BatchResult::Error(err.to_string()),
                                    }
                                }
                                BatchOp::HSetNx { db, table, key, field, val } => {
                                    match block_on_io(e.hash_set_nx(&db, &table, &key, &field, &val)) {
                                        Ok(n) => BatchResult::Integer(n),
                                        Err(err) => BatchResult::Error(err.to_string()),
                                    }
                                }
                                BatchOp::HGet { db, table, key, field } => {
                                    match block_on_io(e.hash_get(&db, &table, &key, &field)) {
                                        Ok(v) => BatchResult::GetValue(v),
                                        Err(err) => BatchResult::Error(err.to_string()),
                                    }
                                }
                                BatchOp::HMGet { db, table, key, fields } => {
                                    match block_on_io(e.hash_get_many(&db, &table, &key, &fields)) {
                                        Ok(vs) => BatchResult::Values(vs),
                                        Err(err) => BatchResult::Error(err.to_string()),
                                    }
                                }
                                BatchOp::HDel { db, table, key, fields } => {
                                    match block_on_io(e.hash_del(&db, &table, &key, &fields)) {
                                        Ok(n) => BatchResult::Integer(n),
                                        Err(err) => BatchResult::Error(err.to_string()),
                                    }
                                }
                                BatchOp::HLen { db, table, key } => {
                                    match block_on_io(e.hash_len(&db, &table, &key)) {
                                        Ok(n) => BatchResult::Integer(n),
                                        Err(err) => BatchResult::Error(err.to_string()),
                                    }
                                }
                                BatchOp::HGetAll { db, table, key } => {
                                    match block_on_io(e.hash_get_all(&db, &table, &key)) {
                                        Ok(ps) => BatchResult::Pairs(ps),
                                        Err(err) => BatchResult::Error(err.to_string()),
                                    }
                                }
                                BatchOp::HIncrBy { db, table, key, field, delta } => {
                                    exec_hincrby(e, &db, &table, &key, &field, delta)
                                }
                                BatchOp::HIncrByFloat { db, table, key, field, delta } => {
                                    exec_hincrbyfloat(e, &db, &table, &key, &field, delta)
                                }
                                BatchOp::SAdd { db, table, key, members } => {
                                    match block_on_io(e.set_add(&db, &table, &key, &members)) {
                                        Ok(n) => BatchResult::Integer(n),
                                        Err(err) => BatchResult::Error(err.to_string()),
                                    }
                                }
                                BatchOp::SRem { db, table, key, members } => {
                                    match block_on_io(e.set_rem(&db, &table, &key, &members)) {
                                        Ok(n) => BatchResult::Integer(n),
                                        Err(err) => BatchResult::Error(err.to_string()),
                                    }
                                }
                                BatchOp::SIsMember { db, table, key, member } => {
                                    match block_on_io(e.set_is_member(&db, &table, &key, &member)) {
                                        Ok(b) => BatchResult::Integer(i64::from(b)),
                                        Err(err) => BatchResult::Error(err.to_string()),
                                    }
                                }
                                BatchOp::SCard { db, table, key } => {
                                    match block_on_io(e.set_card(&db, &table, &key)) {
                                        Ok(n) => BatchResult::Integer(n),
                                        Err(err) => BatchResult::Error(err.to_string()),
                                    }
                                }
                                BatchOp::SMembers { db, table, key } => {
                                    match block_on_io(e.set_members(&db, &table, &key)) {
                                        Ok(ms) => BatchResult::Members(ms),
                                        Err(err) => BatchResult::Error(err.to_string()),
                                    }
                                }
                                BatchOp::SPop { db, table, key } => exec_spop(e, &db, &table, &key),
                                BatchOp::SRandMember { db, table, key } => {
                                    match block_on_io(e.set_pick_one(&db, &table, &key)) {
                                        Ok(m) => BatchResult::Members(m.into_iter().collect()),
                                        Err(err) => BatchResult::Error(err.to_string()),
                                    }
                                }
                                BatchOp::LPush { db, table, key, values, left } => {
                                    match block_on_io(e.list_push(&db, &table, &key, &values, left)) {
                                        Ok(n) => BatchResult::Integer(n),
                                        Err(err) => BatchResult::Error(err.to_string()),
                                    }
                                }
                                BatchOp::LPop { db, table, key, left, count } => {
                                    exec_lpop(e, &db, &table, &key, left, count as usize)
                                }
                                BatchOp::LLen { db, table, key } => {
                                    match block_on_io(e.list_len(&db, &table, &key)) {
                                        Ok(n) => BatchResult::Integer(n),
                                        Err(err) => BatchResult::Error(err.to_string()),
                                    }
                                }
                                BatchOp::LRange { db, table, key, start, end } => {
                                    exec_lrange(e, &db, &table, &key, start, end)
                                }
                                BatchOp::LIndex { db, table, key, idx } => {
                                    match block_on_io(e.list_index(&db, &table, &key, idx)) {
                                        Ok(v) => BatchResult::GetValue(v),
                                        Err(err) => BatchResult::Error(err.to_string()),
                                    }
                                }
                                BatchOp::LSet { db, table, key, idx, val } => {
                                    exec_lset(e, &db, &table, &key, idx, &val)
                                }
                                BatchOp::ZAdd { db, table, key, pairs } => {
                                    match block_on_io(e.zset_add(&db, &table, &key, &pairs)) {
                                        Ok(n) => BatchResult::Integer(n),
                                        Err(err) => BatchResult::Error(err.to_string()),
                                    }
                                }
                                BatchOp::ZRem { db, table, key, members } => {
                                    match block_on_io(e.zset_rem(&db, &table, &key, &members)) {
                                        Ok(n) => BatchResult::Integer(n),
                                        Err(err) => BatchResult::Error(err.to_string()),
                                    }
                                }
                                BatchOp::ZScore { db, table, key, member } => {
                                    match block_on_io(e.zset_score(&db, &table, &key, &member)) {
                                        Ok(s) => BatchResult::OptMember(s.map(fmt_score)),
                                        Err(err) => BatchResult::Error(err.to_string()),
                                    }
                                }
                                BatchOp::ZCard { db, table, key } => {
                                    match block_on_io(e.zset_card(&db, &table, &key)) {
                                        Ok(n) => BatchResult::Integer(n),
                                        Err(err) => BatchResult::Error(err.to_string()),
                                    }
                                }
                                BatchOp::ZIncrBy { db, table, key, delta, member } => {
                                    match block_on_io(e.zset_incr(&db, &table, &key, delta, &member)) {
                                        Ok(s) => BatchResult::Double(s),
                                        Err(err) => BatchResult::Error(err.to_string()),
                                    }
                                }
                                BatchOp::ZRange { db, table, key, start, end, rev, withscores } => {
                                    match block_on_io(e.zset_range(&db, &table, &key, start, end, rev)) {
                                        Ok(rows) => BatchResult::Members(zrows_to_members(rows, withscores)),
                                        Err(err) => BatchResult::Error(err.to_string()),
                                    }
                                }
                                BatchOp::ZRangeByScore { db, table, key, min, max, withscores } => {
                                    match block_on_io(e.zset_range_by_score(&db, &table, &key, min, max)) {
                                        Ok(rows) => BatchResult::Members(zrows_to_members(rows, withscores)),
                                        Err(err) => BatchResult::Error(err.to_string()),
                                    }
                                }
                                BatchOp::ZRank { db, table, key, member, rev } => {
                                    match block_on_io(e.zset_rank(&db, &table, &key, &member, rev)) {
                                        Ok(Some(r)) => BatchResult::Integer(r),
                                        Ok(None) => BatchResult::OptMember(None),
                                        Err(err) => BatchResult::Error(err.to_string()),
                                    }
                                }
                                BatchOp::ZCount { db, table, key, min, max } => {
                                    match block_on_io(e.zset_range_by_score(&db, &table, &key, min, max)) {
                                        Ok(rows) => BatchResult::Integer(rows.len() as i64),
                                        Err(err) => BatchResult::Error(err.to_string()),
                                    }
                                }
                                BatchOp::ZMScore { db, table, key, members } => {
                                    match block_on_io(e.zset_mscore(&db, &table, &key, &members)) {
                                        Ok(scores) => BatchResult::Values(
                                            scores.into_iter().map(|s| s.map(fmt_score)).collect(),
                                        ),
                                        Err(err) => BatchResult::Error(err.to_string()),
                                    }
                                }
                                BatchOp::ZPop { db, table, key, rev, count } => {
                                    match block_on_io(e.zset_pop(&db, &table, &key, rev, count as usize)) {
                                        Ok(rows) => BatchResult::Members(zrows_to_members(rows, true)),
                                        Err(err) => BatchResult::Error(err.to_string()),
                                    }
                                }
                                BatchOp::SMisMember { db, table, key, members } => {
                                    match block_on_io(e.set_mismember(&db, &table, &key, &members)) {
                                        Ok(bs) => BatchResult::IntList(
                                            bs.into_iter().map(i64::from).collect(),
                                        ),
                                        Err(err) => BatchResult::Error(err.to_string()),
                                    }
                                }
                                BatchOp::SPopN { db, table, key, count } => {
                                    match block_on_io(e.set_pop_n(&db, &table, &key, count as usize)) {
                                        Ok(ms) => BatchResult::Members(ms),
                                        Err(err) => BatchResult::Error(err.to_string()),
                                    }
                                }
                                BatchOp::SRandCount { db, table, key, count } => {
                                    match block_on_io(e.set_rand_n(&db, &table, &key, count as usize)) {
                                        Ok(ms) => BatchResult::Members(ms),
                                        Err(err) => BatchResult::Error(err.to_string()),
                                    }
                                }
                                BatchOp::HRandField { db, table, key, count, .. } => {
                                    match block_on_io(e.hash_rand(&db, &table, &key, count as usize)) {
                                        Ok(ps) => BatchResult::Pairs(ps),
                                        Err(err) => BatchResult::Error(err.to_string()),
                                    }
                                }
                                BatchOp::LRem { db, table, key, count, val } => {
                                    match block_on_io(e.list_rem(&db, &table, &key, count, &val)) {
                                        Ok(n) => BatchResult::Integer(n),
                                        Err(err) => BatchResult::Error(err.to_string()),
                                    }
                                }
                                BatchOp::LTrim { db, table, key, start, stop } => {
                                    match block_on_io(e.list_trim(&db, &table, &key, start, stop)) {
                                        Ok(()) => BatchResult::Integer(1),
                                        Err(err) => BatchResult::Error(err.to_string()),
                                    }
                                }
                                BatchOp::LPos { db, table, key, val, rank, count } => {
                                    exec_lpos(e, &db, &table, &key, &val, rank, count)
                                }
                                BatchOp::LInsert { db, table, key, before, pivot, val } => {
                                    match block_on_io(e.list_insert(&db, &table, &key, before, &pivot, &val)) {
                                        Ok(n) => BatchResult::Integer(n),
                                        Err(err) => BatchResult::Error(err.to_string()),
                                    }
                                }
                                BatchOp::SetBit { db, table, key, offset, bit } => {
                                    exec_setbit(e, &db, &table, &key, offset, bit)
                                }
                                // ---- ⭐ Q5: SQL row 表 ----
                                BatchOp::RowPut { db, table, pk, values } => {
                                    exec_row_put(e, &db, &table, &pk, &values)
                                }
                                BatchOp::RowGet { db, table, pk } => {
                                    match block_on_io(e.row_get(&db, &table, &pk)) {
                                        Ok(v) => BatchResult::GetValue(v),
                                        Err(err) => BatchResult::Error(err.to_string()),
                                    }
                                }
                                BatchOp::RowDelete { db, table, pk } => {
                                    match block_on_io(e.row_delete(&db, &table, &pk)) {
                                        Ok(existed) => BatchResult::DeleteExisted(existed),
                                        Err(err) => BatchResult::Error(err.to_string()),
                                    }
                                }
                                BatchOp::RowUpdate { db, table, pk, sets } => {
                                    match block_on_io(e.row_update(&db, &table, &pk, &sets)) {
                                        Ok(updated) => BatchResult::DeleteExisted(updated),
                                        Err(err) => BatchResult::Error(err.to_string()),
                                    }
                                }
                                BatchOp::DropTableOp { db, table } => {
                                    match block_on_io(e.drop_table_sql(&db, &table)) {
                                        Ok(_) => BatchResult::PutOk,
                                        Err(err) => BatchResult::Error(err.to_string()),
                                    }
                                }
                                BatchOp::TableScan { db, table, limit } => {
                                    exec_table_scan(e, &db, &table, limit)
                                }
                                BatchOp::ScanFiltered { db, table, preds, proj, index_hint, key_set_hint, limit } => {
                                    exec_scan_filtered(e, &db, &table, &preds, &proj, index_hint.as_ref(), key_set_hint.as_ref(), limit)
                                }
                                BatchOp::ScanFilteredRows { db, table, index_hint, limit } => {
                                    exec_scan_filtered_rows(e, &db, &table, index_hint.as_ref(), limit)
                                }
                                BatchOp::IndexScan { db, table, iid, lo, hi, limit, with_rows } => {
                                    exec_index_scan(
                                        e, &db, &table, iid, lo.as_ref(), hi.as_ref(), limit,
                                        with_rows,
                                    )
                                }
                                BatchOp::SetSchemaOp { db, table, bytes } => {
                                    exec_set_schema(e, &db, &table, &bytes)
                                }
                                BatchOp::GetSchemaOp { db, table } => {
                                    exec_get_schema(e, &db, &table)
                                }
                            };
                            results.push(r);
                        }
                        // ⭐ WAL (F60) strict: 回复前持久化屏障 (一个 Batch 多 op
                        // 天然共享一次 fsync); reply 到达 ⇒ 已落盘
                        if e.wal_mode() == storage::wal::WalMode::Strict
                            && e.wal_needs_sync()
                            && let Err(err) = block_on_io(e.wal_barrier())
                        {
                            nlog::error!("shard", "WAL barrier failed: {err}");
                        }
                        let _ = reply.send(Ok(ShardReply::BatchResults(results)));
                        // reply_bus 支持
                        if req_id > 0 {
                            let sink_opt = reply_sink.lock().expect("reply_sink lock").clone();
                            if let Some(sink) = sink_opt {
                                sink.push_reply(req_id, shard_id as u32, Ok(ShardReply::PutOk));
                            }
                        }
                    } else {
                        let _ = reply.send(Err(ShardErrorKind::StorageError(
                            "engine not init".into(),
                        )));
                    }
                }
                req => {
                    handle_request_blocking(&engine, req, shard_id, &reply_sink);
                }
            }
        }
        // ⭐ 退出完整性: break 后置到 tasks 处理之后 — Shutdown 同轮 drain 到的
        // tasks 先执行并回复, 不静默丢弃 (break 在下方 tasks 块之后).

        // ⭐ 处理 ShardTask (从 spin loop 中一起取到的)
        if !tasks.is_empty() {
            let mut e_borrow = engine.borrow_mut();
            if let Some(e) = e_borrow.as_mut() {
                // ⭐ WAL (F60) strict 组提交: 本轮有未 sync 写时回复押后,
                // 轮末一次 fsync 后统一 push (N 个写共享一次 fsync)
                let strict = e.wal_mode() == storage::wal::WalMode::Strict;
                let mut held: Vec<(u32, crate::request::TaskResult)> = Vec::new();
                for task in tasks {
                    // ⭐ T1: 惰性建表 (已存在 = registry 纯内存查表);
                    // ⭐ F66: CatalogDump 等无表名的元 op 跳过 (table 空)
                    {
                        let (db, table, _) = task.op.locator();
                        if !table.is_empty()
                            && let Err(err) = block_on_io(e.ensure_table(db, table))
                        {
                            reply_bus_set.get(task.worker_id).push(crate::request::TaskResult {
                                conn_id: task.conn_id,
                                req_id: task.req_id,
                                group: task.group,
                                result: crate::request::BatchResult::Error(err.to_string()),
                            });
                            continue;
                        }
                    }
                    let result = exec_task_op(e, task.op);
                    // ⭐ WAL (F60) strict: 本轮已有未持久化写 → 回复押到轮末
                    // barrier 后 (读 op 在无待 sync 内容时仍直发)
                    let tr = crate::request::TaskResult {
                        conn_id: task.conn_id,
                        req_id: task.req_id,
                        group: task.group,
                        result,
                    };
                    if strict && e.wal_needs_sync() {
                        held.push((task.worker_id, tr));
                    } else {
                        reply_bus_set.get(task.worker_id).push(tr);
                    }
                }
                if !held.is_empty() {
                    if let Err(err) = block_on_io(e.wal_barrier()) {
                        nlog::error!("shard", "WAL group-commit barrier failed: {err}");
                    }
                    for (wid, tr) in held {
                        reply_bus_set.get(wid).push(tr);
                    }
                }
            }
        }

        // ⭐ Shutdown: 同轮 tasks 已处理完, 退出主循环 (随后 engine.close 做最终 flush)
        if should_shutdown {
            break;
        }

        // ⭐ 每轮循环末尾: 驱动异步落盘 (收割/spawn/周期检查/drive).
        // 磁盘 IO 在协程里并发进行, 不阻塞下一轮请求处理.
        drive_async_flush(&engine, &rt, &flush_done);
    }

    // ⭐ 退出完整性: 先排空异步落盘 backlog, 再 final close (flush 契约).
    drain_async_flush(&engine, &rt, &flush_done);

    // ⭐ 退出完整性: final close = drive_write_queue + flush (nowchunks → meta).
    // 用完成标志等待 (非固定预算), 保证 flush 真正做完才退出线程.
    if let Some(e) = engine.borrow_mut().take() {
        let done = std::rc::Rc::new(std::cell::RefCell::new(false));
        let done2 = done.clone();
        let close_fut = Box::pin(async move {
            if let Err(err) = e.close().await {
                nlog::error!("shard", "shard-{shard_id} close flush failed: {err}");
            }
            *done2.borrow_mut() = true;
        });
        scheduler::spawn_on(&rt, close_fut);
        while !*done.borrow() {
            rt.clone().drive_until_idle(1000);
        }
        nlog::info!("shard", "shard-{shard_id} closed (final flush done)");
    }
}

/// **inline 处理请求**: 同步阻塞当前 shard 线程, 跑 engine async API.
pub(crate) fn handle_request_blocking(
    engine: &std::rc::Rc<std::cell::RefCell<Option<StorageEngine>>>,
    req: ShardRequest,
    shard_id: usize,
    reply_sink: &StdMutex<Option<Arc<dyn ReplySink>>>,
) {
    // 从 req 中取出 reply 句柄
    let reply = match &req {
        ShardRequest::Put { reply, .. }
        | ShardRequest::Get { reply, .. }
        | ShardRequest::Delete { reply, .. }
        | ShardRequest::CreateTable { reply, .. }
        | ShardRequest::CreateDb { reply, .. }
        | ShardRequest::ListDbsWithIds { reply }
        | ShardRequest::SetSchema { reply, .. }
        | ShardRequest::PrepareCreateDb { reply, .. }
        | ShardRequest::CommitCreateDb { reply, .. }
        | ShardRequest::AbortCreateDb { reply, .. }
        | ShardRequest::PrepareCreateTable { reply, .. }
        | ShardRequest::CommitCreateTable { reply, .. }
        | ShardRequest::AbortCreateTable { reply, .. }
        | ShardRequest::Shutdown { reply }
        | ShardRequest::Flush { reply }
        | ShardRequest::Batch { reply, .. } => reply.clone(),
    };

    // 辅助: 同时写 reply 和 (如启用) reply_bus
    //
    // ⭐ 顺序修复 (2026-07-26): 先推 sink 再 reply.send —— reply.send 会唤醒
    // client 线程, 若 sink 后推, client 醒来立即读 sink 可能读到缺条目
    // (全量并行测试下 integration_reply_bus 偶发 1/2 失败).
    let send_reply = |resp: ShardResponse, req_id: u64| {
        // 1. 网络 reply bus (req_id > 0 时) — 先于唤醒
        if req_id > 0 {
            let sink_opt = reply_sink.lock().expect("reply_sink lock").clone();
            if let Some(sink) = sink_opt {
                sink.push_reply(req_id, shard_id as u32, resp.clone());
            }
        }
        // 2. 旧 channel reply (兼容) — reply.send 消耗 self, 所以 clone
        let _ = reply.clone().send(resp);
    };

    let mut e_borrow = engine.borrow_mut();
    let e = match e_borrow.as_mut() {
        Some(e) => e,
        None => {
            send_reply(
                Err(ShardErrorKind::StorageError("engine not init".into())),
                0,
            );
            return;
        }
    };
    match req {
        ShardRequest::Put {
            db,
            table,
            key,
            val,
            req_id,
            ..
        } => {
            let r = block_on_io(e.table_put(&db, &table, &key, &val));
            // ⭐ WAL (F60) strict: 同步慢路径也保证回复前持久化
            if e.wal_mode() == storage::wal::WalMode::Strict && e.wal_needs_sync() {
                let _ = block_on_io(e.wal_barrier());
            }
            send_reply(
                match r {
                    Ok(_) => Ok(ShardReply::PutOk),
                    Err(err) => Err(ShardErrorKind::from_storage_display(&err)),
                },
                req_id,
            );
        }
        ShardRequest::Get {
            db,
            table,
            key,
            req_id,
            ..
        } => {
            let r = block_on_io(e.table_get(&db, &table, &key));
            send_reply(
                match r {
                    Ok(v) => Ok(ShardReply::GetValue(v)),
                    Err(err) => Err(ShardErrorKind::from_storage_display(&err)),
                },
                req_id,
            );
        }
        ShardRequest::Delete {
            db,
            table,
            key,
            req_id,
            ..
        } => {
            let r = block_on_io(e.table_delete(&db, &table, &key));
            if e.wal_mode() == storage::wal::WalMode::Strict && e.wal_needs_sync() {
                let _ = block_on_io(e.wal_barrier());
            }
            send_reply(
                match r {
                    Ok(b) => Ok(ShardReply::DeleteExisted(b)),
                    Err(err) => Err(ShardErrorKind::from_storage_display(&err)),
                },
                req_id,
            );
        }
        ShardRequest::CreateTable { db, table, .. } => {
            let r = block_on_io(e.create_table(&db, &table));
            // ⭐ WAL (F60): DDL 不进 WAL (catalog 页写), 立即全量落盘保持久
            // (低频; 重放时表必存在)
            if r.is_ok() && e.wal_mode() != storage::wal::WalMode::Off {
                let _ = block_on_io(e.flush());
            }
            let _ = reply.send(match r {
                Ok(vpid) => Ok(ShardReply::CreateTableOk(vpid)),
                Err(err) => Err(ShardErrorKind::from_storage_display(&err)),
            });
        }
        ShardRequest::CreateDb { db, .. } => {
            let r = block_on_io(e.create_db(&db));
            if r.is_ok() && e.wal_mode() != storage::wal::WalMode::Off {
                let _ = block_on_io(e.flush());
            }
            let _ = reply.send(match r {
                Ok(_) => Ok(ShardReply::CreateDbOk),
                Err(err) => Err(ShardErrorKind::from_storage_display(&err)),
            });
        }
        ShardRequest::ListDbsWithIds { .. } => {
            // ⭐ D2 (分库): resolver (id, name) 全表 — DbDirView 初始化/刷新
            let _ = reply.send(Ok(ShardReply::DbList(e.list_dbs_with_ids())));
        }
        ShardRequest::SetSchema { db, table, bytes, .. } => {
            // ⭐ Q5: 反序列化校验后落 [$] 行 + 常驻镜像 (幂等)
            let r = storage::schema::TableSchema::decode(&bytes)
                .map_err(|err| ShardErrorKind::StorageError(err.to_string()))
                .and_then(|schema| {
                    block_on_io(e.set_schema(&db, &table, &schema))
                        .map_err(|err| ShardErrorKind::from_storage_display(&err))
                });
            let _ = reply.send(match r {
                Ok(()) => Ok(ShardReply::PutOk),
                Err(kind) => Err(kind),
            });
        }
        ShardRequest::PrepareCreateDb { db, .. } => {
            let r = block_on_io(e.create_db(&db));
            // ⭐ WAL (F60): DDL 不进 WAL → 立即落盘 (2PC 生产建库路径)
            if r.is_ok() && e.wal_mode() != storage::wal::WalMode::Off {
                let _ = block_on_io(e.flush());
            }
            let _ = reply.send(match r {
                Ok(_) => Ok(ShardReply::PrepareOk),
                Err(err) => Err(ShardErrorKind::from_storage_display(&err)),
            });
        }
        ShardRequest::CommitCreateDb { .. } => {
            let _ = reply.send(Ok(ShardReply::CommitOk));
        }
        ShardRequest::AbortCreateDb { db, .. } => {
            let _ = block_on_io(e.drop_db(&db));
            let _ = reply.send(Ok(ShardReply::AbortOk));
        }
        ShardRequest::PrepareCreateTable { db, table, .. } => {
            let r = block_on_io(e.create_table(&db, &table));
            // ⭐ WAL (F60): DDL 不进 WAL → 立即落盘 (2PC 生产建表路径)
            if r.is_ok() && e.wal_mode() != storage::wal::WalMode::Off {
                let _ = block_on_io(e.flush());
            }
            let _ = reply.send(match r {
                Ok(_) => Ok(ShardReply::PrepareOk),
                Err(err) => Err(ShardErrorKind::from_storage_display(&err)),
            });
        }
        ShardRequest::CommitCreateTable { .. } => {
            let _ = reply.send(Ok(ShardReply::CommitOk));
        }
        ShardRequest::AbortCreateTable { db, table, .. } => {
            let _ = block_on_io(e.drop_table(&db, &table));
            let _ = reply.send(Ok(ShardReply::AbortOk));
        }
        ShardRequest::Shutdown { .. } => {
            let _ = reply.send(Ok(ShardReply::ShutdownOk));
        }
        ShardRequest::Flush { .. } => {
            let _ = reply.send(Err(ShardErrorKind::StorageError(
                "flush should be handled in main loop".into(),
            )));
        }
        ShardRequest::Batch { .. } => {
            let _ = reply.send(Err(ShardErrorKind::StorageError(
                "batch should be handled in main loop".into(),
            )));
        }
    }
}
