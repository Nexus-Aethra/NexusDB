#[test]
fn fresh_pool_starts_empty() {
    let pool = scheduler::Pool::new();
    assert_eq!(pool.in_use(), 0);
}

#[test]
fn free_path_returns_same_slot_after_release() {
    let mut pool = scheduler::Pool::new();
    let a = pool.acquire().unwrap();
    pool.release(a);
    let b = pool.acquire().unwrap();
    assert_eq!(a, b, "released slot should be re-acquired next");
}

#[test]
fn capacity_exhaustion_never_reuses_live_slot() {
    // 满载必须显式失败；复用活跃 slot 会覆盖 future 并让 IO completion 串任务。
    let mut pool = scheduler::Pool::new();
    let mut got = Vec::with_capacity(scheduler::POOL_SIZE);
    for _ in 0..scheduler::POOL_SIZE {
        got.push(pool.acquire().expect("capacity not exhausted yet"));
    }
    let mut seen = std::collections::HashSet::new();
    let mut first_pass_unique = 0;
    for &s in &got[..1024] {
        if seen.insert(s) {
            first_pass_unique += 1;
        }
    }
    assert_eq!(
        first_pass_unique, 1024,
        "first 1024 acquires must all be distinct"
    );
    assert_eq!(
        pool.acquire(),
        None,
        "capacity exhaustion must not reuse an active slot"
    );
    assert_eq!(pool.in_use(), scheduler::POOL_SIZE);
    assert_eq!(pool.available(), 0);

    pool.release(got[0]);
    assert_eq!(pool.acquire(), Some(got[0]));
}
