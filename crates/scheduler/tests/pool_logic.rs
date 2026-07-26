#[test]
fn fresh_pool_starts_empty() {
    let pool = scheduler::Pool::new();
    assert_eq!(pool.in_use(), 0);
}

#[test]
fn free_path_returns_same_slot_after_release() {
    let mut pool = scheduler::Pool::new();
    let a = pool.acquire();
    pool.release(a);
    let b = pool.acquire();
    assert_eq!(a, b, "released slot should be re-acquired next");
}

#[test]
fn rr_path_used_after_free_is_exhausted() {
    // POOL_SIZE = 1024; 第一次 1024 个 acquire 应该都不同, 第 1025 个 RR 复用 got[0]
    let mut pool = scheduler::Pool::new();
    let mut got = Vec::with_capacity(1025);
    for _ in 0..1025 {
        got.push(pool.acquire());
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
        got[1024], got[0],
        "RR wrap-around should reuse slot 0 (since free list is empty)"
    );
}
