use std::future::Future;
use std::sync::Arc;
use std::task::{Wake, Waker};

struct CountingWaker(Arc<std::sync::atomic::AtomicUsize>);
impl Wake for CountingWaker {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }
}
fn make_counting_waker(c: Arc<std::sync::atomic::AtomicUsize>) -> Waker {
    Arc::new(CountingWaker(c)).into()
}

#[test]
fn handle_returns_pending_then_ready_after_complete() {
    use scheduler::test_support;

    let handle = test_support::make_pending_handle::<i32>();
    let waker_count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let waker = make_counting_waker(waker_count.clone());
    let mut cx = std::task::Context::from_waker(&waker);

    // poll #1: pending, 注册 waker
    let mut pinned = Box::pin(handle);
    assert!(matches!(
        Future::poll(pinned.as_mut(), &mut cx),
        std::task::Poll::Pending
    ));
    assert_eq!(waker_count.load(std::sync::atomic::Ordering::SeqCst), 0);

    // 触发 complete (用 pinned 内部的 handle)
    test_support::complete(pinned.as_ref().get_ref(), Ok(7));
    assert_eq!(waker_count.load(std::sync::atomic::Ordering::SeqCst), 1);

    // poll #2: ready
    assert!(matches!(
        Future::poll(pinned.as_mut(), &mut cx),
        std::task::Poll::Ready(Ok(7))
    ));
}

#[test]
fn clone_then_drop_one_handle_keeps_result_available() {
    use scheduler::test_support;

    let h1 = test_support::make_pending_handle::<i32>();
    let h2 = h1.clone();
    let waker_count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let waker = make_counting_waker(waker_count);
    let mut cx = std::task::Context::from_waker(&waker);

    let mut pinned1 = Box::pin(h1);
    assert!(matches!(
        Future::poll(pinned1.as_mut(), &mut cx),
        std::task::Poll::Pending
    ));

    // drop 一个 clone, inner 仍活着 (另一 clone 持着 Arc)
    drop(h2);

    // 通过 pinned 里的 handle complete
    test_support::complete(pinned1.as_ref().get_ref(), Ok(99));
    assert!(matches!(
        Future::poll(pinned1.as_mut(), &mut cx),
        std::task::Poll::Ready(Ok(99))
    ));
}
