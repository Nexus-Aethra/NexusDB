#[test]
fn wake_pushes_slot_id_to_ready_queue() {
    use scheduler::{make_waker_for_test, new_ready_queue};

    let ready = new_ready_queue();
    let waker = make_waker_for_test(7, &ready);

    // 第一次 wake (consume)
    waker.wake_by_ref();
    // 第二次用 wake_by_ref
    waker.wake_by_ref();
    assert!(!ready.borrow().is_empty());

    let mut drained = std::mem::take(&mut *ready.borrow_mut());
    assert_eq!(drained.pop_front(), Some(7));

    // 再 wake 一次仍然能拿到 (幂等)
    waker.wake_by_ref();
    let mut drained = std::mem::take(&mut *ready.borrow_mut());
    assert_eq!(drained.pop_front(), Some(7));
}

#[test]
fn drain_returns_empty_when_queue_is_empty() {
    use scheduler::new_ready_queue;
    let ready = new_ready_queue();
    let drained = std::mem::take(&mut *ready.borrow_mut());
    assert!(drained.is_empty());
    assert!(ready.borrow().is_empty());
}
