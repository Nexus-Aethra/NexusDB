use std::task::Waker;

fn noop() -> Waker {
    Waker::noop().clone()
}

#[test]
fn register_then_take_returns_state() {
    let mut reg = scheduler::IoRegistry::new();
    let ud = reg.register(7, noop());
    let taken = reg.take(ud).expect("must be present");
    assert_eq!(taken.slot_id, 7);
    assert!(reg.take(ud).is_none(), "take is consuming");
}

#[test]
fn cancel_removes_entry() {
    let mut reg = scheduler::IoRegistry::new();
    let ud = reg.register(1, noop());
    reg.cancel(ud);
    assert!(reg.take(ud).is_none());
}

#[test]
fn cancel_slot_removes_all_for_that_slot() {
    let mut reg = scheduler::IoRegistry::new();
    let a = reg.register(5, noop());
    let b = reg.register(5, noop());
    let c = reg.register(6, noop());
    reg.cancel_slot(5);
    assert!(reg.take(a).is_none());
    assert!(reg.take(b).is_none());
    assert!(reg.take(c).is_some(), "other slot untouched");
}

#[test]
fn refresh_waker_replaces_existing() {
    let mut reg = scheduler::IoRegistry::new();
    let ud = reg.register(1, noop());
    reg.refresh_waker(ud, noop());
    assert!(reg.take(ud).is_some());
}

#[test]
fn user_data_is_unique_and_monotonic() {
    let mut reg = scheduler::IoRegistry::new();
    let a = reg.register(1, noop());
    reg.take(a);
    let b = reg.register(1, noop());
    assert_ne!(a, b, "never reuse — even across take/re-register");
    assert!(b > a);
}

#[test]
fn stats_distinguish_completion_cancellation_and_unknown_cqe() {
    let mut reg = scheduler::IoRegistry::new();
    let complete = reg.register(1, noop());
    let cancel = reg.register(2, noop());
    assert!(reg.mark_completed(complete, 7));
    assert!(reg.cancel(cancel));
    assert!(!reg.mark_completed(999, 0));
    reg.record_unknown_cqe();

    assert_eq!(
        reg.stats(),
        scheduler::IoRegistryStats {
            registered: 2,
            completed: 1,
            cancelled: 1,
            unknown_cqe: 1,
        }
    );
}
