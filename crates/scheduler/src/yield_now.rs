//! `yield_now`: 把当前协程让出一次, 调度器下一轮再次 poll 它.
//!
//! 用于模拟"非阻塞内存计算"步骤: 让协程在多个 await 点之间穿插执行,
//! 验证调度器公平性 (其他 task 能跑).

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

/// 让出当前协程. 调度器下一轮 (或更晚) 会重新 poll 它.
pub async fn yield_now() {
    YieldNow { yielded: false }.await
}

struct YieldNow {
    yielded: bool,
}

impl Future for YieldNow {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if self.yielded {
            Poll::Ready(())
        } else {
            self.yielded = true;
            // wake 自己, 让调度器下一轮再 poll.
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    }
}
