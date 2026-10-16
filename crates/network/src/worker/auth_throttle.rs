//! ⭐ 审计 P2 (2026-10-17): 认证失败限速 (按逻辑集群共享, 跨 worker / 跨门面).
//!
//! ## 旧实现的问题
//!
//! 认证失败**无限次**:
//! - MySQL / PG: 每次失败关连接 (一次 TCP 连接只能猜一次), 但攻击者可以
//!   全速重连重试 — 等同于无速率限制的暴力破解;
//! - RESP (AUTH): 失败只回错误、**不关连接**, 一次连接可以无限猜.
//!
//! ## 机制
//!
//! 滑动窗口 + 锁定:
//! - `AUTH_WINDOW_SECS` 窗口内失败次数达到 `AUTH_MAX_FAILS` → 锁定
//!   `AUTH_LOCKOUT_SECS` 秒;
//! - 锁定期间任何认证请求**直接拒绝** (连口令都不比对), 返回剩余秒数;
//! - 认证成功清零计数 (正常用户不受累积失败影响).
//!
//! ## 作用域
//!
//! 实例挂在进程级共享的 `SqlSharedRoutes` 上 (同一数据集群的全部门面/worker
//! 共用一个) — 攻击者换连接/换门面都绕不过同一把闸; 不同集群 (测试各起一个)
//! 互相独立, 用例之间不会连锁锁定.
//!
//! 这是粗粒度防护 (不按来源 IP 细分): 对"单机 + 单口令"的部署已经能把在线
//! 暴力破解的成本抬高几个数量级; 按 IP 的精细限流留作后续项 (需要 acceptor
//! 侧连接来源状态, 见审计报告遗留清单).

use std::sync::Mutex;
use std::time::{Duration, Instant};

/// 窗口内允许的最大失败次数.
pub const AUTH_MAX_FAILS: u32 = 10;
/// 滑动窗口长度 (秒).
pub const AUTH_WINDOW_SECS: u64 = 60;
/// 触发锁定后的锁定时长 (秒).
pub const AUTH_LOCKOUT_SECS: u64 = 30;
/// 单连接允许的连续失败次数 (超过即断开, 防单连接内无限猜).
pub const MAX_FAILS_PER_CONN: u32 = 5;

/// 单个集群的限流状态.
#[derive(Debug)]
pub struct AuthThrottle {
    state: Mutex<ThrottleState>,
}

#[derive(Debug)]
struct ThrottleState {
    /// 当前窗口内的失败计数.
    fails: u32,
    /// 窗口起点.
    window_start: Instant,
    /// 锁定截止时刻 (None = 未锁定).
    locked_until: Option<Instant>,
}

impl Default for AuthThrottle {
    fn default() -> Self {
        Self {
            state: Mutex::new(ThrottleState {
                fails: 0,
                window_start: Instant::now(),
                locked_until: None,
            }),
        }
    }
}

impl AuthThrottle {
    /// 认证前检查: 处于锁定期 → `Err(剩余秒数)`.
    pub fn check(&self) -> Result<(), u64> {
        let mut st = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(until) = st.locked_until {
            let now = Instant::now();
            if now < until {
                return Err((until - now).as_secs().max(1));
            }
            st.locked_until = None; // 锁定已过期
        }
        Ok(())
    }

    /// 记录一次认证失败. 返回 `true` 表示本次失败**触发**了锁定.
    pub fn record_failure(&self) -> bool {
        let mut st = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let now = Instant::now();
        if now.duration_since(st.window_start) > Duration::from_secs(AUTH_WINDOW_SECS) {
            st.fails = 0;
            st.window_start = now;
        }
        st.fails += 1;
        if st.fails >= AUTH_MAX_FAILS {
            st.locked_until = Some(now + Duration::from_secs(AUTH_LOCKOUT_SECS));
            st.fails = 0;
            st.window_start = now;
            true
        } else {
            false
        }
    }

    /// 认证成功: 清零并解除锁定 (正常用户不被历史失败拖累; 口令正确本身
    /// 就是合法性的证明).
    pub fn record_success(&self) {
        let mut st = self.state.lock().unwrap_or_else(|e| e.into_inner());
        st.fails = 0;
        st.window_start = Instant::now();
        st.locked_until = None;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lockout_constants_are_sane() {
        const { assert!(AUTH_MAX_FAILS > 0 && AUTH_WINDOW_SECS > 0 && AUTH_LOCKOUT_SECS > 0) };
        const { assert!(MAX_FAILS_PER_CONN < AUTH_MAX_FAILS) };
    }

    #[test]
    fn failures_then_lockout_then_success_resets() {
        let t = AuthThrottle::default();
        assert!(t.check().is_ok(), "初始不应锁定");
        let mut locked = false;
        for _ in 0..AUTH_MAX_FAILS {
            if t.record_failure() {
                locked = true;
            }
        }
        assert!(locked, "达到阈值必须进入锁定");
        assert!(t.check().is_err(), "锁定期间必须拒绝");
        // 成功清零后立即可用 (模拟锁定过期后的正常登录)
        t.record_success();
        assert!(t.check().is_ok());
    }

    /// 每次失败都计数, 但未达阈值不锁定.
    #[test]
    fn below_threshold_not_locked() {
        let t = AuthThrottle::default();
        for _ in 0..(AUTH_MAX_FAILS - 1) {
            assert!(!t.record_failure());
        }
        assert!(t.check().is_ok(), "未达阈值不得锁定");
    }
}
