//! ⭐ 回归: pid 的 page_idx 越界 (>= PAGES_PER_CHUNK) 必须被 checked 转换
//! 拒绝 — 不得静默 `as u8` 截断寻址错误页 / 切片越界 panic (审计 P2).

use storage::test_support::PidLocation;
use storage::types::pid_page_idx_checked;

#[test]
fn pid_page_idx_checked_bounds() {
    // 合法: chunk 内 0..64
    for p in [0u16, 1, 32, 63] {
        let mut b = [0u8; 8];
        b[5..7].copy_from_slice(&p.to_le_bytes());
        b[7] = 0x01; // ALIVE
        assert_eq!(
            pid_page_idx_checked(&PidLocation::from_bytes(&b)),
            Some(p as u8),
            "page_idx={p} 应合法"
        );
    }
    // 越界: >= 64 (u16 字段能表达, 但 chunk 只有 64 个页)
    for p in [64u16, 200, 255, 256, 1000] {
        let mut b = [0u8; 8];
        b[5..7].copy_from_slice(&p.to_le_bytes());
        b[7] = 0x01;
        assert_eq!(
            pid_page_idx_checked(&PidLocation::from_bytes(&b)),
            None,
            "page_idx={p} 必须被拒绝 (旧实现 as u8 截断: {} → 错页/panic)",
            p as u8
        );
    }
}
