//! T1 占位测试: 验证 storage crate 能被 workspace 编译 + 核心常量正确.

#[test]
fn storage_crate_compiles_and_exports_constants() {
    // 类型 + 常量 + 函数 (T1 必暴露)
    let _chunk: usize = storage::CHUNK_SIZE;
    let _block: usize = storage::BLOCK_SIZE;
    let _page: usize = storage::PAGE_SIZE;
    let _pages_per_chunk: usize = storage::PAGES_PER_CHUNK;
    let _mate_cache: usize = storage::MATE_CACHE_SIZE;
    let _index: usize = storage::INDEX_SIZE;
    let _index_count: usize = storage::INDEX_COUNT;
    let _slots: usize = storage::SLOTS_PER_INDEX;
}

#[test]
fn pid_location_is_8_bytes_via_storage_exports() {
    use storage::PidLocation;
    assert_eq!(
        std::mem::size_of::<PidLocation>(),
        8,
        "MetaCache 一项 8B, PidLocation 必须 packed 8B"
    );
    assert_eq!(std::mem::align_of::<PidLocation>(), 1);
}

#[test]
fn pid_to_offset_basic_roundtrip() {
    use storage::{PID_ALIVE, PidLocation, pid_to_offset};

    let pid = PidLocation {
        file_id: 0,
        chunk_idx: 5,
        page_idx: 3,
        flags: PID_ALIVE,
    };
    let off = pid_to_offset(&pid);
    // 5 chunks (5 MB) + 3 pages (48KB) = 5243280
    assert_eq!(
        off,
        5 * storage::CHUNK_SIZE as u64 + 3 * storage::PAGE_SIZE as u64
    );
}

#[test]
fn pid_to_offset_block_separator() {
    use storage::{PID_ALIVE, PidLocation, pid_to_offset};

    let block0 = PidLocation {
        file_id: 0,
        chunk_idx: 0,
        page_idx: 0,
        flags: PID_ALIVE,
    };
    let block1 = PidLocation {
        file_id: 1,
        chunk_idx: 0,
        page_idx: 0,
        flags: PID_ALIVE,
    };
    assert_eq!(
        pid_to_offset(&block1) - pid_to_offset(&block0),
        storage::BLOCK_SIZE as u64
    );
}

#[test]
fn offset_to_pid_default_alive_flag() {
    use storage::{PID_ALIVE, offset_to_pid};
    let pid = offset_to_pid(7, 3, 11);
    assert_eq!(pid.file_id(), 7);
    assert_eq!(pid.chunk_idx(), 3);
    assert_eq!(pid.page_idx(), 11);
    assert_eq!(pid.flags(), PID_ALIVE);
}
