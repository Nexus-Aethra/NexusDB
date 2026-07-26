//! T10 TableDirectory: 每 db 一棵 BTree (table_name → table_root_vpid).
//!
//! ## 设计 (来自 plan §Task 10)
//!
//! - **每个 db 独立 BTree**: db_name 决定 TableDirectory BTree 的 root vpid (从 MetaPage 查)
//! - **第一版单 leaf page 实现**: table 数量 < ~200 时单页足够
//! - **复用 page crate leaf API**: `leaf_new / leaf_insert / leaf_get / leaf_delete`
//! - **value 是 u64 LE**: 8 字节小端, 表示 table 的 BTree root vpid
//! - **空 TableDirectory BTree 也要占 1 个 vpid**: 至少一个 leaf page (含哨兵)
//!
//! ## PageWriteBatch 协议 (强制)
//!
//! 所有修改 (create_table / drop_table) **必须**走 `Pager::write_page`,
//! 内部用 `PageWriteBatch::submit` 一次性提交, 保证:
//! - MetaCache dirty 标记 → flush 时持久化
//! - chunk_list 旧 page 失效但保留 (COW 友好, LRU 自然驱逐)
//! - crash 恢复: recover 扫描 .block 时只看到最新 pid 的内容
//!
//! ## 借用模型 (修复 aliasing UB)
//!
//! `TableDirectory` **不持有** Pager 引用 / 指针. 所有需要 Pager 的方法
//! 都接收 `&mut Pager` 作为参数, 由 borrow checker 在调用点静态保证
//! aliasing 安全. `create_new` / `open` 也只是把 `root_vpid` 存下来, 不存
//! `Pager` 引用.
//!
//! **单线程约束 (PhantomData<*mut Pager> 强制 !Send / !Sync)**:
//! 即使 `TableDirectory` 不持有 raw pointer, 仍用 `PhantomData<*mut Pager>`
//! 让 Rust 编译器推导 TableDirectory 是 !Send / !Sync, 防止跨线程误用.

use std::collections::BTreeMap;
use std::io;
use std::marker::PhantomData;

use page::{
    ItemKind, PAGE_HEADER_SIZE, PAGE_SIZE, PageType, Vpid, decode_item, leaf_new, page_check_magic,
    page_init_header, page_set_vpid, page_type,
};

use crate::btree::BTreeError;
use crate::pager::Pager;

// =====================================================================
// TableDirectoryError
// =====================================================================

/// TableDirectory 操作错误类型.
#[derive(Debug, thiserror::Error)]
pub enum TableDirError {
    #[error("table already exists: {0}")]
    AlreadyExists(String),
    #[error("table not found: {0}")]
    NotFound(String),
    #[error("non-utf8 table name")]
    NonUtf8Name,
    #[error("table value should be 8 bytes, got {0}")]
    BadValueSize(usize),
    #[error("table directory page bad magic")]
    BadMagic,
    #[error("table directory page invalid vpid: expected {expected}, got {got}")]
    InvalidVpid { expected: u64, got: u64 },
    #[error("table directory page invalid page type: expected Leaf/Internal, got {got:?}")]
    InvalidPageType { got: PageType },
    #[error("page decode error: {0}")]
    PageDecode(String),
    #[error("page operation error: {0}")]
    PageOp(String),
    #[error("btree error: {0}")]
    BTree(#[from] BTreeError),
    #[error("io error: {0}")]
    Io(#[from] io::Error),
}

impl From<page::PageError> for TableDirError {
    fn from(e: page::PageError) -> Self {
        TableDirError::PageOp(format!("{:?}", e))
    }
}

// =====================================================================
// TableDirectory
// =====================================================================

/// TableDirectory BTree: 持久化在 `root_vpid` 指向的 leaf page.
///
/// **生命周期**:
/// - `create_new`: 分配新 vpid, 写一个空 leaf page (含哨兵), 返回 TableDirectory
/// - `open`: 用已知的 root_vpid 打开 (caller 负责 vpid 已分配且映射到合法 leaf page)
/// - `create_table / drop_table`: 修改 leaf, 走 Pager::write_page (PageWriteBatch 路径)
/// - `get_table / list_tables`: 读 leaf, 不修改
///
/// **不使用内存镜像**: 每次操作都从 Pager 读最新 page 字节. 这是最简单一致的实现,
/// 性能上 leaf page < 200 entries 时光走 pager read 不构成瓶颈 (page 在 nowchunks 或
/// chunk_list 缓存里). 后续如需优化可加 BTreeMap 镜像 (类似 MetaPage), 但与 MetaPage
/// 不同, TableDirectory 数量大 (每 db 一份) 镜像成本不值得.
///
/// **不持有 Pager (修复 aliasing UB)**: 所有方法都接收 `&mut Pager` 参数.
pub struct TableDirectory {
    /// TableDirectory BTree 的 root vpid (对应一个 leaf page).
    pub root_vpid: Vpid,
    /// PhantomData 让 TableDirectory 仍然 !Send / !Sync.
    /// 即使不实际持有 *mut Pager, 也阻止跨线程传递 (因为我们设计上 per-shard 单线程).
    _not_send_sync: PhantomData<*mut Pager>,
}

// =====================================================================
// 公开 API
// =====================================================================

impl TableDirectory {
    /// 创建一个新的 TableDirectory, 分配一个 vpid 作为 root, 写空 leaf page.
    ///
    /// **流程**:
    /// 1. 构造一个空 leaf page (`leaf_new` 写 page header, page_type=Leaf, free_off=PAGE_HEADER_SIZE)
    /// 2. `pager.create(page)` 内部会:
    ///    - 分配新 vpid
    ///    - 把 page 写到 nowchunks, 覆盖 [0..0x28] 的 page header 写入新 vpid + magic + page_type
    /// 3. 返回 TableDirectory { root_vpid: 新 vpid }
    ///
    /// **关键修复 (2026-07-19)**: 不再保存 `pager as *mut Pager` 到 self, 避免
    /// 与 caller 持有的 `&mut Pager` 形成 aliasing UB.
    pub async fn create_new(pager: &mut Pager) -> Result<Self, TableDirError> {
        // 1. 构造空 leaf page (page_type=Leaf, key_count=0, free_off=PAGE_HEADER_SIZE)
        //    第一次 leaf_insert 时 init_sentinel 会自动触发.
        let page = leaf_new();

        // 2. 分配 vpid + 写 page (PageWriteBatch 内部)
        let vpid = pager.create(Box::new(page)).await?;

        // 3. 返回 (不存 Pager 引用)
        Ok(Self {
            root_vpid: vpid,
            _not_send_sync: PhantomData,
        })
    }

    /// 打开已存在的 TableDirectory.
    ///
    /// **验证**: 读 root page, 校验 magic + vpid + page_type (Leaf 或 Internal, T15 升级).
    pub async fn open(root_vpid: Vpid, pager: &mut Pager) -> Result<Self, TableDirError> {
        // 读 root page 验证 (Pager::read 走 nowchunks / chunk_list / disk)
        let page = pager.read(root_vpid).await?;
        page_check_magic(&*page).map_err(|_| TableDirError::BadMagic)?;

        let v = page::page_vpid(&*page);
        if v != root_vpid {
            return Err(TableDirError::InvalidVpid {
                expected: root_vpid,
                got: v,
            });
        }

        // T15 升级: root 可能是 Leaf 或 Internal
        let pt = page_type(&*page);
        if pt != PageType::Leaf && pt != PageType::Internal {
            return Err(TableDirError::InvalidPageType { got: pt });
        }

        Ok(Self {
            root_vpid,
            _not_send_sync: PhantomData,
        })
    }

    /// 创建表: 分配新 vpid 作为 table BTree root, 插入 mapping, 写回.
    /// 返回新 table 的 root vpid.
    ///
    /// **T15 升级**: 走 `crate::btree::btree_insert` (多层 BTree + split 传播).
    /// 不再假设 TableDirectory 是单 leaf page.
    ///
    /// **强制 PageWriteBatch**: btree_insert 内部用 PageWriteBatch 一次提交.
    ///
    /// **⭐ T15 修复**: 如果 `btree_insert` 返回 `Some(new_root)` (说明 TableDirectory
    /// BTree 自己 root split 了, 树高+1), 必须更新 `self.root_vpid` 让后续操作
    /// 走新 root. 否则后续 create_table / drop_table 还在用旧 root, 路径断裂.
    pub async fn create_table(
        &mut self,
        pager: &mut Pager,
        name: &str,
    ) -> Result<Vpid, TableDirError> {
        use crate::btree::btree_insert;

        // 1. 检查已存在
        if self.get_table(pager, name).await?.is_some() {
            return Err(TableDirError::AlreadyExists(name.to_string()));
        }

        // 2. 分配新 vpid 作为 table BTree root
        let table_root_vpid = pager.create(Box::new(leaf_new())).await?;

        // 3. 走 btree_insert 插入 (name, root_vpid.to_le_bytes())
        //    可能触发 split + 多层 BTree 创建
        if let Some(new_root) = btree_insert(
            pager,
            self.root_vpid,
            name.as_bytes(),
            &table_root_vpid.to_le_bytes(),
        )
        .await?
        {
            // 4. ⭐ root split: 更新 self.root_vpid
            self.root_vpid = new_root;
        }

        Ok(table_root_vpid)
    }

    /// 删除表. 返回 true 表示存在并删除, false 表示不存在.
    ///
    /// **T15 升级**: 走 `crate::btree::btree_delete`.
    ///
    /// **⭐ T15 修复**: 同 create_table, root 变了要更新 `self.root_vpid`.
    /// (虽然当前 btree_delete 暂不实现 merge, root 不会变, 留作防御性.)
    ///
    /// **注意**: table BTree root vpid 的 page 仍留在 nowchunks/chunk_list
    /// (孤儿 page, 不再被引用), 等待 LRU 驱逐或 chunk 满时回收. 这是 vpid
    /// 永不重用设计 + COW 写路径的天然结果.
    pub async fn drop_table(
        &mut self,
        pager: &mut Pager,
        name: &str,
    ) -> Result<bool, TableDirError> {
        use crate::btree::btree_delete;
        let existed = btree_delete(pager, self.root_vpid, name.as_bytes()).await?;
        // drop_table 暂不触发 root split (no merge), 但仍按防御性更新
        // (将来实现 merge 时如果触发 root 重建, 这里会自动跟踪)
        // 实际上 btree_delete 当前不返回 new_root, 所以这段是占位.
        Ok(existed)
    }

    /// 查表. 返回 Some(vpid) 表示存在, None 表示不存在.
    ///
    /// **T15 升级**: 走 `crate::btree::btree_lookup` (跨多层 BTree).
    pub async fn get_table(
        &self,
        pager: &mut Pager,
        name: &str,
    ) -> Result<Option<Vpid>, TableDirError> {
        use crate::btree::btree_lookup;
        let value = btree_lookup(pager, self.root_vpid, name.as_bytes()).await?;
        match value {
            Some(v) => {
                if v.len() != 8 {
                    return Err(TableDirError::BadValueSize(v.len()));
                }
                let bytes: [u8; 8] = v[..8].try_into().unwrap();
                Ok(Some(u64::from_le_bytes(bytes)))
            }
            None => Ok(None),
        }
    }

    /// ⭐ T15: 更新某个已存在 table 的 root_vpid. 用于 table BTree 内部 root split 时
    /// 同步 TableDirectory BTree 自身, 保证 crash recovery 后能拿到新 root.
    ///
    /// **实现**: 走 `crate::btree::btree_update` (跨多层 BTree, 不触发 split).
    /// 不存在返回 `NotFound`.
    ///
    /// **调用方**: `engine::table_put` 在 `registry::table_put` 返回
    /// `Some(new_root_vpid)` 时调用, 让 TableDirectory BTree 持久化新 root.
    /// 否则 reopen 后 `DbRegistry::load` 读 TableDirectory 拿到旧 root,
    /// 旧 root 只含 split 左半数据, 右半数据"丢失" (其实在, 但没人引用).
    pub async fn update_table(
        &mut self,
        pager: &mut Pager,
        name: &str,
        new_root_vpid: Vpid,
    ) -> Result<bool, TableDirError> {
        use crate::btree::btree_update;
        let updated = btree_update(
            pager,
            self.root_vpid,
            name.as_bytes(),
            &new_root_vpid.to_le_bytes(),
        )
        .await?;
        // update 不触发 split, 但 root 仍可能因 TableDirectory 自身 root split 而变
        // (当前 btree_update 不返回 new_root, 留作 future merge 防御)
        Ok(updated)
    }

    /// 列出所有 table (按 name 升序).
    ///
    /// **T15 升级**: 走 `crate::btree::btree_collect_leaves` 收集所有 leaf vpid,
    /// 然后逐 leaf 扫描 item, 跳过哨兵, 解析 name (UTF-8). 用 BTreeMap 收集
    /// 后排序.
    pub async fn list_tables(&self, pager: &mut Pager) -> Result<Vec<String>, TableDirError> {
        use crate::btree::btree_collect_leaves;
        let leaves = btree_collect_leaves(pager, self.root_vpid).await?;

        let mut tables = BTreeMap::new();
        for leaf_vpid in leaves {
            let page = pager.read(leaf_vpid).await?;
            let _idx = page::PageIndex::load(&*page, ItemKind::Leaf)
                .map_err(|e| TableDirError::PageDecode(format!("PageIndex::load: {:?}", e)))?;

            // 顺序扫描: 从 PAGE_HEADER_SIZE 开始, 跳过哨兵
            let mut prev_key: Vec<u8> = Vec::new();
            let mut off = PAGE_HEADER_SIZE;
            let free = page::page_free_off(&*page) as usize;
            let mut i = 0;
            while off < free {
                let (item, n) = decode_item(&*page, off, ItemKind::Leaf)
                    .map_err(|e| TableDirError::PageDecode(format!("decode_item: {:?}", e)))?;
                let full = item.full_key(&prev_key);
                if i > 0 {
                    // 跳过哨兵
                    let name =
                        String::from_utf8(full.clone()).map_err(|_| TableDirError::NonUtf8Name)?;
                    tables.insert(name, ());
                }
                prev_key = full;
                off += n;
                i += 1;
            }
        }

        Ok(tables.into_keys().collect())
    }

    /// 当前 table 数.
    pub async fn table_count(&self, pager: &mut Pager) -> Result<usize, TableDirError> {
        Ok(self.list_tables(pager).await?.len())
    }

    /// 强制 flush: 走 Pager::flush 把所有 dirty nowchunks 落盘 + meta flush.
    ///
    /// **调用方通常不需要调**: 我们的 `write_page` 走 nowchunks, 已经在内存
    /// 可读. 但如果调用方想保证 crash safety (例如 drop_table 后立刻进程退出),
    /// 应调 `Pager::flush` 触发实际落盘.
    pub async fn flush(&self, pager: &mut Pager) -> Result<(), TableDirError> {
        pager.flush().await?;
        Ok(())
    }
}

// =====================================================================
// Debug (手写, 不打印 raw pointer)
// =====================================================================

impl std::fmt::Debug for TableDirectory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TableDirectory")
            .field("root_vpid", &self.root_vpid)
            .finish_non_exhaustive()
    }
}

// =====================================================================
// helper: 创建空 leaf page (供 TableDirectory 内部使用)
// =====================================================================

/// 创建一个标记 vpid 的空 leaf page (用于 TableDirectory 自身 + table BTree root).
///
/// **为什么不复用 `leaf_new`?** `leaf_new` 不写 vpid 到 header, 而我们的 page
/// 在 nowchunks 落盘时 `write_page_with_vpid` 会重写 vpid 字段, 所以 leaf_new
/// 也能用. 这里暴露 `empty_leaf_with_vpid` 主要是给 create_table 的"空 table BTree"
/// 一个明确接口 (page_type = Leaf, 含 page header).
#[allow(dead_code)]
fn empty_leaf_with_vpid(vpid: Vpid) -> Box<[u8; PAGE_SIZE]> {
    let mut page = Box::new([0u8; PAGE_SIZE]);
    page_init_header(&mut page, PageType::Leaf);
    page_set_vpid(&mut page[..], vpid);
    page
}

// =====================================================================
// 单元测试
// =====================================================================

#[cfg(test)]
mod tests {
    // 注意: 单元测试需要 Pager, 而 Pager 涉及 IO (MetaCache::open 等).
    // 简单单元测试只覆盖不依赖 Pager 的纯逻辑 (error 类型等).
    // 集成测试在 tests/table_directory_tests.rs 里覆盖 Pager 集成场景.

    use super::*;

    #[test]
    fn table_dir_error_display_format() {
        let e = TableDirError::AlreadyExists("users".to_string());
        assert!(format!("{}", e).contains("users"));
        let e = TableDirError::BadValueSize(7);
        assert!(format!("{}", e).contains("7"));
    }

    #[test]
    fn empty_leaf_with_vpid_writes_header() {
        let page = empty_leaf_with_vpid(42);
        // magic
        assert_eq!(&page[0..4], b"LCBP");
        // page_type = Leaf
        assert_eq!(page[4], PageType::Leaf as u8);
        // vpid = 42
        let v = u64::from_le_bytes(page[0x18..0x20].try_into().unwrap());
        assert_eq!(v, 42);
    }
}
