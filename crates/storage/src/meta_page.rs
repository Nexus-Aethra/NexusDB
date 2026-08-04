//! T9 MetaPage: chunk 0 page 0 的 catalog 根节点.
//!
//! ## 设计 (来自 plan §Task 9)
//!
//! - **MetaPage 固定位置**: chunk 0 page 0 (vpid 0), 整个 catalog 树的根
//! - **存储内容**: db_name (UTF-8) → table_dir_root_vpid (u64) 的 BTree
//! - **复用 page crate 的 leaf item 编码** (key=value 字节), `page_type = Leaf`
//!   这样可以直接用 `leaf_insert` / `leaf_get` / `leaf_delete` API
//! - **整页重写 flush**: db 数量少 (< 100) 时整页重写性能可接受
//! - **BTreeMap 镜像**: 内存 `BTreeMap<String, u64>` 是事实源, flush 时序列化
//!
//! ## ⭐ T12.14 MetaPage 布局: page header + DbNameResolver 段 + item 区
//!
//! ```text
//! [0..40]            page header (magic + type + key_count + free_off + vpid ...)
//! [40..104]          DbNameResolver 序列化段 (固定 64B, T12.14 新增)
//! [104..PAGE_SIZE-16] item 区 (leaf BTree items, 从 offset 104 开始)
//! [PAGE_SIZE-16..]   page footer (16B, magic + version + checksum)
//! ```
//!
//! **为什么 resolver 在 [40..104]**: 必须在 page header 之后, item 区之前.
//! free_off 起点设为 104, item 编码自动从 104 开始.
//!
//! ## 单线程使用
//!
//! 沿用 `Pager` / `MetaCache` 契约, per-shard thread 单线程使用, 无锁.
//!
//! ## PageWriteBatch 集成
//!
//! MetaPage 的所有写 (add_db / remove_db / flush) **必须**走 `PageWriteBatch::submit`
//! (T9 plan 强调), 即使是单 page 修改. 实施细节在 `DbRegistry` (T11) 里统一处理,
//! 此模块只提供纯逻辑接口.

use std::collections::BTreeMap;

use page::{
    Checkpoint, CheckpointHeader, ItemKind, PAGE_HEADER_SIZE, PAGE_SIZE, PageIndex, PageType,
    decode_item, encode_leaf_item, leaf_insert, page_check_magic, page_free_off, page_init_header,
    page_key_count, page_set_free_off, page_type, page_vpid, write_checkpoint,
    write_checkpoint_header,
};

use crate::db_name_resolver::{DbNameResolver, RESOLVER_HEADER_SIZE};

// 重新导出 META_VPID / META_PID (来自 types.rs), 方便用户从 meta_page 模块直接拿
pub use crate::types::{META_PID, META_VPID};

/// ⭐ T12.14 item 区起始偏移 = PAGE_HEADER_SIZE + RESOLVER_HEADER_SIZE.
const META_ITEM_AREA_START: usize = PAGE_HEADER_SIZE + RESOLVER_HEADER_SIZE;

// =====================================================================
// StorageError
// =====================================================================

/// MetaPage 操作错误类型.
#[derive(Debug, thiserror::Error)]
pub enum MetaError {
    #[error("meta page bad magic")]
    BadMagic,
    #[error("meta page invalid vpid: expected {expected}, got {got}")]
    InvalidVpid { expected: u64, got: u64 },
    #[error("meta page invalid page type: expected {expected:?}, got {got:?}")]
    InvalidPageType { expected: PageType, got: PageType },
    #[error("page decode error: {0}")]
    PageDecode(String),
    #[error("db already exists: {0}")]
    AlreadyExists(String),
    #[error("non-utf8 db name")]
    NonUtf8Name,
    #[error("db value should be 8 bytes, got {0}")]
    BadValueSize(usize),
    #[error("page full, cannot add more dbs (max ~2000)")]
    PageFull,
}

impl From<page::PageError> for MetaError {
    fn from(e: page::PageError) -> Self {
        MetaError::PageDecode(format!("{:?}", e))
    }
}

// =====================================================================
// MetaPage
// =====================================================================

/// 内存中的 MetaPage 镜像.
///
/// - `dbs`: 事实源, 加载后即可查询
/// - `resolver`: db name ↔ DbId 映射 (T12.14)
/// - `page_buf`: 当前 page 字节副本, 用于 flush 时回写
///
/// 加载方式:
/// - `new_empty()` 创建一个空 MetaPage (b 树镜像空, page bytes 是初始空 leaf)
/// - `load(&page)` 从已存在的 page 字节 (从 disk / pager) 解析
pub struct MetaPage {
    /// db_name (UTF-8) → table_dir_root_vpid (u64)
    pub dbs: BTreeMap<String, u64>,
    /// ⭐ T12.14 db name ↔ DbId 双向映射, 持久化到 page 头部 [40..104]
    pub resolver: DbNameResolver,
    /// 底层 page 字节副本 (用于 flush 序列化)
    page_buf: Box<[u8; PAGE_SIZE]>,
}

impl MetaPage {
    /// 创建一个新的空 MetaPage.
    ///
    /// 内部 page_buf 在 flush 时才真正构造 (清空 dbs 后一次性 insert).
    /// resolver 默认含 "default" → DbId 0.
    pub fn new_empty() -> Self {
        Self {
            dbs: BTreeMap::new(),
            resolver: DbNameResolver::new(),
            page_buf: crate::page_pool::alloc_zeroed(),
        }
    }

    /// 从 page 字节构造 MetaPage.
    ///
    /// 校验 magic + vpid + free_off, 解析 [40..104] resolver 段 + item 区 leaf items.
    /// 失败返回 `MetaError` (不抛 panic).
    pub fn load(page: &[u8]) -> Result<Self, MetaError> {
        // 1. 校验 magic
        page_check_magic(page).map_err(|_| MetaError::BadMagic)?;

        // 2. 校验 vpid
        let v = page_vpid(page);
        if v != META_VPID {
            return Err(MetaError::InvalidVpid {
                expected: META_VPID,
                got: v,
            });
        }

        // 3. 校验 page type (必须是 Leaf, 因为我们用 leaf item 编码)
        let pt = page_type(page);
        if pt != PageType::Leaf {
            return Err(MetaError::InvalidPageType {
                expected: PageType::Leaf,
                got: pt,
            });
        }

        // 4. 解析 [40..104] resolver 段
        let resolver_bytes: [u8; RESOLVER_HEADER_SIZE] = page
            [PAGE_HEADER_SIZE..PAGE_HEADER_SIZE + RESOLVER_HEADER_SIZE]
            .try_into()
            .map_err(|_| MetaError::PageDecode("resolver header slice failed".into()))?;
        let resolver = DbNameResolver::deserialize(&resolver_bytes)
            .map_err(|e| MetaError::PageDecode(format!("resolver deserialize: {:?}", e)))?;

        // 5. 解析 item 区 leaf items 到 dbs 镜像
        //    item 区从 META_ITEM_AREA_START 开始, 终点 PAGE_SIZE - PAGE_FOOTER_SIZE
        //    free_off 应当 >= META_ITEM_AREA_START
        let key_count = page_key_count(page) as usize;
        let mut dbs = BTreeMap::new();
        if key_count > 0 {
            // 校验 free_off 是否对齐到 META_ITEM_AREA_START (item 区起点)
            let actual_free_off = page_free_off(page);
            if actual_free_off < META_ITEM_AREA_START as u16 {
                return Err(MetaError::PageDecode(format!(
                    "MetaPage free_off={} < META_ITEM_AREA_START={} (resolver 段未对齐)",
                    actual_free_off, META_ITEM_AREA_START
                )));
            }

            // 用 PageIndex 拿 cp array, 顺序遍历所有 item
            let _idx = PageIndex::load(page, ItemKind::Leaf)
                .map_err(|e| MetaError::PageDecode(format!("PageIndex::load: {:?}", e)))?;

            let mut prev_key: Vec<u8> = Vec::new();
            let mut off = META_ITEM_AREA_START;
            // 遍历所有 item (含哨兵, 所以是 key_count + 1)
            for i in 0..key_count + 1 {
                let (item, n) = decode_item(page, off, ItemKind::Leaf)
                    .map_err(|e| MetaError::PageDecode(format!("decode_item: {:?}", e)))?;
                let full_key = item.full_key(&prev_key);
                if i == 0 {
                    // 哨兵: shared=0, key_unshared_len=0
                    if !full_key.is_empty() {
                        return Err(MetaError::PageDecode(
                            "expected empty sentinel at item 0".into(),
                        ));
                    }
                } else {
                    if item.value.len() != 8 {
                        return Err(MetaError::BadValueSize(item.value.len()));
                    }
                    let vpid = u64::from_le_bytes(item.value[..8].try_into().unwrap());
                    let name =
                        String::from_utf8(full_key.clone()).map_err(|_| MetaError::NonUtf8Name)?;
                    dbs.insert(name, vpid);
                }
                prev_key = full_key;
                off += n;
            }
        }

        // 6. 复制 page bytes
        let mut page_buf = crate::page_pool::alloc_zeroed();
        page_buf.copy_from_slice(page);
        Ok(Self {
            dbs,
            resolver,
            page_buf,
        })
    }

    /// 序列化为 page 字节 (返回 Box page, 减少栈占用).
    ///
    /// **整页重写**: 不在原 page 上增量修改, 而是新建一个空 leaf,
    /// 然后把 `dbs` 全部 insert 进去. db 数量少时性能可接受.
    ///
    /// ⭐ T12.14: 头部 [40..104] 写 resolver 序列化段.
    pub fn flush(&self) -> Box<[u8; PAGE_SIZE]> {
        let mut page = Box::new(leaf_new_with_vpid(META_VPID));
        // 写 resolver 段 [PAGE_HEADER_SIZE..PAGE_HEADER_SIZE+RESOLVER_HEADER_SIZE]
        let resolver_bytes = self.resolver.serialize();
        page[PAGE_HEADER_SIZE..PAGE_HEADER_SIZE + RESOLVER_HEADER_SIZE]
            .copy_from_slice(&resolver_bytes);
        // 按 key 顺序插 (BTreeMap 已排序, 保证确定性)
        for (name, &vpid) in &self.dbs {
            leaf_insert(&mut *page, name.as_bytes(), &vpid.to_le_bytes())
                .expect("MetaPage flush: leaf_insert failed (page full?)");
        }
        page
    }

    /// 获取 db 对应的 table_dir_root_vpid.
    pub fn get_db(&self, name: &str) -> Option<u64> {
        self.dbs.get(name).copied()
    }

    /// 添加一个 db. 重复添加返回 AlreadyExists.
    ///
    /// ⭐ T12.14: 同时在 resolver 注册 name → DbId 映射.
    pub fn add_db(&mut self, name: &str, table_dir_root_vpid: u64) -> Result<(), MetaError> {
        if self.dbs.contains_key(name) {
            return Err(MetaError::AlreadyExists(name.to_string()));
        }
        // 同步注册到 resolver
        self.resolver
            .get_or_create(name)
            .map_err(|e| MetaError::PageDecode(format!("resolver get_or_create: {:?}", e)))?;
        self.dbs.insert(name.to_string(), table_dir_root_vpid);
        Ok(())
    }

    /// 删除一个 db. 返回 true 表示存在并删除, false 表示不存在.
    ///
    /// ⭐ T12.14: 注意, 不从 resolver 删除 name (id 永不重用).
    pub fn remove_db(&mut self, name: &str) -> bool {
        self.dbs.remove(name).is_some()
    }

    /// 列出所有 db (按 name 升序, BTreeMap 顺序).
    pub fn list_dbs(&self) -> Vec<(String, u64)> {
        self.dbs.iter().map(|(k, v)| (k.clone(), *v)).collect()
    }

    /// db 总数.
    pub fn db_count(&self) -> usize {
        self.dbs.len()
    }

    /// ⭐ T12.14: 根据 db name 查 DbId.
    pub fn db_id(&self, name: &str) -> Option<crate::types::DbId> {
        self.resolver.resolve(name)
    }

    /// ⭐ T12.14: 根据 DbId 查 db name.
    pub fn db_name(&self, id: crate::types::DbId) -> Option<&str> {
        self.resolver.name(id)
    }

    /// ⭐ T12.14: 获取 resolver 引用 (调试 / 高级用).
    pub fn resolver(&self) -> &DbNameResolver {
        &self.resolver
    }

    /// 当前持有的 page 字节 (调试 / 测试用).
    pub fn page_bytes(&self) -> &[u8; PAGE_SIZE] {
        &self.page_buf
    }
}

/// 创建一个带 META_VPID 标记的空 leaf page (用于 MetaPage flush).
///
/// ⭐ T12.14: free_off 起点设为 META_ITEM_AREA_START (= 104), 把 [40..104] 留给
/// DbNameResolver 段.
///
/// **为什么不能直接用 `leaf_new()`?**  `leaf_new()` 设 free_off = PAGE_HEADER_SIZE = 40,
/// 哨兵会写在 40 处, 覆盖我们预留给 resolver 段的位置. 因此我们手写 page header
/// + 哨兵 at 104 + cp[0] 指向 104, 复用 page crate 已暴露的低层 API.
fn leaf_new_with_vpid(vpid: u64) -> [u8; PAGE_SIZE] {
    let mut page = [0u8; PAGE_SIZE];
    page_init_header(&mut page, PageType::Leaf);
    // 写 vpid 到 header [0x18..0x20]
    page[0x18..0x20].copy_from_slice(&vpid.to_le_bytes());
    // ⭐ T12.14: free_off 起点 = META_ITEM_AREA_START, 跳过 resolver 段
    page_set_free_off(&mut page, META_ITEM_AREA_START as u16);
    // 写哨兵到 META_ITEM_AREA_START 处
    init_meta_sentinel(&mut page);
    page
}

/// ⭐ T12.14: 在 MetaPage 的 item area 起点 (META_ITEM_AREA_START = 104) 写哨兵.
///
/// 哨兵编码: shared=0, key_unshared_len=0, key=空, value="[]" (空 value 占位).
/// 同时设置 cp[0] 指向哨兵 (item_count=1, first_item_off=104).
fn init_meta_sentinel(page: &mut [u8; PAGE_SIZE]) {
    let mut buf = [0u8; 4096];
    let n = encode_leaf_item(&mut buf, &[], b"", b"[]").expect("encode sentinel failed");
    let off = META_ITEM_AREA_START;
    page[off..off + n].copy_from_slice(&buf[..n]);
    page_set_free_off(page, (off + n) as u16);
    let hdr = CheckpointHeader {
        checkpoint_count: 1,
        ..Default::default()
    };
    write_checkpoint_header(page, hdr);
    write_checkpoint(
        page,
        0,
        Checkpoint {
            item_count: 1, // 哨兵
            first_item_off: off as u16,
        },
    );
}

// =====================================================================
// 单元测试
// =====================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use page::leaf_get;

    #[test]
    fn empty_meta_page_has_no_dbs() {
        let meta = MetaPage::new_empty();
        assert_eq!(meta.db_count(), 0);
        assert!(meta.list_dbs().is_empty());
        assert!(meta.get_db("default").is_none());
    }

    #[test]
    fn add_db_then_get_returns_root_vpid() {
        let mut meta = MetaPage::new_empty();
        meta.add_db("default", 1).unwrap();
        meta.add_db("analytics", 2).unwrap();
        assert_eq!(meta.db_count(), 2);
        assert_eq!(meta.get_db("default"), Some(1));
        assert_eq!(meta.get_db("analytics"), Some(2));
        assert_eq!(meta.get_db("nonexistent"), None);
    }

    #[test]
    fn add_duplicate_db_errors() {
        let mut meta = MetaPage::new_empty();
        meta.add_db("default", 1).unwrap();
        let result = meta.add_db("default", 2);
        assert!(matches!(result, Err(MetaError::AlreadyExists(_))));
        // 原值不变
        assert_eq!(meta.get_db("default"), Some(1));
    }

    #[test]
    fn remove_db_returns_true_when_existed() {
        let mut meta = MetaPage::new_empty();
        meta.add_db("default", 1).unwrap();
        assert!(meta.remove_db("default"));
        assert!(
            !meta.remove_db("default"),
            "second remove should return false"
        );
        assert_eq!(meta.db_count(), 0);
    }

    #[test]
    fn flush_then_load_roundtrip_preserves_dbs() {
        let mut meta = MetaPage::new_empty();
        meta.add_db("default", 1).unwrap();
        meta.add_db("analytics", 99).unwrap();
        meta.add_db("users", 1024).unwrap();

        let page = meta.flush();
        let loaded = MetaPage::load(&page[..]).unwrap();
        assert_eq!(loaded.db_count(), 3);
        assert_eq!(loaded.get_db("default"), Some(1));
        assert_eq!(loaded.get_db("analytics"), Some(99));
        assert_eq!(loaded.get_db("users"), Some(1024));
    }

    #[test]
    fn list_dbs_returns_sorted_by_name() {
        let mut meta = MetaPage::new_empty();
        meta.add_db("zebra", 1).unwrap();
        meta.add_db("alpha", 2).unwrap();
        meta.add_db("middle", 3).unwrap();
        let dbs = meta.list_dbs();
        assert_eq!(
            dbs,
            vec![
                ("alpha".to_string(), 2),
                ("middle".to_string(), 3),
                ("zebra".to_string(), 1),
            ]
        );
    }

    #[test]
    fn empty_meta_page_flush_load_roundtrip() {
        let meta = MetaPage::new_empty();
        let page = meta.flush();
        let loaded = MetaPage::load(&page[..]).unwrap();
        assert_eq!(loaded.db_count(), 0);
        assert!(loaded.list_dbs().is_empty());
    }

    #[test]
    fn meta_page_at_magic_position_can_be_loaded_by_pager() {
        // 验证 MetaPage 的 page 字节能被 page crate 的 PageIndex::load 正确解析
        let mut meta = MetaPage::new_empty();
        meta.add_db("default", 1).unwrap();
        let page = meta.flush();
        let idx = page::PageIndex::load(&page[..], page::ItemKind::Leaf).unwrap();
        // 1 个 cp 段, 包含哨兵 + 1 db = 2 items
        assert_eq!(idx.segments.len(), 1);
        assert_eq!(idx.segments[0].item_count, 2);
        // 用 leaf_get 验证能拿回 vpid
        let v = leaf_get(&page[..], b"default").unwrap();
        assert_eq!(v, 1u64.to_le_bytes().to_vec());
    }

    #[test]
    fn meta_page_load_rejects_bad_magic() {
        let page = [0u8; PAGE_SIZE];
        // magic 全 0 = bad magic
        let result = MetaPage::load(&page);
        assert!(matches!(result, Err(MetaError::BadMagic)));
    }

    #[test]
    fn meta_page_load_rejects_wrong_vpid() {
        // 写一个 valid magic + vpid=99 的 leaf page
        let mut page = leaf_new_with_vpid(99);
        leaf_insert(&mut page, b"foo", &[0u8; 8]).unwrap();
        let result = MetaPage::load(&page);
        assert!(matches!(result, Err(MetaError::InvalidVpid { .. })));
    }

    #[test]
    fn meta_page_load_rejects_wrong_page_type() {
        // page_type = Meta (非 Leaf) 应被拒绝
        let mut page = [0u8; PAGE_SIZE];
        page_init_header(&mut page, PageType::Meta);
        page[0x18..0x20].copy_from_slice(&META_VPID.to_le_bytes());
        let result = MetaPage::load(&page);
        assert!(matches!(result, Err(MetaError::InvalidPageType { .. })));
    }

    #[test]
    fn meta_page_flush_uses_magic_position_pid() {
        // MetaPage 永远在 chunk 0 page 0
        assert_eq!(META_VPID, 0);
        assert_eq!(META_PID.file_id(), 0);
        assert_eq!(META_PID.chunk_idx(), 0);
        assert_eq!(META_PID.page_idx(), 0);
        assert_eq!(META_PID.flags(), crate::types::PID_ALIVE);

        let mut meta = MetaPage::new_empty();
        meta.add_db("test", 42).unwrap();
        let page = meta.flush();
        // vpid 字段 (offset 0x18..0x20) 必须是 0
        let vpid = u64::from_le_bytes(page[0x18..0x20].try_into().unwrap());
        assert_eq!(vpid, META_VPID);
    }
}
