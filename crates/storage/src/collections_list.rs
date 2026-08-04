// ⭐ 解耦 2026-08: StorageEngine List 集合操作 (从 collections.rs 拆出).
use std::ops::ControlFlow;

use crate::engine::StorageEngine;
use crate::keyspace as ks;
use crate::registry::RegistryError;

impl StorageEngine {
    async fn list_meta(
        &mut self,
        db: &str,
        table: &str,
        key: &[u8],
    ) -> Result<Option<(u64, i64, i64)>, RegistryError> {
        Ok(self
            .get_physical(db, table, &ks::encode_type_meta(key))
            .await?
            .map(|v| {
                if v.len() < 25 {
                    (0, 0, 0)
                } else {
                    (
                        u64::from_le_bytes(v[1..9].try_into().expect("8B")),
                        i64::from_le_bytes(v[9..17].try_into().expect("8B")),
                        i64::from_le_bytes(v[17..25].try_into().expect("8B")),
                    )
                }
            }))
    }

    pub(crate) async fn put_list_meta(
        &mut self,
        db: &str,
        table: &str,
        key: &[u8],
        count: u64,
        head: i64,
        tail: i64,
    ) -> Result<(), RegistryError> {
        self.mark_composite(db, table); // ⭐ F49 (List 全部写路径的单点)
        let mut v = Vec::with_capacity(25);
        v.push(ks::KIND_LIST);
        v.extend_from_slice(&count.to_le_bytes());
        v.extend_from_slice(&head.to_le_bytes());
        v.extend_from_slice(&tail.to_le_bytes());
        self.put_physical(db, table, &ks::encode_type_meta(key), &v)
            .await
            .map(|_| ())
    }

    /// LPUSH(left=true)/RPUSH: 逐个 push, 返回新长度.
    /// LPUSH 多值逆序落地 (Redis: LPUSH k a b c → c b a).
    pub async fn list_push(
        &mut self,
        db: &str,
        table: &str,
        key: &[u8],
        values: &[Vec<u8>],
        left: bool,
    ) -> Result<i64, RegistryError> {
        self.ensure_kind(db, table, key, ks::KIND_LIST).await?;
        let (mut count, mut head, mut tail) =
            self.list_meta(db, table, key).await?.unwrap_or((0, 0, 0));
        for v in values {
            let idx = if count == 0 {
                0
            } else if left {
                head - 1
            } else {
                tail + 1
            };
            let ek = ks::encode_data(ks::KIND_LIST, key, &ks::encode_idx(idx));
            self.put_physical(db, table, &ek, v).await?;
            if count == 0 {
                head = idx;
                tail = idx;
            } else if left {
                head = idx;
            } else {
                tail = idx;
            }
            count += 1;
        }
        self.put_list_meta(db, table, key, count, head, tail).await?;
        Ok(count as i64)
    }

    /// LPOP(left=true)/RPOP: 弹出 count 个, 返回 stored 值 (caller 渲染).
    ///
    /// ⭐ C2: 容忍中段空洞 (LREM/LTRIM 产生) — extreme idx 上 get miss 时
    /// 朝内收缩一格重试 (步数上限 = 历史删除数, 摊还可接受).
    pub async fn list_pop(
        &mut self,
        db: &str,
        table: &str,
        key: &[u8],
        left: bool,
        count: usize,
    ) -> Result<Vec<Vec<u8>>, RegistryError> {
        self.ensure_kind(db, table, key, ks::KIND_LIST).await?;
        let Some((mut cnt, mut head, mut tail)) = self.list_meta(db, table, key).await? else {
            return Ok(vec![]);
        };
        let mut out = Vec::new();
        'pop: for _ in 0..count {
            if cnt == 0 {
                break;
            }
            loop {
                if head > tail {
                    cnt = 0;
                    break 'pop;
                }
                let idx = if left { head } else { tail };
                let ek = ks::encode_data(ks::KIND_LIST, key, &ks::encode_idx(idx));
                if let Some(v) = self.get_physical(db, table, &ek).await? {
                    out.push(v);
                    self.delete_physical(db, table, &ek).await?;
                    cnt -= 1;
                    if left {
                        head += 1;
                    } else {
                        tail -= 1;
                    }
                    break;
                }
                // 空洞: 收缩一格再试 (不减 cnt)
                if left {
                    head += 1;
                } else {
                    tail -= 1;
                }
            }
        }
        if cnt == 0 {
            self.delete_physical(db, table, &ks::encode_type_meta(key))
                .await?;
        } else {
            self.put_list_meta(db, table, key, cnt, head, tail).await?;
        }
        Ok(out)
    }

    /// LLEN.
    pub async fn list_len(
        &mut self,
        db: &str,
        table: &str,
        key: &[u8],
    ) -> Result<i64, RegistryError> {
        self.ensure_kind(db, table, key, ks::KIND_LIST).await?;
        Ok(self.list_meta(db, table, key).await?.map(|(c, _, _)| c).unwrap_or(0) as i64)
    }

    /// LRANGE start end (含负索引, end inclusive). 返回 stored 值 (caller 渲染).
    pub async fn list_range(
        &mut self,
        db: &str,
        table: &str,
        key: &[u8],
        start: i64,
        end: i64,
    ) -> Result<Vec<Vec<u8>>, RegistryError> {
        self.ensure_kind(db, table, key, ks::KIND_LIST).await?;
        let Some((cnt, _, _)) = self.list_meta(db, table, key).await? else {
            return Ok(vec![]);
        };
        let len = cnt as i64;
        if len == 0 {
            return Ok(vec![]);
        }
        let mut s = if start < 0 { len + start } else { start };
        let mut e = if end < 0 { len + end } else { end };
        if s < 0 {
            s = 0;
        }
        if e >= len {
            e = len - 1;
        }
        if s > e {
            return Ok(vec![]);
        }
        // 扫描全部元素 (BTree 序 = 列表左到右), 取 [s..=e]
        let root = self.open_table(db, table).await?.ok_or_else(|| {
            RegistryError::TableNotFound(db.to_string(), table.to_string())
        })?;
        let prefix = ks::data_prefix(ks::KIND_LIST, key);
        let mut pos = 0i64;
        let mut out = Vec::new();
        crate::registry::table_scan_prefix(self.pager_mut(), root, &prefix, &mut |_k, v| {
            if pos >= s && pos <= e {
                out.push(v.to_vec());
            }
            pos += 1;
            if pos > e {
                ControlFlow::Break(())
            } else {
                ControlFlow::Continue(())
            }
        })
        .await?;
        Ok(out)
    }

    /// LINDEX idx (含负索引). 返回 stored 值.
    ///
    /// ⭐ C2: 按扫描序取第 pos 个 (O(n), 符合 Redis) — 不再假设 idx 连续
    /// (LREM/LTRIM/LINSERT 会留空洞).
    pub async fn list_index(
        &mut self,
        db: &str,
        table: &str,
        key: &[u8],
        idx: i64,
    ) -> Result<Option<Vec<u8>>, RegistryError> {
        self.ensure_kind(db, table, key, ks::KIND_LIST).await?;
        let Some((cnt, _, _)) = self.list_meta(db, table, key).await? else {
            return Ok(None);
        };
        let len = cnt as i64;
        let pos = if idx < 0 { len + idx } else { idx };
        if pos < 0 || pos >= len {
            return Ok(None);
        }
        let root = self.open_table(db, table).await?.ok_or_else(|| {
            RegistryError::TableNotFound(db.to_string(), table.to_string())
        })?;
        let prefix = ks::data_prefix(ks::KIND_LIST, key);
        let mut i = 0i64;
        let mut found: Option<Vec<u8>> = None;
        crate::registry::table_scan_prefix(self.pager_mut(), root, &prefix, &mut |_k, v| {
            if i == pos {
                found = Some(v.to_vec());
                return ControlFlow::Break(());
            }
            i += 1;
            ControlFlow::Continue(())
        })
        .await?;
        // 扫描拿到的是裸 stored, 溢出描述符需展开
        if let Some(v) = &mut found
            && crate::overflow::is_indirect(v)
        {
            *v = crate::overflow::read_overflow(self.pager_mut(), v).await?;
        }
        Ok(found)
    }

    /// LSET idx val (含负索引). 返回 false = 越界.
    ///
    /// ⭐ C2: 扫描定位第 pos 个行的物理 idx 后原位覆写 (旧溢出链自动释放).
    pub async fn list_set(
        &mut self,
        db: &str,
        table: &str,
        key: &[u8],
        idx: i64,
        value: &[u8],
    ) -> Result<bool, RegistryError> {
        self.ensure_kind(db, table, key, ks::KIND_LIST).await?;
        let Some((cnt, _, _)) = self.list_meta(db, table, key).await? else {
            return Ok(false);
        };
        let len = cnt as i64;
        let pos = if idx < 0 { len + idx } else { idx };
        if pos < 0 || pos >= len {
            return Ok(false);
        }
        let root = self.open_table(db, table).await?.ok_or_else(|| {
            RegistryError::TableNotFound(db.to_string(), table.to_string())
        })?;
        let prefix = ks::data_prefix(ks::KIND_LIST, key);
        let mut i = 0i64;
        let mut target: Option<i64> = None;
        crate::registry::table_scan_prefix(self.pager_mut(), root, &prefix, &mut |k, _v| {
            if i == pos {
                if let Some((_, suf)) = ks::split_data(k)
                    && suf.len() == 8
                {
                    target = Some(ks::decode_idx(suf.try_into().expect("8B")));
                }
                return ControlFlow::Break(());
            }
            i += 1;
            ControlFlow::Continue(())
        })
        .await?;
        let Some(actual) = target else {
            return Ok(false);
        };
        self.put_physical(
            db,
            table,
            &ks::encode_data(ks::KIND_LIST, key, &ks::encode_idx(actual)),
            value,
        )
        .await?;
        Ok(true)
    }

    // =================================================================
    // ⭐ C2: List 中段操作 (LREM / LTRIM / LPOS / LINSERT)
    // =================================================================

    /// 扫描全部 (物理 idx, 裸 stored) 行, 扫描序 = 列表左到右.
    async fn list_rows(
        &mut self,
        db: &str,
        table: &str,
        key: &[u8],
    ) -> Result<Vec<(i64, Vec<u8>)>, RegistryError> {
        let root = self.open_table(db, table).await?.ok_or_else(|| {
            RegistryError::TableNotFound(db.to_string(), table.to_string())
        })?;
        let prefix = ks::data_prefix(ks::KIND_LIST, key);
        let mut rows: Vec<(i64, Vec<u8>)> = Vec::new();
        crate::registry::table_scan_prefix(self.pager_mut(), root, &prefix, &mut |k, v| {
            if let Some((_, suf)) = ks::split_data(k)
                && suf.len() == 8
            {
                rows.push((ks::decode_idx(suf.try_into().expect("8B")), v.to_vec()));
            }
            ControlFlow::Continue(())
        })
        .await?;
        Ok(rows)
    }

    /// stored 与目标值比较 (溢出描述符先展开).
    async fn stored_eq(&mut self, stored: &[u8], want: &[u8]) -> Result<bool, RegistryError> {
        if crate::overflow::is_indirect(stored) {
            let full = crate::overflow::read_overflow(self.pager_mut(), stored).await?;
            Ok(full == want)
        } else {
            Ok(stored == want)
        }
    }

    /// 搜行后更新 meta: 从剩余行重算 count/head/tail (全删 → 删 meta).
    async fn refresh_list_meta(
        &mut self,
        db: &str,
        table: &str,
        key: &[u8],
    ) -> Result<(), RegistryError> {
        let rows = self.list_rows(db, table, key).await?;
        if rows.is_empty() {
            self.delete_physical(db, table, &ks::encode_type_meta(key))
                .await?;
        } else {
            let head = rows.first().expect("non-empty").0;
            let tail = rows.last().expect("non-empty").0;
            self.put_list_meta(db, table, key, rows.len() as u64, head, tail)
                .await?;
        }
        Ok(())
    }

    /// LREM count value: 删除匹配行. count>0 从头数 N; <0 从尾; =0 全部.
    /// value 为 stored 布局 ([tag][payload]).
    pub async fn list_rem(
        &mut self,
        db: &str,
        table: &str,
        key: &[u8],
        count: i64,
        value: &[u8],
    ) -> Result<i64, RegistryError> {
        self.ensure_kind(db, table, key, ks::KIND_LIST).await?;
        if self.list_meta(db, table, key).await?.is_none() {
            return Ok(0);
        }
        let rows = self.list_rows(db, table, key).await?;
        // 匹配行的物理 idx (扫描序)
        let mut matched: Vec<i64> = Vec::new();
        for (idx, stored) in &rows {
            if self.stored_eq(stored, value).await? {
                matched.push(*idx);
            }
        }
        let victims: Vec<i64> = if count > 0 {
            matched.into_iter().take(count as usize).collect()
        } else if count < 0 {
            let n = count.unsigned_abs() as usize;
            let skip = matched.len().saturating_sub(n);
            matched.into_iter().skip(skip).collect()
        } else {
            matched
        };
        for idx in &victims {
            self.delete_physical(db, table, &ks::encode_data(ks::KIND_LIST, key, &ks::encode_idx(*idx)))
                .await?;
        }
        if !victims.is_empty() {
            self.refresh_list_meta(db, table, key).await?;
        }
        Ok(victims.len() as i64)
    }

    /// LTRIM start stop (含负索引, 保留 [start, stop]).
    pub async fn list_trim(
        &mut self,
        db: &str,
        table: &str,
        key: &[u8],
        start: i64,
        stop: i64,
    ) -> Result<(), RegistryError> {
        self.ensure_kind(db, table, key, ks::KIND_LIST).await?;
        if self.list_meta(db, table, key).await?.is_none() {
            return Ok(());
        }
        let rows = self.list_rows(db, table, key).await?;
        let len = rows.len() as i64;
        let mut s = if start < 0 { len + start } else { start };
        let mut e = if stop < 0 { len + stop } else { stop };
        if s < 0 {
            s = 0;
        }
        if e >= len {
            e = len - 1;
        }
        for (pos, (idx, _)) in rows.iter().enumerate() {
            let pos = pos as i64;
            if s > e || pos < s || pos > e {
                self.delete_physical(
                    db,
                    table,
                    &ks::encode_data(ks::KIND_LIST, key, &ks::encode_idx(*idx)),
                )
                .await?;
            }
        }
        self.refresh_list_meta(db, table, key).await?;
        Ok(())
    }

    /// LPOS value rank count: 返回匹配位置 (0-based, 扫描序).
    /// rank>0 从第 rank 个匹配起; rank<0 从尾倒数. count=0 → 全部.
    pub async fn list_pos(
        &mut self,
        db: &str,
        table: &str,
        key: &[u8],
        value: &[u8],
        rank: i64,
        count: usize,
    ) -> Result<Vec<i64>, RegistryError> {
        self.ensure_kind(db, table, key, ks::KIND_LIST).await?;
        if self.list_meta(db, table, key).await?.is_none() {
            return Ok(vec![]);
        }
        let rows = self.list_rows(db, table, key).await?;
        let mut matches: Vec<i64> = Vec::new();
        for (pos, (_, stored)) in rows.iter().enumerate() {
            if self.stored_eq(stored, value).await? {
                matches.push(pos as i64);
            }
        }
        if rank < 0 {
            matches.reverse();
        }
        let skip = rank.unsigned_abs().saturating_sub(1) as usize;
        let iter = matches.into_iter().skip(skip);
        Ok(if count == 0 {
            iter.collect()
        } else {
            iter.take(count).collect()
        })
    }

    /// 搬行: 读完整 value (溢出展开) → 写新 idx → 删旧 idx (释放旧溢出链).
    async fn move_list_row(
        &mut self,
        db: &str,
        table: &str,
        key: &[u8],
        from: i64,
        to: i64,
    ) -> Result<(), RegistryError> {
        let from_k = ks::encode_data(ks::KIND_LIST, key, &ks::encode_idx(from));
        let Some(full) = self.get_physical(db, table, &from_k).await? else {
            return Ok(()); // 空洞容忍
        };
        self.put_physical(
            db,
            table,
            &ks::encode_data(ks::KIND_LIST, key, &ks::encode_idx(to)),
            &full,
        )
        .await?;
        self.delete_physical(db, table, &from_k).await?;
        Ok(())
    }

    /// LINSERT BEFORE|AFTER pivot value. 返回新长度; pivot 不存在 -1; key 不存在 0.
    /// 优先复用相邻 idx 间的空洞; 无稺密时才整体移位较小一侧 (O(n) 搬行).
    pub async fn list_insert(
        &mut self,
        db: &str,
        table: &str,
        key: &[u8],
        before: bool,
        pivot: &[u8],
        value: &[u8],
    ) -> Result<i64, RegistryError> {
        self.ensure_kind(db, table, key, ks::KIND_LIST).await?;
        if self.list_meta(db, table, key).await?.is_none() {
            return Ok(0);
        }
        let rows = self.list_rows(db, table, key).await?;
        let mut pivot_pos: Option<usize> = None;
        for (pos, (_, stored)) in rows.iter().enumerate() {
            if self.stored_eq(stored, pivot).await? {
                pivot_pos = Some(pos);
                break;
            }
        }
        let Some(p) = pivot_pos else {
            return Ok(-1);
        };
        let ins = if before { p } else { p + 1 }; // 新元素在扫描序中的位置
        let n = rows.len();
        let new_idx: i64 = if ins == 0 {
            rows[0].0 - 1 // 头前插入 (同 LPUSH)
        } else if ins == n {
            rows[n - 1].0 + 1 // 尾后插入 (同 RPUSH)
        } else if rows[ins].0 - rows[ins - 1].0 > 1 {
            rows[ins - 1].0 + 1 // 复用空洞, O(1)
        } else if ins <= n - ins {
            // 左侧更小: [0..ins) 全部下移 1 (升序处理避免碰撞), 腾出 rows[ins-1].0
            for (idx, _) in rows[..ins].iter() {
                self.move_list_row(db, table, key, *idx, idx - 1).await?;
            }
            rows[ins - 1].0
        } else {
            // 右侧更小: [ins..) 全部上移 1 (降序处理), 腾出 rows[ins].0
            for (idx, _) in rows[ins..].iter().rev() {
                self.move_list_row(db, table, key, *idx, idx + 1).await?;
            }
            rows[ins].0
        };
        self.put_physical(
            db,
            table,
            &ks::encode_data(ks::KIND_LIST, key, &ks::encode_idx(new_idx)),
            value,
        )
        .await?;
        self.refresh_list_meta(db, table, key).await?;
        Ok((n + 1) as i64)
    }
}
