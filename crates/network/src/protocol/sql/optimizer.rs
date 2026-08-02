//! ⭐ 逻辑优化 (RBO) — 纯 AST 变换, 不改变 SQL 语义.
//!
//! 安全规则集 (P0):
//! - 结构归一: `And([x]) → x` / `Or([x]) → x` / 空 And = 恒真 / 空 Or = 恒假
//! - NOT 化简: `NOT NOT x` → `x`; 德摩根: `NOT (A AND B)` → `(NOT A) OR (NOT B)`
//! - 叶子反转: `negate_cond` — `NOT (a = 1)` → `a <> 1` (等值/范围算子)
//! - 恒真/恒假短路: 恒假谓词可直接返回空结果
//!
//! 原则: 所有规则确定性、幂等 (对结果再应用不改变); 便于与执行结果一致性对比。

use super::ast::{sort_in_set, CmpOp, Cond, JoinCond, Pred, SqlValue};

/// 归一化谓词树 — 展开单元素集合 / 消 NOT NOT / 德摩根 / 恒真恒假标记.
/// 返回 (归一后的谓词, 是否恒假).
///
/// 注意: NOT 直接作用于叶子的情况**不在此反转** (泛型 C 无法构造反转叶子),
/// 由具体类型的 `negate_cond` (worker 层) 在需要时调用。
pub fn normalize_pred<C>(pred: &Pred<C>) -> (Pred<C>, bool)
where
    C: Clone + PartialEq,
{
    match pred {
        // 恒假: 空 OR
        Pred::Or(v) if v.is_empty() => (Pred::Or(vec![]), true),
        // 恒真: 空 AND
        Pred::And(v) if v.is_empty() => (Pred::And(vec![]), false),
        Pred::Leaf(c) => (Pred::Leaf(c.clone()), false),
        Pred::And(v) => {
            let mut children = Vec::with_capacity(v.len());
            let mut is_false = false;
            for p in v {
                let (np, nf) = normalize_pred(p);
                if nf {
                    is_false = true; // AND 中一项恒假 → 整体恒假
                    continue;
                }
                children.push(np);
            }
            if is_false {
                (Pred::And(children), true)
            } else if children.len() == 1 {
                (children.pop().unwrap(), false)
            } else {
                (Pred::And(children), false)
            }
        }
        Pred::Or(v) => {
            let mut children = Vec::with_capacity(v.len());
            let mut is_true = false;
            for p in v {
                let (np, nf) = normalize_pred(p);
                if !nf && is_always_true(&np) {
                    is_true = true;
                }
                children.push(np);
            }
            if is_true {
                // OR 含恒真项 → 整体恒真; 返回结构 (调用方短路)
                (Pred::Or(children), false)
            } else if children.len() == 1 {
                (children.pop().unwrap(), false)
            } else {
                (Pred::Or(children), false)
            }
        }
        Pred::Not(inner) => {
            // NOT NOT x → x
            if let Pred::Not(inner2) = inner.as_ref() {
                return normalize_pred(inner2);
            }
            // 德摩根下推
            match inner.as_ref() {
                Pred::And(v) => {
                    // NOT (A AND B) → (NOT A) OR (NOT B)
                    let parts: Vec<Pred<C>> = v
                        .iter()
                        .map(|p| normalize_pred(&Pred::Not(Box::new(p.clone()))).0)
                        .collect();
                    let or = Pred::Or(parts);
                    normalize_pred(&or)
                }
                Pred::Or(v) => {
                    // NOT (A OR B) → (NOT A) AND (NOT B)
                    let parts: Vec<Pred<C>> = v
                        .iter()
                        .map(|p| normalize_pred(&Pred::Not(Box::new(p.clone()))).0)
                        .collect();
                    let and = Pred::And(parts);
                    normalize_pred(&and)
                }
                // NOT 直接作用于叶子 — 泛型下无法反转, 保守保留 (语义不变)
                Pred::Leaf(_) | Pred::Not(_) => (Pred::Not(inner.clone()), false),
            }
        }
    }
}

/// 谓词是否恒真 (空 AND).
pub fn is_always_true<C>(pred: &Pred<C>) -> bool {
    matches!(pred, Pred::And(v) if v.is_empty())
}

/// 谓词是否恒假 (空 OR).
pub fn is_always_false<C>(pred: &Pred<C>) -> bool {
    matches!(pred, Pred::Or(v) if v.is_empty())
}

/// 取析取 (OR) 的叶分支集合. 仅当顶层是 OR 且每个分支都是单叶子时返回 Some,
/// 否则 None. ⭐ M2: 供 OR → 索引并集展开.
pub fn as_disjuncts<C>(pred: &Pred<C>) -> Option<Vec<&C>>
where
    C: Clone,
{
    let Pred::Or(branches) = pred else { return None };
    let mut out = Vec::with_capacity(branches.len());
    for b in branches {
        match b {
            Pred::Leaf(c) => out.push(c),
            // 含 AND/NOT/嵌套 OR 的分支无法单一区间 → 保守不展开
            _ => return None,
        }
    }
    Some(out)
}

/// 反转单个叶子条件 (等值/范围算子) — `NOT (a = 1)` → `a <> 1` 等.
/// 对 IN / LIKE 等无法确定补集的算子返回 None (保守保留).
pub fn negate_cond(cond: &Cond) -> Option<Cond> {
    let op = match cond.op {
        CmpOp::Eq => CmpOp::Ne,
        CmpOp::Ne => CmpOp::Eq,
        CmpOp::Gt => CmpOp::Le,
        CmpOp::Ge => CmpOp::Lt,
        CmpOp::Lt => CmpOp::Ge,
        CmpOp::Le => CmpOp::Gt,
        _ => return None,
    };
    Some(Cond {
        col: cond.col.clone(),
        op,
        val: cond.val.clone(),
        set: vec![],
    })
}

/// 叶子类型的最小协议 — 泛型 OR→IN 合并所需访问器.
pub trait OrEqInLeaf {
    fn op(&self) -> CmpOp;
    fn val(&self) -> &SqlValue;
    fn set(&self) -> &Vec<SqlValue>;
    fn col_key(&self) -> &str;
    fn build_in(col: &Self, set: Vec<SqlValue>) -> Self;
}

impl OrEqInLeaf for Cond {
    fn op(&self) -> CmpOp {
        self.op
    }
    fn val(&self) -> &SqlValue {
        &self.val
    }
    fn set(&self) -> &Vec<SqlValue> {
        &self.set
    }
    fn col_key(&self) -> &str {
        &self.col
    }
    fn build_in(col: &Self, set: Vec<SqlValue>) -> Self {
        Cond { col: col.col.clone(), op: CmpOp::In, val: SqlValue::Null, set }
    }
}

impl OrEqInLeaf for JoinCond {
    fn op(&self) -> CmpOp {
        self.op
    }
    fn val(&self) -> &SqlValue {
        &self.val
    }
    fn set(&self) -> &Vec<SqlValue> {
        &self.set
    }
    fn col_key(&self) -> &str {
        &self.col.col
    }
    fn build_in(col: &Self, set: Vec<SqlValue>) -> Self {
        JoinCond { col: col.col.clone(), op: CmpOp::In, val: SqlValue::Null, set }
    }
}

/// ⭐ M2c: 同列等值 OR → IN 合并.
///
/// `a=1 OR a=2 OR a=3` → `a IN (1,2,3)` (sort_in_set 排序去重).
/// 让含 OR 的 AND 谓词 (如 `(a=1 OR a=2) AND b>15`) 重新进入 AND 下推路径:
/// 单表 `sql_plan_select` 的索引计分 / JOIN `ScanFiltered` 广播均可利用.
/// 保守: 仅当 OR 全分支为同列 Eq 叶子; 混合算子/跨列/嵌套 → 原样保留 (语义不变).
pub fn or_eq_to_in<C>(pred: &Pred<C>) -> Pred<C>
where
    C: OrEqInLeaf + Clone,
{
    match pred {
        Pred::Or(v) => {
            let mut col: Option<&str> = None;
            let mut vals: Vec<SqlValue> = Vec::with_capacity(v.len());
            let mut ok = true;
            for b in v {
                match b {
                    Pred::Leaf(c) if c.op() == CmpOp::Eq && c.set().is_empty() => match col {
                        None => col = Some(c.col_key()),
                        Some(ex) if ex != c.col_key() => {
                            ok = false;
                            break;
                        }
                        _ => {}
                    },
                    _ => {
                        ok = false;
                        break;
                    }
                }
                if ok {
                    if let Pred::Leaf(c) = b {
                        vals.push(c.val().clone());
                    }
                }
            }
            // OR 单分支已被结构归一展开为 Leaf, 到这里必然 ≥2 分支 (且全 Eq 同列)
            if ok {
                if let Some(c0) = v.iter().find_map(|b| match b {
                    Pred::Leaf(c) => Some(c),
                    _ => None,
                }) {
                    let mut set = vals;
                    sort_in_set(&mut set);
                    return Pred::Leaf(C::build_in(c0, set));
                }
            }
            pred.clone()
        }
        Pred::And(v) => Pred::And(v.iter().map(or_eq_to_in).collect()),
        Pred::Not(b) => Pred::Not(Box::new(or_eq_to_in(b))),
        Pred::Leaf(_) => pred.clone(),
    }
}

/// ⭐ 对 `Pred<Cond>` 做完整归一 (含叶子反转): NOT 叶子 → 反转算子.
/// 供 worker 物理规划在索引选择前调用。
pub fn normalize_pred_cond(pred: &Pred<Cond>) -> (Pred<Cond>, bool) {
    // 第一遍: 结构归一
    let (np, is_false) = normalize_pred(pred);
    if is_false {
        return (np, true);
    }
    // 第二遍: 处理 NOT 直接作用叶子 (结构归一后 NOT 只可能直接包叶子)
    let np2 = push_not_to_leaf(&np);
    // 第三遍: 再归一 (展开单元素/恒真恒假)
    let (np3, is_false2) = normalize_pred(&np2);
    if is_false2 {
        return (np3, true);
    }
    // ⭐ M2c: 同列等值 OR → IN 合并 (含 OR 的 AND 谓词可重新进入下推路径)
    let np4 = or_eq_to_in(&np3);
    // 第四遍: In 叶子可能改变上层 And/Or 结构, 再归一一次 (幂等)
    normalize_pred(&np4)
}

/// 递归把 `NOT(Leaf)` 反转, `NOT(复合)` 已由归一处理.
fn push_not_to_leaf(pred: &Pred<Cond>) -> Pred<Cond> {
    match pred {
        Pred::Not(inner) => match inner.as_ref() {
            Pred::Leaf(c) => match negate_cond(c) {
                Some(nc) => Pred::Leaf(nc),
                None => pred.clone(),
            },
            Pred::And(v) => Pred::And(
                v.iter().map(|p| push_not_to_leaf(p)).collect(),
            ),
            Pred::Or(v) => Pred::Or(v.iter().map(|p| push_not_to_leaf(p)).collect()),
            Pred::Not(b) => push_not_to_leaf(b),
        },
        Pred::And(v) => Pred::And(v.iter().map(push_not_to_leaf).collect()),
        Pred::Or(v) => Pred::Or(v.iter().map(push_not_to_leaf).collect()),
        Pred::Leaf(_) => pred.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::ast::SqlValue;

    fn cond(col: &str, op: CmpOp, val: i64) -> Cond {
        Cond {
            col: col.into(),
            op,
            val: SqlValue::Int(val),
            set: vec![],
        }
    }

    #[test]
    fn not_eq_becomes_ne() {
        let p = Pred::Not(Box::new(Pred::Leaf(cond("a", CmpOp::Eq, 1))));
        let (np, _) = normalize_pred_cond(&p);
        match np {
            Pred::Leaf(c) => assert_eq!(c.op, CmpOp::Ne),
            other => panic!("expected leaf, got {other:?}"),
        }
    }

    #[test]
    fn not_gt_becomes_le() {
        let p = Pred::Not(Box::new(Pred::Leaf(cond("a", CmpOp::Gt, 5))));
        let (np, _) = normalize_pred_cond(&p);
        match np {
            Pred::Leaf(c) => assert_eq!(c.op, CmpOp::Le),
            other => panic!("expected leaf, got {other:?}"),
        }
    }

    #[test]
    fn double_not_eliminated() {
        let p = Pred::Not(Box::new(Pred::Not(Box::new(Pred::Leaf(cond(
            "a",
            CmpOp::Eq,
            1,
        ))))));
        let (np, _) = normalize_pred_cond(&p);
        match np {
            Pred::Leaf(c) => assert_eq!(c.op, CmpOp::Eq),
            other => panic!("expected leaf, got {other:?}"),
        }
    }

    #[test]
    fn and_singleton_unwraps() {
        let p = Pred::And(vec![Pred::Leaf(cond("a", CmpOp::Eq, 1))]);
        let (np, _) = normalize_pred_cond(&p);
        assert!(matches!(np, Pred::Leaf(_)));
    }

    #[test]
    fn and_containing_false_is_false() {
        let p = Pred::And(vec![
            Pred::Leaf(cond("a", CmpOp::Eq, 1)),
            Pred::Or(vec![]), // 恒假
        ]);
        let (np, is_false) = normalize_pred_cond(&p);
        assert!(is_false, "AND containing false must be false");
        let _ = np;
    }

    #[test]
    fn de_morgan_or_under_not() {
        // NOT (a = 1 OR b = 2) → (a <> 1) AND (b <> 2)
        let p = Pred::Not(Box::new(Pred::Or(vec![
            Pred::Leaf(cond("a", CmpOp::Eq, 1)),
            Pred::Leaf(cond("b", CmpOp::Eq, 2)),
        ])));
        let (np, _) = normalize_pred_cond(&p);
        match np {
            Pred::And(children) => {
                assert_eq!(children.len(), 2);
                assert!(children
                    .iter()
                    .all(|c| matches!(c, Pred::Leaf(cc) if cc.op == CmpOp::Ne)));
            }
            other => panic!("expected And after de morgan, got {other:?}"),
        }
    }

    #[test]
    fn normalize_is_idempotent() {
        let p = Pred::And(vec![
            Pred::Not(Box::new(Pred::Not(Box::new(Pred::Leaf(cond(
                "a",
                CmpOp::Eq,
                1,
            )))))),
            Pred::Leaf(cond("b", CmpOp::Gt, 2)),
        ]);
        let (np1, _) = normalize_pred_cond(&p);
        let (np2, _) = normalize_pred_cond(&np1);
        assert_eq!(np1, np2, "normalize must be idempotent");
    }

    // ===== ⭐ M2c: 同列等值 OR → IN 合并 =====

    fn eq(col: &str, val: i64) -> Pred<Cond> {
        Pred::Leaf(cond(col, CmpOp::Eq, val))
    }

    #[test]
    fn or_eq_in_merges_and_dedups() {
        let p = Pred::Or(vec![eq("a", 1), eq("a", 2), eq("a", 1)]);
        let (np, _) = normalize_pred_cond(&p);
        match np {
            Pred::Leaf(c) => {
                assert_eq!(c.op, CmpOp::In, "OR(Eq) → IN");
                assert_eq!(c.set.len(), 2, "重复值去重: {c:?}");
                assert_eq!(c.set[0], SqlValue::Int(1));
                assert_eq!(c.set[1], SqlValue::Int(2));
            }
            other => panic!("expected In leaf, got {other:?}"),
        }
    }

    #[test]
    fn or_eq_in_cross_col_keeps_or() {
        let p = Pred::Or(vec![eq("a", 1), eq("b", 2)]);
        let (np, _) = normalize_pred_cond(&p);
        assert!(matches!(np, Pred::Or(_)), "跨列 OR 不合并: {np:?}");
    }

    #[test]
    fn or_eq_in_mixed_op_keeps_or() {
        let p = Pred::Or(vec![eq("a", 1), Pred::Leaf(cond("a", CmpOp::Gt, 5))]);
        let (np, _) = normalize_pred_cond(&p);
        assert!(matches!(np, Pred::Or(_)), "混合算子 OR 不合并: {np:?}");
    }

    #[test]
    fn or_eq_in_inside_and_merges() {
        // (a=1 OR a=2) AND b>15 → a IN (1,2) AND b>15
        let p = Pred::And(vec![
            Pred::Or(vec![eq("a", 1), eq("a", 2)]),
            Pred::Leaf(cond("b", CmpOp::Gt, 15)),
        ]);
        let (np, _) = normalize_pred_cond(&p);
        match np {
            Pred::And(children) => {
                assert_eq!(children.len(), 2);
                assert!(children
                    .iter()
                    .any(|c| matches!(c, Pred::Leaf(cc) if cc.op == CmpOp::In)));
            }
            other => panic!("expected And, got {other:?}"),
        }
    }

    #[test]
    fn or_eq_in_mixed_structure_keeps_or() {
        // a=1 OR (a=2 AND b=3): AND 分支不能并入 IN
        let p = Pred::Or(vec![
            eq("a", 1),
            Pred::And(vec![eq("a", 2), eq("b", 3)]),
        ]);
        let (np, _) = normalize_pred_cond(&p);
        assert!(matches!(np, Pred::Or(_)), "嵌套 AND 分支不合并: {np:?}");
    }

    #[test]
    fn or_eq_in_is_idempotent() {
        let p = Pred::Or(vec![eq("a", 3), eq("a", 1)]);
        let (np1, _) = normalize_pred_cond(&p);
        let (np2, _) = normalize_pred_cond(&np1);
        assert_eq!(np1, np2, "M2c 幂等");
        assert!(matches!(np1, Pred::Leaf(c) if c.op == CmpOp::In));
    }
}
