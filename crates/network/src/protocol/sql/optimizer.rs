//! ⭐ 逻辑优化 (RBO) — 纯 AST 变换, 不改变 SQL 语义.
//!
//! 安全规则集 (P0):
//! - 结构归一: `And([x]) → x` / `Or([x]) → x` / 空 And = 恒真 / 空 Or = 恒假
//! - NOT 化简: `NOT NOT x` → `x`; 德摩根: `NOT (A AND B)` → `(NOT A) OR (NOT B)`
//! - 叶子反转: `negate_cond` — `NOT (a = 1)` → `a <> 1` (等值/范围算子)
//! - 恒真/恒假短路: 恒假谓词可直接返回空结果
//!
//! 原则: 所有规则确定性、幂等 (对结果再应用不改变); 便于与执行结果一致性对比。

use super::ast::{CmpOp, Cond, Pred};

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
    normalize_pred(&np2)
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
}
