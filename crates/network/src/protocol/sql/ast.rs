use storage::schema::{Column, TableSchema};

/// SQL 字面量 (类型转换在 worker 按 schema 列类型做).
#[derive(Debug, Clone, PartialEq)]
pub enum SqlValue {
    Int(i64),
    Float(f64),
    Str(Vec<u8>),
    Null,
    /// ⭐ P1: 预处理占位符 (`?` 按序 / `$n` 显式, 0-based); 执行前必经
    /// bind_params 替换, 泄漏到执行层是 bug (sql_to_col 防御报错).
    Param(u16),
    /// ⭐ F71 (子查询): 非关联子查询占位 (scalar / IN / EXISTS 内层 SELECT).
    /// worker dispatch 前必经子查询折叠替换为字面量, 泄漏到执行层是 bug
    /// (sql_to_col 防御报错). 与 Param 同构的“执行前必解”占位.
    Subquery(Box<SqlStmt>),
    /// ⭐ F74 (关联子查询): 列引用 `[表/别名.]列` — 仅出现在内层
    /// WHERE 比较 RHS (相关条件). decorrelate_pred 改写为非关联 IN 后消失;
    /// 泄漏到执行层是 bug (sql_to_col 防御报错). 同 “执行前必解”占位家族.
    ColRef(String),
    /// ⭐ PG 兼容 (UPDATE SET 表达式): `SET c = <expr>` — 二值算术/一元 NOT/
    /// 列自引用. 仅在 UPDATE SET RHS 出现; worker 读旧行后对旧值求值.
    Expr(Box<ScalarExpr>),
    /// ⭐ PG 兼容 (UPDATE SET `= NOW()`): 当前时间戳字面量. 求值时展开为
    /// 当前 Unix 微秒 (时间列) 或 ISO 字符串 (其他列), 由 sql_to_col 转换.
    Now,
}

/// ⭐ F73: IN 集合原地排序去重 (同型集合: 全 Int 按值 / 全 Str 按字节序).
/// 大集合求值走二分依赖有序; 混型 (含 Float / 跨型) 保持原序不动 (求值回退线性).
/// 成员语义不变 (集合无序), 仅重排 + 去重.
pub fn sort_in_set(set: &mut Vec<SqlValue>) {
    let all_int = set.iter().all(|v| matches!(v, SqlValue::Int(_)));
    let all_str = set.iter().all(|v| matches!(v, SqlValue::Str(_)));
    if all_int {
        set.sort_by_key(|v| match v {
            SqlValue::Int(i) => *i,
            _ => unreachable!(),
        });
        set.dedup();
    } else if all_str {
        set.sort_by(|a, b| match (a, b) {
            (SqlValue::Str(x), SqlValue::Str(y)) => x.cmp(y),
            _ => unreachable!(),
        });
        set.dedup();
    }
}

/// ⭐ F76: 剥 db 限定前缀 — `db.tbl` / `db.tbl` (反引号已在 tokenizer 拼为单 Ident) → `tbl`.
/// 表名不含 '.', 取最后一段即去 db 限定. v1 不支持真跨库 (归一为当前库同名表).
pub(crate) fn strip_db_qual(name: String) -> String {
    match name.rsplit_once('.') {
        Some((_, tbl)) => tbl.to_string(),
        None => name,
    }
}

/// 比较算子 (WHERE 条件).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CmpOp {
    Eq,
    Gt,
    Ge,
    Lt,
    Le,
    /// ⭐ S2: 不等 (纯残余过滤, 不产界).
    Ne,
    /// ⭐ S2: IN 集合 (值在 Cond::set; 索引列可提 [min,max] 界 + 残余精确).
    In,
    /// ⭐ compat: JSONB 存在操作符 `j ? 'key'` (列含 JSON 顶层键; 纯残余过滤, 不下推).
    JsonExists,
}

/// ⭐ G1 (F63): 聚合函数.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggFn {
    Count,
    Sum,
    Avg,
    Min,
    Max,
}

impl AggFn {
    /// 输出列 label (与 HAVING/ORDER BY 匹配规则同源: 大写函数名 + 原列名).
    pub fn label(&self, col: Option<&str>) -> String {
        let name = match self {
            AggFn::Count => "COUNT",
            AggFn::Sum => "SUM",
            AggFn::Avg => "AVG",
            AggFn::Min => "MIN",
            AggFn::Max => "MAX",
        };
        format!("{name}({})", col.unwrap_or("*"))
    }
}

/// ⭐ G1 (F63): SELECT 投影项 — 纯列或聚合函数. ⭐ F76: 可带输出列别名 (AS).
#[derive(Debug, Clone, PartialEq)]
pub enum SelectItem {
    Col {
        name: String,
        alias: Option<String>,
    },
    /// arg = None 仅 COUNT(*). ⭐ F77: distinct = COUNT(DISTINCT col). ⭐ F78: arg 可为表达式.
    Agg {
        func: AggFn,
        arg: Option<ScalarExpr>,
        distinct: bool,
        alias: Option<String>,
    },
    /// ⭐ compat: 标量函数投影 (NOW()/version()) — worker 渲染常量.
    ScalarFn {
        name: String,
    },
    /// ⭐ compat: 表达式投影 (JSONB 取字段 j->'a' / j->>'a', v1 仅列+常量键).
    Expr {
        expr: ScalarExpr,
        alias: Option<String>,
    },
}

/// ⭐ F78: 算术运算符.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArithOp {
    Add,
    Sub,
    Mul,
    Div,
}

impl ArithOp {
    fn sym(self) -> &'static str {
        match self {
            ArithOp::Add => "+",
            ArithOp::Sub => "-",
            ArithOp::Mul => "*",
            ArithOp::Div => "/",
        }
    }
}

/// ⭐ F78: 聚合内标量表达式 (列引用 / 字面量 / 二元算术 / JSONB 取字段).
#[derive(Debug, Clone, PartialEq)]
pub enum ScalarExpr {
    Col(String),
    Lit(SqlValue),
    Bin {
        op: ArithOp,
        l: Box<ScalarExpr>,
        r: Box<ScalarExpr>,
    },
    /// ⭐ compat: JSONB 取字段 `base->key` (as_text=false) / `base->>key` (as_text=true).
    /// 仅支持 base=列 + key=字符串字面量 (v1).
    JsonGet {
        base: Box<ScalarExpr>,
        key: Box<ScalarExpr>,
        as_text: bool,
    },
    /// ⭐ PG 兼容 (UPDATE SET): 一元 `NOT expr` (布尔取反; SET 表达式 RHS).
    Not(Box<ScalarExpr>),
}

impl ScalarExpr {
    /// 原文重建 (聚合 label / HAVING/ORDER 匹配用).
    pub fn render(&self) -> String {
        match self {
            ScalarExpr::Col(c) => c.clone(),
            ScalarExpr::Lit(v) => match v {
                SqlValue::Int(i) => i.to_string(),
                SqlValue::Float(f) => f.to_string(),
                SqlValue::Str(b) => format!("'{}'", String::from_utf8_lossy(b)),
                _ => "?".to_string(),
            },
            ScalarExpr::Bin { op, l, r } => format!("{} {} {}", l.render(), op.sym(), r.render()),
            ScalarExpr::JsonGet { base, key, as_text } => {
                format!(
                    "{} {} {}",
                    base.render(),
                    if *as_text { "->>" } else { "->" },
                    key.render()
                )
            }
            ScalarExpr::Not(e) => format!("NOT {}", e.render()),
        }
    }
    /// 是否单一裸列引用 (COUNT(DISTINCT col) / 单列聚合退化判定).
    pub fn as_col(&self) -> Option<&str> {
        match self {
            ScalarExpr::Col(c) => Some(c),
            _ => None,
        }
    }
    /// 收集所有列引用 (只读, 供绑定/校验).
    pub fn for_each_col<F: FnMut(&str)>(&self, f: &mut F) {
        match self {
            ScalarExpr::Col(c) => f(c),
            ScalarExpr::Lit(_) => {}
            ScalarExpr::Bin { l, r, .. } => {
                l.for_each_col(f);
                r.for_each_col(f);
            }
            ScalarExpr::JsonGet { base, key, .. } => {
                base.for_each_col(f);
                key.for_each_col(f);
            }
            ScalarExpr::Not(e) => e.for_each_col(f),
        }
    }
    /// 就地改写所有列名 (供 strip_qual 剥表限定前缀).
    pub fn for_each_col_mut<F: FnMut(&mut String)>(&mut self, f: &mut F) {
        match self {
            ScalarExpr::Col(c) => f(c),
            ScalarExpr::Lit(_) => {}
            ScalarExpr::Bin { l, r, .. } => {
                l.for_each_col_mut(f);
                r.for_each_col_mut(f);
            }
            ScalarExpr::JsonGet { base, key, .. } => {
                base.for_each_col_mut(f);
                key.for_each_col_mut(f);
            }
            ScalarExpr::Not(e) => e.for_each_col_mut(f),
        }
    }
}

impl SelectItem {
    /// ⭐ F76: 输出列名 — alias 优先, 否则列名 / 聚合 label / 表达式原文.
    pub fn out_name(&self) -> String {
        match self {
            SelectItem::ScalarFn { name } => name.clone(),
            SelectItem::Col { name, alias } => alias.clone().unwrap_or_else(|| name.clone()),
            SelectItem::Agg {
                func,
                arg,
                distinct,
                alias,
            } => alias.clone().unwrap_or_else(|| {
                let inner = match arg {
                    None => "*".to_string(),
                    Some(e) => e.render(),
                };
                let fname = func.label(None).trim_end_matches("(*)").to_string();
                if *distinct {
                    format!("{fname}(DISTINCT {inner})")
                } else {
                    format!("{fname}({inner})")
                }
            }),
            SelectItem::Expr { expr, alias } => alias.clone().unwrap_or_else(|| expr.render()),
        }
    }
}

/// 单个 WHERE 条件 `col op lit` (AND 连接).
#[derive(Debug, Clone, PartialEq)]
pub struct Cond {
    pub col: String,
    pub op: CmpOp,
    pub val: SqlValue,
    /// ⭐ S2: 仅 In 使用 (非空); 其它算子恒空.
    pub set: Vec<SqlValue>,
}

/// ⭐ F71 (子查询): EXISTS 哨兵列名 — 真实列名不可为空, 以此区分 EXISTS(无列)
/// 与 scalar/IN(有列) 子查询叶子. 折叠前临时存在, 折叠后消失.
pub const EXISTS_SENTINEL_COL: &str = "";

/// ⭐ F69: 谓词表达式树 (AND/OR/NOT/括号) — 泛型于叶子类型 C.
/// WHERE 用 `Pred<Cond>`, JOIN WHERE 用 `Pred<JoinCond>`, HAVING 用下标域叶子.
/// 无 WHERE = `And(vec![])` (恒真).
#[derive(Debug, Clone, PartialEq)]
pub enum Pred<C> {
    Leaf(C),
    And(Vec<Pred<C>>),
    Or(Vec<Pred<C>>),
    Not(Box<Pred<C>>),
}

impl<C> Pred<C> {
    /// 纯 leaf 合取 (无 Or/Not) → Some(平铺叶子); 含 Or/Not → None.
    /// 供索引界推导/下推/bloom 等 AND-优化路径提取平铺列表.
    pub fn as_conjuncts(&self) -> Option<Vec<&C>> {
        match self {
            Pred::Leaf(c) => Some(vec![c]),
            Pred::And(v) => {
                let mut out = Vec::new();
                for p in v {
                    out.extend(p.as_conjuncts()?);
                }
                Some(out)
            }
            Pred::Or(_) | Pred::Not(_) => None,
        }
    }

    /// 全部叶子 (不论结构; 覆盖索引列名收集用).
    pub fn leaves(&self) -> Vec<&C> {
        match self {
            Pred::Leaf(c) => vec![c],
            Pred::And(v) | Pred::Or(v) => v.iter().flat_map(|p| p.leaves()).collect(),
            Pred::Not(b) => b.leaves(),
        }
    }

    /// 恒真 (空 AND) — 等价于无 WHERE.
    pub fn is_true(&self) -> bool {
        matches!(self, Pred::And(v) if v.is_empty())
    }

    /// 递归换叶子类型 (Cond → JoinCond 复用).
    pub fn map<D>(&self, f: &impl Fn(&C) -> D) -> Pred<D> {
        match self {
            Pred::Leaf(c) => Pred::Leaf(f(c)),
            Pred::And(v) => Pred::And(v.iter().map(|p| p.map(f)).collect()),
            Pred::Or(v) => Pred::Or(v.iter().map(|p| p.map(f)).collect()),
            Pred::Not(b) => Pred::Not(Box::new(b.map(f))),
        }
    }

    /// 递归换叶子 (可失败; bind_params 占位符替换用).
    pub fn try_map<D, E>(&self, f: &impl Fn(&C) -> Result<D, E>) -> Result<Pred<D>, E> {
        Ok(match self {
            Pred::Leaf(c) => Pred::Leaf(f(c)?),
            Pred::And(v) => Pred::And(v.iter().map(|p| p.try_map(f)).collect::<Result<_, _>>()?),
            Pred::Or(v) => Pred::Or(v.iter().map(|p| p.try_map(f)).collect::<Result<_, _>>()?),
            Pred::Not(b) => Pred::Not(Box::new(b.try_map(f)?)),
        })
    }
}

/// ⭐ F67 (JOIN): 限定列名 `[表/别名.]列`. qualifier=None 表示未限定.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QualCol {
    pub qualifier: Option<String>,
    pub col: String,
}

impl QualCol {
    /// 按首个 `.` 拆 (tokenizer 把 `u.id` 当单 Ident); 无点 → qualifier None.
    pub fn parse(s: &str) -> QualCol {
        match s.split_once('.') {
            Some((q, c)) => QualCol {
                qualifier: Some(q.to_string()),
                col: c.to_string(),
            },
            None => QualCol {
                qualifier: None,
                col: s.to_string(),
            },
        }
    }
}

/// ⭐ F67/F68 (JOIN): JOIN 种类.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinKind {
    Inner,
    Left,
    /// ⭐ F68: 未匹配右行补左 NULL.
    Right,
    /// ⭐ F68: 双侧未匹配都补 NULL.
    Full,
    /// ⭐ F68: 笛卡尔积 (无 ON).
    Cross,
}

/// ⭐ F68 (JOIN): 表引用 `表名 [AS] 别名` (无别名时 alias = 表名).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableRef {
    pub table: String,
    pub alias: String,
}

/// ⭐ F68 (JOIN): 单个 ON 谓词. Eq 供组合 hash 键; Cmp = 非等值 col-col 残余.
#[derive(Debug, Clone, PartialEq)]
pub enum OnPred {
    Eq(QualCol, QualCol),
    Cmp {
        left: QualCol,
        op: CmpOp,
        right: QualCol,
    },
}

/// ⭐ F68 (JOIN): 一个 JOIN 子句 (左深链中的一步). CROSS 时 on 空.
#[derive(Debug, Clone, PartialEq)]
pub struct JoinClause {
    pub kind: JoinKind,
    pub table: TableRef,
    pub on: Vec<OnPred>,
}

/// ⭐ F67 (JOIN): 投影项 (v1 无聚合/输出别名). 空 items = `*`.
#[derive(Debug, Clone, PartialEq)]
pub enum JoinItem {
    Col(QualCol),
}

/// ⭐ F67 (JOIN): WHERE 条件 `限定列 op 字面量` (复用 CmpOp/SqlValue).
#[derive(Debug, Clone, PartialEq)]
pub struct JoinCond {
    pub col: QualCol,
    pub op: CmpOp,
    pub val: SqlValue,
    pub set: Vec<SqlValue>,
}

/// 解析结果 AST.
#[derive(Debug, Clone, PartialEq)]
pub enum SqlStmt {
    /// CREATE TABLE: schema 已构建完成 (含 pk / 索引 iid 分配).
    /// `if_not_exists=true` → 表已存在时静默跳过 (不报错).
    CreateTable {
        table: String,
        schema: TableSchema,
        if_not_exists: bool,
    },
    /// INSERT: cols 为空 = 全列序; ⭐ S1: rows 支持多行 VALUES.
    Insert {
        table: String,
        cols: Vec<String>,
        rows: Vec<Vec<SqlValue>>,
    },
    /// SELECT: items 空 = `*` 全列 (⭐ O1 投影; ⭐ G1/F63 列+聚合混合).
    /// group_by/having 见 G1; order = (列名或聚合 label, desc); offset 排序后截断.
    Select {
        table: String,
        items: Vec<SelectItem>,
        conds: Pred<Cond>,
        limit: Option<u32>,
        order: Vec<(String, bool)>,
        offset: Option<u32>,
        group_by: Vec<String>,
        having: Pred<Cond>,
        /// ⭐ PG 兼容: LIMIT $n / OFFSET $n 的参数索引 (bind_params 时填字面量).
        limit_param: Option<u16>,
        offset_param: Option<u16>,
    },
    /// ⭐ S1: DELETE FROM t WHERE ... (WHERE 必带 — 全删由全表扫路径支撑).
    Delete { table: String, conds: Pred<Cond> },
    /// ⭐ S1: UPDATE t SET c=v[, ...] WHERE ... (禁改 pk 列, 规划层拦).
    Update {
        table: String,
        sets: Vec<(String, SqlValue)>,
        conds: Pred<Cond>,
    },
    /// ⭐ S1: DROP TABLE t.
    DropTable { table: String },
    /// ⭐ compat: CREATE INDEX [IF NOT EXISTS] name ON t (col[, col]) [WHERE ...] — v1 吞/建索引.
    CreateIndex {
        table: String,
        cols: Vec<String>,
        if_not_exists: bool,
    },
    /// ⭐ compat: 纯 PG 专有 DDL 吞掉 (EXTENSION / FUNCTION / TRIGGER / SEQUENCE ...) — 无副作用.
    DdlStub,
    /// ⭐ F79: ALTER TABLE t ADD COLUMN c TYPE (v1 仅追加可空列);
    /// ⭐ compat: DROP COLUMN c (标记删除, 物理保留).
    AlterTable {
        table: String,
        add: Option<Column>,
        drop: Option<String>,
        if_not_exists: bool,
    },
    /// 显式打开/关闭 SQL 表的 RESP 行适配。
    /// 语法: `ALTER TABLE t SET RESP ADAPTER ON|OFF`.
    SetRespRowAdapter { table: String, enabled: bool },
    /// ⭐ S3: USE db — 连接级切库 (worker 校验存在).
    Use { db: String },
    /// ⭐ S3: DESCRIBE t / DESC t — schema 渲染 (worker 本地).
    Describe { table: String },
    /// ⭐ S3: psql/工具兼容 stub — `SET ...` 忽略回 OK.
    SetStub,
    /// ⭐ S3: `SELECT version()` — 单行版本串.
    VersionStub,
    /// ⭐ S5: `SELECT DATABASE()` — 单行当前库名 (mysql cli USE 后探测).
    DatabaseStub,
    /// ⭐ compat: 无 FROM 的标量函数投影 (`SELECT NOW()`) — worker 渲染常量单行.
    ScalarSelect { items: Vec<SelectItem> },
    /// ⭐ F66: 系统表查询 (information_schema.* / pg_catalog.*) — 虚拟表,
    /// worker 从活元数据合成结果集. cols 空 = `*`.
    SystemQuery {
        catalog: String,
        table: String,
        cols: Vec<String>,
        conds: Pred<Cond>,
        order: Vec<(String, bool)>,
        limit: Option<u32>,
        offset: Option<u32>,
        limit_param: Option<u16>,
        offset_param: Option<u16>,
    },
    /// ⭐ F66: `SELECT @@var [, @@var2]` 系统变量 (SQLAlchemy/驱动初始化探测).
    /// vars = 去 @@ 的变量名列表; worker 回合理值单行.
    SystemVarStub { vars: Vec<String> },
    /// ⭐ PG 兼容: `SELECT EXISTS (SELECT ...)` — 内层非空 → 单行布尔 t/f.
    /// v1: 内层仅支持 SystemQuery (pg_database / information_schema 探测).
    ExistsStub { inner: Box<SqlStmt> },
    /// ⭐ PG 兼容: `CREATE DATABASE name` — worker 走 shard 2PC 建库.
    CreateDb { name: String },
    /// ⭐ F72 (子查询): FROM 派生表 `(SELECT ...) alias`. inner 先物化成
    /// 虚拟表 (worker 内存), 外层在其上过滤/投影/排序/截断.
    /// v1: 派生表为唯一数据源 (不参与 JOIN); 外层无 GROUP BY/HAVING/聚合
    /// 投影 (COUNT(*) worker 特判除外); 外层 WHERE 不含子查询.
    SelectDerived {
        inner: Box<SqlStmt>,
        alias: String,
        items: Vec<SelectItem>,
        conds: Pred<Cond>,
        order: Vec<(String, bool)>,
        limit: Option<u32>,
        offset: Option<u32>,
        limit_param: Option<u16>,
        offset_param: Option<u16>,
    },
    /// ⭐ F67/F68 (JOIN): N 表左深 hash join — worker 侧执行.
    /// 别名无时 = 表名; items 空 = `*` 展开各表全列; joins 为左深链.
    /// ⭐ F75: from_inner=Some 时 from 为派生表 (from.table=别名), 内层先物化预填 tables[0].
    SelectJoin {
        from: TableRef,
        from_inner: Option<Box<SqlStmt>>,
        joins: Vec<JoinClause>,
        items: Vec<JoinItem>,
        conds: Pred<JoinCond>,
        order: Vec<(QualCol, bool)>,
        limit: Option<u32>,
        offset: Option<u32>,
        limit_param: Option<u16>,
        offset_param: Option<u16>,
    },
    /// ⭐ 事务 v1 (F61): BEGIN / START TRANSACTION.
    /// ⭐ v2 (F62): 可选隔离级别与读写属性尾缀.
    Begin {
        iso: Option<TxnIso>,
        read_only: Option<bool>,
    },
    /// ⭐ 事务 v1 (F61): COMMIT.
    Commit,
    /// ⭐ 事务 v1 (F61): ROLLBACK.
    Rollback,
    /// ⭐ v2 (F62): SET [SESSION] TRANSACTION ... (session=连接默认 / 否则当前事务).
    SetTransaction {
        iso: Option<TxnIso>,
        read_only: Option<bool>,
        session: bool,
    },
    /// ⭐ v2 (F62): SAVEPOINT name.
    Savepoint { name: String },
    /// ⭐ v2 (F62): ROLLBACK TO [SAVEPOINT] name.
    RollbackTo { name: String },
    /// ⭐ v2 (F62): RELEASE [SAVEPOINT] name.
    Release { name: String },
}

/// ⭐ v2 (F62): 隔离级别 (四级归并两档: RU→RC, RR→Serializable —
/// 行级 OCC backward validation, 不防幻读, 文档化).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TxnIso {
    #[default]
    ReadCommitted,
    Serializable,
}

// =====================================================================
