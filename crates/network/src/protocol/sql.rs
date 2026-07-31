//! ⭐ X1 (SQL 落地): 最小 SQL 子集解析器 — 纯函数, 手写 tokenizer, 零依赖.
//!
//! 支持 (v1):
//! - `CREATE TABLE t (col TYPE [PRIMARY KEY] [NOT NULL], ..., INDEX(col), ...)`
//! - `INSERT INTO t [(c1,...)] VALUES (v1,...)`
//! - `SELECT * FROM t [WHERE col op lit [AND ...]] [LIMIT n]`
//!
//! 关键字大小写不敏感; 表/列名保留原样. 字符串字面量 '单引号' ('' 转义).
//! RESP array 参数已被客户端分词 → caller 先空格 join 再整体 tokenize
//! (引号内空格由 redis-cli 的引号语法保留在单参数内, join 后仍在引号内).

use storage::schema::{ColType, Column, TableSchema};

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

/// ⭐ G1 (F63): SELECT 投影项 — 纯列或聚合函数 (无表达式/别名).
#[derive(Debug, Clone, PartialEq)]
pub enum SelectItem {
    Col(String),
    /// col = None 仅 COUNT(*).
    Agg { func: AggFn, col: Option<String> },
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
            Some((q, c)) => QualCol { qualifier: Some(q.to_string()), col: c.to_string() },
            None => QualCol { qualifier: None, col: s.to_string() },
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
    Cmp { left: QualCol, op: CmpOp, right: QualCol },
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
    CreateTable { table: String, schema: TableSchema },
    /// INSERT: cols 为空 = 全列序; ⭐ S1: rows 支持多行 VALUES.
    Insert { table: String, cols: Vec<String>, rows: Vec<Vec<SqlValue>> },
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
    },
    /// ⭐ S1: DELETE FROM t WHERE ... (WHERE 必带 — 全删由全表扫路径支撑).
    Delete { table: String, conds: Pred<Cond> },
    /// ⭐ S1: UPDATE t SET c=v[, ...] WHERE ... (禁改 pk 列, 规划层拦).
    Update { table: String, sets: Vec<(String, SqlValue)>, conds: Pred<Cond> },
    /// ⭐ S1: DROP TABLE t.
    DropTable { table: String },
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
    },
    /// ⭐ F66: `SELECT @@var [, @@var2]` 系统变量 (SQLAlchemy/驱动初始化探测).
    /// vars = 去 @@ 的变量名列表; worker 回合理值单行.
    SystemVarStub { vars: Vec<String> },
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
    },
    /// ⭐ 事务 v1 (F61): BEGIN / START TRANSACTION.
    /// ⭐ v2 (F62): 可选隔离级别与读写属性尾缀.
    Begin { iso: Option<TxnIso>, read_only: Option<bool> },
    /// ⭐ 事务 v1 (F61): COMMIT.
    Commit,
    /// ⭐ 事务 v1 (F61): ROLLBACK.
    Rollback,
    /// ⭐ v2 (F62): SET [SESSION] TRANSACTION ... (session=连接默认 / 否则当前事务).
    SetTransaction { iso: Option<TxnIso>, read_only: Option<bool>, session: bool },
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
// tokenizer
// =====================================================================

#[derive(Debug, Clone, PartialEq)]
enum Tok {
    /// 标识符/关键字 (原样保留, 关键字比较用 eq_ignore_ascii_case).
    Ident(String),
    /// 数字字面量 (原文, 含负号/小数点).
    Num(String),
    /// '字符串' (已解转义).
    Str(Vec<u8>),
    LParen,
    RParen,
    Comma,
    Star,
    Eq,
    Gt,
    Ge,
    Lt,
    Le,
    /// ⭐ S2: `!=` / `<>`
    Ne,
    /// ⭐ P1: `?` 占位符 (MySQL 风格, 按出现序编号).
    Question,
    /// ⭐ P1: `$n` 占位符 (PG 风格, 1-based 显式编号).
    Dollar(u16),
}

fn tokenize(input: &str) -> Result<Vec<Tok>, String> {
    let b = input.as_bytes();
    let mut toks = Vec::new();
    let mut i = 0usize;
    while i < b.len() {
        let c = b[i];
        match c {
            b' ' | b'\t' | b'\r' | b'\n' => i += 1,
            b'(' => {
                toks.push(Tok::LParen);
                i += 1;
            }
            b')' => {
                toks.push(Tok::RParen);
                i += 1;
            }
            b',' => {
                toks.push(Tok::Comma);
                i += 1;
            }
            b'*' => {
                toks.push(Tok::Star);
                i += 1;
            }
            b'=' => {
                toks.push(Tok::Eq);
                i += 1;
            }
            b'>' => {
                if b.get(i + 1) == Some(&b'=') {
                    toks.push(Tok::Ge);
                    i += 2;
                } else {
                    toks.push(Tok::Gt);
                    i += 1;
                }
            }
            b'<' => {
                if b.get(i + 1) == Some(&b'=') {
                    toks.push(Tok::Le);
                    i += 2;
                } else if b.get(i + 1) == Some(&b'>') {
                    // ⭐ S2: `<>` = 不等
                    toks.push(Tok::Ne);
                    i += 2;
                } else {
                    toks.push(Tok::Lt);
                    i += 1;
                }
            }
            b'!' => {
                // ⭐ S2: `!=`
                if b.get(i + 1) == Some(&b'=') {
                    toks.push(Tok::Ne);
                    i += 2;
                } else {
                    return Err("unexpected '!'".into());
                }
            }
            b'?' => {
                // ⭐ P1: 预处理占位符
                toks.push(Tok::Question);
                i += 1;
            }
            b'$' => {
                // ⭐ P1: $n (1-based)
                let start = i + 1;
                let mut j = start;
                while j < b.len() && b[j].is_ascii_digit() {
                    j += 1;
                }
                if j == start {
                    return Err("expected digits after '$'".into());
                }
                let n: u16 = std::str::from_utf8(&b[start..j])
                    .ok()
                    .and_then(|s| s.parse().ok())
                    .filter(|&n| n >= 1)
                    .ok_or("bad $n placeholder")?;
                toks.push(Tok::Dollar(n));
                i = j;
            }
            b'\'' => {
                // '字符串', '' 转义为单引号
                let mut s = Vec::new();
                i += 1;
                loop {
                    match b.get(i) {
                        None => return Err("unterminated string literal".into()),
                        Some(b'\'') => {
                            if b.get(i + 1) == Some(&b'\'') {
                                s.push(b'\'');
                                i += 2;
                            } else {
                                i += 1;
                                break;
                            }
                        }
                        Some(&ch) => {
                            s.push(ch);
                            i += 1;
                        }
                    }
                }
                toks.push(Tok::Str(s));
            }
            b'-' | b'0'..=b'9' => {
                // 数字 (负号/小数点/科学计数不含 e, v1 够用)
                let start = i;
                i += 1;
                while i < b.len() && (b[i].is_ascii_digit() || b[i] == b'.') {
                    i += 1;
                }
                let t = &input[start..i];
                if t == "-" {
                    return Err("bare '-' is not a number".into());
                }
                toks.push(Tok::Num(t.to_string()));
            }
            _ if c.is_ascii_alphabetic() || c == b'_' => {
                let start = i;
                while i < b.len() && (b[i].is_ascii_alphanumeric() || b[i] == b'_' || b[i] == b'.')
                {
                    i += 1;
                }
                toks.push(Tok::Ident(input[start..i].to_string()));
            }
            // ⭐ F66: 反引号标识符 `name` (MySQL 引用; SQLAlchemy SHOW ... FROM `db`)
            b'`' => {
                i += 1;
                let start = i;
                while i < b.len() && b[i] != b'`' {
                    i += 1;
                }
                if i >= b.len() {
                    return Err("unterminated `identifier`".into());
                }
                toks.push(Tok::Ident(input[start..i].to_string()));
                i += 1; // 跳过右反引号
            }
            _ => return Err(format!("unexpected character '{}'", c as char)),
        }
    }
    Ok(toks)
}

// =====================================================================
// parser (顺序读取器)
// =====================================================================

struct P {
    toks: Vec<Tok>,
    i: usize,
    /// ⭐ P1: `?` 自动编号计数.
    next_param: u16,
    /// ⭐ P1: 占位符风格混用检测 (?/$ 二选一).
    saw_question: bool,
    saw_dollar: bool,
}

impl P {
    fn peek(&self) -> Option<&Tok> {
        self.toks.get(self.i)
    }

    fn next(&mut self) -> Result<Tok, String> {
        let t = self.toks.get(self.i).cloned().ok_or("unexpected end of statement")?;
        self.i += 1;
        Ok(t)
    }

    /// 消费一个关键字 (大小写不敏感), 不匹配报错.
    fn kw(&mut self, want: &str) -> Result<(), String> {
        match self.next()? {
            Tok::Ident(s) if s.eq_ignore_ascii_case(want) => Ok(()),
            other => Err(format!("expected {want}, got {other:?}")),
        }
    }

    /// 试探关键字: 匹配则消费返回 true.
    /// ⭐ G1 (F63): 比较算子 (HAVING 用, 与 WHERE 同集去 IN/BETWEEN/LIKE).
    fn cmp_op(&mut self) -> Result<CmpOp, String> {
        match self.next()? {
            Tok::Eq => Ok(CmpOp::Eq),
            Tok::Gt => Ok(CmpOp::Gt),
            Tok::Ge => Ok(CmpOp::Ge),
            Tok::Lt => Ok(CmpOp::Lt),
            Tok::Le => Ok(CmpOp::Le),
            Tok::Ne => Ok(CmpOp::Ne),
            other => Err(format!("expected comparison operator, got {other:?}")),
        }
    }

    fn try_kw(&mut self, want: &str) -> bool {
        if let Some(Tok::Ident(s)) = self.peek()
            && s.eq_ignore_ascii_case(want)
        {
            self.i += 1;
            return true;
        }
        false
    }

    fn ident(&mut self) -> Result<String, String> {
        match self.next()? {
            Tok::Ident(s) => Ok(s),
            other => Err(format!("expected identifier, got {other:?}")),
        }
    }

    fn expect(&mut self, want: &Tok, what: &str) -> Result<(), String> {
        let t = self.next()?;
        if &t == want { Ok(()) } else { Err(format!("expected {what}, got {t:?}")) }
    }

    fn value(&mut self) -> Result<SqlValue, String> {
        match self.next()? {
            Tok::Num(n) => {
                if n.contains('.') {
                    n.parse::<f64>().map(SqlValue::Float).map_err(|_| format!("bad number {n}"))
                } else {
                    n.parse::<i64>().map(SqlValue::Int).map_err(|_| format!("bad integer {n}"))
                }
            }
            Tok::Str(s) => Ok(SqlValue::Str(s)),
            Tok::Ident(s) if s.eq_ignore_ascii_case("NULL") => Ok(SqlValue::Null),
            // ⭐ P1: 占位符 → Param (0-based; ?/$ 混用报错)
            Tok::Question => {
                if self.saw_dollar {
                    return Err("cannot mix ? and $n placeholders".into());
                }
                self.saw_question = true;
                let idx = self.next_param;
                self.next_param += 1;
                Ok(SqlValue::Param(idx))
            }
            Tok::Dollar(n) => {
                if self.saw_question {
                    return Err("cannot mix ? and $n placeholders".into());
                }
                self.saw_dollar = true;
                self.next_param = self.next_param.max(n);
                Ok(SqlValue::Param(n - 1))
            }
            other => Err(format!("expected literal, got {other:?}")),
        }
    }

    fn done(&self) -> Result<(), String> {
        if self.i == self.toks.len() {
            Ok(())
        } else {
            Err(format!("trailing tokens after statement: {:?}", &self.toks[self.i..]))
        }
    }

    /// ⭐ F71: 仅顶层语句校验尾部无残余 token; 子查询 (top=false) 不校验.
    fn done_if(&self, top: bool) -> Result<(), String> {
        if top { self.done() } else { Ok(()) }
    }

    /// ⭐ F71: 当前是 `( SELECT` (子查询开头), 区分于 `(` 分组/字面量列表.
    fn peek_paren_select(&self) -> bool {
        self.peek() == Some(&Tok::LParen)
            && matches!(self.toks.get(self.i + 1), Some(Tok::Ident(s)) if s.eq_ignore_ascii_case("SELECT"))
    }
}

/// 入口: RESP 参数 join 后的完整语句 → AST.
/// 首关键字必须是 CREATE / INSERT / SELECT (caller 已粗判).
pub fn parse(input: &[u8]) -> Result<SqlStmt, String> {
    let (stmt, params) = parse_prepared(input)?;
    if params > 0 {
        return Err("placeholders are not allowed outside prepared statements".into());
    }
    Ok(stmt)
}

/// ⭐ P1: 预处理入口 — 允许 ?/$n 占位符, 返回 (模板, 参数个数).
/// ⭐ v2 (F62): 解析事务属性尾缀 — `[ISOLATION LEVEL <lvl>] [READ ONLY|READ
/// WRITE]` 任意顺序, 逗号可选 (PG/MySQL 两方言均容).
fn parse_txn_attrs(p: &mut P) -> Result<(Option<TxnIso>, Option<bool>), String> {
    let mut iso: Option<TxnIso> = None;
    let mut read_only: Option<bool> = None;
    loop {
        if matches!(p.peek(), Some(Tok::Comma)) {
            p.i += 1;
        }
        if p.try_kw("ISOLATION") {
            p.kw("LEVEL")?;
            iso = Some(if p.try_kw("SERIALIZABLE") {
                TxnIso::Serializable
            } else if p.try_kw("REPEATABLE") {
                p.kw("READ")?;
                TxnIso::Serializable // RR 归并 (本实现中与 SER 等价)
            } else if p.try_kw("READ") {
                // COMMITTED 与 UNCOMMITTED 均归并 RC (PG 同款把 RU 当 RC)
                #[allow(clippy::if_same_then_else)]
                if p.try_kw("COMMITTED") {
                    TxnIso::ReadCommitted
                } else if p.try_kw("UNCOMMITTED") {
                    TxnIso::ReadCommitted
                } else {
                    return Err("expected COMMITTED / UNCOMMITTED".into());
                }
            } else {
                return Err(
                    "expected SERIALIZABLE / REPEATABLE READ / READ COMMITTED / READ UNCOMMITTED"
                        .into(),
                );
            });
        } else if p.try_kw("READ") {
            if p.try_kw("ONLY") {
                read_only = Some(true);
            } else if p.try_kw("WRITE") {
                read_only = Some(false);
            } else {
                return Err("expected ONLY / WRITE after READ".into());
            }
        } else {
            break;
        }
    }
    Ok((iso, read_only))
}

pub fn parse_prepared(input: &[u8]) -> Result<(SqlStmt, u16), String> {
    let text = std::str::from_utf8(input).map_err(|_| "statement is not valid UTF-8")?;
    let head = text.trim_start();
    // ⭐ F66: `SELECT @@var...` 系统变量探测 (SQLAlchemy 方言初始化发;
    // '@' 不过 tokenizer, tokenize 前拦). 提取 @@ 变量名, 其余忽略.
    {
        let hu = head.to_ascii_uppercase();
        if hu.starts_with("SELECT") && head.contains("@@") {
            let mut vars = Vec::new();
            let bytes = head.as_bytes();
            let mut i = 0;
            while i < bytes.len() {
                if bytes[i] == b'@' && i + 1 < bytes.len() && bytes[i + 1] == b'@' {
                    let mut j = i + 2;
                    while j < bytes.len()
                        && (bytes[j].is_ascii_alphanumeric() || bytes[j] == b'_' || bytes[j] == b'.')
                    {
                        j += 1;
                    }
                    vars.push(head[i + 2..j].to_string());
                    i = j;
                } else {
                    i += 1;
                }
            }
            if !vars.is_empty() {
                return Ok((SqlStmt::SystemVarStub { vars }, 0));
            }
        }
    }
    // ⭐ P4: SET 语句在 tokenize 前整吞 (驱动噪声如 `SET @@session.autocommit=1`
    // 含 tokenizer 不认识的 '@'; 语义本就忽略)
    // ⭐ v2 (F62): 例外 — SET [SESSION] TRANSACTION ... 剔出解析 (隔离级别标准)
    if head.len() >= 3
        && head[..3].eq_ignore_ascii_case("SET")
        && head[3..].starts_with([' ', '\t', '\n'])
    {
        let rest = head[3..].trim_start();
        let rest_upper = rest.to_ascii_uppercase();
        let (session, body) = if let Some(b) = rest_upper.strip_prefix("SESSION ") {
            (true, b.trim_start().to_string())
        } else {
            (false, rest_upper.clone())
        };
        if body.starts_with("TRANSACTION") {
            let toks = tokenize(rest)?;
            let mut p = P { toks, i: 0, next_param: 0, saw_question: false, saw_dollar: false };
            let _ = p.try_kw("SESSION");
            p.kw("TRANSACTION")?;
            let (iso, read_only) = parse_txn_attrs(&mut p)?;
            p.done()?;
            if iso.is_none() && read_only.is_none() {
                return Err("SET TRANSACTION: expected ISOLATION LEVEL / READ ONLY".into());
            }
            // MySQL 方言: SET TRANSACTION (无 SESSION) 作用于下一个/当前事务;
            // PG 同形态作用于当前事务 — worker 按 session 标志分流
            return Ok((SqlStmt::SetTransaction { iso, read_only, session }, 0));
        }
        return Ok((SqlStmt::SetStub, 0));
    }
    let toks = tokenize(text)?;
    let mut p = P { toks, i: 0, next_param: 0, saw_question: false, saw_dollar: false };
    let stmt = match p.peek() {
        Some(Tok::Ident(s)) if s.eq_ignore_ascii_case("CREATE") => parse_create(&mut p),
        Some(Tok::Ident(s)) if s.eq_ignore_ascii_case("INSERT") => parse_insert(&mut p),
        Some(Tok::Ident(s)) if s.eq_ignore_ascii_case("SELECT") => parse_select(&mut p, true),
        Some(Tok::Ident(s)) if s.eq_ignore_ascii_case("SHOW") => parse_show(&mut p),
        Some(Tok::Ident(s)) if s.eq_ignore_ascii_case("DELETE") => parse_delete(&mut p),
        Some(Tok::Ident(s)) if s.eq_ignore_ascii_case("UPDATE") => parse_update(&mut p),
        Some(Tok::Ident(s)) if s.eq_ignore_ascii_case("DROP") => parse_drop(&mut p),
        // ⭐ S3: 工具命令
        Some(Tok::Ident(s)) if s.eq_ignore_ascii_case("USE") => {
            p.next()?;
            let db = p.ident()?;
            p.done()?;
            Ok(SqlStmt::Use { db })
        }
        Some(Tok::Ident(s))
            if s.eq_ignore_ascii_case("DESCRIBE") || s.eq_ignore_ascii_case("DESC") =>
        {
            p.next()?;
            let table = p.ident()?;
            p.done()?;
            Ok(SqlStmt::Describe { table })
        }
        // SET ... → 全吞忽略 (psql/驱动启动噪声); TRANSACTION 子句已在
        // tokenize 前剔出 (见 parse_prepared 头部)
        Some(Tok::Ident(s)) if s.eq_ignore_ascii_case("SET") => Ok(SqlStmt::SetStub),
        // ⭐ 事务 v1/v2: BEGIN / START TRANSACTION [尾缀] / COMMIT / ROLLBACK [TO]
        Some(Tok::Ident(s)) if s.eq_ignore_ascii_case("BEGIN") => {
            p.next()?;
            let _ = p.try_kw("WORK"); // BEGIN WORK 兼容
            let (iso, read_only) = parse_txn_attrs(&mut p)?;
            p.done()?;
            Ok(SqlStmt::Begin { iso, read_only })
        }
        Some(Tok::Ident(s)) if s.eq_ignore_ascii_case("START") => {
            p.next()?;
            p.kw("TRANSACTION")?;
            let (iso, read_only) = parse_txn_attrs(&mut p)?;
            p.done()?;
            Ok(SqlStmt::Begin { iso, read_only })
        }
        Some(Tok::Ident(s)) if s.eq_ignore_ascii_case("COMMIT") => {
            p.next()?;
            let _ = p.try_kw("WORK");
            p.done()?;
            Ok(SqlStmt::Commit)
        }
        Some(Tok::Ident(s)) if s.eq_ignore_ascii_case("ROLLBACK") => {
            p.next()?;
            let _ = p.try_kw("WORK");
            if p.try_kw("TO") {
                let _ = p.try_kw("SAVEPOINT");
                let name = p.ident()?;
                p.done()?;
                Ok(SqlStmt::RollbackTo { name })
            } else {
                p.done()?;
                Ok(SqlStmt::Rollback)
            }
        }
        // ⭐ v2 (F62): SAVEPOINT / RELEASE
        Some(Tok::Ident(s)) if s.eq_ignore_ascii_case("SAVEPOINT") => {
            p.next()?;
            let name = p.ident()?;
            p.done()?;
            Ok(SqlStmt::Savepoint { name })
        }
        Some(Tok::Ident(s)) if s.eq_ignore_ascii_case("RELEASE") => {
            p.next()?;
            let _ = p.try_kw("SAVEPOINT");
            let name = p.ident()?;
            p.done()?;
            Ok(SqlStmt::Release { name })
        }
        _ => Err("expected CREATE / INSERT / SELECT / DELETE / UPDATE / DROP / USE / DESCRIBE / SET / BEGIN / COMMIT / ROLLBACK / SAVEPOINT".into()),
    }?;
    Ok((stmt, p.next_param))
}

/// ⭐ P1: 参数绑定 — 深拷贝模板, 替换全部 Param(i) 为 params[i].
/// 个数不符/越界/params 内含 Param 报错.
pub fn bind_params(stmt: &SqlStmt, params: &[SqlValue]) -> Result<SqlStmt, String> {
    if params.iter().any(|v| matches!(v, SqlValue::Param(_))) {
        return Err("bind value cannot be a placeholder".into());
    }
    let subst = |v: &SqlValue| -> Result<SqlValue, String> {
        match v {
            SqlValue::Param(i) => params
                .get(*i as usize)
                .cloned()
                .ok_or_else(|| format!("missing parameter {}", i + 1)),
            // ⭐ F71: 子查询内层递归绑定 (占位符编号全局连续, 同一 params)
            SqlValue::Subquery(s) => {
                Ok(SqlValue::Subquery(Box::new(bind_params(s, params)?)))
            }
            // ⭐ F74: 列引用原样 (decorrelate 前不参与绑定)
            SqlValue::ColRef(_) => Ok(v.clone()),
            other => Ok(other.clone()),
        }
    };
    let bind_cond = |c: &Cond| -> Result<Cond, String> {
        Ok(Cond {
            col: c.col.clone(),
            op: c.op,
            val: subst(&c.val)?,
            set: c.set.iter().map(&subst).collect::<Result<_, _>>()?,
        })
    };
    let bind_conds = |pred: &Pred<Cond>| -> Result<Pred<Cond>, String> { pred.try_map(&bind_cond) };
    Ok(match stmt {
        SqlStmt::Insert { table, cols, rows } => SqlStmt::Insert {
            table: table.clone(),
            cols: cols.clone(),
            rows: rows
                .iter()
                .map(|r| r.iter().map(&subst).collect::<Result<_, _>>())
                .collect::<Result<_, _>>()?,
        },
        SqlStmt::Select { table, items, conds, limit, order, offset, group_by, having } => {
            SqlStmt::Select {
                table: table.clone(),
                items: items.clone(),
                conds: bind_conds(conds)?,
                limit: *limit,
                order: order.clone(),
                offset: *offset,
                group_by: group_by.clone(),
                having: bind_conds(having)?,
            }
        }
        SqlStmt::Delete { table, conds } => SqlStmt::Delete {
            table: table.clone(),
            conds: bind_conds(conds)?,
        },
        SqlStmt::Update { table, sets, conds } => SqlStmt::Update {
            table: table.clone(),
            sets: sets
                .iter()
                .map(|(c, v)| Ok::<_, String>((c.clone(), subst(v)?)))
                .collect::<Result<_, _>>()?,
            conds: bind_conds(conds)?,
        },
        // ⭐ F67/F68 (JOIN): 替换 WHERE 限定条件里的占位符 (ON/from/joins 无字面量)
        SqlStmt::SelectJoin { from, from_inner, joins, items, conds, order, limit, offset } => {
            SqlStmt::SelectJoin {
                from: from.clone(),
                // ⭐ F75: 派生表内层递归绑定
                from_inner: match from_inner {
                    Some(s) => Some(Box::new(bind_params(s, params)?)),
                    None => None,
                },
                joins: joins.clone(),
                items: items.clone(),
                conds: conds.try_map(&|c: &JoinCond| {
                    Ok::<_, String>(JoinCond {
                        col: c.col.clone(),
                        op: c.op,
                        val: subst(&c.val)?,
                        set: c.set.iter().map(&subst).collect::<Result<_, _>>()?,
                    })
                })?,
                order: order.clone(),
                limit: *limit,
                offset: *offset,
            }
        }
        // ⭐ F72: 派生表 — 内层递归绑定 + 外层 WHERE 绑定
        SqlStmt::SelectDerived { inner, alias, items, conds, order, limit, offset } => {
            SqlStmt::SelectDerived {
                inner: Box::new(bind_params(inner, params)?),
                alias: alias.clone(),
                items: items.clone(),
                conds: bind_conds(conds)?,
                order: order.clone(),
                limit: *limit,
                offset: *offset,
            }
        }
        // 无参数位的语句原样克隆
        other => other.clone(),
    })
}

/// `CREATE TABLE t (col TYPE [PRIMARY KEY] [NOT NULL], ..., INDEX(col), ...)`
fn parse_create(p: &mut P) -> Result<SqlStmt, String> {
    p.kw("CREATE")?;
    p.kw("TABLE")?;
    let table = p.ident()?;
    p.expect(&Tok::LParen, "(")?;

    let mut columns: Vec<Column> = Vec::new();
    let mut pk: Option<u16> = None;
    let mut index_names: Vec<String> = Vec::new();
    let mut unique_names: Vec<String> = Vec::new(); // ⭐ O3
    let mut global_unique_names: Vec<String> = Vec::new(); // ⭐ F65
    loop {
        if p.try_kw("INDEX") {
            p.expect(&Tok::LParen, "(")?;
            index_names.push(p.ident()?);
            p.expect(&Tok::RParen, ")")?;
        } else {
            let name = p.ident()?;
            let ty_name = p.ident()?;
            let ty = match ty_name.to_ascii_uppercase().as_str() {
                "INT" | "BIGINT" | "INTEGER" | "SMALLINT" => ColType::I64,
                // ⭐ S3: BOOLEAN → I64 (0/1); PG `DOUBLE PRECISION` 双词在下方吞
                "BOOLEAN" | "BOOL" => ColType::I64,
                "DOUBLE" | "FLOAT" | "REAL" => ColType::F64,
                "TEXT" | "VARCHAR" | "CHAR" | "STRING" => ColType::Str,
                "BLOB" | "BYTES" | "BYTEA" => ColType::Bytes, // ⭐ S3: PG BYTEA
                other => return Err(format!("unknown type {other}")),
            };
            // ⭐ S3: 方言噪声 — `DOUBLE PRECISION` 第二词 / `VARCHAR(n)` 长度参数
            if ty_name.eq_ignore_ascii_case("DOUBLE") {
                p.try_kw("PRECISION");
            }
            if p.peek() == Some(&Tok::LParen) {
                p.next()?;
                match p.next()? {
                    Tok::Num(_) => {}
                    other => return Err(format!("expected type length, got {other:?}")),
                }
                p.expect(&Tok::RParen, ")")?;
            }
            let mut nullable = true;
            let mut is_pk = false;
            loop {
                if p.try_kw("PRIMARY") {
                    p.kw("KEY")?;
                    is_pk = true;
                    nullable = false;
                } else if p.try_kw("NOT") {
                    p.kw("NULL")?;
                    nullable = false;
                } else if p.try_kw("GLOBAL") {
                    // ⭐ F65: 列级 GLOBAL UNIQUE = 跨 shard 全局唯一 (email-shard 占坑)
                    p.kw("UNIQUE")?;
                    global_unique_names.push(name.clone());
                    nullable = false;
                } else if p.try_kw("UNIQUE") {
                    // ⭐ O3: 列级 UNIQUE = 自动建唯一索引 (隐含 NOT NULL —
                    // NULL 不入索引, 无法参与唯一性, 直接拒绝更诚实)
                    unique_names.push(name.clone());
                    nullable = false;
                } else {
                    break;
                }
            }
            if is_pk {
                if pk.is_some() {
                    return Err("multiple PRIMARY KEY".into());
                }
                pk = Some(columns.len() as u16);
            }
            columns.push(Column { name, ty, nullable });
        }
        match p.next()? {
            Tok::Comma => continue,
            Tok::RParen => break,
            other => return Err(format!("expected ',' or ')', got {other:?}")),
        }
    }
    p.done()?;

    let pk = pk.ok_or("PRIMARY KEY required")?;
    let col_pos = |n: &str, what: &str| -> Result<u16, String> {
        columns
            .iter()
            .position(|c| c.name == n)
            .map(|i| i as u16)
            .ok_or_else(|| format!("{what} on unknown column {n}"))
    };
    let mut index_cols: Vec<u16> = Vec::with_capacity(index_names.len());
    for n in &index_names {
        index_cols.push(col_pos(n, "INDEX")?);
    }
    let mut unique_cols: Vec<u16> = Vec::with_capacity(unique_names.len());
    for n in &unique_names {
        unique_cols.push(col_pos(n, "UNIQUE")?);
    }
    let mut global_unique_cols: Vec<u16> = Vec::with_capacity(global_unique_names.len());
    for n in &global_unique_names {
        global_unique_cols.push(col_pos(n, "GLOBAL UNIQUE")?);
    }
    let schema = TableSchema::new(columns, pk, &index_cols, &unique_cols, &global_unique_cols)
        .map_err(|e| e.to_string())?;
    Ok(SqlStmt::CreateTable { table, schema })
}

/// `INSERT INTO t [(c1,...)] VALUES (v1,...)`
fn parse_insert(p: &mut P) -> Result<SqlStmt, String> {
    p.kw("INSERT")?;
    p.kw("INTO")?;
    let table = p.ident()?;
    let mut cols: Vec<String> = Vec::new();
    if p.peek() == Some(&Tok::LParen) {
        p.next()?;
        loop {
            cols.push(p.ident()?);
            match p.next()? {
                Tok::Comma => continue,
                Tok::RParen => break,
                other => return Err(format!("expected ',' or ')', got {other:?}")),
            }
        }
    }
    p.kw("VALUES")?;
    // ⭐ S1: 多行 VALUES (v1,...), (v2,...), ...
    let mut rows: Vec<Vec<SqlValue>> = Vec::new();
    loop {
        p.expect(&Tok::LParen, "(")?;
        let mut vals = Vec::new();
        loop {
            vals.push(p.value()?);
            match p.next()? {
                Tok::Comma => continue,
                Tok::RParen => break,
                other => return Err(format!("expected ',' or ')', got {other:?}")),
            }
        }
        if !cols.is_empty() && cols.len() != vals.len() {
            return Err("column list and VALUES arity mismatch".into());
        }
        if let Some(first) = rows.first()
            && first.len() != vals.len()
        {
            return Err("VALUES rows have inconsistent arity".into());
        }
        rows.push(vals);
        if p.peek() == Some(&Tok::Comma) {
            p.next()?;
        } else {
            break;
        }
    }
    p.done()?;
    Ok(SqlStmt::Insert { table, cols, rows })
}

/// ⭐ F71: 解 `( SELECT ... )` 子查询 (LParen 未消费), 返回内层 stmt.
fn parse_paren_subselect(p: &mut P) -> Result<Box<SqlStmt>, String> {
    p.expect(&Tok::LParen, "(")?;
    let inner = parse_select(p, false)?;
    p.expect(&Tok::RParen, ")")?;
    Ok(Box::new(inner))
}

/// ⭐ F72: 派生表叶子含子查询判定 (外层 WHERE 不允许嵌套子查询).
fn cond_has_subquery(c: &Cond) -> bool {
    matches!(c.val, SqlValue::Subquery(_))
        || c.set.iter().any(|v| matches!(v, SqlValue::Subquery(_)))
        || c.col == EXISTS_SENTINEL_COL
}

fn pred_has_subquery(pred: &Pred<Cond>) -> bool {
    pred.leaves().iter().any(|c| cond_has_subquery(c))
}

/// ⭐ F72: FROM 派生表 `(SELECT ...) [AS] alias [WHERE ...] [ORDER/LIMIT/OFFSET]`.
/// 外层投影 items 已在 FROM 前解完 (传入). v1: 无聚合投影; 无别名报错;
/// 外层 WHERE 不得含子查询 (双层编排留后).
fn parse_derived(p: &mut P, items: Vec<SelectItem>, top: bool) -> Result<SqlStmt, String> {
    let inner = parse_paren_subselect(p)?;
    let alias =
        parse_opt_alias(p).ok_or_else(|| "every derived table must have its own alias".to_string())?;
    // ⭐ F75: 派生表参与 JOIN — 别名后接 JOIN 子句 → 走 JOIN 主体 (from=派生表)
    if is_join_ahead(p) {
        let from = TableRef { table: alias.clone(), alias };
        return parse_join_from(p, items, from, Some(inner));
    }
    if items.iter().any(|i| matches!(i, SelectItem::Agg { .. })) {
        // v1 特判: 唯一投影项为 COUNT(*) 允许 (行数统计); 其余聚合拒
        let lone_count = items.len() == 1
            && matches!(&items[0], SelectItem::Agg { func: AggFn::Count, col: None });
        if !lone_count {
            return Err("aggregate on derived table is not supported (v1, except lone COUNT(*))".into());
        }
    }
    let conds = parse_where(p)?;
    if pred_has_subquery(&conds) {
        return Err("subquery in derived-table outer WHERE is not supported (v1)".into());
    }
    let (order, limit, offset) = parse_select_tail(p)?;
    p.done_if(top)?;
    Ok(SqlStmt::SelectDerived { inner, alias, items, conds, order, limit, offset })
}

/// WHERE 子句 (AND 平铺; caller 决定是否必带).
/// ⭐ S2: BETWEEN → Ge+Le, LIKE 'p%' → 前缀范围 (解析期 desugar);
/// IN → CmpOp::In (set); `!=`/`<>` → Ne.
fn parse_where(p: &mut P) -> Result<Pred<Cond>, String> {
    if p.try_kw("WHERE") {
        parse_or_expr(p)
    } else {
        Ok(Pred::And(Vec::new())) // 无 WHERE = 恒真
    }
}

/// ⭐ F69: OR 层 (最低优先级).
fn parse_or_expr(p: &mut P) -> Result<Pred<Cond>, String> {
    let mut terms = vec![parse_and_expr(p)?];
    while p.try_kw("OR") {
        terms.push(parse_and_expr(p)?);
    }
    Ok(if terms.len() == 1 { terms.pop().unwrap() } else { Pred::Or(terms) })
}

/// ⭐ F69: AND 层.
fn parse_and_expr(p: &mut P) -> Result<Pred<Cond>, String> {
    let mut terms = vec![parse_not_expr(p)?];
    while p.try_kw("AND") {
        terms.push(parse_not_expr(p)?);
    }
    Ok(if terms.len() == 1 { terms.pop().unwrap() } else { Pred::And(terms) })
}

/// ⭐ F69: NOT 层.
fn parse_not_expr(p: &mut P) -> Result<Pred<Cond>, String> {
    if p.try_kw("NOT") {
        Ok(Pred::Not(Box::new(parse_not_expr(p)?)))
    } else {
        parse_primary(p)
    }
}

/// ⭐ F69: primary = `( <or_expr> )` | EXISTS 子查询 | 单个比较叶子.
fn parse_primary(p: &mut P) -> Result<Pred<Cond>, String> {
    // ⭐ F71: EXISTS (SELECT ...) — 哨兵列名区分; NOT EXISTS 由 parse_not_expr 包 Pred::Not
    if matches!(p.peek(), Some(Tok::Ident(s)) if s.eq_ignore_ascii_case("EXISTS")) {
        p.next()?;
        let stmt = parse_paren_subselect(p)?;
        return Ok(Pred::Leaf(Cond {
            col: EXISTS_SENTINEL_COL.to_string(),
            op: CmpOp::Eq,
            val: SqlValue::Subquery(stmt),
            set: vec![],
        }));
    }
    if p.peek() == Some(&Tok::LParen) {
        p.next()?;
        let inner = parse_or_expr(p)?;
        p.expect(&Tok::RParen, ")")?;
        Ok(inner)
    } else {
        parse_where_atom(p)
    }
}

/// ⭐ F69: 单个比较谓词 `col op val / IN / BETWEEN / LIKE`.
/// BETWEEN/LIKE desugar 产物 (多条) 包为 `And(vec![Leaf,..])`; 单条 → `Leaf`.
fn parse_where_atom(p: &mut P) -> Result<Pred<Cond>, String> {
    let mut conds: Vec<Cond> = Vec::new();
    let col = p.ident()?;
    // ⭐ F71: col [NOT] IN (...) — NOT IN 包 Pred::Not; 子查询与字面量列表两路
    let negated_in = p.try_kw("NOT");
    if p.try_kw("IN") {
        // ⭐ F71: IN (SELECT ...) → Subquery 占位 (dispatch 前折叠为 set)
        if p.peek_paren_select() {
            let stmt = parse_paren_subselect(p)?;
            let leaf = Pred::Leaf(Cond {
                col,
                op: CmpOp::In,
                val: SqlValue::Subquery(stmt),
                set: vec![],
            });
            return Ok(if negated_in { Pred::Not(Box::new(leaf)) } else { leaf });
        }
        // 字面量列表
        p.expect(&Tok::LParen, "(")?;
        let mut set = Vec::new();
        loop {
            let v = p.value()?;
            if v == SqlValue::Null {
                return Err("NULL is not valid in IN list".into());
            }
            set.push(v);
            match p.next()? {
                Tok::Comma => continue,
                Tok::RParen => break,
                other => return Err(format!("expected ',' or ')', got {other:?}")),
            }
        }
        if set.is_empty() {
            return Err("empty IN list".into());
        }
        sort_in_set(&mut set); // ⭐ F73: 大集合求值二分化
        let leaf = Pred::Leaf(Cond { col, op: CmpOp::In, val: SqlValue::Null, set });
        return Ok(if negated_in { Pred::Not(Box::new(leaf)) } else { leaf });
    } else if negated_in {
        return Err("expected IN after NOT".into());
    }
    if p.try_kw("BETWEEN") {
        // BETWEEN a AND b → col >= a AND col <= b (内部 AND 在此消费)
        let a = p.value()?;
        p.kw("AND")?;
        let b = p.value()?;
        if a == SqlValue::Null || b == SqlValue::Null {
            return Err("NULL is not a valid comparison bound".into());
        }
        conds.push(Cond { col: col.clone(), op: CmpOp::Ge, val: a, set: vec![] });
        conds.push(Cond { col, op: CmpOp::Le, val: b, set: vec![] });
    } else if p.try_kw("LIKE") {
        // 仅前缀模式 'p%' → [p, p+1) 字节范围 (与 starts_with 精确等价);
        // 无 '%' → 等值; 其它模式报错 (v1)
        let SqlValue::Str(pat) = p.value()? else {
            return Err("LIKE pattern must be a string".into());
        };
        let pct = pat.iter().position(|&b| b == b'%');
        match pct {
            None => {
                conds.push(Cond { col, op: CmpOp::Eq, val: SqlValue::Str(pat), set: vec![] });
            }
            Some(i) if i == pat.len() - 1 => {
                let prefix = pat[..i].to_vec();
                if prefix.is_empty() {
                    // LIKE '%' = 恒真, 不产条件
                } else {
                    conds.push(Cond {
                        col: col.clone(),
                        op: CmpOp::Ge,
                        val: SqlValue::Str(prefix.clone()),
                        set: vec![],
                    });
                    // 上界 = 前缀末个非 0xFF 字节 +1 截断; 全 0xFF → 无上界
                    let mut hi = prefix;
                    while hi.last() == Some(&0xFF) {
                        hi.pop();
                    }
                    if let Some(last) = hi.last_mut() {
                        *last += 1;
                        conds.push(Cond { col, op: CmpOp::Lt, val: SqlValue::Str(hi), set: vec![] });
                    }
                }
            }
            _ => return Err("LIKE supports only prefix patterns ('abc%')".into()),
        }
    } else {
        let op = match p.next()? {
            Tok::Eq => CmpOp::Eq,
            Tok::Gt => CmpOp::Gt,
            Tok::Ge => CmpOp::Ge,
            Tok::Lt => CmpOp::Lt,
            Tok::Le => CmpOp::Le,
            Tok::Ne => CmpOp::Ne,
            other => return Err(format!("expected comparison operator, got {other:?}")),
        };
        // ⭐ F71: col op (SELECT ...) — 标量子查询 (dispatch 前折叠为常量)
        if p.peek_paren_select() {
            let stmt = parse_paren_subselect(p)?;
            return Ok(Pred::Leaf(Cond { col, op, val: SqlValue::Subquery(stmt), set: vec![] }));
        }
        // ⭐ F74: col op ident (非 NULL) → ColRef (关联子查询相关列; decorrelate 前收集)
        if let Some(Tok::Ident(s)) = p.peek()
            && !s.eq_ignore_ascii_case("NULL")
        {
            let rhs = p.ident()?;
            return Ok(Pred::Leaf(Cond { col, op, val: SqlValue::ColRef(rhs), set: vec![] }));
        }
        let val = p.value()?;
        if val == SqlValue::Null {
            return Err("NULL is not a valid comparison bound".into());
        }
        conds.push(Cond { col, op, val, set: vec![] });
    }
    // 单条 → Leaf; 多条 (BETWEEN/LIKE desugar) → And; 空 (LIKE '%') → 恒真
    Ok(match conds.len() {
        1 => Pred::Leaf(conds.pop().unwrap()),
        _ => Pred::And(conds.into_iter().map(Pred::Leaf).collect()),
    })
}

/// ⭐ S1: `DELETE FROM t WHERE ...`
fn parse_delete(p: &mut P) -> Result<SqlStmt, String> {
    p.kw("DELETE")?;
    p.kw("FROM")?;
    let table = p.ident()?;
    let conds = parse_where(p)?;
    p.done()?;
    Ok(SqlStmt::Delete { table, conds })
}

/// ⭐ S1: `UPDATE t SET c = v [, c2 = v2 ...] WHERE ...`
fn parse_update(p: &mut P) -> Result<SqlStmt, String> {
    p.kw("UPDATE")?;
    let table = p.ident()?;
    p.kw("SET")?;
    let mut sets: Vec<(String, SqlValue)> = Vec::new();
    loop {
        let col = p.ident()?;
        p.expect(&Tok::Eq, "=")?;
        sets.push((col, p.value()?));
        if p.peek() == Some(&Tok::Comma) {
            p.next()?;
        } else {
            break;
        }
    }
    let conds = parse_where(p)?;
    p.done()?;
    Ok(SqlStmt::Update { table, sets, conds })
}

/// ⭐ S1: `DROP TABLE t`
fn parse_drop(p: &mut P) -> Result<SqlStmt, String> {
    p.kw("DROP")?;
    p.kw("TABLE")?;
    let table = p.ident()?;
    p.done()?;
    Ok(SqlStmt::DropTable { table })
}

/// ⭐ F66: 拆分系统表名 `catalog.table` — 仅 information_schema / pg_catalog
/// (大小写不敏); 返回 (小写 catalog, 小写 table). 非系统表回 None.
fn split_system_table(name: &str) -> Option<(String, String)> {
    let (cat, tbl) = name.split_once('.')?;
    let cat_l = cat.to_ascii_lowercase();
    if cat_l == "information_schema" || cat_l == "pg_catalog" {
        Some((cat_l, tbl.to_ascii_lowercase()))
    } else {
        None
    }
}

/// ⭐ F66: 解 SELECT 尾部 ORDER BY / LIMIT / OFFSET (系统表与普通表共用子集).
#[allow(clippy::type_complexity)]
fn parse_select_tail(
    p: &mut P,
) -> Result<(Vec<(String, bool)>, Option<u32>, Option<u32>), String> {
    let mut order: Vec<(String, bool)> = Vec::new();
    if p.try_kw("ORDER") {
        p.kw("BY")?;
        loop {
            let col = p.ident()?;
            let desc = if p.try_kw("DESC") {
                true
            } else {
                p.try_kw("ASC");
                false
            };
            order.push((col, desc));
            if p.peek() == Some(&Tok::Comma) {
                p.next()?;
            } else {
                break;
            }
        }
    }
    let mut limit = None;
    if p.try_kw("LIMIT") {
        match p.next()? {
            Tok::Num(n) => limit = Some(n.parse::<u32>().map_err(|_| format!("bad LIMIT {n}"))?),
            other => return Err(format!("expected LIMIT count, got {other:?}")),
        }
    }
    let mut offset = None;
    if p.try_kw("OFFSET") {
        match p.next()? {
            Tok::Num(n) => offset = Some(n.parse::<u32>().map_err(|_| format!("bad OFFSET {n}"))?),
            other => return Err(format!("expected OFFSET count, got {other:?}")),
        }
    }
    Ok((order, limit, offset))
}

/// ⭐ F66: SHOW [FULL] TABLES [FROM db] / SHOW [FULL] COLUMNS FROM t [FROM db]
/// / SHOW DATABASES|SCHEMAS — MySQL 反射 (SQLAlchemy 方言走此路).
/// 复用 SystemQuery, catalog="__show__", table 编码具体类型.
fn parse_show(p: &mut P) -> Result<SqlStmt, String> {
    p.kw("SHOW")?;
    let full = p.try_kw("FULL");
    let mk = |table: &str, conds: Pred<Cond>| SqlStmt::SystemQuery {
        catalog: "__show__".to_string(),
        table: table.to_string(),
        cols: Vec::new(),
        conds,
        order: Vec::new(),
        limit: None,
        offset: None,
    };
    // 内部标记 __table__ = 单叶子谓词
    let table_leaf = |table: String| {
        Pred::Leaf(Cond {
            col: "__table__".to_string(),
            op: CmpOp::Eq,
            val: SqlValue::Str(table.into_bytes()),
            set: Vec::new(),
        })
    };
    if p.try_kw("TABLES") {
        // [FROM|IN db] 忽略库名 (仅 current_db); 尾部可有 FROM db
        if p.try_kw("FROM") || p.try_kw("IN") {
            let _ = p.ident()?;
        }
        p.done()?;
        Ok(mk(if full { "full_tables" } else { "tables" }, Pred::And(Vec::new())))
    } else if p.try_kw("COLUMNS") || p.try_kw("FIELDS") {
        // FROM|IN t [FROM|IN db]
        if !(p.try_kw("FROM") || p.try_kw("IN")) {
            return Err("expected FROM after SHOW COLUMNS".into());
        }
        let table = p.ident()?;
        if p.try_kw("FROM") || p.try_kw("IN") {
            let _ = p.ident()?;
        }
        p.done()?;
        Ok(mk(if full { "full_columns" } else { "columns" }, table_leaf(table)))
    } else if p.try_kw("DATABASES") || p.try_kw("SCHEMAS") {
        p.done()?;
        Ok(mk("databases", Pred::And(Vec::new())))
    } else if p.try_kw("CREATE") {
        // SHOW CREATE TABLE t — SQLAlchemy MySQL 方言从 DDL 解析列
        p.kw("TABLE")?;
        let table = p.ident()?;
        p.done()?;
        Ok(mk("create_table", table_leaf(table)))
    } else {
        // 其他 SHOW (STATUS/VARIABLES/…) → 空结果 stub (工具探测容错)
        // 吞剩余 token
        while p.peek().is_some() {
            p.i += 1;
        }
        Ok(mk("__empty__", Pred::And(Vec::new())))
    }
}

/// ⭐ F67/F68 (JOIN): 判断左表名后是否跟着 JOIN (未来 3 token 内有 join 关键字).
/// (不消费; 覆盖 `t JOIN` / `t a JOIN` / `t AS a JOIN` 三种形态)
fn is_join_kw(t: Option<&Tok>) -> bool {
    matches!(t, Some(Tok::Ident(s))
        if s.eq_ignore_ascii_case("JOIN")
            || s.eq_ignore_ascii_case("INNER")
            || s.eq_ignore_ascii_case("LEFT")
            || s.eq_ignore_ascii_case("RIGHT")
            || s.eq_ignore_ascii_case("FULL")
            || s.eq_ignore_ascii_case("CROSS"))
}

fn is_join_ahead(p: &P) -> bool {
    // ⭐ F75: 扫描未来 3 token, 但遇 RParen (子查询边界) 即停 —
    // 防止内层 `(SELECT .. FROM u)` 误视外层 `) t JOIN` 为自身 JOIN.
    for off in 0..3 {
        match p.toks.get(p.i + off) {
            Some(Tok::RParen) => return false,
            t if is_join_kw(t) => return true,
            _ => {}
        }
    }
    false
}

/// ⭐ F67 (JOIN): 可选表别名 — `[AS] alias`; alias 不能是保留子句关键字.
fn parse_opt_alias(p: &mut P) -> Option<String> {
    if p.try_kw("AS") {
        return p.ident().ok();
    }
    if let Some(Tok::Ident(s)) = p.peek() {
        let up = s.to_ascii_uppercase();
        let reserved = matches!(
            up.as_str(),
            "JOIN" | "INNER" | "LEFT" | "RIGHT" | "FULL" | "OUTER" | "CROSS"
                | "ON" | "WHERE" | "ORDER" | "LIMIT" | "OFFSET" | "GROUP" | "HAVING" | "USING"
        );
        if !reserved {
            let a = s.clone();
            p.i += 1;
            return Some(a);
        }
    }
    None
}

/// ⭐ F68 (JOIN): 试解下一个 JOIN 种类 (已消费到 JOIN 关键字); 无则 None.
fn parse_join_kind(p: &mut P) -> Option<JoinKind> {
    if p.try_kw("CROSS") {
        let _ = p.kw("JOIN");
        Some(JoinKind::Cross)
    } else if p.try_kw("LEFT") {
        let _ = p.try_kw("OUTER");
        let _ = p.kw("JOIN");
        Some(JoinKind::Left)
    } else if p.try_kw("RIGHT") {
        let _ = p.try_kw("OUTER");
        let _ = p.kw("JOIN");
        Some(JoinKind::Right)
    } else if p.try_kw("FULL") {
        let _ = p.try_kw("OUTER");
        let _ = p.kw("JOIN");
        Some(JoinKind::Full)
    } else if p.try_kw("INNER") {
        let _ = p.kw("JOIN");
        Some(JoinKind::Inner)
    } else if p.try_kw("JOIN") {
        Some(JoinKind::Inner)
    } else {
        None
    }
}

/// ⭐ F68 (JOIN): 解 ON 谓词链 (AND 连接的 col op col; = → Eq, 其余 → Cmp).
fn parse_on(p: &mut P) -> Result<Vec<OnPred>, String> {
    let mut preds = Vec::new();
    loop {
        let left = QualCol::parse(&p.ident()?);
        let op = p.cmp_op()?;
        let right = QualCol::parse(&p.ident()?);
        if op == CmpOp::Eq {
            preds.push(OnPred::Eq(left, right));
        } else {
            preds.push(OnPred::Cmp { left, op, right });
        }
        if !p.try_kw("AND") {
            break;
        }
    }
    Ok(preds)
}

/// ⭐ F67/F68 (JOIN): `from [a] { [INNER|LEFT|RIGHT|FULL [OUTER]|CROSS] JOIN t [b]
/// (ON <conj> | USING (c,...)) }+ [WHERE ...] [ORDER BY ...] [LIMIT/OFFSET]`.
/// sel_items/first_table 已由 parse_select 消费.
fn parse_join(
    p: &mut P,
    sel_items: Vec<SelectItem>,
    first_table: String,
) -> Result<SqlStmt, String> {
    let first_alias = parse_opt_alias(p).unwrap_or_else(|| first_table.clone());
    let from = TableRef { table: first_table, alias: first_alias };
    parse_join_from(p, sel_items, from, None)
}

/// ⭐ F75: JOIN 主体 (from 已解析). from_inner=Some 时 from 为派生表.
fn parse_join_from(
    p: &mut P,
    sel_items: Vec<SelectItem>,
    from: TableRef,
    from_inner: Option<Box<SqlStmt>>,
) -> Result<SqlStmt, String> {
    let mut joins: Vec<JoinClause> = Vec::new();
    while let Some(kind) = parse_join_kind(p) {
        // ⭐ F75: JOIN 右侧派生表 v1 拒 (仅 FROM 位支持)
        if p.peek_paren_select() {
            return Err("derived table on JOIN right side is not supported (v1)".into());
        }
        let table = p.ident()?;
        let alias = parse_opt_alias(p).unwrap_or_else(|| table.clone());
        let on = if kind == JoinKind::Cross {
            Vec::new()
        } else if p.try_kw("USING") {
            // USING (c[,c]) → Eq(未限定 c, 右.c); 左侧限定由 worker 解析
            p.expect(&Tok::LParen, "(")?;
            let mut preds = Vec::new();
            loop {
                let c = p.ident()?;
                preds.push(OnPred::Eq(
                    QualCol { qualifier: None, col: c.clone() },
                    QualCol { qualifier: Some(alias.clone()), col: c },
                ));
                match p.next()? {
                    Tok::Comma => continue,
                    Tok::RParen => break,
                    other => return Err(format!("expected ',' or ')' in USING, got {other:?}")),
                }
            }
            preds
        } else {
            p.kw("ON")?;
            let preds = parse_on(p)?;
            if !preds.iter().any(|pr| matches!(pr, OnPred::Eq(..))) {
                return Err("JOIN ON requires at least one equality (col = col)".into());
            }
            preds
        };
        joins.push(JoinClause { kind, table: TableRef { table, alias }, on });
    }
    // WHERE / ORDER / LIMIT / OFFSET 复用单表解析后把列名转限定名
    let conds_raw = parse_where(p)?;
    let (order_raw, limit, offset) = parse_select_tail(p)?;
    p.done()?;
    let items: Vec<JoinItem> = sel_items
        .iter()
        .map(|it| match it {
            SelectItem::Col(s) => Ok(JoinItem::Col(QualCol::parse(s))),
            SelectItem::Agg { .. } => {
                Err("aggregate functions are not supported in JOIN queries".to_string())
            }
        })
        .collect::<Result<_, _>>()?;
    let conds = conds_raw.map(&|c: &Cond| JoinCond {
        col: QualCol::parse(&c.col),
        op: c.op,
        val: c.val.clone(),
        set: c.set.clone(),
    });
    let order = order_raw.into_iter().map(|(s, d)| (QualCol::parse(&s), d)).collect();
    Ok(SqlStmt::SelectJoin { from, from_inner, joins, items, conds, order, limit, offset })
}

/// ⭐ F69: HAVING 谓词树 (OR<AND<NOT<primary; 叶子 = 输出列 label op val).
fn parse_having_or(p: &mut P) -> Result<Pred<Cond>, String> {
    let mut terms = vec![parse_having_and(p)?];
    while p.try_kw("OR") {
        terms.push(parse_having_and(p)?);
    }
    Ok(if terms.len() == 1 { terms.pop().unwrap() } else { Pred::Or(terms) })
}

fn parse_having_and(p: &mut P) -> Result<Pred<Cond>, String> {
    let mut terms = vec![parse_having_not(p)?];
    while p.try_kw("AND") {
        terms.push(parse_having_not(p)?);
    }
    Ok(if terms.len() == 1 { terms.pop().unwrap() } else { Pred::And(terms) })
}

fn parse_having_not(p: &mut P) -> Result<Pred<Cond>, String> {
    if p.try_kw("NOT") {
        Ok(Pred::Not(Box::new(parse_having_not(p)?)))
    } else if p.peek() == Some(&Tok::LParen) {
        p.next()?;
        let inner = parse_having_or(p)?;
        p.expect(&Tok::RParen, ")")?;
        Ok(inner)
    } else {
        // 叶子: label [聚合 (形态)] op val
        let mut label = p.ident()?;
        if p.peek() == Some(&Tok::LParen) {
            p.next()?;
            label = label.to_ascii_uppercase();
            label.push('(');
            if p.peek() == Some(&Tok::Star) {
                p.next()?;
                label.push('*');
            } else {
                label.push_str(&p.ident()?);
            }
            p.expect(&Tok::RParen, ")")?;
            label.push(')');
        }
        let op = p.cmp_op()?;
        let val = p.value()?;
        Ok(Pred::Leaf(Cond { col: label, op, val, set: Vec::new() }))
    }
}

/// `SELECT * | COUNT(*) | c1, c2, ... FROM t [WHERE ...] [ORDER BY c [DESC], ...]
/// [LIMIT n] [OFFSET m]`. ⭐ F71: top=false 为子查询上下文 (不调 done, 不走 stub).
fn parse_select(p: &mut P, top: bool) -> Result<SqlStmt, String> {
    p.kw("SELECT")?;
    // ⭐ O1: 投影列表 (Star = 全列); ⭐ G1 (F63): 列/聚合函数混合项
    let mut items: Vec<SelectItem> = Vec::new();
    if p.peek() == Some(&Tok::Star) {
        p.next()?;
    } else if !top && matches!(p.peek(), Some(Tok::Num(_))) {
        // ⭐ F71: 子查询中的字面量投影 (如 EXISTS 的 `SELECT 1`) — 值无关, 视为全列
        p.next()?;
    } else if top && matches!(p.peek(), Some(Tok::Ident(s)) if s.eq_ignore_ascii_case("VERSION")) {
        // ⭐ S3: SELECT version() — psql/驱动探测 stub
        p.next()?;
        p.expect(&Tok::LParen, "(")?;
        p.expect(&Tok::RParen, ")")?;
        p.done()?;
        return Ok(SqlStmt::VersionStub);
    } else if top && matches!(p.peek(), Some(Tok::Ident(s)) if s.eq_ignore_ascii_case("DATABASE")) {
        // ⭐ S5: SELECT DATABASE() — mysql cli USE 后探测
        p.next()?;
        p.expect(&Tok::LParen, "(")?;
        p.expect(&Tok::RParen, ")")?;
        p.done()?;
        return Ok(SqlStmt::DatabaseStub);
    } else {
        loop {
            let name = p.ident()?;
            // ⭐ G1: ident( → 聚合函数 COUNT/SUM/AVG/MIN/MAX
            if p.peek() == Some(&Tok::LParen) {
                let func = match name.to_ascii_uppercase().as_str() {
                    "COUNT" => AggFn::Count,
                    "SUM" => AggFn::Sum,
                    "AVG" => AggFn::Avg,
                    "MIN" => AggFn::Min,
                    "MAX" => AggFn::Max,
                    other => return Err(format!("unknown function '{other}'")),
                };
                p.next()?; // (
                let col = if p.peek() == Some(&Tok::Star) {
                    if func != AggFn::Count {
                        return Err(format!("{name}(*) is not valid (only COUNT(*))"));
                    }
                    p.next()?;
                    None
                } else {
                    Some(p.ident()?)
                };
                p.expect(&Tok::RParen, ")")?;
                items.push(SelectItem::Agg { func, col });
            } else {
                items.push(SelectItem::Col(name));
            }
            if p.peek() == Some(&Tok::Comma) {
                p.next()?;
            } else {
                break;
            }
        }
    }
    p.kw("FROM")?;
    // ⭐ F72: FROM 派生表 `(SELECT ...) alias` — items (外层投影) 已解完, 传入.
    if p.peek_paren_select() {
        return parse_derived(p, items, top);
    }
    let table = p.ident()?;
    // ⭐ F66: 系统表拦截 — `information_schema.X` / `pg_catalog.X` (大小写不敏)
    // 走虚拟表合成路径; 尾部只解 WHERE/ORDER/LIMIT/OFFSET (不支持 GROUP/HAVING)
    if let Some((cat, tbl)) = split_system_table(&table) {
        let conds = parse_where(p)?;
        let (order, limit, offset) = parse_select_tail(p)?;
        if top {
            p.done()?;
        }
        let cols: Vec<String> = items
            .iter()
            .filter_map(|i| match i {
                SelectItem::Col(c) => Some(c.clone()),
                SelectItem::Agg { .. } => None,
            })
            .collect();
        return Ok(SqlStmt::SystemQuery {
            catalog: cat,
            table: tbl,
            cols,
            conds,
            order,
            limit,
            offset,
        });
    }
    // ⭐ F67 (JOIN): 表名后 3 token 内出现 JOIN/INNER/LEFT → 转 JOIN 解析
    if is_join_ahead(p) {
        return parse_join(p, items, table);
    }
    let conds = parse_where(p)?;
    // ⭐ G1 (F63): GROUP BY col [, col]
    let mut group_by: Vec<String> = Vec::new();
    if p.try_kw("GROUP") {
        p.kw("BY")?;
        loop {
            group_by.push(p.ident()?);
            if p.peek() == Some(&Tok::Comma) {
                p.next()?;
            } else {
                break;
            }
        }
    }
    // ⭐ G1 (F63): HAVING — 条件列写聚合原文 (如 SUM(x)) 或 group 列名,
    // 与输出列 label 同规则匹配 (大写归一). ⭐ F69: 支持 OR/NOT/括号.
    let having: Pred<Cond> = if p.try_kw("HAVING") {
        parse_having_or(p)?
    } else {
        Pred::And(Vec::new())
    };
    let has_having = !having.is_true();
    // ⭐ G1 校验: 有 group_by 时非聚合项必须 ∈ group_by (PG 语义);
    // 有聚合项时 * 投影 (items 空) 非法由 worker 拒 (需 schema 不在此层)
    if !group_by.is_empty() {
        for it in &items {
            if let SelectItem::Col(c) = it
                && !group_by.iter().any(|g| g.eq_ignore_ascii_case(c))
            {
                return Err(format!(
                    "column '{c}' must appear in the GROUP BY clause or be used in an aggregate function"
                ));
            }
        }
        if items.is_empty() {
            return Err("SELECT * is not valid with GROUP BY".into());
        }
    }
    if has_having && !items.iter().any(|i| matches!(i, SelectItem::Agg { .. }))
        && group_by.is_empty()
    {
        return Err("HAVING requires GROUP BY or aggregate function".into());
    }
    // ⭐ S2: ORDER BY c [ASC|DESC] [, ...]; ⭐ G1: 也允许聚合形态 (SUM(x))
    let mut order: Vec<(String, bool)> = Vec::new();
    if p.try_kw("ORDER") {
        p.kw("BY")?;
        loop {
            let mut col = p.ident()?;
            if p.peek() == Some(&Tok::LParen) {
                // 聚合 label (与输出列/HAVING 同规则: 大写函数名 + 原列名)
                p.next()?;
                col = col.to_ascii_uppercase();
                col.push('(');
                if p.peek() == Some(&Tok::Star) {
                    p.next()?;
                    col.push('*');
                } else {
                    col.push_str(&p.ident()?);
                }
                p.expect(&Tok::RParen, ")")?;
                col.push(')');
            }
            let desc = if p.try_kw("DESC") {
                true
            } else {
                p.try_kw("ASC");
                false
            };
            order.push((col, desc));
            if p.peek() == Some(&Tok::Comma) {
                p.next()?;
            } else {
                break;
            }
        }
    }
    let mut limit = None;
    if p.try_kw("LIMIT") {
        match p.next()? {
            Tok::Num(n) => {
                limit = Some(n.parse::<u32>().map_err(|_| format!("bad LIMIT {n}"))?);
            }
            other => return Err(format!("expected LIMIT count, got {other:?}")),
        }
    }
    // ⭐ S2: OFFSET n (PG/MySQL 通用形态)
    let mut offset = None;
    if p.try_kw("OFFSET") {
        match p.next()? {
            Tok::Num(n) => {
                offset = Some(n.parse::<u32>().map_err(|_| format!("bad OFFSET {n}"))?);
            }
            other => return Err(format!("expected OFFSET count, got {other:?}")),
        }
    }
    p.done_if(top)?;
    Ok(SqlStmt::Select { table, items, conds, limit, order, offset, group_by, having })
}

#[cfg(test)]
mod tests {
    use super::*;
    use storage::schema::ColType;

    #[test]
    fn create_roundtrip() {
        let s = parse(b"CREATE TABLE users (id INT PRIMARY KEY, name TEXT NOT NULL, score DOUBLE, INDEX(name), INDEX(score))").unwrap();
        let SqlStmt::CreateTable { table, schema } = s else { panic!() };
        assert_eq!(table, "users");
        assert_eq!(schema.columns.len(), 3);
        assert_eq!(schema.pk_col, 0);
        assert!(!schema.columns[0].nullable);
        assert!(!schema.columns[1].nullable);
        assert!(schema.columns[2].nullable);
        assert_eq!(schema.columns[2].ty, ColType::F64);
        assert_eq!(schema.indexes.len(), 2);
        assert_eq!(schema.indexes[0].col, 1);
        assert_eq!(schema.indexes[1].col, 2);
    }

    #[test]
    fn create_errors() {
        assert!(parse(b"CREATE TABLE t (a INT)").unwrap_err().contains("PRIMARY KEY"));
        assert!(
            parse(b"CREATE TABLE t (a INT PRIMARY KEY, b INT PRIMARY KEY)")
                .unwrap_err()
                .contains("multiple")
        );
        assert!(
            parse(b"CREATE TABLE t (a INT PRIMARY KEY, INDEX(zzz))")
                .unwrap_err()
                .contains("unknown column")
        );
        assert!(parse(b"CREATE TABLE t (a WAT PRIMARY KEY)").unwrap_err().contains("unknown type"));
    }

    #[test]
    fn insert_roundtrip() {
        // 大小写不敏感 + 引号转义 + 负数/浮点/NULL
        let s = parse(b"insert into t (id, name, score) values (-5, 'it''s ok', NULL)").unwrap();
        let SqlStmt::Insert { table, cols, rows } = s else { panic!() };
        assert_eq!(table, "t");
        assert_eq!(cols, vec!["id", "name", "score"]);
        assert_eq!(
            rows,
            vec![vec![
                SqlValue::Int(-5),
                SqlValue::Str(b"it's ok".to_vec()),
                SqlValue::Null,
            ]]
        );
        // 无列清单
        let s = parse(b"INSERT INTO t VALUES (1, 2.5)").unwrap();
        let SqlStmt::Insert { cols, rows, .. } = s else { panic!() };
        assert!(cols.is_empty());
        assert_eq!(rows, vec![vec![SqlValue::Int(1), SqlValue::Float(2.5)]]);
        // ⭐ S1: 多行 VALUES
        let s = parse(b"INSERT INTO t (a) VALUES (1), (2), (3)").unwrap();
        let SqlStmt::Insert { rows, .. } = s else { panic!() };
        assert_eq!(rows.len(), 3);
        assert!(parse(b"INSERT INTO t VALUES (1), (2, 3)").unwrap_err().contains("inconsistent"));
        // 列数不符
        assert!(parse(b"INSERT INTO t (a) VALUES (1, 2)").is_err());
    }

    #[test]
    fn select_roundtrip() {
        let s = parse(b"SELECT * FROM t WHERE a = 1 AND b >= 2.5 AND c < 'x' LIMIT 10").unwrap();
        let SqlStmt::Select { table, items, conds, limit, order, offset, group_by, having } = s else { panic!() };
        assert!(order.is_empty() && offset.is_none() && group_by.is_empty() && having.is_true());
        assert_eq!(table, "t");
        assert!(items.is_empty(), "* = 全列");
        assert_eq!(limit, Some(10));
        let cj = conds.as_conjuncts().unwrap();
        assert_eq!(cj.len(), 3);
        assert_eq!(cj[0], &Cond { col: "a".into(), op: CmpOp::Eq, val: SqlValue::Int(1), set: vec![] });
        assert_eq!(cj[1].op, CmpOp::Ge);
        assert_eq!(cj[2], &Cond { col: "c".into(), op: CmpOp::Lt, val: SqlValue::Str(b"x".to_vec()), set: vec![] });
        // 无 WHERE / 无 LIMIT
        let s = parse(b"SELECT * FROM t").unwrap();
        let SqlStmt::Select { conds, limit, .. } = s else { panic!() };
        assert!(conds.is_true());
        assert_eq!(limit, None);
    }

    #[test]
    fn select_errors() {
        // ⭐ O1: 投影列
        let s = parse(b"SELECT a, b FROM t WHERE a = 1").unwrap();
        let SqlStmt::Select { items, .. } = s else { panic!() };
        assert_eq!(items, vec![SelectItem::Col("a".into()), SelectItem::Col("b".into())]);
        // ⭐ S2: 新算子/子句
        let s = parse(b"SELECT COUNT(*) FROM t WHERE a IN (1, 2, 3)").unwrap();
        let SqlStmt::Select { items, conds, .. } = s else { panic!() };
        assert_eq!(items, vec![SelectItem::Agg { func: AggFn::Count, col: None }]);
        let cj = conds.as_conjuncts().unwrap();
        assert_eq!(cj[0].op, CmpOp::In);
        assert_eq!(cj[0].set.len(), 3);
        let s = parse(b"SELECT * FROM t WHERE a BETWEEN 1 AND 5 AND b != 'x'").unwrap();
        let SqlStmt::Select { conds, .. } = s else { panic!() };
        let cj = conds.as_conjuncts().unwrap();
        assert_eq!(cj.len(), 3, "BETWEEN desugar 成 Ge+Le");
        assert_eq!(cj[0].op, CmpOp::Ge);
        assert_eq!(cj[1].op, CmpOp::Le);
        assert_eq!(cj[2].op, CmpOp::Ne);
        let s = parse(b"SELECT * FROM t WHERE c LIKE 'ab%' ORDER BY a DESC, b LIMIT 3 OFFSET 6").unwrap();
        let SqlStmt::Select { conds, order, limit, offset, .. } = s else { panic!() };
        let cj = conds.as_conjuncts().unwrap();
        assert_eq!(cj.len(), 2, "LIKE 前缀 desugar 成 Ge+Lt");
        assert_eq!(cj[0].val, SqlValue::Str(b"ab".to_vec()));
        assert_eq!(cj[1].val, SqlValue::Str(b"ac".to_vec()));
        assert_eq!(order, vec![("a".into(), true), ("b".into(), false)]);
        assert_eq!((limit, offset), (Some(3), Some(6)));
        assert!(parse(b"SELECT * FROM t WHERE c LIKE '%ab'").is_err(), "仅前缀模式");
        assert!(parse(b"SELECT * FROM t WHERE a IN ()").is_err());
        // ⭐ S1: DML 解析
        let s = parse(b"DELETE FROM t WHERE a = 1").unwrap();
        assert!(matches!(s, SqlStmt::Delete { .. }));
        let s = parse(b"UPDATE t SET a = 1, b = 'x' WHERE c > 2").unwrap();
        let SqlStmt::Update { sets, conds, .. } = s else { panic!() };
        assert_eq!(sets.len(), 2);
        assert_eq!(conds.as_conjuncts().unwrap().len(), 1);
        assert_eq!(parse(b"DROP TABLE t").unwrap(), SqlStmt::DropTable { table: "t".into() });
        assert!(parse(b"SELECT * FROM t WHERE a = NULL").unwrap_err().contains("NULL"));
        assert!(parse(b"SELECT * FROM t WHERE a ! 1").is_err());
        assert!(parse(b"SELECT * FROM t LIMIT x").is_err());
        assert!(parse(b"SELECT * FROM t garbage").unwrap_err().contains("trailing"));
        assert!(parse(b"SELECT * FROM t WHERE name = 'unterminated").is_err());
    }

    // ⭐ P1: 预处理占位符与绑定
    #[test]
    fn prepared_params() {
        // ? 按序编号
        let (s, n) = parse_prepared(b"INSERT INTO t (a, b) VALUES (?, ?)").unwrap();
        assert_eq!(n, 2);
        let SqlStmt::Insert { ref rows, .. } = s else { panic!() };
        assert_eq!(rows[0], vec![SqlValue::Param(0), SqlValue::Param(1)]);
        // 绑定 roundtrip
        let bound = bind_params(&s, &[SqlValue::Int(7), SqlValue::Str(b"x".to_vec())]).unwrap();
        let SqlStmt::Insert { rows, .. } = bound else { panic!() };
        assert_eq!(rows[0], vec![SqlValue::Int(7), SqlValue::Str(b"x".to_vec())]);
        // $n 显式编号 (乱序引用)
        let (s, n) = parse_prepared(b"SELECT * FROM t WHERE a = $2 AND b = $1").unwrap();
        assert_eq!(n, 2);
        let bound = bind_params(&s, &[SqlValue::Int(10), SqlValue::Int(20)]).unwrap();
        let SqlStmt::Select { conds, .. } = bound else { panic!() };
        let cj = conds.as_conjuncts().unwrap();
        assert_eq!(cj[0].val, SqlValue::Int(20), "$2 → params[1]");
        assert_eq!(cj[1].val, SqlValue::Int(10));
        // IN / UPDATE sets 占位
        let (s, n) = parse_prepared(b"UPDATE t SET a = ? WHERE b IN (?, ?)").unwrap();
        assert_eq!(n, 3);
        let bound =
            bind_params(&s, &[SqlValue::Int(1), SqlValue::Int(2), SqlValue::Int(3)]).unwrap();
        let SqlStmt::Update { sets, conds, .. } = bound else { panic!() };
        assert_eq!(sets[0].1, SqlValue::Int(1));
        assert_eq!(conds.as_conjuncts().unwrap()[0].set, vec![SqlValue::Int(2), SqlValue::Int(3)]);
        // 个数不符 / 混用 / simple query 拒绝 / LIMIT 位置拒绝
        assert!(bind_params(&s, &[SqlValue::Int(1)]).unwrap_err().contains("missing"));
        assert!(parse_prepared(b"SELECT * FROM t WHERE a = ? AND b = $1").is_err());
        assert!(parse(b"SELECT * FROM t WHERE a = ?").unwrap_err().contains("prepared"));
        assert!(parse_prepared(b"SELECT * FROM t LIMIT ?").is_err(), "LIMIT 占位不支持");
    }
}
