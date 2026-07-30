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

/// 单个 WHERE 条件 `col op lit` (AND 连接).
#[derive(Debug, Clone, PartialEq)]
pub struct Cond {
    pub col: String,
    pub op: CmpOp,
    pub val: SqlValue,
    /// ⭐ S2: 仅 In 使用 (非空); 其它算子恒空.
    pub set: Vec<SqlValue>,
}

/// 解析结果 AST.
#[derive(Debug, Clone, PartialEq)]
pub enum SqlStmt {
    /// CREATE TABLE: schema 已构建完成 (含 pk / 索引 iid 分配).
    CreateTable { table: String, schema: TableSchema },
    /// INSERT: cols 为空 = 全列序; ⭐ S1: rows 支持多行 VALUES.
    Insert { table: String, cols: Vec<String>, rows: Vec<Vec<SqlValue>> },
    /// SELECT: cols 空 = `*` 全列 (⭐ O1: 投影列, 纯列名无表达式/别名).
    /// ⭐ S2: count = `COUNT(*)`; order = (列名, desc); offset 在排序后截断.
    Select {
        table: String,
        cols: Vec<String>,
        conds: Vec<Cond>,
        limit: Option<u32>,
        order: Vec<(String, bool)>,
        offset: Option<u32>,
        count: bool,
    },
    /// ⭐ S1: DELETE FROM t WHERE ... (WHERE 必带 — 全删由全表扫路径支撑).
    Delete { table: String, conds: Vec<Cond> },
    /// ⭐ S1: UPDATE t SET c=v[, ...] WHERE ... (禁改 pk 列, 规划层拦).
    Update { table: String, sets: Vec<(String, SqlValue)>, conds: Vec<Cond> },
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
    // ⭐ P4: SET 语句在 tokenize 前整吞 (驱动噪声如 `SET @@session.autocommit=1`
    // 含 tokenizer 不认识的 '@'; 语义本就忽略)
    // ⭐ v2 (F62): 例外 — SET [SESSION] TRANSACTION ... 剔出解析 (隔离级别标准)
    let head = text.trim_start();
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
        Some(Tok::Ident(s)) if s.eq_ignore_ascii_case("SELECT") => parse_select(&mut p),
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
            other => Ok(other.clone()),
        }
    };
    let bind_conds = |conds: &[Cond]| -> Result<Vec<Cond>, String> {
        conds
            .iter()
            .map(|c| {
                Ok(Cond {
                    col: c.col.clone(),
                    op: c.op,
                    val: subst(&c.val)?,
                    set: c.set.iter().map(&subst).collect::<Result<_, _>>()?,
                })
            })
            .collect()
    };
    Ok(match stmt {
        SqlStmt::Insert { table, cols, rows } => SqlStmt::Insert {
            table: table.clone(),
            cols: cols.clone(),
            rows: rows
                .iter()
                .map(|r| r.iter().map(&subst).collect::<Result<_, _>>())
                .collect::<Result<_, _>>()?,
        },
        SqlStmt::Select { table, cols, conds, limit, order, offset, count } => SqlStmt::Select {
            table: table.clone(),
            cols: cols.clone(),
            conds: bind_conds(conds)?,
            limit: *limit,
            order: order.clone(),
            offset: *offset,
            count: *count,
        },
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
    let schema =
        TableSchema::new(columns, pk, &index_cols, &unique_cols).map_err(|e| e.to_string())?;
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

/// WHERE 子句 (AND 平铺; caller 决定是否必带).
/// ⭐ S2: BETWEEN → Ge+Le, LIKE 'p%' → 前缀范围 (解析期 desugar);
/// IN → CmpOp::In (set); `!=`/`<>` → Ne.
fn parse_where(p: &mut P) -> Result<Vec<Cond>, String> {
    let mut conds = Vec::new();
    if p.try_kw("WHERE") {
        loop {
            let col = p.ident()?;
            // IN (v, ...)
            if p.try_kw("IN") {
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
                conds.push(Cond { col, op: CmpOp::In, val: SqlValue::Null, set });
            } else if p.try_kw("BETWEEN") {
                // BETWEEN a AND b → col >= a AND col <= b
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
                        conds.push(Cond {
                            col,
                            op: CmpOp::Eq,
                            val: SqlValue::Str(pat),
                            set: vec![],
                        });
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
                                conds.push(Cond {
                                    col,
                                    op: CmpOp::Lt,
                                    val: SqlValue::Str(hi),
                                    set: vec![],
                                });
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
                let val = p.value()?;
                if val == SqlValue::Null {
                    return Err("NULL is not a valid comparison bound".into());
                }
                conds.push(Cond { col, op, val, set: vec![] });
            }
            if !p.try_kw("AND") {
                break;
            }
        }
    }
    Ok(conds)
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

/// `SELECT * | COUNT(*) | c1, c2, ... FROM t [WHERE ...] [ORDER BY c [DESC], ...]
/// [LIMIT n] [OFFSET m]`
fn parse_select(p: &mut P) -> Result<SqlStmt, String> {
    p.kw("SELECT")?;
    // ⭐ O1: 投影列表 (Star = 全列); ⭐ S2: COUNT(*)
    let mut cols: Vec<String> = Vec::new();
    let mut count = false;
    if p.peek() == Some(&Tok::Star) {
        p.next()?;
    } else if matches!(p.peek(), Some(Tok::Ident(s)) if s.eq_ignore_ascii_case("COUNT")) {
        p.next()?;
        p.expect(&Tok::LParen, "(")?;
        p.expect(&Tok::Star, "*")?;
        p.expect(&Tok::RParen, ")")?;
        count = true;
    } else if matches!(p.peek(), Some(Tok::Ident(s)) if s.eq_ignore_ascii_case("VERSION")) {
        // ⭐ S3: SELECT version() — psql/驱动探测 stub
        p.next()?;
        p.expect(&Tok::LParen, "(")?;
        p.expect(&Tok::RParen, ")")?;
        p.done()?;
        return Ok(SqlStmt::VersionStub);
    } else if matches!(p.peek(), Some(Tok::Ident(s)) if s.eq_ignore_ascii_case("DATABASE")) {
        // ⭐ S5: SELECT DATABASE() — mysql cli USE 后探测
        p.next()?;
        p.expect(&Tok::LParen, "(")?;
        p.expect(&Tok::RParen, ")")?;
        p.done()?;
        return Ok(SqlStmt::DatabaseStub);
    } else {
        loop {
            cols.push(p.ident()?);
            if p.peek() == Some(&Tok::Comma) {
                p.next()?;
            } else {
                break;
            }
        }
    }
    p.kw("FROM")?;
    let table = p.ident()?;
    let conds = parse_where(p)?;
    // ⭐ S2: ORDER BY c [ASC|DESC] [, ...]
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
    p.done()?;
    Ok(SqlStmt::Select { table, cols, conds, limit, order, offset, count })
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
        let SqlStmt::Select { table, cols, conds, limit, order, offset, count } = s else { panic!() };
        assert!(order.is_empty() && offset.is_none() && !count);
        assert_eq!(table, "t");
        assert!(cols.is_empty(), "* = 全列");
        assert_eq!(limit, Some(10));
        assert_eq!(conds.len(), 3);
        assert_eq!(conds[0], Cond { col: "a".into(), op: CmpOp::Eq, val: SqlValue::Int(1), set: vec![] });
        assert_eq!(conds[1].op, CmpOp::Ge);
        assert_eq!(conds[2], Cond { col: "c".into(), op: CmpOp::Lt, val: SqlValue::Str(b"x".to_vec()), set: vec![] });
        // 无 WHERE / 无 LIMIT
        let s = parse(b"SELECT * FROM t").unwrap();
        let SqlStmt::Select { conds, limit, .. } = s else { panic!() };
        assert!(conds.is_empty());
        assert_eq!(limit, None);
    }

    #[test]
    fn select_errors() {
        // ⭐ O1: 投影列
        let s = parse(b"SELECT a, b FROM t WHERE a = 1").unwrap();
        let SqlStmt::Select { cols, .. } = s else { panic!() };
        assert_eq!(cols, vec!["a", "b"]);
        // ⭐ S2: 新算子/子句
        let s = parse(b"SELECT COUNT(*) FROM t WHERE a IN (1, 2, 3)").unwrap();
        let SqlStmt::Select { count, conds, .. } = s else { panic!() };
        assert!(count);
        assert_eq!(conds[0].op, CmpOp::In);
        assert_eq!(conds[0].set.len(), 3);
        let s = parse(b"SELECT * FROM t WHERE a BETWEEN 1 AND 5 AND b != 'x'").unwrap();
        let SqlStmt::Select { conds, .. } = s else { panic!() };
        assert_eq!(conds.len(), 3, "BETWEEN desugar 成 Ge+Le");
        assert_eq!(conds[0].op, CmpOp::Ge);
        assert_eq!(conds[1].op, CmpOp::Le);
        assert_eq!(conds[2].op, CmpOp::Ne);
        let s = parse(b"SELECT * FROM t WHERE c LIKE 'ab%' ORDER BY a DESC, b LIMIT 3 OFFSET 6").unwrap();
        let SqlStmt::Select { conds, order, limit, offset, .. } = s else { panic!() };
        assert_eq!(conds.len(), 2, "LIKE 前缀 desugar 成 Ge+Lt");
        assert_eq!(conds[0].val, SqlValue::Str(b"ab".to_vec()));
        assert_eq!(conds[1].val, SqlValue::Str(b"ac".to_vec()));
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
        assert_eq!(conds.len(), 1);
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
        assert_eq!(conds[0].val, SqlValue::Int(20), "$2 → params[1]");
        assert_eq!(conds[1].val, SqlValue::Int(10));
        // IN / UPDATE sets 占位
        let (s, n) = parse_prepared(b"UPDATE t SET a = ? WHERE b IN (?, ?)").unwrap();
        assert_eq!(n, 3);
        let bound =
            bind_params(&s, &[SqlValue::Int(1), SqlValue::Int(2), SqlValue::Int(3)]).unwrap();
        let SqlStmt::Update { sets, conds, .. } = bound else { panic!() };
        assert_eq!(sets[0].1, SqlValue::Int(1));
        assert_eq!(conds[0].set, vec![SqlValue::Int(2), SqlValue::Int(3)]);
        // 个数不符 / 混用 / simple query 拒绝 / LIMIT 位置拒绝
        assert!(bind_params(&s, &[SqlValue::Int(1)]).unwrap_err().contains("missing"));
        assert!(parse_prepared(b"SELECT * FROM t WHERE a = ? AND b = $1").is_err());
        assert!(parse(b"SELECT * FROM t WHERE a = ?").unwrap_err().contains("prepared"));
        assert!(parse_prepared(b"SELECT * FROM t LIMIT ?").is_err(), "LIMIT 占位不支持");
    }
}
