//! ⭐ X1 (SQL 落地): 手写 tokenizer + tree-walking parser — 纯函数, 零依赖.
//! 从 sql.rs 拆分 (2026-08). AST 类型见 ast.rs.

use super::ast::*;
use storage::schema::{ColType, Column, TableSchema};

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
    /// ⭐ F78: 算术运算符 (聚合内表达式; * 复用 Star).
    Plus,
    Minus,
    Slash,
    /// ⭐ compat: 数组类型后缀 `[]` (TEXT[] → Str[] 标记).
    LBracket,
    RBracket,
    /// ⭐ compat: `::` 类型转换后缀 (`'{}'::jsonb`).
    Colon,
    /// ⭐ compat: JSONB 操作符 `->` (取字段) / `->>` (取文本).
    Arrow,
    ArrowText,
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
            b'"' => {
                // ⭐ compat: 双引号标识符 `"uuid-ossp"` / `"col name"` → Ident (去引号)
                let mut s = Vec::new();
                i += 1;
                loop {
                    match b.get(i) {
                        None => return Err("unterminated quoted identifier".into()),
                        Some(b'"') => {
                            if b.get(i + 1) == Some(&b'"') {
                                s.push(b'"');
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
                toks.push(Tok::Ident(String::from_utf8_lossy(&s).into_owned()));
            }
            b'[' => {
                toks.push(Tok::LBracket);
                i += 1;
            }
            b']' => {
                toks.push(Tok::RBracket);
                i += 1;
            }
            b':' => {
                // ⭐ compat: `::` 类型转换 (`'{}'::jsonb`) — 双冒号合成单 token
                if b.get(i + 1) == Some(&b':') {
                    toks.push(Tok::Colon);
                    i += 2;
                } else {
                    return Err("unexpected ':'".into());
                }
            }
            b'?' => {
                // ⭐ P1: 预处理占位符
                toks.push(Tok::Question);
                i += 1;
            }
            b'$' => {
                // ⭐ compat: dollar-quote ($$...$$ / $tag$...$tag$) — CREATE FUNCTION
                // 体等 PG 专有 DDL. 与 $n 参数区分: dollar-quote 的 tag 后紧跟 `$`
                // ($$ 或 字母/数字/下划线+$), 而 $n 的数字后不跟 `$`.
                let mut k = i + 1;
                while k < b.len() && (b[k].is_ascii_alphanumeric() || b[k] == b'_') {
                    k += 1;
                }
                if k < b.len() && b[k] == b'$' {
                    let tag = &b[i..=k];
                    let mut scan = k + 1;
                    let mut end = None;
                    while scan < b.len() {
                        if b[scan..].starts_with(tag) {
                            end = Some(scan);
                            break;
                        }
                        scan += 1;
                    }
                    let Some(end) = end else {
                        return Err("unterminated dollar-quoted string".into());
                    };
                    toks.push(Tok::Str(b[i..end + tag.len()].to_vec()));
                    i = end + tag.len();
                    continue;
                }
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
            b'+' => {
                toks.push(Tok::Plus);
                i += 1;
            }
            b'/' => {
                toks.push(Tok::Slash);
                i += 1;
            }
            b'-' => {
                // ⭐ compat: JSONB 操作符 -> / ->> (优先于减号)
                if b.get(i + 1) == Some(&b'>') {
                    if b.get(i + 2) == Some(&b'>') {
                        toks.push(Tok::ArrowText);
                        i += 3;
                    } else {
                        toks.push(Tok::Arrow);
                        i += 2;
                    }
                    continue;
                }
                // ⭐ F78: 二元减号 vs 负数字面量 — 前一 token 是值结尾 (ident/num/str/`)`)
                // 则为二元减; 否则为负数前缀 (保留旧行为: WHERE x = -1).
                let prev_is_value = matches!(
                    toks.last(),
                    Some(Tok::Ident(_) | Tok::Num(_) | Tok::Str(_) | Tok::RParen)
                );
                if prev_is_value {
                    toks.push(Tok::Minus);
                    i += 1;
                } else {
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
            }
            b'0'..=b'9' => {
                // 正数 (小数点; 科计数不含 e, v1 够用)
                let start = i;
                i += 1;
                while i < b.len() && (b[i].is_ascii_digit() || b[i] == b'.') {
                    i += 1;
                }
                toks.push(Tok::Num(input[start..i].to_string()));
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
                // ⭐ F76: 支持 `db`.`tbl` / `db`.tbl 点分限定 — 拼成单 Ident "db.tbl"
                let mut name = String::new();
                loop {
                    if b.get(i) == Some(&b'`') {
                        i += 1;
                        let start = i;
                        while i < b.len() && b[i] != b'`' {
                            i += 1;
                        }
                        if i >= b.len() {
                            return Err("unterminated `identifier`".into());
                        }
                        name.push_str(&input[start..i]);
                        i += 1; // 跳过右反引号
                    } else {
                        // 裸段 (点后未加反引号: `db`.tbl)
                        let start = i;
                        while i < b.len() && (b[i].is_ascii_alphanumeric() || b[i] == b'_') {
                            i += 1;
                        }
                        name.push_str(&input[start..i]);
                    }
                    // 点分隔 → 继续下一段; 否则结束
                    if b.get(i) == Some(&b'.') {
                        name.push('.');
                        i += 1;
                    } else {
                        break;
                    }
                }
                toks.push(Tok::Ident(name));
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

    /// ⭐ F76: 表位置标识符 — 读 ident 并剥 db 限定前缀 (`db.tbl` → `tbl`).
    fn table_ident(&mut self) -> Result<String, String> {
        Ok(strip_db_qual(self.ident()?))
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
            // ⭐ F80: NULL / TRUE / FALSE / 时间前缀字面量 DATE|TIME|TIMESTAMP|DATETIME '...'
            Tok::Ident(s) => {
                match s.to_ascii_uppercase().as_str() {
                    "NULL" => Ok(SqlValue::Null),
                    "TRUE" => Ok(SqlValue::Int(1)),
                    "FALSE" => Ok(SqlValue::Int(0)),
                    "DATE" | "TIME" | "TIMESTAMP" | "DATETIME"
                        if matches!(self.peek(), Some(Tok::Str(_))) =>
                    {
                        // 前缀标注: 内层字符串按目标列 ColType 在 worker 解析
                        match self.next()? {
                            Tok::Str(b) => Ok(SqlValue::Str(b)),
                            _ => unreachable!(),
                        }
                    }
                    _ => Err(format!("expected literal, got identifier {s}")),
                }
            }
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
/// ⭐ compat: 按 `;` 分割多条 SQL 语句 (字符串/注释感知).
/// 处理: `'...'`(含 '' 转义) / `"..."`(含 "" 转义) / `$$...$$` dollar-quote /
/// `--` 行注释 / `/* */` 块注释。返回非空语句 (trim 后, 尾分号已去)。
pub fn split_sql_statements(input: &str) -> Vec<String> {
    let b = input.as_bytes();
    let mut out = Vec::new();
    let mut start = 0usize;
    let mut i = 0usize;
    while i < b.len() {
        let c = b[i];
        match c {
            b'\'' => {
                i += 1;
                while i < b.len() {
                    if b[i] == b'\'' {
                        if b.get(i + 1) == Some(&b'\'') {
                            i += 2;
                            continue;
                        }
                        i += 1;
                        break;
                    }
                    i += 1;
                }
            }
            b'"' => {
                i += 1;
                while i < b.len() {
                    if b[i] == b'"' {
                        if b.get(i + 1) == Some(&b'"') {
                            i += 2;
                            continue;
                        }
                        i += 1;
                        break;
                    }
                    i += 1;
                }
            }
            b'$' => {
                // dollar-quote: $$...$$ 或 $tag$...$tag$
                let mut tag_end = i + 1;
                while tag_end < b.len() && b[tag_end].is_ascii_alphanumeric() {
                    tag_end += 1;
                }
                if tag_end < b.len() && b[tag_end] == b'$' {
                    let tag = &input[i..=tag_end];
                    // 查找闭合 tag
                    let rest = &input[tag_end + 1..];
                    if let Some(pos) = rest.find(tag) {
                        i = tag_end + 1 + pos + tag.len();
                        continue;
                    }
                }
                i += 1;
            }
            b'-' if b.get(i + 1) == Some(&b'-') => {
                // 行注释
                while i < b.len() && b[i] != b'\n' {
                    i += 1;
                }
            }
            b'/' if b.get(i + 1) == Some(&b'*') => {
                // 块注释
                i += 2;
                while i + 1 < b.len() && !(b[i] == b'*' && b[i + 1] == b'/') {
                    i += 1;
                }
                i = (i + 2).min(b.len());
            }
            b';' => {
                let stmt = input[start..i].trim();
                if !stmt.is_empty() {
                    out.push(stmt.to_string());
                }
                i += 1;
                start = i;
            }
            _ => i += 1,
        }
    }
    let tail = input[start..].trim();
    if !tail.is_empty() {
        out.push(tail.to_string());
    }
    out
}

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
        Some(Tok::Ident(s)) if s.eq_ignore_ascii_case("ALTER") => parse_alter(&mut p),
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
            let table = p.table_ident()?;
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
        // ⭐ PG 兼容: SELECT EXISTS — 内层 (SystemQuery) 递归绑定 $n
        SqlStmt::ExistsStub { inner } => SqlStmt::ExistsStub {
            inner: Box::new(bind_params(inner, params)?),
        },
        // ⭐ F66: 系统表查询 — WHERE 条件支持 $n (migrator 探测)
        SqlStmt::SystemQuery { catalog, table, cols, conds, order, limit, offset } => {
            SqlStmt::SystemQuery {
                catalog: catalog.clone(),
                table: table.clone(),
                cols: cols.clone(),
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

/// ⭐ F76: 读 `(col [, col ...])` 列名列表 (表级约束/索引列; 反引号已在 tokenizer 去).
/// ⭐ compat: 吞可选 ASC/DESC 排序后缀 (CREATE INDEX ... (col DESC)).
fn read_col_list(p: &mut P) -> Result<Vec<String>, String> {
    p.expect(&Tok::LParen, "(")?;
    let mut cols = Vec::new();
    loop {
        cols.push(p.ident()?);
        // 排序后缀 (仅索引列场景)
        p.try_kw("ASC");
        p.try_kw("DESC");
        match p.next()? {
            Tok::Comma => continue,
            Tok::RParen => break,
            other => return Err(format!("expected ',' or ')' in column list, got {other:?}")),
        }
    }
    Ok(cols)
}

/// ⭐ PG 兼容 (FMT_VER 8): 外键原始定义 (解析期; 列位转数字后入 TableSchema).
struct FkDefRaw {
    col: String,
    ref_table: String,
    ref_col: String,
    action: storage::schema::FkAction,
}

/// ⭐ PG 兼容: 解析外键 `ON DELETE [CASCADE|SET NULL|NO ACTION|RESTRICT]` 动作,
/// 吞掉后续 `ON UPDATE ...` 子句 (v1 不实现 UPDATE 级联).
fn parse_fk_action(p: &mut P) -> Result<storage::schema::FkAction, String> {
    use storage::schema::FkAction;
    let mut action = FkAction::NoAction;
    loop {
        let Some(first) = p.peek().and_then(|t| match t {
            Tok::Ident(s) => Some(s.to_ascii_lowercase()),
            _ => None,
        }) else {
            break;
        };
        if first != "on" {
            break;
        }
        p.next()?; // ON
        let w = p.ident()?.to_ascii_lowercase(); // DELETE / UPDATE
        let a = p.ident()?.to_ascii_lowercase();
        if w == "delete" {
            action = match a.as_str() {
                "cascade" => FkAction::Cascade,
                "set" => {
                    p.kw("null")?;
                    FkAction::SetNull
                }
                "no" => {
                    let _ = p.ident()?; // ACTION
                    FkAction::NoAction
                }
                _ => FkAction::NoAction, // RESTRICT / 其他 → v1 不检查
            };
        }
        // ON UPDATE 的 action 已吞 (a 已消费; set null 已吞 null)
    }
    Ok(action)
}

/// ⭐ PG 兼容: 解析列 DEFAULT 表达式 → ColDefault (v1: 字面量 / NOW /
/// uuid_generate_v4; 未知函数/表达式 → None, 吞掉不落默认). 含 `::type` 后缀.
fn parse_col_default(
    p: &mut P,
    ty: ColType,
) -> Result<Option<storage::schema::ColDefault>, String> {
    use storage::schema::ColDefault;
    // 函数调用 / 裸标识 (true/false/null/未加引号文本)
    if let Some(Tok::Ident(_)) = p.peek() {
        let name = p.ident()?.to_ascii_lowercase();
        if p.peek() == Some(&Tok::LParen) {
            // 函数: NOW() / uuid_generate_v4() — 吞括号及参数
            p.next()?;
            let mut depth = 1;
            while depth > 0 {
                match p.next()? {
                    Tok::LParen => depth += 1,
                    Tok::RParen => depth -= 1,
                    _ => {}
                }
            }
            let d = match name.as_str() {
                "now" | "current_timestamp" | "current_date" | "current_time" => {
                    Some(ColDefault::Now)
                }
                "uuid_generate_v4" => Some(ColDefault::UuidGenV4),
                _ => None, // 未知函数 → 吞掉不落默认
            };
            // ::type 后缀
            if p.peek() == Some(&Tok::Colon) {
                p.next()?;
                let _ = p.ident()?;
            }
            return Ok(d);
        }
        // 裸标识 (PG 允许 DEFAULT true / 'text')
        let val = match name.as_str() {
            "true" => crate::protocol::sql::SqlValue::Int(1),
            "false" => crate::protocol::sql::SqlValue::Int(0),
            "null" => crate::protocol::sql::SqlValue::Null,
            _ => crate::protocol::sql::SqlValue::Str(name.into_bytes()),
        };
        let cv = crate::worker::sql_to_col(ty, &val)?;
        if p.peek() == Some(&Tok::Colon) {
            p.next()?;
            let _ = p.ident()?;
        }
        return Ok(Some(ColDefault::Lit(cv)));
    }
    // 字面量 (含 '{}'::jsonb 等)
    let val = p.value()?;
    if p.peek() == Some(&Tok::Colon) {
        p.next()?;
        let _ = p.ident()?;
    }
    let cv = crate::worker::sql_to_col(ty, &val)?;
    Ok(Some(ColDefault::Lit(cv)))
}

/// `CREATE TABLE t (...) | CREATE INDEX [IF NOT EXISTS] name ON t (cols) | CREATE EXTENSION ... | ...`
fn parse_create(p: &mut P) -> Result<SqlStmt, String> {
    p.kw("CREATE")?;
    // ⭐ compat: CREATE OR REPLACE FUNCTION — 吞掉 (v1 不做触发器/函数)
    if p.try_kw("OR") {
        p.kw("REPLACE")?;
        p.kw("FUNCTION")?;
        return Ok(SqlStmt::DdlStub);
    }
    if p.try_kw("INDEX") {
        // CREATE [UNIQUE] INDEX [IF NOT EXISTS] name ON t (col, ...) [WHERE ...]
        p.try_kw("UNIQUE");
        let if_not_exists = if p.try_kw("IF") {
            p.kw("NOT")?;
            p.kw("EXISTS")?;
            true
        } else {
            false
        };
        let _ = p.ident()?; // 索引名 (吞)
        p.kw("ON")?;
        let table = p.table_ident()?;
        let cols = read_col_list(p)?;
        // 吞可选 WHERE 部分索引 (至语句结束)
        if p.try_kw("WHERE") {
            while !matches!(p.peek(), None) {
                p.i += 1;
            }
        }
        p.done()?;
        return Ok(SqlStmt::CreateIndex { table, cols, if_not_exists });
    }
    if p.try_kw("EXTENSION") {
        // CREATE EXTENSION [IF NOT EXISTS] "name" — 吞掉 (uuid-ossp 等)
        let _ = p.try_kw("IF");
        let _ = p.try_kw("NOT");
        let _ = p.try_kw("EXISTS");
        let _ = p.ident()?; // 扩展名 (含双引号已去)
        p.done()?;
        return Ok(SqlStmt::DdlStub);
    }
    if p.try_kw("TRIGGER") {
        // CREATE TRIGGER name ... — 吞掉
        return Ok(SqlStmt::DdlStub);
    }
    if p.try_kw("DATABASE") {
        // ⭐ PG 兼容: CREATE DATABASE name — 真实建库 (worker 走 shard 2PC)
        let name = p.ident()?;
        p.done()?;
        return Ok(SqlStmt::CreateDb { name });
    }
    if p.try_kw("SEQUENCE") || p.try_kw("TYPE") {
        return Ok(SqlStmt::DdlStub);
    }
    p.kw("TABLE")?;
    // ⭐ IF NOT EXISTS: `CREATE TABLE IF NOT EXISTS t (...)` — 表已存在时静默跳过
    let if_not_exists = if p.try_kw("IF") {
        p.kw("NOT")?;
        p.kw("EXISTS")?;
        true
    } else {
        false
    };
    let table = p.table_ident()?;
    p.expect(&Tok::LParen, "(")?;

    let mut columns: Vec<Column> = Vec::new();
    let mut pk: Option<u16> = None;
    let mut pk_name: Option<String> = None; // ⭐ F76: 表级 PRIMARY KEY (col)
    let mut index_names: Vec<String> = Vec::new();
    let mut unique_names: Vec<String> = Vec::new(); // ⭐ O3
    let mut global_unique_names: Vec<String> = Vec::new(); // ⭐ F65
    let mut composite_unique_names: Vec<Vec<String>> = Vec::new(); // ⭐ PG 兼容 (FMT_VER 7)
    let mut fks: Vec<FkDefRaw> = Vec::new(); // ⭐ PG 兼容 (FMT_VER 8): 外键 (列级+表级)
    loop {
        if p.try_kw("INDEX") {
            p.expect(&Tok::LParen, "(")?;
            index_names.push(p.ident()?);
            p.expect(&Tok::RParen, ")")?;
        } else if p.try_kw("CONSTRAINT") {
            // ⭐ F76: CONSTRAINT [name] <PRIMARY KEY|UNIQUE|FOREIGN KEY> — 吃可选名后
            // continue 重进循环, 由下方 PRIMARY/UNIQUE/FOREIGN 分支处理实体
            if !matches!(p.peek(), Some(Tok::Ident(s)) if
                s.eq_ignore_ascii_case("PRIMARY") || s.eq_ignore_ascii_case("UNIQUE")
                    || s.eq_ignore_ascii_case("FOREIGN") || s.eq_ignore_ascii_case("KEY"))
            {
                let _ = p.ident()?; // 约束名
            }
            continue;
        } else if p.try_kw("PRIMARY") {
            p.kw("KEY")?;
            let cols = read_col_list(p)?;
            pk_name = cols.into_iter().next(); // v1 单列 pk (复合取首列)
        } else if p.try_kw("UNIQUE") {
            p.try_kw("KEY");
            // 可选索引名 (非左括号时)
            if p.peek() != Some(&Tok::LParen) {
                let _ = p.ident()?;
            }
            let cols = read_col_list(p)?;
            if cols.len() == 1 {
                unique_names.push(cols[0].clone());
            } else {
                // ⭐ PG 兼容 (FMT_VER 7): 复合 UNIQUE → 整组保留 (schema 拼 key 唯一)
                composite_unique_names.push(cols);
            }
        } else if p.try_kw("KEY") {
            if p.peek() != Some(&Tok::LParen) {
                let _ = p.ident()?;
            }
            let cols = read_col_list(p)?;
            if let Some(c) = cols.into_iter().next() {
                index_names.push(c);
            }
        } else if p.try_kw("FOREIGN") {
            // ⭐ PG 兼容 (FMT_VER 8): 表级外键 FOREIGN KEY (col) REFERENCES t(col) [ON ...]
            p.kw("KEY")?;
            let cols = read_col_list(p)?;
            p.kw("REFERENCES")?;
            let ref_table = p.ident()?;
            let ref_cols = if p.peek() == Some(&Tok::LParen) {
                read_col_list(p)?
            } else {
                Vec::new()
            };
            let action = parse_fk_action(p)?;
            // v1: 单列外键 (复合外键取首列)
            if let Some(c) = cols.into_iter().next() {
                fks.push(FkDefRaw {
                    col: c,
                    ref_table,
                    ref_col: ref_cols.into_iter().next().unwrap_or_else(|| "id".to_string()),
                    action,
                });
            }
        } else if p.try_kw("CHECK") {
            // ⭐ compat: 表级 CHECK (expr) — v1 吞 (不强制约束)
            if p.peek() == Some(&Tok::LParen) {
                p.next()?;
                let mut depth = 1;
                while depth > 0 {
                    match p.next()? {
                        Tok::LParen => depth += 1,
                        Tok::RParen => depth -= 1,
                        _ => {}
                    }
                }
            }
        } else {
            let name = p.ident()?;
            let ty = parse_col_type(p)?;
            let mut nullable = true;
            let mut is_pk = false;
            let mut default: Option<storage::schema::ColDefault> = None;
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
                } else if p.try_kw("AUTO_INCREMENT") || p.try_kw("AUTOINCREMENT") {
                    // ⭐ F76: 吃 AUTO_INCREMENT (v1 不做服务端自增; ORM 提供显式 id)
                } else if p.try_kw("DEFAULT") {
                    // ⭐ PG 兼容: 捕获 DEFAULT 表达式 → Column.default
                    default = parse_col_default(p, ty)?;
                } else if p.try_kw("COMMENT") {
                    // ⭐ F76: 吃 COMMENT '…'
                    let _ = p.value()?;
                } else if p.try_kw("REFERENCES") {
                    // ⭐ PG 兼容 (FMT_VER 8): 列级外键 `REFERENCES t (c) [ON DELETE ...]`
                    let ref_table = p.ident()?;
                    let ref_col = if p.peek() == Some(&Tok::LParen) {
                        p.next()?;
                        let c = p.ident()?;
                        p.expect(&Tok::RParen, ")")?;
                        c
                    } else {
                        // PG: REFERENCES t (无列) → 引用 t 的主键 (v1 记名 "id" 占位,
                        // 级联时按引用表实际 pk 列解析)
                        "id".to_string()
                    };
                    let action = parse_fk_action(p)?;
                    fks.push(FkDefRaw { col: name.clone(), ref_table, ref_col, action });
                } else if p.try_kw("CHECK") {
                    // ⭐ compat: 列级 CHECK (expr) — v1 吞 (不强制约束)
                    if p.peek() == Some(&Tok::LParen) {
                        p.next()?;
                        let mut depth = 1;
                        while depth > 0 {
                            match p.next()? {
                                Tok::LParen => depth += 1,
                                Tok::RParen => depth -= 1,
                                _ => {}
                            }
                        }
                    }
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
            columns.push(Column { name, ty, nullable, default });
        }
        match p.next()? {
            Tok::Comma => continue,
            Tok::RParen => break,
            other => return Err(format!("expected ',' or ')', got {other:?}")),
        }
    }
    p.done()?;

    // ⭐ F76: 表级 PRIMARY KEY (col) → 解析列位 (内联 pk 优先); 并置该列 NOT NULL
    if pk.is_none()
        && let Some(n) = &pk_name
    {
        let i = columns
            .iter()
            .position(|c| c.name == *n)
            .ok_or_else(|| format!("PRIMARY KEY on unknown column {n}"))?;
        columns[i].nullable = false;
        pk = Some(i as u16);
    }
    // ⭐ compat (自动主键): 无 PRIMARY KEY 时降级 —
    // L1: 恰一个单列 UNIQUE → 提升为 PK (零膨胀; 该列不重复建唯一索引);
    // L2: 否则注入隐藏自增列 __rowid BIGINT NOT NULL 为 PK (8B, worker 进程级
    // Atomic 递增, seed=启动时间戳 → 免恢复; SELECT * 对外隐藏该列).
    let pk = match pk {
        Some(p) => p,
        None => {
            if unique_names.len() == 1 && global_unique_names.is_empty() {
                let u = unique_names.pop().unwrap();
                columns
                    .iter()
                    .position(|c| c.name == u)
                    .map(|i| i as u16)
                    .ok_or_else(|| format!("UNIQUE on unknown column {u}"))?
            } else {
                if columns.iter().any(|c| c.name.eq_ignore_ascii_case("__rowid")) {
                    return Err("column name '__rowid' is reserved for auto rowid".into());
                }
                columns.push(Column {
                    name: "__rowid".to_string(),
                    ty: ColType::I64,
                    nullable: false,
                    default: None,
                });
                (columns.len() - 1) as u16
            }
        }
    };
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
    let mut composite_unique_cols: Vec<Vec<u16>> = Vec::with_capacity(composite_unique_names.len());
    for g in &composite_unique_names {
        let mut cols = Vec::with_capacity(g.len());
        for n in g {
            cols.push(col_pos(n, "UNIQUE")?);
        }
        composite_unique_cols.push(cols);
    }
    let mut fk_defs: Vec<storage::schema::FkDef> = Vec::with_capacity(fks.len());
    for fk in &fks {
        let col = col_pos(&fk.col, "FOREIGN KEY")?;
        fk_defs.push(storage::schema::FkDef {
            col,
            ref_table: fk.ref_table.clone(),
            ref_col: fk.ref_col.clone(),
            on_delete: fk.action,
        });
    }
    let schema = TableSchema::new(
        columns,
        pk,
        &index_cols,
        &unique_cols,
        &global_unique_cols,
        &composite_unique_cols,
        &fk_defs,
    )
    .map_err(|e| e.to_string())?;
    Ok(SqlStmt::CreateTable { table, schema, if_not_exists })
}

/// `INSERT INTO t [(c1,...)] VALUES (v1,...)`
fn parse_insert(p: &mut P) -> Result<SqlStmt, String> {
    p.kw("INSERT")?;
    p.kw("INTO")?;
    let table = p.table_ident()?;
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
    // ⭐ compat: 吞 RETURNING ... (v1 不返回受影响行值)
    if p.try_kw("RETURNING") {
        while !matches!(p.peek(), None) {
            p.i += 1;
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
            && matches!(&items[0], SelectItem::Agg { func: AggFn::Count, arg: None, .. });
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
    // ⭐ P0-1: 常量比较短路 — `1=1`/`0=1`/`'a'<'b'` (无列引用) → 恒真 (空 AND) /
    // 恒假 (空 OR). 恒假由 dispatch 短路返回空; 恒真由 normalize 消除.
    if matches!(p.peek(), Some(Tok::Num(_)) | Some(Tok::Str(_))) {
        let lhs = p.value()?;
        let op = match p.next()? {
            Tok::Eq => CmpOp::Eq,
            Tok::Gt => CmpOp::Gt,
            Tok::Ge => CmpOp::Ge,
            Tok::Lt => CmpOp::Lt,
            Tok::Le => CmpOp::Le,
            Tok::Ne => CmpOp::Ne,
            other => {
                return Err(format!("expected comparison operator after constant, got {other:?}"))
            }
        };
        let rv = p.value()?;
        let rhs = fold_cond_arith(p, rv)?;
        let truthy = const_cmp(lhs, op, rhs)?;
        return Ok(if truthy { Pred::And(vec![]) } else { Pred::Or(vec![]) });
    }
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
    } else if p.try_kw("IS") {
        // ⭐ compat: `col IS [NOT] NULL` — desugar 为 col = NULL / col <> NULL
        let not = p.try_kw("NOT");
        p.kw("NULL")?;
        let leaf = Pred::Leaf(Cond {
            col,
            op: if not { CmpOp::Ne } else { CmpOp::Eq },
            val: SqlValue::Null,
            set: vec![],
        });
        return Ok(leaf);
    } else {
        // ⭐ compat: `j ? 'key'` — 操作符位置 `?` → JSONB 存在 (值位置 `?` 仍为
        // prepared 占位符, 由 p.value() 处理). v1: 键须字面量 (Str/Int), 纯残余过滤.
        if p.peek() == Some(&Tok::Question) {
            p.next()?;
            let key = p.value()?;
            if key == SqlValue::Null || matches!(key, SqlValue::ColRef(_) | SqlValue::Subquery(_))
            {
                return Err("JSONB '?' key must be a literal".into());
            }
            conds.push(Cond { col, op: CmpOp::JsonExists, val: key, set: vec![] });
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
            // ⭐ F80: 但排除字面量前导关键字 (NULL/TRUE/FALSE 及 DATE|TIME|TIMESTAMP|DATETIME '...'),
            //   它们应落到 value() 解析为字面量而非列引用.
            if let Some(Tok::Ident(s)) = p.peek()
                && !matches!(
                    s.to_ascii_uppercase().as_str(),
                    "NULL" | "TRUE" | "FALSE" | "DATE" | "TIME" | "TIMESTAMP" | "DATETIME"
                )
            {
                let rhs = p.ident()?;
                return Ok(Pred::Leaf(Cond { col, op, val: SqlValue::ColRef(rhs), set: vec![] }));
            }
            // ⭐ P0-1: 字面量算术折叠 (`a = 1+2` → `a = 3`)
            let rv = p.value()?;
            let val = fold_cond_arith(p, rv)?;
            if val == SqlValue::Null {
                return Err("NULL is not a valid comparison bound".into());
            }
            conds.push(Cond { col, op, val, set: vec![] });
        }
    }
    // 单条 → Leaf; 多条 (BETWEEN/LIKE desugar) → And; 空 (LIKE '%') → 恒真
    Ok(match conds.len() {
        1 => Pred::Leaf(conds.pop().unwrap()),
        _ => Pred::And(conds.into_iter().map(Pred::Leaf).collect()),
    })
}

/// ⭐ P0-1: 折叠 cond 右值的字面量算术 (`a = 1+2` → `a = 3`). 仅数值;
/// 含列引用/字符串遇算术符报错 (v1).
fn fold_cond_arith(p: &mut P, first: SqlValue) -> Result<SqlValue, String> {
    let mut acc = first;
    loop {
        let op = match p.peek() {
            Some(Tok::Plus) => ArithOp::Add,
            Some(Tok::Minus) => ArithOp::Sub,
            Some(Tok::Star) => ArithOp::Mul,
            Some(Tok::Slash) => ArithOp::Div,
            _ => break,
        };
        p.next()?;
        let rhs = p.value()?;
        acc = eval_const_bin(op, acc, rhs)?;
    }
    Ok(acc)
}

/// ⭐ P0-1: 常量二元算术求值 (Int/Float; 溢出/除零/非数值报错).
fn eval_const_bin(op: ArithOp, l: SqlValue, r: SqlValue) -> Result<SqlValue, String> {
    use SqlValue::{Float, Int};
    match (l, r) {
        (Int(a), Int(b)) => {
            let v = match op {
                ArithOp::Add => a.checked_add(b),
                ArithOp::Sub => a.checked_sub(b),
                ArithOp::Mul => a.checked_mul(b),
                ArithOp::Div => a.checked_div(b),
            };
            v.map(Int)
                .ok_or_else(|| "integer overflow/div-by-zero in constant expression".into())
        }
        (Float(a), Float(b)) => Ok(Float(match op {
            ArithOp::Add => a + b,
            ArithOp::Sub => a - b,
            ArithOp::Mul => a * b,
            ArithOp::Div => a / b,
        })),
        (Float(a), Int(b)) => eval_const_bin(op, Float(a), Float(b as f64)),
        (Int(a), Float(b)) => eval_const_bin(op, Float(a as f64), Float(b)),
        _ => Err("constant arithmetic requires numeric operands".into()),
    }
}

/// ⭐ P0-1: 常量比较求值 (Int/Float/Str; 混合类型报错).
fn const_cmp(l: SqlValue, op: CmpOp, r: SqlValue) -> Result<bool, String> {
    use std::cmp::Ordering;
    use SqlValue::{Float, Int, Str};
    let ord = match (l, r) {
        (Int(a), Int(b)) => a.cmp(&b),
        (Float(a), Float(b)) => a
            .partial_cmp(&b)
            .ok_or_else(|| "constant comparison with NaN".to_string())?,
        (Float(a), Int(b)) => a
            .partial_cmp(&(b as f64))
            .ok_or_else(|| "constant comparison with NaN".to_string())?,
        (Int(a), Float(b)) => (a as f64)
            .partial_cmp(&b)
            .ok_or_else(|| "constant comparison with NaN".to_string())?,
        (Str(a), Str(b)) => a.cmp(&b),
        _ => return Err("constant comparison requires numeric/string operands".into()),
    };
    Ok(match op {
        CmpOp::Eq => ord == Ordering::Equal,
        CmpOp::Ne => ord != Ordering::Equal,
        CmpOp::Gt => ord == Ordering::Greater,
        CmpOp::Ge => ord != Ordering::Less,
        CmpOp::Lt => ord == Ordering::Less,
        CmpOp::Le => ord != Ordering::Greater,
        CmpOp::In => return Err("IN not valid in constant comparison".into()),
        CmpOp::JsonExists => return Err("JSONB '?' not valid in constant comparison".into()),
    })
}

/// ⭐ S1: `DELETE FROM t WHERE ...`
fn parse_delete(p: &mut P) -> Result<SqlStmt, String> {
    p.kw("DELETE")?;
    p.kw("FROM")?;
    let table = p.table_ident()?;
    let conds = parse_where(p)?;
    p.done()?;
    Ok(SqlStmt::Delete { table, conds })
}

/// ⭐ S1: `UPDATE t SET c = v [, c2 = v2 ...] WHERE ...`
fn parse_update(p: &mut P) -> Result<SqlStmt, String> {
    p.kw("UPDATE")?;
    let table = p.table_ident()?;
    p.kw("SET")?;
    let mut sets: Vec<(String, SqlValue)> = Vec::new();
    loop {
        let col = p.ident()?;
        p.expect(&Tok::Eq, "=")?;
        // ⭐ PG 兼容: SET 值 — 表达式 (`col+1` / `NOT col`) 或 单字面量/列引用
        let val = parse_update_set_value(p)?;
        sets.push((col, val));
        if p.peek() == Some(&Tok::Comma) {
            p.next()?;
        } else {
            break;
        }
    }
    let conds = parse_where(p)?;
    // ⭐ compat: 吞 RETURNING ... (v1 不返回受影响行值)
    if p.try_kw("RETURNING") {
        while !matches!(p.peek(), None) {
            p.i += 1;
        }
    }
    p.done()?;
    Ok(SqlStmt::Update { table, sets, conds })
}

/// ⭐ PG 兼容 (UPDATE SET): 解析 SET 右侧值 — 字面量 / 列引用 / 表达式
/// (`col+1` / `col-1` / `NOT col`). 表达式折叠成 `SqlValue::Expr(ScalarExpr)`.
fn parse_update_set_value(p: &mut P) -> Result<SqlValue, String> {
    use crate::protocol::sql::{ArithOp, ScalarExpr};
    // 解析一个"项" (字面量 / 列引用 / NOT 前缀)
    fn atom(p: &mut P) -> Result<ScalarExpr, String> {
        match p.peek().cloned() {
            Some(Tok::Ident(s)) => {
                let up = s.to_ascii_uppercase();
                match up.as_str() {
                    "NULL" => {
                        p.next()?;
                        Ok(ScalarExpr::Lit(SqlValue::Null))
                    }
                    "TRUE" => {
                        p.next()?;
                        Ok(ScalarExpr::Lit(SqlValue::Int(1)))
                    }
                    "FALSE" => {
                        p.next()?;
                        Ok(ScalarExpr::Lit(SqlValue::Int(0)))
                    }
                    "NOT" => {
                        p.next()?;
                        let e = atom(p)?;
                        Ok(ScalarExpr::Not(Box::new(e)))
                    }
                    _ => {
                        // 列引用
                        p.next()?;
                        Ok(ScalarExpr::Col(s))
                    }
                }
            }
            Some(Tok::Num(_)) | Some(Tok::Str(_)) | Some(Tok::Minus) | Some(Tok::LParen) => {
                let v = p.value()?;
                Ok(ScalarExpr::Lit(v))
            }
            // ⭐ P1: 占位符 (MySQL `?` / PG `$n`) — 走 p.value() 产出 SqlValue::Param
            Some(Tok::Question) | Some(Tok::Dollar(_)) => {
                let v = p.value()?;
                Ok(ScalarExpr::Lit(v))
            }
            _ => Ok(ScalarExpr::Lit(SqlValue::Null)),
        }
    }
    let left = match atom(p) {
        Ok(e) => e,
        Err(_) => ScalarExpr::Lit(SqlValue::Null),
    };
    // 链式二元算术: 左结合, 支持 `a + b - c * d` (v1: 无优先级, 从左到右)
    let mut acc = left;
    let mut saw_op = false;
    while let Some(op) = match p.peek() {
        Some(Tok::Plus) => Some(ArithOp::Add),
        Some(Tok::Minus) => Some(ArithOp::Sub),
        Some(Tok::Star) => Some(ArithOp::Mul),
        Some(Tok::Slash) => Some(ArithOp::Div),
        _ => None,
    } {
        p.next()?;
        let Ok(right) = atom(p) else { break };
        acc = ScalarExpr::Bin { op, l: Box::new(acc), r: Box::new(right) };
        saw_op = true;
    }
    // 有算术 → 表达式; 无算术 → 折叠为原 SqlValue (列引用 / 字面量 / NOT)
    if saw_op {
        return Ok(SqlValue::Expr(Box::new(acc)));
    }
    Ok(match acc {
        ScalarExpr::Col(c) => SqlValue::ColRef(c),
        ScalarExpr::Lit(v) => v,
        ScalarExpr::Not(e) => SqlValue::Expr(Box::new(ScalarExpr::Not(e))),
        other => SqlValue::Expr(Box::new(other)),
    })
}

/// ⭐ F80: 列类型名 → ColType (parse_create/parse_alter 共用). 吞方言噪声
/// (`DOUBLE PRECISION` / `VARCHAR(n)` / `DECIMAL(p,s)` 长度与精度参数).
fn parse_col_type(p: &mut P) -> Result<ColType, String> {
    let ty_name = p.ident()?;
    let up = ty_name.to_ascii_uppercase();
    // ⭐ F81: DECIMAL/NUMERIC(p,s) — 捕获精度与标度存入类型
    if up == "DECIMAL" || up == "NUMERIC" || up == "DEC" {
        let (mut precision, mut scale) = (10u8, 0u8);
        if p.peek() == Some(&Tok::LParen) {
            p.next()?;
            precision = match p.next()? {
                Tok::Num(n) => n.parse::<u8>().map_err(|_| "bad DECIMAL precision".to_string())?,
                other => return Err(format!("expected DECIMAL precision, got {other:?}")),
            };
            if p.peek() == Some(&Tok::Comma) {
                p.next()?;
                scale = match p.next()? {
                    Tok::Num(n) => n.parse::<u8>().map_err(|_| "bad DECIMAL scale".to_string())?,
                    other => return Err(format!("expected DECIMAL scale, got {other:?}")),
                };
            }
            p.expect(&Tok::RParen, ")")?;
        }
        if scale > 38 || precision > 38 {
            return Err("DECIMAL precision/scale must be <= 38".into());
        }
        return Ok(ColType::Decimal { precision, scale });
    }
    let ty = match up.as_str() {
        "INT" | "BIGINT" | "INTEGER" | "SMALLINT" => ColType::I64,
        "BOOLEAN" | "BOOL" => ColType::Bool,
        "DOUBLE" | "FLOAT" | "REAL" => ColType::F64,
        "TEXT" | "VARCHAR" | "CHAR" | "STRING" => ColType::Str,
        "BLOB" | "BYTES" | "BYTEA" => ColType::Bytes,
        "DATE" => ColType::Date,
        "TIME" => ColType::Time,
        "TIMESTAMP" | "DATETIME" | "TIMESTAMPTZ" => ColType::Timestamp, // ⭐ compat: TIMESTAMPTZ 别名
        "JSON" | "JSONB" => ColType::Json,
        "UUID" => ColType::Uuid,
        other => return Err(format!("unknown type {other}")),
    };
    if ty_name.eq_ignore_ascii_case("DOUBLE") {
        p.try_kw("PRECISION");
    }
    // 长度参数: (n) — 吞
    if p.peek() == Some(&Tok::LParen) {
        p.next()?;
        loop {
            match p.next()? {
                Tok::Num(_) => {}
                other => return Err(format!("expected type length, got {other:?}")),
            }
            match p.next()? {
                Tok::Comma => continue,
                Tok::RParen => break,
                other => return Err(format!("expected ',' or ')' in type params, got {other:?}")),
            }
        }
    }
    // ⭐ compat: 数组后缀 `TEXT[]` — v1 映射为标量类型 (语义: 存 JSON 序列化数组)
    let is_array = p.try_kw("ARRAY")
        || if matches!(p.peek(), Some(Tok::LBracket)) {
            p.next()?;
            p.expect(&Tok::RBracket, "]")?;
            true
        } else {
            false
        };
    if is_array {
        // 数组类型映射为 Str 列 (值为 JSON 数组文本); 保持类型信息在注释/元数据
        return Ok(match ty {
            ColType::I64 => ColType::I64,
            _ => ColType::Str,
        });
    }
    Ok(ty)
}

/// ⭐ S1: `DROP TABLE [IF EXISTS] t`
fn parse_drop(p: &mut P) -> Result<SqlStmt, String> {
    p.kw("DROP")?;
    if p.try_kw("TRIGGER") {
        // ⭐ compat: DROP TRIGGER [IF EXISTS] name ON t — 吞掉
        return Ok(SqlStmt::DdlStub);
    }
    if p.try_kw("INDEX") {
        // ⭐ compat: DROP INDEX [IF EXISTS] name — 吞掉
        return Ok(SqlStmt::DdlStub);
    }
    p.kw("TABLE")?;
    let _if_exists = p.try_kw("IF") && { p.try_kw("NOT"); p.try_kw("EXISTS"); true };
    // 支持逗号分隔多表 DROP: 只取首表 (其余吞)
    let table = p.table_ident()?;
    while p.try_kw("CASCADE") || p.try_kw("RESTRICT") {}
    if matches!(p.peek(), Some(Tok::Comma)) {
        while !matches!(p.peek(), None) {
            p.i += 1;
        }
    }
    p.done()?;
    Ok(SqlStmt::DropTable { table })
}

/// ⭐ F79: `ALTER TABLE t ADD [COLUMN] [IF NOT EXISTS] name TYPE [NULL|NOT NULL] [DEFAULT v]`.
/// v1 仅支持追加可空列; DROP/MODIFY/RENAME 拒.
/// ⭐ compat: `ALTER TABLE t DROP [COLUMN] c` — 标记删除 (列号/布局不变, 存量零重写).
fn parse_alter(p: &mut P) -> Result<SqlStmt, String> {
    p.kw("ALTER")?;
    p.kw("TABLE")?;
    let table = p.table_ident()?;
    if p.try_kw("DROP") {
        p.try_kw("COLUMN"); // 可选
        let name = p.ident()?;
        p.done()?;
        return Ok(SqlStmt::AlterTable { table, add: None, drop: Some(name), if_not_exists: false });
    }
    if !p.try_kw("ADD") {
        return Err("only ALTER TABLE ADD COLUMN / DROP COLUMN is supported (v1)".into());
    }
    p.try_kw("COLUMN"); // 可选
    // ⭐ compat: ADD COLUMN IF NOT EXISTS
    let if_not_exists = p.try_kw("IF") && { p.try_kw("NOT"); p.try_kw("EXISTS"); true };
    let name = p.ident()?;
    let ty = parse_col_type(p)?;
    // 列属性: NULL/NOT NULL/DEFAULT
    let mut nullable = true;
    let mut default: Option<storage::schema::ColDefault> = None;
    loop {
        if p.try_kw("NOT") {
            p.kw("NULL")?;
            nullable = false;
        } else if p.try_kw("NULL") {
            nullable = true;
        } else if p.try_kw("DEFAULT") {
            // ⭐ PG 兼容: 捕获 DEFAULT 表达式 (由 worker 对新行求值回填)
            default = parse_col_default(p, ty)?;
        } else {
            break;
        }
    }
    // ⭐ compat: NOT NULL 且无 DEFAULT → 旧行无法回填, 保持 v1 拒绝;
    //   有 DEFAULT (如迁移的 NOT NULL DEFAULT false) → 接受 (由 worker 回填默认).
    if !nullable && default.is_none() {
        return Err("ADD COLUMN NOT NULL requires a DEFAULT (v1: cannot backfill existing rows)".into());
    }
    p.done()?;
    Ok(SqlStmt::AlterTable {
        table,
        add: Some(Column { name, ty, nullable, default }),
        drop: None,
        if_not_exists,
    })
}

/// ⭐ F78: 聚合内标量表达式递归下降 — 加减 > 乘除 > 因子 (列/字面量/括号).
fn parse_scalar_expr(p: &mut P) -> Result<ScalarExpr, String> {
    let mut lhs = parse_scalar_term(p)?;
    loop {
        let op = match p.peek() {
            Some(Tok::Plus) => ArithOp::Add,
            Some(Tok::Minus) => ArithOp::Sub,
            _ => break,
        };
        p.next()?;
        let rhs = parse_scalar_term(p)?;
        lhs = ScalarExpr::Bin { op, l: Box::new(lhs), r: Box::new(rhs) };
    }
    Ok(lhs)
}

fn parse_scalar_term(p: &mut P) -> Result<ScalarExpr, String> {
    let mut lhs = parse_scalar_factor(p)?;
    loop {
        let op = match p.peek() {
            Some(Tok::Star) => ArithOp::Mul,
            Some(Tok::Slash) => ArithOp::Div,
            _ => break,
        };
        p.next()?;
        let rhs = parse_scalar_factor(p)?;
        lhs = ScalarExpr::Bin { op, l: Box::new(lhs), r: Box::new(rhs) };
    }
    Ok(lhs)
}

fn parse_scalar_factor(p: &mut P) -> Result<ScalarExpr, String> {
    match p.peek() {
        Some(Tok::LParen) => {
            p.next()?;
            let e = parse_scalar_expr(p)?;
            p.expect(&Tok::RParen, ")")?;
            Ok(e)
        }
        Some(Tok::Num(_)) | Some(Tok::Str(_)) => Ok(ScalarExpr::Lit(p.value()?)),
        Some(Tok::Ident(_)) => Ok(ScalarExpr::Col(p.ident()?)),
        other => Err(format!("expected column/number/expression, got {other:?}")),
    }
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
    let mut offset = None;
    if p.try_kw("LIMIT") {
        match p.next()? {
            Tok::Num(n) => {
                let a = n.parse::<u32>().map_err(|_| format!("bad LIMIT {n}"))?;
                // ⭐ F76: MySQL `LIMIT offset, count` 逗号形态
                if p.peek() == Some(&Tok::Comma) {
                    p.next()?;
                    match p.next()? {
                        Tok::Num(m) => {
                            offset = Some(a);
                            limit = Some(m.parse::<u32>().map_err(|_| format!("bad LIMIT {m}"))?);
                        }
                        other => return Err(format!("expected LIMIT count, got {other:?}")),
                    }
                } else {
                    limit = Some(a);
                }
            }
            other => return Err(format!("expected LIMIT count, got {other:?}")),
        }
    }
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
        let table = p.table_ident()?;
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
        let table = p.table_ident()?;
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

/// ⭐ F76: 投影列输出别名 — `[AS] alias` (仅非保留字; FROM/子句关键字不当别名).
fn parse_col_alias(p: &mut P) -> Option<String> {
    if p.try_kw("AS") {
        return p.ident().ok();
    }
    if let Some(Tok::Ident(s)) = p.peek() {
        let up = s.to_ascii_uppercase();
        let reserved = matches!(
            up.as_str(),
            "FROM" | "AS" | "WHERE" | "ORDER" | "GROUP" | "HAVING" | "LIMIT" | "OFFSET"
                | "JOIN" | "INNER" | "LEFT" | "RIGHT" | "FULL" | "CROSS" | "ON" | "USING"
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
        let table = p.table_ident()?;
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
            SelectItem::Col { name, .. } => Ok(JoinItem::Col(QualCol::parse(name))),
            SelectItem::Agg { .. } => {
                Err("aggregate functions are not supported in JOIN queries".to_string())
            }
            SelectItem::ScalarFn { .. } => {
                Err("scalar functions are not supported in JOIN queries".to_string())
            }
            SelectItem::Expr { .. } => {
                Err("expression projections are not supported in JOIN queries (v1)".to_string())
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
    // ⭐ F77: SELECT DISTINCT — 在投影前捕获; 后续 desugar 成 GROUP BY 全投影列.
    let distinct = p.try_kw("DISTINCT");
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
    } else if matches!(p.peek(), Some(Tok::Ident(s)) if s.eq_ignore_ascii_case("FROM")) {
        // ⭐ PG 兼容: 空投影 `SELECT FROM t` — 等价 SELECT * FROM t (migrator 探表)
    } else {
        loop {
            let name = p.ident()?;
            // ⭐ PG 兼容: SELECT EXISTS (SELECT ...) — 标量布尔探测 (migrator 建库/探表)
            if name.eq_ignore_ascii_case("EXISTS") && p.peek_paren_select() {
                let inner = parse_paren_subselect(p)?;
                p.done()?;
                return Ok(SqlStmt::ExistsStub { inner });
            }
            // ⭐ G1: ident( → 聚合函数 COUNT/SUM/AVG/MIN/MAX
            if p.peek() == Some(&Tok::LParen) {
                // ⭐ compat: 标量函数 (NOW()/CURRENT_TIMESTAMP) → ScalarFn (投影常量)
                if matches!(
                    name.to_ascii_uppercase().as_str(),
                    "NOW" | "CURRENT_TIMESTAMP" | "CURRENT_DATE" | "CURRENT_TIME"
                ) {
                    p.next()?; // (
                    let mut depth = 1;
                    while depth > 0 {
                        match p.next()? {
                            Tok::LParen => depth += 1,
                            Tok::RParen => depth -= 1,
                            _ => {}
                        }
                    }
                    items.push(SelectItem::ScalarFn { name: name.to_ascii_lowercase() });
                    break;
                }
                let func = match name.to_ascii_uppercase().as_str() {
                    "COUNT" => AggFn::Count,
                    "SUM" => AggFn::Sum,
                    "AVG" => AggFn::Avg,
                    "MIN" => AggFn::Min,
                    "MAX" => AggFn::Max,
                    other => return Err(format!("unknown function '{other}'")),
                };
                p.next()?; // (
                // ⭐ F77: COUNT(DISTINCT ...) — DISTINCT 仅 COUNT
                let distinct = p.try_kw("DISTINCT");
                if distinct && func != AggFn::Count {
                    return Err("DISTINCT is only supported in COUNT (v1)".into());
                }
                let arg = if p.peek() == Some(&Tok::Star) {
                    if func != AggFn::Count {
                        return Err(format!("{name}(*) is not valid (only COUNT(*))"));
                    }
                    if distinct {
                        return Err("COUNT(DISTINCT *) is not valid".into());
                    }
                    p.next()?;
                    None
                } else {
                    // ⭐ F78: 聚合内标量表达式 (裸列退化为 ScalarExpr::Col)
                    let e = parse_scalar_expr(p)?;
                    // ⭐ F77: DISTINCT 仅允许单裸列
                    if distinct && e.as_col().is_none() {
                        return Err("COUNT(DISTINCT ...) requires a single column (v1)".into());
                    }
                    Some(e)
                };
                p.expect(&Tok::RParen, ")")?;
                let alias = parse_col_alias(p);
                items.push(SelectItem::Agg { func, arg, distinct, alias });
            } else if matches!(p.peek(), Some(Tok::Arrow | Tok::ArrowText)) {
                // ⭐ compat: JSONB 操作符 j->'a' / j->>'a' (v1: 列 + 字面量键, 可链式)
                let mut expr = ScalarExpr::Col(name);
                loop {
                    let as_text = match p.peek() {
                        Some(Tok::Arrow) => false,
                        Some(Tok::ArrowText) => true,
                        _ => break,
                    };
                    p.next()?;
                    let key = p.value()?;
                    expr = ScalarExpr::JsonGet {
                        base: Box::new(expr),
                        key: Box::new(ScalarExpr::Lit(key)),
                        as_text,
                    };
                }
                let alias = parse_col_alias(p);
                items.push(SelectItem::Expr { expr, alias });
            } else {
                let alias = parse_col_alias(p);
                items.push(SelectItem::Col { name, alias });
            }
            if p.peek() == Some(&Tok::Comma) {
                p.next()?;
            } else {
                break;
            }
        }
    }
    if !matches!(p.peek(), Some(Tok::Ident(s)) if s.eq_ignore_ascii_case("FROM")) {
        // ⭐ compat: 无 FROM 的标量函数投影 (SELECT NOW()/CURRENT_TIMESTAMP) — 常量单行
        if items.iter().all(|i| matches!(i, SelectItem::ScalarFn { .. })) && !items.is_empty() {
            p.done()?;
            return Ok(SqlStmt::ScalarSelect { items });
        }
    }
    p.kw("FROM")?;
    // ⭐ F77: DISTINCT 仅支持单表命名列投影; 派生表/JOIN/系统表 拒
    if distinct && p.peek_paren_select() {
        return Err("DISTINCT with a derived table is not supported (v1)".into());
    }
    // ⭐ F72: FROM 派生表 `(SELECT ...) alias` — items (外层投影) 已解完, 传入.
    if p.peek_paren_select() {
        return parse_derived(p, items, top);
    }
    let table = p.ident()?;
    // ⭐ PG 兼容: 裸名 pg_* 系统表 → 映射 pg_catalog.X (PG search_path 默认含 pg_catalog)
    let table = if !table.contains('.')
        && matches!(
            table.to_ascii_lowercase().as_str(),
            "pg_database"
                | "pg_namespace"
                | "pg_class"
                | "pg_attribute"
                | "pg_tables"
                | "pg_indexes"
                | "pg_views"
                | "pg_settings"
        ) {
        format!("pg_catalog.{table}")
    } else {
        table
    };
    // ⭐ F66: 系统表拦截 — `information_schema.X` / `pg_catalog.X` (大小写不敏)
    // 走虚拟表合成路径; 尾部只解 WHERE/ORDER/LIMIT/OFFSET (不支持 GROUP/HAVING)
    if let Some((cat, tbl)) = split_system_table(&table) {
        if distinct {
            return Err("DISTINCT on system tables is not supported (v1)".into());
        }
        let conds = parse_where(p)?;
        let (order, limit, offset) = parse_select_tail(p)?;
        if top {
            p.done()?;
        }
        let cols: Vec<String> = items
            .iter()
            .filter_map(|i| match i {
                SelectItem::Col { name, .. } => Some(name.clone()),
                SelectItem::Agg { .. } => None,
                SelectItem::ScalarFn { .. } => None,
                SelectItem::Expr { .. } => None,
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
    // ⭐ F76: 非系统表 → 剥 db 限定前缀 (`default.t` → `t`); 系统表已在上方按全名分派
    let table = strip_db_qual(table);
    // ⭐ F67 (JOIN): 表名后 3 token 内出现 JOIN/INNER/LEFT → 转 JOIN 解析
    if is_join_ahead(p) {
        if distinct {
            return Err("DISTINCT with JOIN is not supported (v1)".into());
        }
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
    // ⭐ F77: SELECT DISTINCT desugar → GROUP BY 全投影列 (复用分桶去重路径)
    if distinct {
        if items.iter().any(|i| matches!(i, SelectItem::Agg { .. })) {
            return Err("DISTINCT with aggregate is not supported (v1)".into());
        }
        if !group_by.is_empty() {
            return Err("DISTINCT with GROUP BY is not supported (v1)".into());
        }
        if items.is_empty() {
            return Err("SELECT DISTINCT * is not supported (v1); list columns explicitly".into());
        }
        group_by = items
            .iter()
            .filter_map(|i| match i {
                SelectItem::Col { name, .. } => Some(name.clone()),
                SelectItem::Agg { .. } => None,
                SelectItem::ScalarFn { .. } => None,
                SelectItem::Expr { .. } => None,
            })
            .collect();
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
            if let SelectItem::Col { name: c, .. } = it
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
    let mut offset = None;
    if p.try_kw("LIMIT") {
        match p.next()? {
            Tok::Num(n) => {
                let a = n.parse::<u32>().map_err(|_| format!("bad LIMIT {n}"))?;
                // ⭐ F76: MySQL `LIMIT offset, count` 逗号形态
                if p.peek() == Some(&Tok::Comma) {
                    p.next()?;
                    match p.next()? {
                        Tok::Num(m) => {
                            offset = Some(a);
                            limit = Some(m.parse::<u32>().map_err(|_| format!("bad LIMIT {m}"))?);
                        }
                        other => return Err(format!("expected LIMIT count, got {other:?}")),
                    }
                } else {
                    limit = Some(a);
                }
            }
            other => return Err(format!("expected LIMIT count, got {other:?}")),
        }
    }
    // ⭐ S2: OFFSET n (PG/MySQL 通用形态)
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