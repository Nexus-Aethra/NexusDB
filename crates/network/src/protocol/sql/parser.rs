//! ⭐ X1 (SQL 落地): 手写 tokenizer + tree-walking parser — 纯函数, 零依赖.
//! 从 sql.rs 拆分 (2026-08). AST 类型见 ast.rs.

use super::ast::*;
use super::parser_ddl::*;
use super::parser_select::*;
use super::parser_where::*;

// tokenizer
// =====================================================================

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Tok {
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
                // ⭐ PG 兼容 (multi-statement): 跳过 `--` 行注释 (拆分后语句
                // 可能以注释行开头, 或语句内嵌注释)
                if b.get(i + 1) == Some(&b'-') {
                    i += 2;
                    while i < b.len() && b[i] != b'\n' {
                        i += 1;
                    }
                    continue;
                }
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

pub(crate) struct P {
    pub(crate) toks: Vec<Tok>,
    pub(crate) i: usize,
    /// ⭐ P1: `?` 自动编号计数.
    pub(crate) next_param: u16,
    /// ⭐ P1: 占位符风格混用检测 (?/$ 二选一).
    pub(crate) saw_question: bool,
    pub(crate) saw_dollar: bool,
}

impl P {
    pub(crate) fn peek(&self) -> Option<&Tok> {
        self.toks.get(self.i)
    }

    /// ⭐ PG 兼容: 第 i+1 个 token 是否为 `(` (区分 `version(` 函数 vs `version` 列).
    pub(crate) fn peek2_is_lparen(&self) -> bool {
        matches!(self.toks.get(self.i + 1), Some(Tok::LParen))
    }

    pub(crate) fn next(&mut self) -> Result<Tok, String> {
        let t = self.toks.get(self.i).cloned().ok_or("unexpected end of statement")?;
        self.i += 1;
        Ok(t)
    }

    /// 消费一个关键字 (大小写不敏感), 不匹配报错.
    pub(crate) fn kw(&mut self, want: &str) -> Result<(), String> {
        match self.next()? {
            Tok::Ident(s) if s.eq_ignore_ascii_case(want) => Ok(()),
            other => Err(format!("expected {want}, got {other:?}")),
        }
    }

    /// 试探关键字: 匹配则消费返回 true.
    /// ⭐ G1 (F63): 比较算子 (HAVING 用, 与 WHERE 同集去 IN/BETWEEN/LIKE).
    pub(crate) fn cmp_op(&mut self) -> Result<CmpOp, String> {
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

    pub(crate) fn try_kw(&mut self, want: &str) -> bool {
        if let Some(Tok::Ident(s)) = self.peek()
            && s.eq_ignore_ascii_case(want)
        {
            self.i += 1;
            return true;
        }
        false
    }

    pub(crate) fn ident(&mut self) -> Result<String, String> {
        match self.next()? {
            Tok::Ident(s) => Ok(s),
            other => Err(format!("expected identifier, got {other:?}")),
        }
    }

    /// ⭐ F76: 表位置标识符 — 读 ident 并剥 db 限定前缀 (`db.tbl` → `tbl`).
    pub(crate) fn table_ident(&mut self) -> Result<String, String> {
        Ok(strip_db_qual(self.ident()?))
    }

    pub(crate) fn expect(&mut self, want: &Tok, what: &str) -> Result<(), String> {
        let t = self.next()?;
        if &t == want { Ok(()) } else { Err(format!("expected {what}, got {t:?}")) }
    }

    pub(crate) fn value(&mut self) -> Result<SqlValue, String> {
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

    pub(crate) fn done(&self) -> Result<(), String> {
        if self.i == self.toks.len() {
            Ok(())
        } else {
            Err(format!("trailing tokens after statement: {:?}", &self.toks[self.i..]))
        }
    }

    /// ⭐ F71: 仅顶层语句校验尾部无残余 token; 子查询 (top=false) 不校验.
    pub(crate) fn done_if(&self, top: bool) -> Result<(), String> {
        if top { self.done() } else { Ok(()) }
    }

    /// ⭐ F71: 当前是 `( SELECT` (子查询开头), 区分于 `(` 分组/字面量列表.
    pub(crate) fn peek_paren_select(&self) -> bool {
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

/// ⭐ PG 兼容 (LIMIT/OFFSET $n): 解析 limit 字面量或其参数索引, bind 后填真实值.
/// 参数值支持 Int / Str (数字串).
fn resolve_limit(lit: Option<u32>, param: Option<u16>, params: &[SqlValue]) -> Result<Option<u32>, String> {
    if let Some(i) = param {
        let v = params
            .get(i as usize)
            .ok_or_else(|| format!("missing parameter {}", i + 1))?;
        match v {
            SqlValue::Int(x) => u32::try_from(*x)
                .map(Some)
                .map_err(|_| format!("LIMIT/OFFSET out of range: {x}")),
            SqlValue::Str(s) => {
                let s = String::from_utf8_lossy(s).trim().to_string();
                s.parse::<u32>().map(Some).map_err(|_| format!("bad LIMIT/OFFSET {s}"))
            }
            other => Err(format!("LIMIT/OFFSET must be integer, got {other:?}")),
        }
    } else {
        Ok(lit)
    }
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
        SqlStmt::Select { table, items, conds, limit, order, offset, group_by, having, limit_param, offset_param } => {
            SqlStmt::Select {
                table: table.clone(),
                items: items.clone(),
                conds: bind_conds(conds)?,
                limit: resolve_limit(*limit, *limit_param, params)?,
                order: order.clone(),
                offset: resolve_limit(*offset, *offset_param, params)?,
                group_by: group_by.clone(),
                having: bind_conds(having)?,
                limit_param: None,
                offset_param: None,
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
        SqlStmt::SelectJoin { from, from_inner, joins, items, conds, order, limit, offset, limit_param, offset_param } => {
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
                limit: resolve_limit(*limit, *limit_param, params)?,
                offset: resolve_limit(*offset, *offset_param, params)?,
                limit_param: None,
                offset_param: None,
            }
        }
        // ⭐ F72: 派生表 — 内层递归绑定 + 外层 WHERE 绑定
        SqlStmt::SelectDerived { inner, alias, items, conds, order, limit, offset, limit_param, offset_param } => {
            SqlStmt::SelectDerived {
                inner: Box::new(bind_params(inner, params)?),
                alias: alias.clone(),
                items: items.clone(),
                conds: bind_conds(conds)?,
                order: order.clone(),
                limit: resolve_limit(*limit, *limit_param, params)?,
                offset: resolve_limit(*offset, *offset_param, params)?,
                limit_param: None,
                offset_param: None,
            }
        }
        // ⭐ PG 兼容: SELECT EXISTS — 内层 (SystemQuery) 递归绑定 $n
        SqlStmt::ExistsStub { inner } => SqlStmt::ExistsStub {
            inner: Box::new(bind_params(inner, params)?),
        },
        // ⭐ F66: 系统表查询 — WHERE 条件支持 $n (migrator 探测)
        SqlStmt::SystemQuery { catalog, table, cols, conds, order, limit, offset, limit_param, offset_param } => {
            SqlStmt::SystemQuery {
                catalog: catalog.clone(),
                table: table.clone(),
                cols: cols.clone(),
                conds: bind_conds(conds)?,
                order: order.clone(),
                limit: resolve_limit(*limit, *limit_param, params)?,
                offset: resolve_limit(*offset, *offset_param, params)?,
                limit_param: None,
                offset_param: None,
            }
        }
        // 无参数位的语句原样克隆
        other => other.clone(),
    })
}

/// ⭐ F76: 读 `(col [, col ...])` 列名列表 (表级约束/索引列; 反引号已在 tokenizer 去).
/// ⭐ compat: 吞可选 ASC/DESC 排序后缀 (CREATE INDEX ... (col DESC)).
/// `CREATE TABLE t (...) | CREATE INDEX [IF NOT EXISTS] name ON t (cols) | CREATE EXTENSION ... | ...`
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

/// ⭐ F80: 列类型名 → ColType (parse_create/parse_alter 共用). 吞方言噪声
/// (`DOUBLE PRECISION` / `VARCHAR(n)` / `DECIMAL(p,s)` 长度与精度参数).
/// 返回 (ColType, is_serial) — SERIAL 列由调用方设自动递增默认值.
pub(crate) fn parse_scalar_expr(p: &mut P) -> Result<ScalarExpr, String> {
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
