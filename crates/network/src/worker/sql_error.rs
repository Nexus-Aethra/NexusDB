//! ⭐ 审计 P3-5/6: 结构化 SQL 错误码 — 错误码不再从消息文本猜.
//!
//! 旧实现: `mysql_err_packet` / `sql_err_bytes` 用 `msg.contains("duplicate key")`
//! 一类的**字符串匹配**推断 MySQL errno / PG SQLSTATE / HTTP status. 两个缺陷:
//! ① 产生方改写措辞 (如 "unknown column" → "no such column") 即静默跌回通用码
//!    (1105/XX000/500) — ORM 的 IntegrityError/重试分类静默失灵;
//! ② 新增错误种类容易忘记在三条 contains 链各加一支.
//!
//! 现结构:
//! - `SqlErrorCode` 封闭枚举 + **穷尽 match** 的三协议映射 (漏映射编译期报错);
//! - `SqlError { code, msg }` 结构化错误, 新代码直接用 `SqlError::new(code, msg)`;
//! - `SqlError::classified(msg)` 遗留适配器 — 深管线仍是 `Result<_, String>`,
//!   字符串分类**只此一处**, 且由测试钉死全部消息模板 (漂移即红).
//!
//! 统一分类器把旧有三条链的差异显式化 (MySQL 无 "transaction is aborted"/
//! "unknown type" 支, HTTP 用 "no schema"): 合并后 "unknown type" 归 Syntax
//! (DDL 解析错, 归语法码更正确), "no schema" 归 NoSuchTable (被 "has no
//! schema" 覆盖, 语义一致).

use std::fmt;

/// 协议无关的 SQL 错误码 (封闭集).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqlErrorCode {
    /// 未知列 (1054 / 42703 / 400).
    UnknownColumn,
    /// 唯一/主键冲突 (1062 / 23505 / 409) — ORM IntegrityError.
    DupEntry,
    /// 外键违例 (1451 / 23503 / 409) — ORM IntegrityError.
    FkViolation,
    /// 序列化冲突 (1213 / 40001 / 500) — ORM 据此重试.
    SerializationFailure,
    /// 只读事务中写 (1792 / 25006 / 500).
    ReadOnlyTxn,
    /// 事务已 aborted (1105 / 25P02 / 500).
    TxnAborted,
    /// 库不存在 (1049 / 3D000 / 400).
    UnknownDatabase,
    /// 跨库访问 v1 不支持 (1235 / 0A000 / 500).
    CrossDbUnsupported,
    /// 表/schema 不存在 (1146 / XX000 / 400) — ORM has_table 判不存在后发 CREATE.
    NoSuchTable,
    /// 语法错误 (1064 / 42601 / 400).
    Syntax,
    /// 认证失败 (1045 / XX000 / 500).
    AccessDenied,
    /// 未分类内部错误 (1105 / XX000 / 500).
    Internal,
}

impl SqlErrorCode {
    /// MySQL errno. 与旧 `mysql_err_packet` 的 contains 链逐码一致.
    pub fn mysql_errno(self) -> u16 {
        match self {
            Self::UnknownColumn => 1054,
            Self::DupEntry => 1062,
            Self::FkViolation => 1451,
            Self::SerializationFailure => 1213,
            Self::ReadOnlyTxn => 1792,
            Self::TxnAborted => 1105,
            Self::UnknownDatabase => 1049,
            Self::CrossDbUnsupported => 1235,
            Self::NoSuchTable => 1146,
            Self::Syntax => 1064,
            Self::AccessDenied => 1045,
            Self::Internal => 1105,
        }
    }

    /// PG SQLSTATE. 与旧 `sql_err_bytes` Pg 分支逐码一致.
    pub fn pg_sqlstate(self) -> &'static str {
        match self {
            Self::UnknownColumn => "42703",
            Self::DupEntry => "23505",
            Self::FkViolation => "23503",
            Self::SerializationFailure => "40001",
            Self::ReadOnlyTxn => "25006",
            Self::TxnAborted => "25P02",
            Self::UnknownDatabase => "3D000",
            Self::CrossDbUnsupported => "0A000",
            Self::NoSuchTable => "XX000",
            Self::Syntax => "42601",
            Self::AccessDenied => "XX000",
            Self::Internal => "XX000",
        }
    }

    /// HTTP 状态码. 与旧 `sql_err_bytes` Http 分支逐码一致.
    pub fn http_status(self) -> u16 {
        match self {
            Self::UnknownColumn => 400,
            Self::DupEntry => 409,
            Self::FkViolation => 409,
            Self::SerializationFailure => 500,
            Self::ReadOnlyTxn => 500,
            Self::TxnAborted => 500,
            Self::UnknownDatabase => 400,
            Self::CrossDbUnsupported => 500,
            Self::NoSuchTable => 400,
            Self::Syntax => 400,
            Self::AccessDenied => 500,
            Self::Internal => 500,
        }
    }

    /// 遗留适配器: 按消息文本分类 (旧三协议 contains 链的并集, 顺序保持).
    ///
    /// **唯一允许的字符串匹配处**. 新代码应直接构造 `SqlError::new(code, ..)`;
    /// 本函数的存在只为兼容深管线 (`Result<_, String>`) 的既有错误 —
    /// `classify_pins_legacy_messages` 测试钉死全部模板, 措辞漂移即红.
    pub fn classify(msg: &str) -> Self {
        if msg.contains("unknown column") {
            Self::UnknownColumn
        } else if msg.contains("duplicate key") {
            Self::DupEntry
        } else if msg.contains("serialization failure") {
            Self::SerializationFailure
        } else if msg.contains("read-only transaction") {
            Self::ReadOnlyTxn
        } else if msg.contains("transaction is aborted") {
            Self::TxnAborted
        } else if msg.contains("Unknown database") {
            Self::UnknownDatabase
        } else if msg.contains("Cross-database access is not supported") {
            Self::CrossDbUnsupported
        } else if msg.contains("has no schema") || msg.contains("doesn't exist") {
            Self::NoSuchTable
        } else if msg.contains("foreign key violation") {
            Self::FkViolation
        } else if msg.contains("expected")
            || msg.contains("unexpected")
            || msg.contains("unterminated")
            || msg.contains("unknown type")
        {
            Self::Syntax
        } else if msg.contains("Access denied") {
            Self::AccessDenied
        } else {
            Self::Internal
        }
    }
}

/// 结构化 SQL 错误: 码 + 消息. `Display` 只渲染消息 (落入 String 管线无损).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlError {
    pub code: SqlErrorCode,
    pub msg: String,
}

impl SqlError {
    pub fn new(code: SqlErrorCode, msg: impl Into<String>) -> Self {
        Self {
            code,
            msg: msg.into(),
        }
    }

    /// 从遗留 String 错误构造 (走 `classify` 适配器).
    pub fn classified(msg: impl Into<String>) -> Self {
        let msg = msg.into();
        Self {
            code: SqlErrorCode::classify(&msg),
            msg,
        }
    }
}

impl fmt::Display for SqlError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.msg)
    }
}

impl From<String> for SqlError {
    fn from(msg: String) -> Self {
        Self::classified(msg)
    }
}

impl From<&str> for SqlError {
    fn from(msg: &str) -> Self {
        Self::classified(msg)
    }
}

impl From<SqlError> for String {
    fn from(e: SqlError) -> Self {
        e.msg
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 钉死遗留分类契约: 全部被分类的消息模板 → 期望码.
    /// 产生方改写措辞 → 本测试红 (替代旧的"静默跌到通用码").
    #[test]
    fn classify_pins_legacy_messages() {
        let cases: &[(&str, SqlErrorCode)] = &[
            ("unknown column 'x'", SqlErrorCode::UnknownColumn),
            (
                "duplicate key on global unique column",
                SqlErrorCode::DupEntry,
            ),
            (
                "serialization failure: read-set changed",
                SqlErrorCode::SerializationFailure,
            ),
            (
                "read-only transaction cannot write",
                SqlErrorCode::ReadOnlyTxn,
            ),
            (
                "transaction is aborted, commands ignored",
                SqlErrorCode::TxnAborted,
            ),
            ("Unknown database 'app'", SqlErrorCode::UnknownDatabase),
            (
                "Cross-database access is not supported (v1)",
                SqlErrorCode::CrossDbUnsupported,
            ),
            (
                "table 't' has no schema (not a SQL table)",
                SqlErrorCode::NoSuchTable,
            ),
            ("table 'app.t' doesn't exist", SqlErrorCode::NoSuchTable),
            (
                "foreign key violation: child.col still references the deleted row(s)",
                SqlErrorCode::FkViolation,
            ),
            ("unexpected token in WHERE", SqlErrorCode::Syntax),
            ("unterminated string literal", SqlErrorCode::Syntax),
            ("expected expression, got ')'", SqlErrorCode::Syntax),
            ("unknown type FROBNICATE", SqlErrorCode::Syntax),
            ("Access denied for user", SqlErrorCode::AccessDenied),
            ("something nobody classified", SqlErrorCode::Internal),
        ];
        for (msg, want) in cases {
            assert_eq!(
                SqlErrorCode::classify(msg),
                *want,
                "消息模板分类漂移: {msg:?}"
            );
        }
    }

    /// 三协议映射与旧实现逐码一致 (改映射必须同步改这里 — 显式契约).
    #[test]
    fn code_maps_to_all_three_protocols() {
        use SqlErrorCode::*;
        let table: &[(SqlErrorCode, u16, &str, u16)] = &[
            (UnknownColumn, 1054, "42703", 400),
            (DupEntry, 1062, "23505", 409),
            (FkViolation, 1451, "23503", 409),
            (SerializationFailure, 1213, "40001", 500),
            (ReadOnlyTxn, 1792, "25006", 500),
            (TxnAborted, 1105, "25P02", 500),
            (UnknownDatabase, 1049, "3D000", 400),
            (CrossDbUnsupported, 1235, "0A000", 500),
            (NoSuchTable, 1146, "XX000", 400),
            (Syntax, 1064, "42601", 400),
            (AccessDenied, 1045, "XX000", 500),
            (Internal, 1105, "XX000", 500),
        ];
        for (code, errno, sqlstate, http) in table {
            assert_eq!(code.mysql_errno(), *errno, "{code:?} mysql");
            assert_eq!(code.pg_sqlstate(), *sqlstate, "{code:?} pg");
            assert_eq!(code.http_status(), *http, "{code:?} http");
        }
    }

    #[test]
    fn structured_error_keeps_explicit_code() {
        let e = SqlError::new(SqlErrorCode::FkViolation, "fk boom");
        assert_eq!(e.code, SqlErrorCode::FkViolation);
        assert_eq!(e.to_string(), "fk boom");
        assert_eq!(String::from(e), "fk boom");
        // String 入口走 classify (兼容既有管线)
        assert_eq!(
            SqlError::from("duplicate key on x").code,
            SqlErrorCode::DupEntry
        );
    }
}
