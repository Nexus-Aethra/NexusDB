// ⭐ F66: 系统表查询 (information_schema / pg_catalog 虚拟表合成) — 从 sql_dispatch.rs
// 拆出 (解耦 2026-08). 职责: SysQuerySpec 规格 + CatalogDump 合成虚拟表 + 渲染.
use super::*;

pub(crate) struct SysQuerySpec {
    pub(crate) catalog: String,
    pub(crate) table: String,
    pub(crate) cols: Vec<String>,
    pub(crate) conds: Pred<Cond>,
    pub(crate) order: Vec<(String, bool)>,
    pub(crate) limit: Option<u32>,
    pub(crate) offset: Option<u32>,
    pub(crate) exists: bool,
}

impl SysQuerySpec {
    /// 需要表/列元数据 (发 CatalogDump); 否则仅 db 列表.
    pub(crate) fn needs_catalog(&self) -> bool {
        !matches!(
            (self.catalog.as_str(), self.table.as_str()),
            ("information_schema", "schemata")
                | ("pg_catalog", "pg_namespace")
                | ("pg_catalog", "pg_database")
                | ("__show__", "databases")
                | ("__show__", "__empty__")
        )
    }
}

/// ⭐ F66: ColType → information_schema.columns 的 data_type 字符串.
pub(crate) fn coltype_sql_name(ty: ColType) -> &'static str {
    match ty {
        ColType::I64 => "bigint",
        ColType::F64 => "double",
        ColType::Str => "text",
        ColType::Bytes => "blob",
        ColType::Bool => "boolean",
        ColType::Date => "date",
        ColType::Time => "time",
        ColType::Timestamp => "timestamp",
        ColType::Json => "json",
        ColType::Uuid => "uuid",
        ColType::Decimal { .. } => "decimal",
    }
}

/// ⭐ F66: 用合成列名+行跑完成点 (过滤/投影/排序/截断) → 三门面渲染.
/// 虚拟列均为 Str; 行值用 ColValue::Bytes (NULL 用 ColValue::Null).
pub(crate) fn sysq_finish(
    proto: ProtocolKind,
    binary: bool,
    spec: &SysQuerySpec,
    all_cols: &[&str],
    mut rows: Vec<Vec<ColValue>>,
) -> Vec<u8> {
    // 合成 schema (全 Str) 用于 WHERE 过滤 + 投影 + 排序列定位
    let schema = TableSchema {
        version: 1,
        columns: all_cols
            .iter()
            .map(|n| storage::schema::Column {
                name: n.to_string(),
                ty: ColType::Str,
                nullable: true,
                default: None,
            })
            .collect(),
        pk_col: 0,
        indexes: Vec::new(),
        dropped: Vec::new(),
        next_iid: 0,
        version_ncols: Vec::new(),
        fks: Vec::new(),
        resp_row_adapter: Default::default(),
    };
    // WHERE 残余过滤 (递归 eval; `__` 前缀的内部标记叶子如 __table__ 视为真,
    // 已在生成器里处理; 未知真实列的条件 → 不匹配则滤掉)
    rows.retain(|r| eval_pred_sysq(&schema, r, &spec.conds));
    // ORDER BY (按输出列字典序; 未知列忽略)
    for (name, desc) in spec.order.iter().rev() {
        if let Some(ci) = all_cols.iter().position(|c| c.eq_ignore_ascii_case(name)) {
            rows.sort_by(|a, b| {
                let o = cmp_colvalue(&a[ci], &b[ci]);
                if *desc { o.reverse() } else { o }
            });
        }
    }
    // OFFSET / LIMIT
    let start = (spec.offset.unwrap_or(0) as usize).min(rows.len());
    let end = match spec.limit {
        Some(l) => (start + l as usize).min(rows.len()),
        None => rows.len(),
    };
    let rows = &rows[start..end];
    // 投影: cols 空 = 全列; 否则按名选 (未知列 → 全 NULL 列)
    if spec.cols.is_empty() {
        let cols: Vec<(&str, ColType)> = all_cols.iter().map(|c| (*c, ColType::Str)).collect();
        sql_rows_bytes(proto, binary, &cols, rows)
    } else {
        let idxs: Vec<Option<usize>> = spec
            .cols
            .iter()
            .map(|c| all_cols.iter().position(|a| a.eq_ignore_ascii_case(c)))
            .collect();
        let cols: Vec<(&str, ColType)> = spec
            .cols
            .iter()
            .map(|c| (c.as_str(), ColType::Str))
            .collect();
        let proj: Vec<Vec<ColValue>> = rows
            .iter()
            .map(|r| {
                idxs.iter()
                    .map(|oi| oi.and_then(|i| r.get(i).cloned()).unwrap_or(ColValue::Null))
                    .collect()
            })
            .collect();
        sql_rows_bytes(proto, binary, &cols, &proj)
    }
}

pub(crate) fn sbytes(s: &str) -> ColValue {
    ColValue::Bytes(s.as_bytes().to_vec())
}

/// ⭐ F66: db 列表类虚拟表 (schemata / pg_namespace) — 零任务合成.
pub(crate) fn sysq_render_dblist(
    proto: ProtocolKind,
    binary: bool,
    spec: &SysQuerySpec,
    dbs: &[String],
) -> Vec<u8> {
    let (all_cols, rows): (Vec<&str>, Vec<Vec<ColValue>>) =
        match (spec.catalog.as_str(), spec.table.as_str()) {
            ("information_schema", "schemata") => (
                vec!["catalog_name", "schema_name", "default_character_set_name"],
                dbs.iter()
                    .map(|d| vec![sbytes("def"), sbytes(d), sbytes("utf8mb4")])
                    .collect(),
            ),
            ("pg_catalog", "pg_namespace") => (
                vec!["nspname", "oid"],
                dbs.iter()
                    .enumerate()
                    .map(|(i, d)| vec![sbytes(d), sbytes(&(i as u32 + 1).to_string())])
                    .collect(),
            ),
            // ⭐ PG 兼容: pg_database — migrator 建库探测 `WHERE datname=$1`
            ("pg_catalog", "pg_database") => (
                vec!["datname"],
                dbs.iter().map(|d| vec![sbytes(d)]).collect(),
            ),
            // ⭐ F66: SHOW DATABASES — 单列 "Database"
            ("__show__", "databases") => (
                vec!["Database"],
                dbs.iter().map(|d| vec![sbytes(d)]).collect(),
            ),
            // ⭐ F66: 其他 SHOW stub → 空
            ("__show__", "__empty__") => (vec![""], vec![]),
            _ => (vec![], vec![]),
        };
    if spec.exists {
        return sysq_exists(proto, binary, spec, &all_cols, rows);
    }
    sysq_finish(proto, binary, spec, &all_cols, rows)
}

/// ⭐ PG 兼容: SELECT EXISTS 判定 — 过滤后非空 → 单行布尔 t/f (OID bool,
/// pgx Scan(&bool) 可用). 复用 sysq_finish 的 WHERE 过滤语义 (eval_pred_sysq).
pub(crate) fn sysq_exists(
    proto: ProtocolKind,
    binary: bool,
    spec: &SysQuerySpec,
    all_cols: &[&str],
    rows: Vec<Vec<ColValue>>,
) -> Vec<u8> {
    let schema = TableSchema {
        version: 1,
        columns: all_cols
            .iter()
            .map(|n| storage::schema::Column {
                name: n.to_string(),
                ty: ColType::Str,
                nullable: true,
                default: None,
            })
            .collect(),
        pk_col: 0,
        indexes: Vec::new(),
        dropped: Vec::new(),
        next_iid: 0,
        version_ncols: Vec::new(),
        fks: Vec::new(),
        resp_row_adapter: Default::default(),
    };
    let mut rows = rows;
    rows.retain(|r| eval_pred_sysq(&schema, r, &spec.conds));
    let hit = !rows.is_empty();
    sql_rows_bytes(
        proto,
        binary,
        &[("?column?", ColType::Bool)],
        &[vec![ColValue::I64(if hit { 1 } else { 0 })]],
    )
}

/// ⭐ F66: 需 catalog 快照的虚拟表合成 (tables/columns/key_column_usage/pg_*).
/// `entries` = CatalogDump 回的 (table_name, TableSchema).
pub(crate) fn sysq_render_catalog(
    proto: ProtocolKind,
    binary: bool,
    spec: &SysQuerySpec,
    db: &str,
    entries: &[(String, TableSchema)],
) -> Vec<u8> {
    let key = (spec.catalog.as_str(), spec.table.as_str());
    // ⭐ F66: SHOW TABLES 动态列名 (函数级存活, 避免每次查询泄漏)
    let tables_in = format!("Tables_in_{db}");
    let (all_cols, rows): (Vec<&str>, Vec<Vec<ColValue>>) = match key {
        // ⭐ F66: SHOW [FULL] TABLES — 列名 Tables_in_<db> [+ Table_type]
        ("__show__", "tables") | ("__show__", "full_tables") => {
            let full = spec.table == "full_tables";
            let mut rows = Vec::new();
            for (t, _) in entries {
                if full {
                    rows.push(vec![sbytes(t), sbytes("BASE TABLE")]);
                } else {
                    rows.push(vec![sbytes(t)]);
                }
            }
            if full {
                (vec![tables_in.as_str(), "Table_type"], rows)
            } else {
                (vec![tables_in.as_str()], rows)
            }
        }
        // ⭐ F66: SHOW [FULL] COLUMNS FROM t — Field/Type/Null/Key/Default/Extra
        ("__show__", "columns") | ("__show__", "full_columns") => {
            let full = spec.table == "full_columns";
            // 从 __table__ cond 取目标表名
            let target = spec
                .conds
                .leaves()
                .into_iter()
                .find(|c| c.col == "__table__")
                .and_then(|c| match &c.val {
                    crate::protocol::sql::SqlValue::Str(b) => {
                        Some(String::from_utf8_lossy(b).to_string())
                    }
                    _ => None,
                });
            let mut rows = Vec::new();
            for (t, sc) in entries {
                if let Some(tt) = &target
                    && !t.eq_ignore_ascii_case(tt)
                {
                    continue;
                }
                for (i, c) in sc.columns.iter().enumerate() {
                    let key = if i as u16 == sc.pk_col {
                        "PRI"
                    } else if let Some(idx) = sc.indexes.iter().find(|x| x.col == i as u16) {
                        if idx.unique { "UNI" } else { "MUL" }
                    } else {
                        ""
                    };
                    let mut row = vec![
                        sbytes(&c.name),
                        sbytes(coltype_sql_name(c.ty)),
                        sbytes(if c.nullable { "YES" } else { "NO" }),
                        sbytes(key),
                        ColValue::Null, // Default
                        sbytes(""),     // Extra
                    ];
                    if full {
                        row.push(ColValue::Null); // Collation
                        row.push(sbytes("select,insert,update,references")); // Privileges
                        row.push(sbytes("")); // Comment
                    }
                    rows.push(row);
                }
            }
            if full {
                (
                    vec![
                        "Field",
                        "Type",
                        "Null",
                        "Key",
                        "Default",
                        "Extra",
                        "Collation",
                        "Privileges",
                        "Comment",
                    ],
                    rows,
                )
            } else {
                (
                    vec!["Field", "Type", "Null", "Key", "Default", "Extra"],
                    rows,
                )
            }
        }
        // ⭐ F66: SHOW CREATE TABLE t — 重建 MySQL DDL (SQLAlchemy 从此解析列)
        ("__show__", "create_table") => {
            let target = spec
                .conds
                .leaves()
                .into_iter()
                .find(|c| c.col == "__table__")
                .and_then(|c| match &c.val {
                    crate::protocol::sql::SqlValue::Str(b) => {
                        Some(String::from_utf8_lossy(b).to_string())
                    }
                    _ => None,
                })
                .unwrap_or_default();
            let mut rows = Vec::new();
            if let Some((t, sc)) = entries
                .iter()
                .find(|(t, _)| t.eq_ignore_ascii_case(&target))
            {
                let mut lines: Vec<String> = Vec::new();
                for (i, c) in sc.columns.iter().enumerate() {
                    let ty: std::borrow::Cow<str> = match c.ty {
                        ColType::I64 => "int".into(),
                        ColType::F64 => "double".into(),
                        ColType::Str => "text".into(),
                        ColType::Bytes => "blob".into(),
                        ColType::Bool => "tinyint(1)".into(),
                        ColType::Date => "date".into(),
                        ColType::Time => "time".into(),
                        ColType::Timestamp => "timestamp".into(),
                        ColType::Json => "json".into(),
                        ColType::Uuid => "char(36)".into(),
                        ColType::Decimal { precision, scale } => {
                            format!("decimal({precision},{scale})").into()
                        }
                    };
                    let nullness = if i as u16 == sc.pk_col || !c.nullable {
                        " NOT NULL".to_string()
                    } else {
                        " DEFAULT NULL".to_string()
                    };
                    lines.push(format!("  `{}` {}{}", c.name, ty, nullness));
                }
                let pkc = &sc.columns[sc.pk_col as usize].name;
                lines.push(format!("  PRIMARY KEY (`{pkc}`)"));
                for idx in &sc.indexes {
                    let cn = &sc.columns[idx.col as usize].name;
                    if idx.unique {
                        lines.push(format!("  UNIQUE KEY `{cn}` (`{cn}`)"));
                    } else {
                        lines.push(format!("  KEY `{cn}` (`{cn}`)"));
                    }
                }
                let ddl = format!(
                    "CREATE TABLE `{}` (\n{}\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4",
                    t,
                    lines.join(",\n")
                );
                rows.push(vec![sbytes(t), sbytes(&ddl)]);
            }
            (vec!["Table", "Create Table"], rows)
        }
        ("information_schema", "tables") => (
            vec!["table_catalog", "table_schema", "table_name", "table_type"],
            entries
                .iter()
                .map(|(t, _)| {
                    // ⭐ PG 兼容: table_schema 固定 'public' (migrator 以 'public' 探表)
                    vec![
                        sbytes("def"),
                        sbytes("public"),
                        sbytes(t),
                        sbytes("BASE TABLE"),
                    ]
                })
                .collect(),
        ),
        ("information_schema", "columns") => {
            let cols = vec![
                "table_catalog",
                "table_schema",
                "table_name",
                "column_name",
                "ordinal_position",
                "is_nullable",
                "data_type",
                "column_default",
            ];
            let mut rows = Vec::new();
            for (t, sc) in entries {
                for (i, c) in sc.columns.iter().enumerate() {
                    rows.push(vec![
                        sbytes("def"),
                        sbytes(db),
                        sbytes(t),
                        sbytes(&c.name),
                        sbytes(&(i + 1).to_string()),
                        sbytes(if c.nullable { "YES" } else { "NO" }),
                        sbytes(coltype_sql_name(c.ty)),
                        ColValue::Null,
                    ]);
                }
            }
            (cols, rows)
        }
        ("information_schema", "key_column_usage") => {
            let cols = vec![
                "table_schema",
                "table_name",
                "column_name",
                "constraint_name",
                "ordinal_position",
            ];
            let mut rows = Vec::new();
            for (t, sc) in entries {
                // pk
                let pkc = &sc.columns[sc.pk_col as usize].name;
                rows.push(vec![
                    sbytes(db),
                    sbytes(t),
                    sbytes(pkc),
                    sbytes("PRIMARY"),
                    sbytes("1"),
                ]);
                // unique 索引
                for idx in sc.indexes.iter().filter(|i| i.unique) {
                    let cn = &sc.columns[idx.col as usize].name;
                    rows.push(vec![
                        sbytes(db),
                        sbytes(t),
                        sbytes(cn),
                        sbytes(&format!("uniq_{cn}")),
                        sbytes("1"),
                    ]);
                }
            }
            (cols, rows)
        }
        ("pg_catalog", "pg_class") => (
            vec!["relname", "relkind", "oid"],
            entries
                .iter()
                .enumerate()
                .map(|(i, (t, _))| {
                    vec![sbytes(t), sbytes("r"), sbytes(&(i as u32 + 1).to_string())]
                })
                .collect(),
        ),
        ("pg_catalog", "pg_attribute") => {
            let cols = vec!["attrelid", "attname", "attnum", "attnotnull"];
            let mut rows = Vec::new();
            for (ri, (_, sc)) in entries.iter().enumerate() {
                for (i, c) in sc.columns.iter().enumerate() {
                    rows.push(vec![
                        sbytes(&(ri as u32 + 1).to_string()),
                        sbytes(&c.name),
                        sbytes(&(i + 1).to_string()),
                        sbytes(if c.nullable { "f" } else { "t" }),
                    ]);
                }
            }
            (cols, rows)
        }
        // 未知系统表 → 空结果 (工具探测容错)
        _ => (vec!["unknown"], vec![]),
    };
    if spec.exists {
        return sysq_exists(proto, binary, spec, &all_cols, rows);
    }
    sysq_finish(proto, binary, spec, &all_cols, rows)
}
