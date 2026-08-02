# NexusDB SQL 兼容性补全计划（feat/sql-compat 分支）

> 目标：让实际应用（Story Loom 等）的迁移脚本与运行时查询能在 NexusDB 上跑通。
> 策略：**语法吞掉 / 别名映射 / 最小语义实现** 三档，按 story-loom 迁移依赖排序。

## 现状（2026-08-02 实测）

`scripts/sql_compat_test.sh` → **37 PASS / 25 FAIL**（脚本已修正事务/常量误报）。

## 缺失清单与优先级

### P0 — story-loom 迁移脚本必须通过（当前 migrator 整文件 multi-statement 执行）

| # | 缺陷 | 现状 | 方案 |
|---|------|------|------|
| 1 | **multi-statement 查询** | `conn.Exec` 整文件分号语句 → "multi-statement query is unsupported" | worker 按 `;` 分割逐条执行（或完整支持） |
| 2 | `CREATE EXTENSION IF NOT EXISTS "uuid-ossp"` | `"` 报错 | tokenizer 支持双引号标识符 + 吞掉扩展语句 |
| 3 | `TIMESTAMPTZ` 类型 | unknown type | `parse_col_type` 加别名 `TIMESTAMPTZ`→Timestamp |
| 4 | `TEXT[]` 数组类型 | `[` 报错 | tokenizer 支持 `[]` 后缀 → 类型别名（存 Str/Json） |
| 5 | 列级外键 `REFERENCES ... ON DELETE CASCADE` | 报错 | `parse_create` 列属性吞 `REFERENCES t (c) ON ...` |
| 6 | 表级 `UNIQUE(col)` | PRIMARY KEY required | 表级 UNIQUE 已在读，补 pk 缺失兼容或建唯一索引 |
| 7 | `CHECK (...)` | 报错 | 吞掉 |
| 8 | `DEFAULT NOW()` / `DEFAULT uuid_generate_v4()` | expected literal | DEFAULT 支持函数调用/吞掉 |
| 9 | `CREATE INDEX [IF NOT EXISTS]`（含部分 WHERE） | 无此语句 | 新增 `CREATE INDEX` 解析（吞/建索引） |
| 10 | `ALTER TABLE ADD COLUMN IF NOT EXISTS` | unknown type NOT | ALTER 支持 IF NOT EXISTS + DEFAULT |
| 11 | `DROP TABLE IF EXISTS` | trailing tokens | DROP 支持 IF EXISTS |
| 12 | `CREATE OR REPLACE FUNCTION` / `CREATE TRIGGER` / `DROP TRIGGER` | multi-stmt / 不支持 | ✅ (2026-08-02): tokenizer 支持 dollar-quote `$$...$$`/`$tag$...$tag$` → 函数体吞掉为 DdlStub |

### P1 — 运行时查询依赖（story-loom repo 层常用）

| # | 缺陷 | 方案 |
|---|------|------|
| 13 | `UPDATE ... SET c = c2`（列引用） | 部分 ✅ (2026-08-02): `SET pk = pk` 同值放行 (no-op 跳过); 真实列引用 c=c2 需行级求值 (v1 留) |
| 14 | `WHERE x IS NOT NULL` / `IS NULL` | 谓词支持 IS [NOT] NULL |
| 15 | `SELECT NOW()` | ✅ (2026-08-02): `SqlStmt::ScalarSelect` — 无 FROM 标量函数投影常量单行 |
| 16 | `RETURNING` | INSERT/UPDATE/DELETE 吞 RETURNING 子句 |
| 17 | JSONB 操作符 `->` `->>` `?` | 部分 ✅ (2026-08-02): `->`/`->>` 表达式投影 (tokenizer + ScalarExpr::JsonGet + 逐行求值, serde_json); `?` 与 MySQL `?` 占位符歧义, v1 留 |

### P2 — 完整性 / 未来

| # | 缺陷 | 方案 |
|---|------|------|
| 18 | `ALTER TABLE DROP COLUMN` | v1 拒绝 → 保留（低优先） |
| 19 | 数组字面量读写 | P0 吞类型后，运行时 `TEXT[]` 读写映射 |
| 20 | 触发器语义（updated_at 自动更新） | 由应用层代码维护 updated_at（story-loom 已手动 SET） |

## 实现顺序

1. **P0 全部** → story-loom 迁移可整体跑通
2. **P1** → 运行时 CRUD 完整
3. 回归测试 `scripts/sql_compat_test.sh` 全绿后打 tag

## 风险

- multi-statement 分割需注意字符串/注释内的分号
- 吞掉外键/CHECK 意味着无完整性约束 —— 文档明示，由应用层保证
