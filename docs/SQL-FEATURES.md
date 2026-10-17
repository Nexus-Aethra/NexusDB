# NexusDB SQL 功能支持清单 (集中标注)

> 日期: 2026-10-17 | 来源: 审计报告 P3 项"顶层 SELECT/UNION/窗口函数/GROUP_CONCAT/
> 列列比较/WHERE 函数调用等缺口, 建议集中标注".
>
> 本文档是 SQL 方言能力的**唯一权威清单**: 解析器/执行层支持什么、明确拒绝什么,
> 以这里为准 (与 `docs/pg-dialect-gap-loom.md` 的 Loom 对接实测互为补充 —
> 后者是"某个客户端能不能用"的视角, 本文是"语法面"的视角).
>
> **总原则 (审计确立)**: 凡未实现的语法/约束/失败路径, **一律显式报错**,
> 禁止静默吞掉回 OK — 静默降级在数据库里比 panic 危害更大 (数据错了用户不知道).

## 1. 已支持

### DDL
- `CREATE TABLE` (列类型: BIGINT/INT/DOUBLE/TEXT/BLOB/BOOLEAN/DATE/TIME/TIMESTAMP/
  JSON/UUID/DECIMAL(p,s); PRIMARY KEY / NOT NULL / DEFAULT 字面量 / DEFAULT NOW() /
  DEFAULT uuid_generate_v4() / SERIAL / INDEX / UNIQUE / FOREIGN KEY ... REFERENCES)
- `DROP TABLE` / `ALTER TABLE ADD COLUMN` (可空列)
- `CREATE INDEX` **仅表内联形式** (`CREATE TABLE t (..., INDEX(col))`);
  独立 `CREATE INDEX` 显式报错并指引内联写法

### DML
- `INSERT ... VALUES (...)` 多行 / 部分列 / 预处理占位符 (`?` / `$n`)
- `UPDATE ... SET col = 字面量|列|col±1|NOT col ... WHERE`
- `DELETE ... WHERE`
- 预处理语句: MySQL `COM_STMT_PREPARE/EXECUTE/CLOSE`, PG `Parse/Bind/Describe/Execute/Sync`

### 查询
- 投影: `*` / 命名列 / 别名 (`AS`) / 聚合 (`COUNT/SUM/AVG/MIN/MAX`, 含
  `COUNT(DISTINCT col)` / 聚合内算术表达式) / 标量函数 (`NOW()`,
  `CURRENT_TIMESTAMP`, `CURRENT_DATE`, `CURRENT_TIME`) / **顶层字面量投影**
  (`SELECT 1`, `SELECT 'a', 2 AS b`) / JSONB 取字段 (`j->'a'`, `j->>'a'`)
- `WHERE`: AND/OR/NOT/括号; 比较 `= <> > >= < <=`; `IN (常量列表)` /
  `IN (子查询)` / `NOT IN`; `LIKE '前缀%'`; `IS [NOT] NULL`; JSONB `? 'key'`;
  `EXISTS (子查询)`; 相关/非相关子查询 (去相关折叠)
- `GROUP BY` / `HAVING` / `ORDER BY` (多键, ASC/DESC) / `LIMIT` / `OFFSET`
  (`LIMIT n,m` MySQL 形式)
- 两表及多表 `INNER/LEFT/RIGHT/FULL/CROSS JOIN ... ON` (等值 + 非等值残余条件)
- FROM 派生表 `(SELECT ...) alias`
- 事务: `BEGIN [ISOLATION LEVEL ...] [READ ONLY]` / `COMMIT` / `ROLLBACK` /
  `SAVEPOINT` / `ROLLBACK TO`; SERIALIZABLE = 行级 OCC 读集验证
- 系统表: `information_schema.*` / `pg_catalog.*` (虚拟表, 从活元数据合成)
- 系统变量探测: `SELECT @@version` / `@@transaction_isolation` 等 (按连接实际
  隔离级别回答)
- 工具语句: `USE`/`DESCRIBE`/`SHOW`/`SET`(吞) / `SELECT VERSION()` /
  `SELECT DATABASE()`

## 2. 明确拒绝 (显式报错, 不静默)

| 语法 | 现状 |
|---|---|
| `UNION` / `UNION ALL` / `INTERSECT` / `EXCEPT` | 不支持, 解析期报错 |
| 窗口函数 (`OVER (...)`, `ROW_NUMBER()` 等) | 不支持 |
| `GROUP_CONCAT` / `STRING_AGG` / `ARRAY_AGG` | 不支持 |
| 列列比较 (`WHERE a = b`) | 解析为列引用后执行期报 `unresolved column reference` |
| `WHERE` 中的函数调用 (`WHERE ABS(x) > 1`) | 不支持, 解析期报错 |
| `RETURNING` (INSERT/UPDATE/DELETE) | 不支持, 显式报错 (v1 不返回受影响行值) |
| 独立 `CREATE INDEX` | 显式报错, 指引内联写法 |
| `LIKE` 的 `_` 通配符 / 中缀 `%` | 显式报错 (v1 仅支持 `前缀%`) |
| 非 UUID 列的 `DEFAULT uuid_generate_v4()` | 显式报错 (求值期) |
| 跨库访问 (`db.table` 跨库) | 显式报错 |
| 同包多条 SQL (MySQL `;` 分隔多语句) | 不支持 (PG 扩展协议 Parse/Bind 批次可用) |
| 全文检索 / 地理空间 SQL 函数 / CTE (`WITH`) | 不支持 |

## 3. 已知语义边界 (文档化取舍, 非缺陷)

- **隔离级别归并**: SQL 四级 (RU/RC/RR/SERIALIZABLE) 归并为两档 — RU→RC,
  RR→SERIALIZABLE; SERIALIZABLE 是**行级 OCC** (backward validation):
  防脏读/不可重复读/丢失更新/行级写偏斜, **不防幻读** (无谓词/范围锁).
- **JOIN 的 gather 读不进入 OCC 读集** (投影下推无 pk/row 字节) — 事务内 JOIN
  的隔离保证弱于单表扫描.
- **跨 shard COMMIT 是 best-effort**: 某 shard 验证失败时其余 shard 可能已应用.
- **索引下推界对 DECIMAL 禁用** (shard 端 ordered-bytes 与 worker 端定标语义
  不一致) → Decimal 索引列走全表扫 + 残余过滤.
- **ORDER BY**: 全量物化排序 (有 `LIMIT` 下推与索引序消排优化, 无通用 top-k).
- **页校验和**: 磁盘→内存边界验证; 字段为 0 的存量页 (本特性上线前落盘) 跳过
  校验, 下次写入时自动补齐.
- **空串不能作主键** (空 key 保留给 B+Tree 哨兵 item).
