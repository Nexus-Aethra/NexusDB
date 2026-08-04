#!/usr/bin/env bash
# ============================================================================
# NexusDB SQL 门面兼容性测试 — 生成缺失 feature 清单
# ============================================================================
# 用法: ./scripts/sql_compat_test.sh
# 输出: 逐条测试语句的 PASS/FAIL, 末尾汇总
#
# 覆盖维度:
#   DDL: CREATE TABLE (类型/约束/索引), ALTER, DROP, CREATE INDEX
#   DML: INSERT / SELECT / UPDATE / DELETE / JOIN / 子查询 / 聚合
#   类型: 数值 / 文本 / 时间 / JSON / 数组 / UUID / 布尔 / 二进制
#   PG 专有: 外键级联 / 部分索引 / 触发器 / 函数 / uuid-ossp / 事务
# ============================================================================
set -uo pipefail

PG="psql -h localhost -p 5435 -U nexus -d default -qAt -X"
export PGPASSWORD=""

PASS=0; FAIL=0; FAILED_ITEMS=()

run() {
  local desc="$1"; shift
  local out
  out=$("$@" 2>&1)
  local rc=$?
  if [ $rc -eq 0 ] && ! echo "$out" | grep -qi "error"; then
    echo "[PASS] $desc"
    PASS=$((PASS+1))
  else
    echo "[FAIL] $desc"
    echo "       └─ $out" | head -2
    FAIL=$((FAIL+1))
    FAILED_ITEMS+=("$desc")
  fi
}

sql() { $PG -c "$1"; }

echo "════════════════════════════════════════════════"
echo " 1. DDL — CREATE TABLE 基础"
echo "════════════════════════════════════════════════"

run "IF NOT EXISTS" sql "CREATE TABLE IF NOT EXISTS compat_t1 (id INT PRIMARY KEY)"
run "重复 IF NOT EXISTS (幂等)" sql "CREATE TABLE IF NOT EXISTS compat_t1 (id INT PRIMARY KEY)"
run "INT PRIMARY KEY" sql "CREATE TABLE compat_t2 (id INT PRIMARY KEY)"
run "BIGINT / SMALLINT" sql "CREATE TABLE compat_t3 (a BIGINT PRIMARY KEY, b SMALLINT)"
run "DOUBLE PRECISION" sql "CREATE TABLE compat_t4 (a DOUBLE PRECISION PRIMARY KEY)"
run "VARCHAR(n) + TEXT" sql "CREATE TABLE compat_t5 (a VARCHAR(100) PRIMARY KEY, b TEXT)"
run "BOOLEAN + DEFAULT" sql "CREATE TABLE compat_t6 (a INT PRIMARY KEY, b BOOLEAN NOT NULL DEFAULT true)"
run "TIMESTAMP 类型" sql "CREATE TABLE compat_t7 (a INT PRIMARY KEY, t TIMESTAMP NOT NULL DEFAULT NOW())"
run "TIMESTAMPTZ 类型" sql "CREATE TABLE compat_t8 (a INT PRIMARY KEY, t TIMESTAMPTZ NOT NULL DEFAULT NOW())"
run "DATE 类型" sql "CREATE TABLE compat_t9 (a INT PRIMARY KEY, d DATE)"
run "UUID 类型" sql "CREATE TABLE compat_t10 (id UUID PRIMARY KEY)"
run "UUID DEFAULT uuid_generate_v4()" sql "CREATE TABLE compat_t11 (id UUID PRIMARY KEY DEFAULT uuid_generate_v4())"
run "JSONB 类型 + DEFAULT" sql "CREATE TABLE compat_t12 (a INT PRIMARY KEY, j JSONB NOT NULL DEFAULT '{}')"
run "TEXT[] 数组类型" sql "CREATE TABLE compat_t13 (a INT PRIMARY KEY, arr TEXT[] DEFAULT '{}')"
run "列级 UNIQUE" sql "CREATE TABLE compat_t14 (a INT PRIMARY KEY, b TEXT UNIQUE)"
run "表级 UNIQUE(col)" sql "CREATE TABLE compat_t15 (a INT, b INT, UNIQUE(a, b))"
run "列级外键 REFERENCES" sql "CREATE TABLE compat_t16 (a INT PRIMARY KEY, pid INT REFERENCES compat_t1(id) ON DELETE CASCADE)"
run "表级 FOREIGN KEY" sql "CREATE TABLE compat_t17 (a INT, pid INT, FOREIGN KEY (pid) REFERENCES compat_t1(id))"
run "CHECK 约束" sql "CREATE TABLE compat_t18 (a INT PRIMARY KEY, CHECK (a > 0))"
run "AUTO_INCREMENT 吞掉" sql "CREATE TABLE compat_t19 (id INT PRIMARY KEY AUTO_INCREMENT, name TEXT)"

echo ""
echo "════════════════════════════════════════════════"
echo " 2. DDL — 索引 / ALTER / DROP"
echo "════════════════════════════════════════════════"

run "CREATE INDEX" sql "CREATE INDEX idx_c2 ON compat_t2 (id)"
run "CREATE INDEX IF NOT EXISTS" sql "CREATE INDEX IF NOT EXISTS idx_c2 ON compat_t2 (id)"
run "CREATE INDEX (复合列)" sql "CREATE INDEX idx_c2b ON compat_t2 (id, id)"
run "CREATE INDEX (部分 WHERE)" sql "CREATE INDEX idx_c2c ON compat_t2 (id) WHERE id > 0"
run "ALTER TABLE ADD COLUMN" sql "ALTER TABLE compat_t2 ADD COLUMN extra TEXT"
run "ALTER TABLE ADD COLUMN IF NOT EXISTS" sql "ALTER TABLE compat_t2 ADD COLUMN IF NOT EXISTS extra TEXT"
run "ALTER TABLE DROP COLUMN" sql "ALTER TABLE compat_t2 DROP COLUMN extra"
run "DROP TABLE" sql "DROP TABLE IF EXISTS compat_t18"
run "CREATE EXTENSION uuid-ossp" sql "CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\""
run "CREATE OR REPLACE FUNCTION" sql "CREATE OR REPLACE FUNCTION f() RETURNS TRIGGER AS \$\$ BEGIN RETURN NEW; END; \$\$ LANGUAGE plpgsql"
run "CREATE TRIGGER" sql "CREATE TRIGGER trg BEFORE UPDATE ON compat_t2 FOR EACH ROW EXECUTE FUNCTION f()"

echo ""
echo "════════════════════════════════════════════════"
echo " 3. DML — INSERT / SELECT / UPDATE / DELETE"
echo "════════════════════════════════════════════════"

run "INSERT 单行" sql "INSERT INTO compat_t2 (id) VALUES (1)"
run "INSERT 多行 VALUES" sql "INSERT INTO compat_t2 (id) VALUES (2), (3), (4)"
run "SELECT *" sql "SELECT * FROM compat_t2"
run "SELECT WHERE" sql "SELECT * FROM compat_t2 WHERE id > 1"
run "SELECT 列投影" sql "SELECT id FROM compat_t2 WHERE id = 1"
run "SELECT ORDER BY" sql "SELECT id FROM compat_t2 ORDER BY id DESC"
run "SELECT LIMIT" sql "SELECT id FROM compat_t2 LIMIT 2"
run "SELECT OFFSET" sql "SELECT id FROM compat_t2 LIMIT 1 OFFSET 1"
run "SELECT 聚合 COUNT" sql "SELECT COUNT(*) FROM compat_t2"
run "SELECT 聚合 SUM" sql "SELECT SUM(id) FROM compat_t2"
run "SELECT GROUP BY" sql "SELECT id, COUNT(*) FROM compat_t2 GROUP BY id"
run "UPDATE" sql "UPDATE compat_t2 SET id = id WHERE id = 1"
run "DELETE" sql "DELETE FROM compat_t2 WHERE id = 4"
run "JOIN 两表" sql "SELECT * FROM compat_t2 t2 JOIN compat_t5 t5 ON t2.id = t5.a LIMIT 1"
run "INNER JOIN" sql "SELECT * FROM compat_t2 INNER JOIN compat_t5 ON compat_t2.id = compat_t5.a LIMIT 1"
run "LEFT JOIN" sql "SELECT * FROM compat_t2 LEFT JOIN compat_t5 ON compat_t2.id = compat_t5.a LIMIT 1"
run "子查询 IN" sql "SELECT id FROM compat_t2 WHERE id IN (SELECT id FROM compat_t2)"
run "子查询 EXISTS" sql "SELECT id FROM compat_t2 WHERE EXISTS (SELECT 1 FROM compat_t5)"
run "WHERE NOT NULL 判断" sql "SELECT * FROM compat_t5 WHERE b IS NOT NULL"

echo ""
echo "════════════════════════════════════════════════"
echo " 4. PG 专有 / 生态能力"
echo "════════════════════════════════════════════════"

run "事务 BEGIN" sql "BEGIN"
run "事务 COMMIT" sql "COMMIT"
run "事务 BEGIN+INSERT" sql "BEGIN"
sql "INSERT INTO compat_t2 (id) VALUES (99)" >/dev/null 2>&1
run "事务 COMMIT (含写入)" sql "COMMIT"
run "事务 ROLLBACK 流程" sql "BEGIN"
sql "INSERT INTO compat_t2 (id) VALUES (98)" >/dev/null 2>&1
run "事务 ROLLBACK" sql "ROLLBACK"
run "RETURNING" sql "INSERT INTO compat_t2 (id) VALUES (97) RETURNING id"
run "SELECT NOW()" sql "SELECT NOW()"
run "SELECT version()" sql "SELECT version()"
run "JSONB 操作符 ->" sql "SELECT j->'a' FROM compat_t12"
run "JSONB 操作符 ->>" sql "SELECT j->>'a' FROM compat_t12"
run "JSONB 查询 ?" sql "SELECT * FROM compat_t12 WHERE j ? 'a'"

echo ""
echo "════════════════════════════════════════════════"
echo " 汇总"
echo "════════════════════════════════════════════════"
echo "PASS: $PASS  FAIL: $FAIL"
if [ ${#FAILED_ITEMS[@]} -gt 0 ]; then
  echo ""
  echo "缺失/不兼容的 feature:"
  for item in "${FAILED_ITEMS[@]}"; do
    echo "  ✗ $item"
  done
fi
