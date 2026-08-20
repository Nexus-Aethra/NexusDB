//! Output formatting.
//!
//! Two modes:
//! - **Human** (default): tab-separated, one record per line. Bad rows are
//!   prefixed `[BAD-PAGE]` so they sort to the top under `grep`/`less`.
//! - **JSON** (`--json`): NDJSON, one JSON object per line.
//!
//! Output formats are deterministic — no timestamps, no path separators,
//! no locale-dependent formatting. See `docs/DESIGN.md` §4.
//!
//! The two modes share a [`RowSink`] view: each row is either a header (for
//! human) or a JSON `Object` (for `--json`), and the sink knows nothing
//! about the command-specific record type.

use std::io::Write;

// `HumanRow::is_bad` is reserved for the human-mode sort-by-bad-first
// formatter used by `export` (PR4) and `merge` (PR5).
#[allow(dead_code)]

/// Output mode picked from `--json`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutputMode {
    Human,
    Json,
}

/// A single column name on the human-mode header row.
///
/// Length chosen for readability, not screen width. Always ASCII.
pub type ColumnName = &'static str;

/// Builder for a single human-mode row.
#[derive(Debug, Default)]
pub struct HumanRow {
    fields: Vec<String>,
}

impl HumanRow {
    pub fn new() -> Self {
        Self::default()
    }

    /// Append a single field to the row. Use `bad()` to mark the row as a
    /// `[BAD-PAGE]` row; it is rendered first, regardless of column order.
    pub fn field(mut self, value: impl Into<String>) -> Self {
        self.fields.push(value.into());
        self
    }

    /// Whether this row represents an unrecoverable read.
    pub fn is_bad(&self) -> bool {
        self.fields.iter().any(|f| f.starts_with("[BAD-PAGE]"))
    }
}

/// JSON view of a single record; for PR1 only the minimum is supported.
#[derive(Debug, Default)]
pub struct JsonObject {
    pairs: Vec<(&'static str, JsonValue)>,
}

/// Reduced JSON value set sufficient for PR1. Avoids pulling in `serde_json`'s
/// `Value` here so the core module stays dependency-light; commands convert
/// to `serde_json::Value` only at the boundary.
#[derive(Debug, Clone)]
pub enum JsonValue {
    Null,
    Bool(bool),
    U64(u64),
    I64(i64),
    Str(String),
}

impl JsonObject {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn field(mut self, name: &'static str, value: impl Into<JsonValue>) -> Self {
        self.pairs.push((name, value.into()));
        self
    }
}

impl From<bool> for JsonValue {
    fn from(v: bool) -> Self {
        JsonValue::Bool(v)
    }
}
impl From<u64> for JsonValue {
    fn from(v: u64) -> Self {
        JsonValue::U64(v)
    }
}
impl From<i64> for JsonValue {
    fn from(v: i64) -> Self {
        JsonValue::I64(v)
    }
}
impl From<&str> for JsonValue {
    fn from(v: &str) -> Self {
        JsonValue::Str(v.to_string())
    }
}
impl From<String> for JsonValue {
    fn from(v: String) -> Self {
        JsonValue::Str(v)
    }
}
impl From<&String> for JsonValue {
    fn from(v: &String) -> Self {
        JsonValue::Str(v.clone())
    }
}
impl From<Option<u64>> for JsonValue {
    fn from(v: Option<u64>) -> Self {
        match v {
            Some(n) => JsonValue::U64(n),
            None => JsonValue::Null,
        }
    }
}
impl From<Option<u32>> for JsonValue {
    fn from(v: Option<u32>) -> Self {
        match v {
            Some(n) => JsonValue::U64(n as u64),
            None => JsonValue::Null,
        }
    }
}

/// Convert a `JsonObject` into a `serde_json::Value` (one-object map).
pub fn to_serde_json(obj: &JsonObject) -> serde_json::Value {
    let mut map = serde_json::Map::new();
    for (k, v) in &obj.pairs {
        map.insert(
            (*k).to_string(),
            match v {
                JsonValue::Null => serde_json::Value::Null,
                JsonValue::Bool(b) => serde_json::Value::Bool(*b),
                JsonValue::U64(n) => serde_json::Value::Number(serde_json::Number::from(*n)),
                JsonValue::I64(n) => serde_json::Value::Number(serde_json::Number::from(*n)),
                JsonValue::Str(s) => serde_json::Value::String(s.clone()),
            },
        );
    }
    serde_json::Value::Object(map)
}

/// Render a human-mode header row.
pub fn human_header<W: Write>(out: &mut W, columns: &[ColumnName]) -> std::io::Result<()> {
    let line = columns.join("\t");
    writeln!(out, "{line}")
}

/// Render a single human-mode row.
pub fn human_row<W: Write>(out: &mut W, row: &HumanRow) -> std::io::Result<()> {
    let line = row.fields.join("\t");
    writeln!(out, "{line}")
}

/// Render a single JSON row (NDJSON).
pub fn json_row<W: Write>(out: &mut W, obj: &JsonObject) -> std::io::Result<()> {
    let v = to_serde_json(obj);
    serde_json::to_writer(&mut *out, &v).map_err(|e| std::io::Error::other(e.to_string()))?;
    out.write_all(b"\n")
}

/// Render a human-mode top-level summary block (e.g. for `dbs` we want a
/// "scanned N artifacts in M shards" preface).
pub fn human_summary<W: Write>(out: &mut W, summary: &str) -> std::io::Result<()> {
    writeln!(out, "{summary}")
}

/// Format a vpid as decimal or hex per `--hex-vpid`.
pub fn format_vpid(vpid: u64, hex: bool) -> String {
    if hex {
        format!("0x{:x}", vpid)
    } else {
        vpid.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn human_row_renders_tab_separated() {
        let mut buf = Vec::new();
        let row = HumanRow::new()
            .field("vpid")
            .field("7")
            .field("Leaf");
        human_row(&mut buf, &row).unwrap();
        assert_eq!(buf, b"vpid\t7\tLeaf\n");
    }

    #[test]
    fn human_row_marks_bad_with_prefix() {
        let mut buf = Vec::new();
        let row = HumanRow::new()
            .field("[BAD-PAGE]")
            .field("0")
            .field("Meta");
        let line = std::str::from_utf8(&buf).unwrap_or_default();
        assert!(!line.contains("[BAD-PAGE]")); // nothing flushed yet
        human_row(&mut buf, &row).unwrap();
        assert!(std::str::from_utf8(&buf)
            .unwrap()
            .starts_with("[BAD-PAGE]"));
    }

    #[test]
    fn json_row_emits_ndjson() {
        let mut buf = Vec::new();
        let obj = JsonObject::new()
            .field("vpid", 7u64)
            .field("kind", "Leaf")
            .field("ok", true);
        json_row(&mut buf, &obj).unwrap();
        let s = std::str::from_utf8(&buf).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(s.trim()).unwrap();
        assert_eq!(parsed["vpid"], serde_json::json!(7));
        assert_eq!(parsed["kind"], serde_json::json!("Leaf"));
        assert_eq!(parsed["ok"], serde_json::json!(true));
    }

    #[test]
    fn format_vpid_decimal_or_hex() {
        assert_eq!(format_vpid(7, false), "7");
        assert_eq!(format_vpid(7, true), "0x7");
        assert_eq!(format_vpid(160, false), "160");
        assert_eq!(format_vpid(160, true), "0xa0");
    }
}
