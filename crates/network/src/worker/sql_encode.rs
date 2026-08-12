//! SQL 值编码/解码工具 — 纯函数 (日期/时间/UUID/Decimal/字节渲染).
//! 从 worker/mod.rs 拆分 (2026-08).

/// ⭐ F80: 民用日期 (y,m,d) → 距 1970-01-01 的天数 (Howard Hinnant days_from_civil).
pub(crate) fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400; // [0, 399]
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5 + d - 1; // [0, 365]
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy; // [0, 146096]
    era * 146097 + doe - 719468
}

/// ⭐ F80: 逆变换 天数 → (y,m,d).
pub(crate) fn civil_from_days(z: i64) -> (i64, i64, i64) {
    let z = z + 719468;
    let era = if z >= 0 { z } else { z - 146096 } / 146097;
    let doe = z - era * 146097; // [0, 146096]
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365; // [0, 399]
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
    let mp = (5 * doy + 2) / 153; // [0, 11]
    let d = doy - (153 * mp + 2) / 5 + 1; // [1, 31]
    let m = if mp < 10 { mp + 3 } else { mp - 9 }; // [1, 12]
    (if m <= 2 { y + 1 } else { y }, m, d)
}

/// ⭐ F80: 解析 'YYYY-MM-DD' → 距 epoch 微秒 (00:00:00).
pub(crate) fn parse_date_micros(s: &str) -> Option<i64> {
    let s = s.trim();
    let mut it = s.splitn(3, '-');
    let y = it.next()?.parse::<i64>().ok()?;
    let m = it.next()?.parse::<i64>().ok()?;
    let d = it.next()?.parse::<i64>().ok()?;
    if !(1..=12).contains(&m) || !(1..=31).contains(&d) {
        return None;
    }
    Some(days_from_civil(y, m, d) * MICROS_PER_DAY)
}

/// ⭐ F80: 解析 'HH:MM:SS[.ffffff]' → 距零点微秒.
pub(crate) fn parse_time_micros(s: &str) -> Option<i64> {
    let s = s.trim();
    let (hms, frac) = match s.split_once('.') {
        Some((a, b)) => (a, b),
        None => (s, ""),
    };
    let mut it = hms.splitn(3, ':');
    let h = it.next()?.parse::<i64>().ok()?;
    let mi = it.next()?.parse::<i64>().ok()?;
    let se = it.next().unwrap_or("0").parse::<i64>().ok()?;
    let mut micros = ((h * 60 + mi) * 60 + se) * 1_000_000;
    if !frac.is_empty() {
        let mut f = frac.to_string();
        f.truncate(6);
        while f.len() < 6 {
            f.push('0');
        }
        micros += f.parse::<i64>().ok()?;
    }
    Some(micros)
}

/// ⭐ F80: 解析 'YYYY-MM-DD[ T]HH:MM:SS[.ffffff]' → 距 epoch 微秒.
pub(crate) fn parse_timestamp_micros(s: &str) -> Option<i64> {
    let s = s.trim();
    let (date, time) = if let Some((d, t)) = s.split_once('T') {
        (d, Some(t))
    } else if let Some((d, t)) = s.split_once(' ') {
        (d, Some(t))
    } else {
        (s, None)
    };
    let base = parse_date_micros(date)?;
    match time {
        Some(t) if !t.trim().is_empty() => Some(base + parse_time_micros(t)?),
        _ => Some(base),
    }
}

/// ⭐ F80: 渲染 (供三门面): 微秒 → 'YYYY-MM-DD'.
pub(crate) fn render_date(micros: i64) -> String {
    let days = micros.div_euclid(MICROS_PER_DAY);
    let (y, m, d) = civil_from_days(days);
    format!("{y:04}-{m:02}-{d:02}")
}

/// ⭐ F80: 微秒 → 'HH:MM:SS' (截去小数; 距零点).
pub(crate) fn render_time(micros: i64) -> String {
    let mut secs = micros.rem_euclid(MICROS_PER_DAY) / 1_000_000;
    let h = secs / 3600;
    secs %= 3600;
    format!("{:02}:{:02}:{:02}", h, secs / 60, secs % 60)
}

/// ⭐ F80: 微秒 → 'YYYY-MM-DD HH:MM:SS'.
pub(crate) fn render_timestamp(micros: i64) -> String {
    format!("{} {}", render_date(micros), render_time(micros))
}

/// ⭐ F80: 微秒 → (年, 月, 日, 时, 分, 秒, 微秒) — MySQL 二进制协议 DATE/DATETIME 编码用.
pub(crate) fn datetime_parts(micros: i64) -> (u16, u8, u8, u8, u8, u8, u32) {
    let days = micros.div_euclid(MICROS_PER_DAY);
    let (y, m, d) = civil_from_days(days);
    let tod = micros.rem_euclid(MICROS_PER_DAY);
    let micro = (tod % 1_000_000) as u32;
    let secs = tod / 1_000_000;
    let hh = (secs / 3600) as u8;
    let mm = ((secs % 3600) / 60) as u8;
    let ss = (secs % 60) as u8;
    (y as u16, m as u8, d as u8, hh, mm, ss, micro)
}

/// ⭐ F80: 距零点微秒 → (时, 分, 秒, 微秒) — MySQL 二进制 TIME 编码用.
pub(crate) fn time_parts(micros: i64) -> (u8, u8, u8, u32) {
    let tod = micros.rem_euclid(MICROS_PER_DAY);
    let micro = (tod % 1_000_000) as u32;
    let secs = tod / 1_000_000;
    (
        (secs / 3600) as u8,
        ((secs % 3600) / 60) as u8,
        (secs % 60) as u8,
        micro,
    )
}

/// ⭐ F80: 16B → 36 字符带连字符 UUID.
pub(crate) fn render_uuid(b: &[u8]) -> String {
    if b.len() != 16 {
        return String::from_utf8_lossy(b).into_owned();
    }
    let h: String = b.iter().map(|x| format!("{x:02x}")).collect();
    format!(
        "{}-{}-{}-{}-{}",
        &h[0..8],
        &h[8..12],
        &h[12..16],
        &h[16..20],
        &h[20..32]
    )
}

/// ⭐ F80: 解析 UUID 文本 (带/不带连字符) → 16B; 失败返回 None.
pub(crate) fn parse_uuid(s: &str) -> Option<Vec<u8>> {
    let hex: String = s.chars().filter(|c| *c != '-').collect();
    if hex.len() != 32 || !hex.chars().all(|c| c.is_ascii_hexdigit()) {
        return None;
    }
    (0..16)
        .map(|i| u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16).ok())
        .collect()
}

/// ⭐ F81: 10^scale (i128; scale<=38 → <i128::MAX). None=溢出.
pub(crate) fn pow10_i128(scale: u8) -> Option<i128> {
    10i128.checked_pow(scale as u32)
}

/// ⭐ F81: 十进制文本 → 定标 i128 (按 scale; 超出小数位截断, 不四舍五入). 非法/溢出→None.
pub(crate) fn parse_decimal(s: &str, scale: u8) -> Option<i128> {
    let s = s.trim();
    let (neg, s) = match s.strip_prefix('-') {
        Some(r) => (true, r),
        None => (false, s.strip_prefix('+').unwrap_or(s)),
    };
    let (int_part, frac_part) = match s.split_once('.') {
        Some((a, b)) => (a, b),
        None => (s, ""),
    };
    if int_part.is_empty() && frac_part.is_empty() {
        return None;
    }
    if !int_part.bytes().all(|c| c.is_ascii_digit())
        || !frac_part.bytes().all(|c| c.is_ascii_digit())
    {
        return None;
    }
    let sc = scale as usize;
    let mut frac = frac_part.to_string();
    if frac.len() > sc {
        frac.truncate(sc);
    }
    while frac.len() < sc {
        frac.push('0');
    }
    let int_val: i128 = if int_part.is_empty() {
        0
    } else {
        int_part.parse().ok()?
    };
    let frac_val: i128 = if sc == 0 || frac.is_empty() {
        0
    } else {
        frac.parse().ok()?
    };
    let scaled = int_val
        .checked_mul(pow10_i128(scale)?)?
        .checked_add(frac_val)?;
    Some(if neg { -scaled } else { scaled })
}

/// ⭐ F81: 定标 i128 + scale → 十进制文本 "123.45".
pub(crate) fn render_decimal(v: i128, scale: u8) -> String {
    if scale == 0 {
        return v.to_string();
    }
    let neg = v < 0;
    let av = v.unsigned_abs();
    let p = 10u128.pow(scale as u32);
    format!(
        "{}{}.{:0width$}",
        if neg { "-" } else { "" },
        av / p,
        av % p,
        width = scale as usize
    )
}
const MICROS_PER_DAY: i64 = 86_400_000_000;
