//! ⭐ Phase G: Geospatial 纯函数 — 52-bit 交织 geohash + haversine.
//!
//! Redis 兼容精度: lon/lat 各 26 bit (cell ~0.6m), geohash 整数 < 2^52
//! 可被 f64 精确表示 → 直接作为 ZSet score 存储, 复用双索引全链路.
//!
//! 编码自洽即可 (不与 Redis 磁盘格式互通): lat 占偶位, lon 占奇位.

/// 经度范围 (Redis 同).
pub const LON_MIN: f64 = -180.0;
pub const LON_MAX: f64 = 180.0;
/// 纬度范围 (Web Mercator 限制, Redis 同).
pub const LAT_MIN: f64 = -85.051_128_78;
pub const LAT_MAX: f64 = 85.051_128_78;
/// 地球半径 (米, Redis geohash_helper.c 同值).
pub const EARTH_RADIUS_M: f64 = 6_372_797.560_856;

const STEP: u32 = 26;
const SCALE: f64 = (1u64 << STEP) as f64;

/// 26-bit 值散布到偶数位 (morton spread).
fn spread(v: u32) -> u64 {
    let mut r = (v as u64) & 0x3FF_FFFF;
    r = (r | (r << 16)) & 0x0000_FFFF_0000_FFFF;
    r = (r | (r << 8)) & 0x00FF_00FF_00FF_00FF;
    r = (r | (r << 4)) & 0x0F0F_0F0F_0F0F_0F0F;
    r = (r | (r << 2)) & 0x3333_3333_3333_3333;
    r = (r | (r << 1)) & 0x5555_5555_5555_5555;
    r
}

/// 偶数位收拢回 26-bit 值 (morton squash).
fn squash(v: u64) -> u32 {
    let mut r = v & 0x5555_5555_5555_5555;
    r = (r | (r >> 1)) & 0x3333_3333_3333_3333;
    r = (r | (r >> 2)) & 0x0F0F_0F0F_0F0F_0F0F;
    r = (r | (r >> 4)) & 0x00FF_00FF_00FF_00FF;
    r = (r | (r >> 8)) & 0x0000_FFFF_0000_FFFF;
    r = (r | (r >> 16)) & 0x0000_0000_FFFF_FFFF;
    r as u32
}

/// (lon, lat) → 52-bit geohash. 超范围 → None.
pub fn encode(lon: f64, lat: f64) -> Option<u64> {
    if !(LON_MIN..=LON_MAX).contains(&lon) || !(LAT_MIN..=LAT_MAX).contains(&lat) {
        return None;
    }
    let lon_off = ((lon - LON_MIN) / (LON_MAX - LON_MIN) * SCALE) as u64;
    let lat_off = ((lat - LAT_MIN) / (LAT_MAX - LAT_MIN) * SCALE) as u64;
    // 边界 (lon=180 / lat=max) clamp 到最后一个 cell
    let lon_off = lon_off.min((1 << STEP) - 1) as u32;
    let lat_off = lat_off.min((1 << STEP) - 1) as u32;
    Some(spread(lat_off) | (spread(lon_off) << 1))
}

/// 52-bit geohash → cell 中心 (lon, lat).
pub fn decode(bits: u64) -> (f64, f64) {
    let lat_off = squash(bits) as f64;
    let lon_off = squash(bits >> 1) as f64;
    let lon = LON_MIN + (lon_off + 0.5) / SCALE * (LON_MAX - LON_MIN);
    let lat = LAT_MIN + (lat_off + 0.5) / SCALE * (LAT_MAX - LAT_MIN);
    (lon, lat)
}

/// 球面距离 (米, haversine).
pub fn haversine_m(lon1: f64, lat1: f64, lon2: f64, lat2: f64) -> f64 {
    let (la1, la2) = (lat1.to_radians(), lat2.to_radians());
    let dlat = (lat2 - lat1).to_radians();
    let dlon = (lon2 - lon1).to_radians();
    let a = (dlat / 2.0).sin().powi(2) + la1.cos() * la2.cos() * (dlon / 2.0).sin().powi(2);
    2.0 * EARTH_RADIUS_M * a.sqrt().asin()
}

/// 单位换算因子 (→ 米). 未知单位 → None.
pub fn unit_factor(unit: &[u8]) -> Option<f64> {
    match unit.to_ascii_lowercase().as_slice() {
        b"m" => Some(1.0),
        b"km" => Some(1000.0),
        b"mi" => Some(1609.34),
        b"ft" => Some(0.3048),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_precision() {
        // cell ~0.6m → roundtrip 误差应 < 1m
        for (lon, lat) in [
            (116.397_128, 39.916_527), // 北京
            (121.473_7, 31.230_4),     // 上海
            (-122.419_4, 37.774_9),    // 旧金山
            (0.0, 0.0),
            (179.999, 85.0),
            (-179.999, -85.0),
        ] {
            let bits = encode(lon, lat).expect("in range");
            assert!(bits < (1 << 52), "52-bit");
            let (dlon, dlat) = decode(bits);
            let err = haversine_m(lon, lat, dlon, dlat);
            assert!(err < 1.0, "({lon},{lat}) roundtrip 误差 {err}m");
        }
    }

    #[test]
    fn out_of_range_rejected() {
        assert!(encode(181.0, 0.0).is_none());
        assert!(encode(0.0, 86.0).is_none());
    }

    #[test]
    fn known_distance() {
        // 北京 ↔ 上海 ≈ 1067km (误差 < 0.5%)
        let d = haversine_m(116.397_128, 39.916_527, 121.473_7, 31.230_4);
        assert!((d - 1_067_000.0).abs() / 1_067_000.0 < 0.005, "d = {d}");
    }

    #[test]
    fn score_f64_exact() {
        // geohash < 2^52 → f64 无损往返
        let bits = encode(116.397_128, 39.916_527).unwrap();
        let f = bits as f64;
        assert_eq!(f as u64, bits);
    }
}
