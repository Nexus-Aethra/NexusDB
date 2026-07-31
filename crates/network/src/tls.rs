//! ⭐ F83: TLS 支持 — rustls (ring 后端) ServerConfig 构建 + PEM 解析.
//!
//! 手写 PEM 解析 (复用 crypto::base64_decode), 免 rustls-pemfile 依赖。
//! 支持证书链 (多段 CERTIFICATE) 与私钥 (PKCS8 / PKCS1 RSA / SEC1 EC)。

use std::sync::Arc;

use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs1KeyDer, PrivatePkcs8KeyDer, PrivateSec1KeyDer};

/// 从 PEM 文本抽取所有 `-----BEGIN {label}-----`..`-----END {label}-----` 段的 DER 字节.
fn pem_blocks(pem: &str, label: &str) -> Vec<Vec<u8>> {
    let begin = format!("-----BEGIN {label}-----");
    let end = format!("-----END {label}-----");
    let mut out = Vec::new();
    let mut rest = pem;
    while let Some(bpos) = rest.find(&begin) {
        let after = &rest[bpos + begin.len()..];
        let Some(epos) = after.find(&end) else { break };
        let body = &after[..epos];
        if let Some(der) = crate::protocol::crypto::base64_decode(body.as_bytes()) {
            out.push(der);
        }
        rest = &after[epos + end.len()..];
    }
    out
}

/// 加载证书链 + 私钥 PEM → rustls ServerConfig (ring provider, 无客户端认证).
pub fn load_server_config(cert_path: &str, key_path: &str) -> Result<Arc<rustls::ServerConfig>, String> {
    let cert_pem = std::fs::read_to_string(cert_path)
        .map_err(|e| format!("read tls_cert {cert_path}: {e}"))?;
    let key_pem = std::fs::read_to_string(key_path)
        .map_err(|e| format!("read tls_key {key_path}: {e}"))?;

    let certs: Vec<CertificateDer<'static>> = pem_blocks(&cert_pem, "CERTIFICATE")
        .into_iter()
        .map(CertificateDer::from)
        .collect();
    if certs.is_empty() {
        return Err("no CERTIFICATE found in tls_cert PEM".into());
    }

    // 私钥: 依次尝试 PKCS8 / RSA(PKCS1) / EC(SEC1)
    let key: PrivateKeyDer<'static> = if let Some(d) = pem_blocks(&key_pem, "PRIVATE KEY").pop() {
        PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(d))
    } else if let Some(d) = pem_blocks(&key_pem, "RSA PRIVATE KEY").pop() {
        PrivateKeyDer::Pkcs1(PrivatePkcs1KeyDer::from(d))
    } else if let Some(d) = pem_blocks(&key_pem, "EC PRIVATE KEY").pop() {
        PrivateKeyDer::Sec1(PrivateSec1KeyDer::from(d))
    } else {
        return Err("no PRIVATE KEY (PKCS8/PKCS1/SEC1) found in tls_key PEM".into());
    };

    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let cfg = rustls::ServerConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(|e| format!("tls protocol versions: {e}"))?
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(|e| format!("tls cert/key: {e}"))?;
    Ok(Arc::new(cfg))
}
