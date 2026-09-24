//! 信封加密（B 方案）：明文 HTTP 之上只加密报文体。
//!
//! 信任根是配对成功那一刻交换的 32 字节 `device_secret`（每台对端一份，双方各存一份相同值）。
//! 之后 push/snapshot 的正文变成 `{v,kid,ts,n,ct}` 信封：
//! - 密钥 = HKDF-SHA256(ikm=device_secret, salt="aw-sync/1", info=接口路径)
//!   —— 按用途派生子密钥，推送密钥与拉取密钥互不相通。
//! - AAD  = "aw-sync/1\n{路径}\n{kid}\n{ts}"，把发送方身份、接口、时间戳全绑进认证标签，
//!   换目标重放、改时间戳重放都会在 GCM 校验阶段直接失败。
//! - nonce 每条消息随机 12 字节；接收端在 ±300s 窗口内对 (kid, nonce) 去重防重放。
//!
//! 明确的弱点：device_secret 本身在配对那几分钟里是明文交换的，同网段中间人可替换它。
//! 因此每次配对完成后两端各算一个 4 位十六进制「安全码」（Signal 安全号码的做法），
//! 人工对一眼即可让中间人失效。加配对码到 6 位把暴力窗口压到可忽略。
//!
//! 依赖刻意只用纯 Rust 的 aes-gcm/hkdf/sha2/base64：当年为了 Android 交叉编译
//! 才把 openssl 换成 rustls，这里绝不能重新引入 C 依赖。

use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};

use aes_gcm::aead::array::Array;
use aes_gcm::aead::consts::U12;
use aes_gcm::aead::{Aead, Payload};
use aes_gcm::{Aes256Gcm, KeyInit};
use base64::Engine;
use hkdf::Hkdf;
use rand::Rng;
use serde::{Deserialize, Serialize};
use sha2::Sha256;

/// GCM/96 位随机 nonce
type Nonce = Array<u8, U12>;
/// nonce 字节数（与上面的类型一致）
const NONCE_BYTES: usize = 12;
/// AES-256-GCM 标签长度
const TAG_BYTES: usize = 16;

/// 设备级共享密钥长度（字节）
pub const SECRET_BYTES: usize = 32;
/// 信封格式版本
pub const ENVELOPE_V: u8 = 1;

/// 派生域分隔常量
const DOMAIN: &str = "aw-sync/1";
/// 时间戳容忍窗口（秒）：超出即判定为无效或重放
const TS_WINDOW_SECS: i64 = 300;

/// 各接口的路径标识：同时作为 HKDF 的 info 与 AAD 的一部分，两端必须完全一致。
pub const PATH_PUSH: &str = "/api/0/sync/push";
pub const PATH_SNAPSHOT: &str = "/api/0/sync/snapshot";

/// 生成一个新的设备密钥（hex 编码，便于直接塞进 JSON 与 SQLite）
pub fn generate_secret() -> String {
    let mut bytes = [0u8; SECRET_BYTES];
    rand::thread_rng().fill(&mut bytes);
    to_hex(&bytes)
}

/// 密钥是否可用（长度/字符集校验，脏数据一律视为「未加密」而非 panic）
pub fn secret_ok(secret: &str) -> bool {
    secret.len() == SECRET_BYTES * 2
        && secret
            .chars()
            .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase())
}

fn to_hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push(DIGITS[(b >> 4) as usize] as char);
        s.push(DIGITS[(b & 0xf) as usize] as char);
    }
    s
}

fn from_hex(s: &str) -> Result<Vec<u8>, String> {
    if s.len() % 2 != 0 {
        return Err("密钥 hex 长度非法".into());
    }
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(s.len() / 2);
    for pair in b.chunks(2) {
        let hi = (pair[0] as char).to_digit(16).ok_or("密钥含非 hex 字符")?;
        let lo = (pair[1] as char).to_digit(16).ok_or("密钥含非 hex 字符")?;
        out.push(((hi << 4) | lo) as u8);
    }
    Ok(out)
}

/// 按接口派生 256 位对称密钥
fn derive_key(secret: &str, path: &str) -> Result<[u8; 32], String> {
    let ikm = from_hex(secret)?;
    if ikm.len() != SECRET_BYTES {
        return Err("密钥长度不符".into());
    }
    let hk = Hkdf::<Sha256>::new(Some(DOMAIN.as_bytes()), &ikm);
    let mut okm = [0u8; 32];
    hk.expand(path.as_bytes(), &mut okm)
        .map_err(|e| format!("HKDF 扩展失败: {e}"))?;
    Ok(okm)
}

fn aad(path: &str, kid: &str, ts: i64) -> Vec<u8> {
    format!("{DOMAIN}\n{path}\n{kid}\n{ts}").into_bytes()
}

/// 信封：明文 HTTP 的 body 只包含这个结构，真实数据在 ct 里。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Envelope {
    /// 版本号
    pub v: u8,
    /// 发送方设备 id（接收端据此选密钥；同时被 AAD 认证）
    pub kid: String,
    /// Unix 秒，参与 AAD 认证，用于时间窗与重放判定
    pub ts: i64,
    /// base64 的 12 字节随机 nonce
    pub n: String,
    /// base64 的密文（GCM tag 拼在尾部）
    pub ct: String,
}

impl Envelope {
    /// 快速判断一个 JSON 值是不是信封（不必先解出完整结构）
    pub fn looks_like(json: &serde_json::Value) -> bool {
        json.get("v").and_then(|v| v.as_u64()) == Some(ENVELOPE_V as u64)
            && json.get("kid").and_then(|v| v.as_str()).is_some_and(|s| !s.is_empty())
            && json.get("n").and_then(|v| v.as_str()).is_some()
            && json.get("ct").and_then(|v| v.as_str()).is_some()
    }

    /// 信封化后的 JSON 文本，直接作为 HTTP body
    pub fn to_json_string(&self) -> String {
        serde_json::to_string(self).unwrap_or_default()
    }
}

fn b64(bytes: &[u8]) -> String {
    base64::engine::general_purpose::STANDARD.encode(bytes)
}

fn unb64(s: &str) -> Result<Vec<u8>, String> {
    base64::engine::general_purpose::STANDARD
        .decode(s)
        .map_err(|e| format!("base64 解码失败: {e}"))
}

/// 用对端密钥封装明文。`self_id` 为发送方设备 id（写入 kid 并绑进 AAD）。
pub fn seal(secret: &str, path: &str, self_id: &str, plaintext: &[u8]) -> Result<Envelope, String> {
    let key = derive_key(secret, path)?;
    let cipher = Aes256Gcm::new_from_slice(&key).map_err(|e| e.to_string())?;
    let mut nonce = [0u8; 12];
    rand::thread_rng().fill(&mut nonce);
    let ts = chrono::Utc::now().timestamp();
    let ct = cipher
        .encrypt(
            &Nonce::try_from(nonce.as_slice()).map_err(|_| "内部错误：nonce 长度不符".to_string())?,
            Payload {
                msg: plaintext,
                aad: &aad(path, self_id, ts),
            },
        )
        .map_err(|e| format!("加密失败: {e}"))?;
    Ok(Envelope {
        v: ENVELOPE_V,
        kid: self_id.to_string(),
        ts,
        n: b64(&nonce),
        ct: b64(&ct),
    })
}

/// 解开信封：校验版本、时间窗、密钥、AAD、GCM 标签，并做 (kid, nonce) 防重放。
pub fn open(secret: &str, path: &str, env: &Envelope) -> Result<Vec<u8>, String> {
    if env.v != ENVELOPE_V {
        return Err(format!("不支持的信封版本: {}", env.v));
    }
    let now = chrono::Utc::now().timestamp();
    if (now - env.ts).abs() > TS_WINDOW_SECS {
        return Err(format!(
            "时间戳超出容忍窗口 {}s（偏差 {}s），拒收",
            TS_WINDOW_SECS,
            now - env.ts
        ));
    }
    if seen_recently(&env.kid, &env.n, env.ts) {
        return Err("检测到重放：相同 nonce 在时间窗内已出现过".into());
    }
    let key = derive_key(secret, path)?;
    let cipher = Aes256Gcm::new_from_slice(&key).map_err(|e| e.to_string())?;
    // 来自局域网的长度不受信：Array::from_slice 对不匹配长度会 panic，必须先自行校验
    let nonce = unb64(&env.n)?;
    if nonce.len() != NONCE_BYTES {
        return Err(format!("nonce 长度应为 {} 字节，实际 {}", NONCE_BYTES, nonce.len()));
    }
    let ct = unb64(&env.ct)?;
    if ct.len() < TAG_BYTES {
        return Err("密文过短，不足以容纳认证标签".into());
    }
    cipher
        .decrypt(
            &Nonce::try_from(nonce.as_slice()).map_err(|_| "nonce 长度非法".to_string())?,
            Payload {
                msg: ct.as_slice(),
                aad: &aad(path, &env.kid, env.ts),
            },
        )
        .map_err(|_| "解密失败：密钥不匹配或报文被篡改".to_string())
}

// ---- HTTP body 层面的封装/解封装（策略集中在这里，两端只写一次）----

/// 出站：有共享密钥 → 发信封 JSON；没有（尚未交换的旧端、热点直连）→ 原样明文。
pub fn seal_body(
    secret: Option<&str>,
    path: &str,
    self_id: &str,
    plaintext_json: &str,
) -> Result<String, String> {
    match secret {
        Some(s) if secret_ok(s) => Ok(seal(s, path, self_id, plaintext_json.as_bytes())?.to_json_string()),
        _ => Ok(plaintext_json.to_string()),
    }
}

/// 入站：把 HTTP body 还原成明文 JSON。
/// - 信封：必须持有该对端密钥才能解开（密钥不对/被篡改/重放都会 Err）。
/// - 明文：仅在该对端从未交换过密钥时放行，否则判定为降级攻击并拒收。
pub fn open_body(secret: Option<&str>, path: &str, raw: &str) -> Result<String, String> {
    let value: serde_json::Value = serde_json::from_str(raw).map_err(|e| format!("body 不是合法 JSON: {e}"))?;
    if !Envelope::looks_like(&value) {
        if secret.map_or(false, |s| secret_ok(s)) {
            return Err("该对端已协商加密密钥，拒收明文报文（疑似降级/重放攻击）".into());
        }
        return Ok(raw.to_string());
    }
    let env: Envelope = serde_json::from_value(value).map_err(|e| format!("信封结构非法: {e}"))?;
    let secret = secret.ok_or_else(|| format!("收到 {} 的加密报文，但本机没有与之交换过密钥", env.kid))?;
    let plain = open(secret, path, &env)?;
    String::from_utf8(plain).map_err(|_| "解密结果不是 UTF-8 JSON".to_string())
}

/// 嗅探一个 body 的信封发送方 id（非信封返回 None）：接收端据此查密钥。
pub fn envelope_kid(raw: &str) -> Option<String> {
    let value: serde_json::Value = serde_json::from_str(raw).ok()?;
    if !Envelope::looks_like(&value) {
        return None;
    }
    value.get("kid").and_then(|v| v.as_str()).map(str::to_string)
}

/// 已收 nonce 的去重表：`"{kid}\n{nonce}" -> ts`，超窗即淘汰。
fn seen_recently(kid: &str, nonce: &str, ts: i64) -> bool {
    static SEEN: OnceLock<Mutex<HashMap<String, i64>>> = OnceLock::new();
    let map = SEEN.get_or_init(|| Mutex::new(HashMap::new()));
    let mut m = map.lock().unwrap_or_else(|p| p.into_inner());
    let now = chrono::Utc::now().timestamp();
    if m.len() > 4096 {
        m.retain(|_, seen| now - *seen < TS_WINDOW_SECS);
    }
    let key = format!("{kid}\n{nonce}");
    if m.contains_key(&key) {
        return true;
    }
    m.insert(key, ts);
    false
}

/// 安全码：两端各自用「共享密钥 + 两个设备 id」算出的 4 位大写十六进制。
/// 设备 id 按字典序归一，保证两端结果一致，人工比对才有意义。
pub fn fingerprint(secret: &str, id_a: &str, id_b: &str) -> Option<String> {
    if !secret_ok(secret) || id_a.is_empty() || id_b.is_empty() {
        return None;
    }
    let (first, second) = if id_a <= id_b { (id_a, id_b) } else { (id_b, id_a) };
    let ikm = from_hex(secret).ok()?;
    let hk = Hkdf::<Sha256>::new(Some(b"aw-sync/fp/1"), &ikm);
    let mut okm = [0u8; 4];
    hk.expand(format!("{first}\n{second}").as_bytes(), &mut okm)
        .ok()?;
    Some(to_hex(&okm[0..2]).to_uppercase())
}

/// 广播别名：未配对设备在 UDP 广播里只暴露设备 id 的哈希短名，
/// 避免主机名（往往是 `Zhang-PC` 这类可识别信息）被被动抓包直接读走。
pub fn hashed_alias(device_id: &str) -> String {
    use sha2::Digest;
    let digest = Sha256::digest(format!("aw-sync/alias/1\n{device_id}").as_bytes());
    to_hex(&digest[0..4])
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dev_id(tag: &str) -> String {
        format!("device-{tag}")
    }

    #[test]
    fn seal_open_roundtrip() {
        let secret = generate_secret();
        assert!(secret_ok(&secret));
        let msg = b"{\"activity\":\"hello\"}";
        let env = seal(&secret, PATH_PUSH, "a", msg).unwrap();
        assert!(Envelope::looks_like(
            &serde_json::from_str::<serde_json::Value>(&env.to_json_string()).unwrap()
        ));
        let out = open(&secret, PATH_PUSH, &env).unwrap();
        assert_eq!(out, msg);
    }

    #[test]
    fn wrong_secret_or_wrong_path_fails() {
        let secret = generate_secret();
        let other = generate_secret();
        let env = seal(&secret, PATH_PUSH, "a", b"x".as_slice()).unwrap();
        assert!(open(&other, PATH_PUSH, &env).is_err());
        assert!(open(&secret, PATH_SNAPSHOT, &env).is_err());
    }

    #[test]
    fn tampered_ciphertext_and_kid_rejected() {
        let secret = generate_secret();
        let mut env = seal(&secret, PATH_PUSH, "a", b"payload".as_slice()).unwrap();
        env.ct = b64(&[0u8; 16]);
        // 先记下合法信封，避免下面的重放表把新用例误判为重放
        let good = seal(&secret, PATH_PUSH, "b", b"payload".as_slice()).unwrap();
        assert!(open(&secret, PATH_PUSH, &env).is_err(), "篡改密文必须失败");
        let mut rerouted = seal(&secret, PATH_PUSH, "c", b"payload".as_slice()).unwrap();
        rerouted.kid = dev_id("spoofed");
        assert!(open(&secret, PATH_PUSH, &rerouted).is_err(), "改写 kid 必须失败");
        assert!(open(&secret, PATH_PUSH, &good).is_ok());
    }

    #[test]
    fn replay_within_window_rejected() {
        let secret = generate_secret();
        let env = seal(&secret, PATH_PUSH, "replay-a", b"one-shot".as_slice()).unwrap();
        assert!(open(&secret, PATH_PUSH, &env).is_ok());
        assert!(
            open(&secret, PATH_PUSH, &env).is_err(),
            "同一 nonce 在时间窗内重放必须被拒"
        );
    }

    #[test]
    fn stale_timestamp_rejected() {
        let secret = generate_secret();
        let mut env = seal(&secret, PATH_PUSH, "stale-a", b"x".as_slice()).unwrap();
        env.ts -= TS_WINDOW_SECS * 2;
        assert!(open(&secret, PATH_PUSH, &env).is_err(), "过期时间戳必须失败");
    }

    #[test]
    fn fingerprint_is_symmetric_and_short() {
        let secret = generate_secret();
        let a = dev_id("aaa");
        let b = dev_id("bbb");
        let fa = fingerprint(&secret, &a, &b).unwrap();
        let fb = fingerprint(&secret, &b, &a).unwrap();
        assert_eq!(fa, fb, "两端必须算出同一个安全码");
        assert_eq!(fa.len(), 4);
        assert!(fa.chars().all(|c| c.is_ascii_hexdigit() && !c.is_ascii_lowercase()));
        // 换密钥 → 安全码必然不同（MITM 换密钥会被这 4 位暴露）
        assert_ne!(fa, fingerprint(&generate_secret(), &a, &b).unwrap());
        assert!(fingerprint("tooshort", &a, &b).is_none());
    }

    #[test]
    fn hashed_alias_hides_hostname() {
        let alias = hashed_alias("some-device-id");
        assert_eq!(alias.len(), 8);
        assert_eq!(alias, hashed_alias("some-device-id"));
        assert_ne!(alias, hashed_alias("other-device-id"));
    }

    #[test]
    fn body_fallback_and_downgrade_rejected() {
        let secret = generate_secret();
        let plain = "{\"activity\":\"{}\"}";
        // 尚未交换密钥的旧端：出站仍是明文
        assert_eq!(seal_body(None, PATH_PUSH, "a", plain).unwrap(), plain);
        let sealed = seal_body(Some(&secret), PATH_PUSH, "a", plain).unwrap();
        assert_ne!(sealed, plain);
        assert!(!sealed.contains("activity"));
        // 入站：信封要用同一把密钥解开
        assert_eq!(open_body(Some(&secret), PATH_PUSH, &sealed).unwrap(), plain);
        // 已协商密钥的对端发来明文 → 拒绝（否则中间人只要把密钥丢掉就能读明文）
        assert!(open_body(Some(&secret), PATH_PUSH, plain).is_err());
        // 从未交换过密钥 → 明文照旧可用（热点直连 / 旧版本）
        assert_eq!(open_body(None, PATH_PUSH, plain).unwrap(), plain);
        assert!(open_body(None, PATH_PUSH, &sealed).is_err(), "没有密钥却收到信封，解不开");
    }

    #[test]
    fn envelope_detection_does_not_match_snapshot_json() {
        // 明文快照里有 v/kid 之外的字段，绝不能被当成信封
        let snap = serde_json::json!({"source_device": {"id": "x"}, "inbox": "{}"});
        assert!(!Envelope::looks_like(&snap));
        let bad = serde_json::json!({"v": 1, "kid": "x", "n": "eA==", "ct": "eA==", "ts": 1});
        assert!(Envelope::looks_like(&bad));
        assert!(!Envelope::looks_like(&serde_json::json!({"v": 2, "kid": "x"})));
    }
}
