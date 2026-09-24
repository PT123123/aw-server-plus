//! 配对码管理：生成 / 校验 / 加入配对。

use rand::Rng;
use chrono::{Duration, Utc};

use crate::models::{Device, PairCode};
use crate::storage::SyncDb;

/// 配对码长度（6 位数字）
///
/// 从 4 位提到 6 位：配对码是密钥交换期唯一的身份凭据，同网段攻击者在 5 分钟有效期内
/// 可无限次试 `/api/0/sync/join`（无速率限制）。4 位只有 1e4 种，6 位是 1e6。
/// 真正的补救是配对后两端比对安全码（见 crypto::fingerprint）。
const CODE_LEN: usize = 6;
/// 配对码有效期（分钟）
const CODE_VALIDITY_MINUTES: i64 = 5;

#[derive(Debug)]
pub enum PairError {
    InvalidOrExpiredCode,
    Db(rusqlite::Error),
}

impl std::fmt::Display for PairError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PairError::InvalidOrExpiredCode => write!(f, "Invalid or expired pairing code"),
            PairError::Db(e) => write!(f, "Database error: {e}"),
        }
    }
}

impl std::error::Error for PairError {}

impl From<rusqlite::Error> for PairError {
    fn from(e: rusqlite::Error) -> Self {
        PairError::Db(e)
    }
}

pub struct PairingManager<'a> {
    db: &'a SyncDb,
}

impl<'a> PairingManager<'a> {
    pub fn new(db: &'a SyncDb) -> Self {
        PairingManager { db }
    }

    /// 生成一个新配对码（CODE_LEN 位、5 分钟内有效）。
    ///
    /// 安全说明：桌面端 aw-server 绑定 0.0.0.0 且 /api/0/sync/log 对同网段任何人可读，
    /// 所以配对码的具体数字绝不能进日志/同步日志；这里只留「已生成」事件。
    pub fn create_pair_code(&self) -> Result<PairCode, PairError> {
        self.db.cleanup_expired_codes()?;
        let now = Utc::now();
        let code = generate_code();
        let pc = PairCode {
            code,
            created_at: now,
            expires_at: now + Duration::minutes(CODE_VALIDITY_MINUTES),
        };
        self.db.store_pair_code(&pc)?;
        crate::dbglog::info(format!(
            "[pair] 已生成 {} 位配对码（有效期 {} 分钟，数字不入日志）",
            CODE_LEN, CODE_VALIDITY_MINUTES
        ));
        Ok(pc)
    }

    /// 校验配对码是否有效（不消费）
    pub fn validate_code(&self, code: &str) -> Result<bool, PairError> {
        Ok(self.db.validate_pair_code(code.trim())?)
    }

    /// 用配对码加入：校验通过则把远端设备加入信任列表并消费该码
    pub fn join_with_code(&self, code: &str, device: Device) -> Result<Device, PairError> {
        let code = code.trim().to_uppercase();
        crate::dbglog::info(format!(
            "[pair] 收到加入请求: 设备={}({}) {}:{}",
            device.name, device.id, device.ip, device.port
        ));
        if !self.db.validate_pair_code(&code)? {
            crate::dbglog::warn("[pair] 加入被拒绝: 配对码无效或已过期");
            return Err(PairError::InvalidOrExpiredCode);
        }
        let mut device = device;
        device.paired = true; // 配对完成
        // 报文里那个 is_self 是「对方眼中的自己」，恒为 true —— 原样入库会让这台刚配好的
        // 设备在码主的列表里被标成本机，前端 `!is_self` 一过滤就整机消失（配了个寂寞）。
        // 广播路径的同一类缺陷见 discovery::record_peer 的固化。
        device.is_self = false;
        self.db.upsert_device(&device)?;
        self.db.delete_pair_code(&code)?;
        crate::dbglog::info(format!(
            "[pair] 加入成功: 已登记设备 {}({}) {}:{}, 配对码已消费",
            device.name, device.id, device.ip, device.port
        ));
        Ok(device)
    }
}

/// 生成 CODE_LEN 位纯数字配对码
fn generate_code() -> String {
    let mut rng = rand::thread_rng();
    let bound: u32 = 10u32.pow(CODE_LEN as u32);
    format!("{:0width$}", rng.gen_range(0..bound), width = CODE_LEN)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    /// 打开位于临时目录（保持守卫存活）的 SyncDb。
    fn open_db(dir: &TempDir) -> SyncDb {
        SyncDb::open(dir.path()).unwrap()
    }

    #[test]
    fn test_create_and_validate() {
        let dir = TempDir::new().unwrap();
        let db = open_db(&dir);
        let mgr = PairingManager::new(&db);
        let pc = mgr.create_pair_code().unwrap();
        assert_eq!(pc.code.len(), CODE_LEN);
        assert!(mgr.validate_code(&pc.code).unwrap());
        assert!(!mgr.validate_code("WRONG1").unwrap());
    }

    #[test]
    fn test_join_consumes_code() {
        let dir = TempDir::new().unwrap();
        let db = open_db(&dir);
        let mgr = PairingManager::new(&db);
        let pc = mgr.create_pair_code().unwrap();
        let device = Device {
            id: "remote-001".into(),
            name: "Remote PC".into(),
            device_kind: crate::models::DeviceKind::Windows,
            ip: "192.168.1.50".into(),
            port: 56001,
            paired_at: Utc::now(),
            last_sync_at: None,
        last_seen_at: None,
            is_online: true,
            is_self: false,
            paired: false,
            alias: None,
            device_secret: None,
            machine_uid: None,
        };
        mgr.join_with_code(&pc.code, device).unwrap();
        assert!(!mgr.validate_code(&pc.code).unwrap());
        let devices = db.get_devices().unwrap();
        assert_eq!(devices.len(), 1);
        assert_eq!(devices[0].id, "remote-001");
    }

    #[test]
    fn test_join_invalid() {
        let dir = TempDir::new().unwrap();
        let db = open_db(&dir);
        let mgr = PairingManager::new(&db);
        let device = Device {
            id: "x".into(),
            name: "x".into(),
            device_kind: crate::models::DeviceKind::Android,
            ip: "1.2.3.4".into(),
            port: 56001,
            paired_at: Utc::now(),
            last_sync_at: None,
        last_seen_at: None,
            is_online: false,
            is_self: false,
            paired: false,
            alias: None,
            device_secret: None,
            machine_uid: None,
        };
        let res = mgr.join_with_code("BADCODE", device);
        assert!(matches!(res, Err(PairError::InvalidOrExpiredCode)));
    }
}