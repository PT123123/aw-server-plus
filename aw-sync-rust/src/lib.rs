//! aw-sync-rust
//!
//! ActivityWatch 局域网同步的独立实现库：
//! 设备发现（UDP 广播 / mDNS，轮询遍历占位）、配对码、目标库序列化传输、
//! HTTP 同步、冲突处理占位、同步状态持久化、REST API 挂载。
//!
//! 通过 aw-server 依赖并挂载进同一个 libaw_server.so，随 APK 打包。

#[macro_use]
extern crate log;

pub mod models;
pub mod storage;
pub mod paircode;
pub mod serialize;
pub mod conflict;
pub mod transport;
pub mod discovery;
pub mod manager;
pub mod dbglog;
pub mod endpoints;

pub use manager::SyncManager;
/// 由 Android（Java）侧注入 Wi-Fi 链路真实 IP（绕过 VPN 隧道）时使用。
pub use manager::set_local_ip_override;

/// 同步端口默认值（五位数）
pub const DEFAULT_SYNC_PORT: u16 = 5600;
/// UDP 广播/发现固定端口（五位数）
pub const DEFAULT_UDP_PORT: u16 = 46000;

use chrono::Utc;
use serde::{Deserialize, Serialize};

/// 已配对设备（对外暴露的简明视图，兼容 webui 字段命名）
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncdDevice {
    pub id: String,
    pub name: String,
    pub deviceKind: String,
    pub ip: String,
    pub port: u16,
    pub lastSyncAt: Option<String>,
    pub isOnline: bool,
    pub isSelf: bool,
}

/// 从全量 Device 构建 webui 对外视图
pub fn to_view_device(d: &models::Device) -> SyncdDevice {
    SyncdDevice {
        id: d.id.clone(),
        name: d.name.clone(),
        deviceKind: d.device_kind.as_str().to_string(),
        ip: d.ip.clone(),
        port: d.port,
        lastSyncAt: d.last_sync_at.as_ref().map(|t| t.to_rfc3339()),
        isOnline: d.is_online,
        isSelf: d.is_self,
    }
}