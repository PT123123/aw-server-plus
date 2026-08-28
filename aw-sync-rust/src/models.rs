//! aw-sync-rust 数据模型
//!
//! 集中定义局域网同步所需的全部数据类型：设备、配对码、同步日志、
//! 同步快照（传输载荷）等。所有枚举用字符串序列化，方便 webui 展示与持久化。

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// 设备类型
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DeviceKind {
    Windows,
    Android,
    Ios,
    Linux,
    Macos,
    Unknown,
}

impl DeviceKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            DeviceKind::Windows => "windows",
            DeviceKind::Android => "android",
            DeviceKind::Ios => "ios",
            DeviceKind::Linux => "linux",
            DeviceKind::Macos => "macos",
            DeviceKind::Unknown => "unknown",
        }
    }
}

/// 同步方向：发出 / 接收
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SyncDirection {
    /// 本设备发往对端
    Out,
    /// 从对端接收
    In,
}

impl SyncDirection {
    pub fn as_str(&self) -> &'static str {
        match self {
            SyncDirection::Out => "out",
            SyncDirection::In => "in",
        }
    }
}

/// 同步协议类型
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SyncProtocol {
    Http,
    UdpBroadcast,
    Mdns,
}

impl SyncProtocol {
    pub fn as_str(&self) -> &'static str {
        match self {
            SyncProtocol::Http => "http",
            SyncProtocol::UdpBroadcast => "udp_broadcast",
            SyncProtocol::Mdns => "mdns",
        }
    }
}

/// 同步事件类型
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SyncEventType {
    Pairing,
    Discovery,
    Sync,
    Conflict,
}

impl SyncEventType {
    pub fn as_str(&self) -> &'static str {
        match self {
            SyncEventType::Pairing => "pairing",
            SyncEventType::Discovery => "discovery",
            SyncEventType::Sync => "sync",
            SyncEventType::Conflict => "conflict",
        }
    }
}

/// 同步状态
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SyncStatus {
    Success,
    Failed,
    Running,
}

impl SyncStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            SyncStatus::Success => "success",
            SyncStatus::Failed => "failed",
            SyncStatus::Running => "running",
        }
    }
}

/// 一台已配对（或通过广播发现加入信任列表）的远端设备
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Device {
    pub id: String,
    pub name: String,
    pub device_kind: DeviceKind,
    pub ip: String,
    /// 五位数同步 HTTP 端口（默认 56001）
    pub port: u16,
    pub paired_at: DateTime<Utc>,
    /// 最近一次成功同步时间
    pub last_sync_at: Option<DateTime<Utc>>,
    /// 最近一次被发现/探测到的时间（用于判断在线状态）
    #[serde(default)]
    pub last_seen_at: Option<DateTime<Utc>>,
    pub is_online: bool,
    pub is_self: bool,
    /// 是否已完成配对（false = 仅广播发现的未配对设备）
    #[serde(default)]
    pub paired: bool,
    /// 用户设置的别名（展示优先于 name）
    #[serde(default)]
    pub alias: Option<String>,
}

impl Device {
    pub fn endpoint(&self) -> String {
        format!("http://{}:{}/api/0/sync", self.ip, self.port)
    }
}

/// 配对码
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PairCode {
    pub code: String,
    pub created_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
}

impl PairCode {
    pub fn is_expired(&self, now: DateTime<Utc>) -> bool {
        now >= self.expires_at
    }
}

/// 一条同步日志（方向/协议/事件/状态/消息）
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncLogEntry {
    pub id: Option<i64>,
    pub timestamp: DateTime<Utc>,
    pub direction: SyncDirection,
    pub protocol: SyncProtocol,
    pub peer_id: Option<String>,
    pub event_type: SyncEventType,
    pub status: SyncStatus,
    pub message: Option<String>,
    pub data_size: Option<u64>,
}

/// 局域网同步设置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncConfig {
    /// 局域网同步总开关
    pub enabled: bool,
    /// HTTP 同步开关
    pub http_enabled: bool,
    /// 设备发现方式：broadcast / mdns / poll（后续预留）
    pub discovery_method: String,
    /// 本机同步 HTTP 监听端口（五位数，默认 56001）
    pub listen_port: u16,
    /// UDP 广播/发现固定端口（五位数，默认 46000）
    pub udp_port: u16,
    /// 同步目标：inbox.db / sqlite.db
    pub sync_inbox: bool,
    pub sync_activity: bool,
    /// 本机别名（广播时随载荷发送；空则使用主机名）
    #[serde(default)]
    pub self_alias: String,
    /// 在线状态探测间隔（秒），用于已配对设备的心跳探测
    pub probe_interval: u16,
}

impl Default for SyncConfig {
    fn default() -> Self {
        SyncConfig {
            enabled: false,
            http_enabled: true,
            discovery_method: "broadcast".to_string(),
            listen_port: 5600,
            udp_port: 46000,
            sync_inbox: true,
            sync_activity: true,
            self_alias: String::new(),
            probe_interval: 10,
        }
    }
}

/// 同步载荷快照：读取目标库（sqlite.db + inbox.db）后序列化而成，经 JSON 传输。
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SyncSnapshot {
    /// 发送方设备信息
    pub source_device: Option<Device>,
    /// ActivityWatch 数据（buckets + events 的 JSON 文本）
    pub activity: Option<String>,
    /// Inbox 数据（notes/tags/comments 的 JSON 文本）
    pub inbox: Option<String>,
}

/// 设备同步统计信息
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeviceSyncStats {
    pub device_id: String,
    /// 待同步条数（本地有但远端还没有的数据）
    pub pending_push_count: i32,
    /// 待解决冲突数
    pub pending_conflict_count: i32,
    /// 总同步条数（历史累计）
    pub total_synced_count: i64,
    /// 总同步大小 (bytes)
    pub total_synced_size: i64,
    /// 本地笔记条数
    pub local_note_count: i32,
    /// 远端笔记条数
    pub remote_note_count: i32,
    /// 上次同步时间
    pub last_sync_at: Option<String>,
    /// 上次全量同步时间
    pub last_full_sync_at: Option<String>,
    /// 同步频率 (分钟)，None 表示尚未同步过
    pub sync_frequency_minutes: Option<i32>,
    /// 最近错误信息
    pub last_error: Option<String>,
    /// 错误发生时间
    pub last_error_at: Option<String>,
}

/// 冲突摘要信息
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConflictSummary {
    /// 笔记 ID
    pub note_id: i64,
    /// 笔记标题/内容摘要
    pub note_title: String,
    /// 检测到冲突的时间
    pub detected_at: String,
    /// 是否已解决
    pub resolved: bool,
    /// 解决方式（如果已解决）
    pub resolution: Option<String>,
}