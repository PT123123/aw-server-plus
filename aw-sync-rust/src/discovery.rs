//! 设备发现：UDP 广播 / mDNS 自动发现。
//!
//! - 通过本地 UDP 广播（固定端口 46000）周期发送自身的设备名、IP、HTTP 同步端口。
//! - 监听同一端口的广播包，解析对端信息后**自动加入本地永久信任列表**，并刷新在线状态。
//!   下次同一局域网内无需重复配对。
//! - mDNS 预留增强接口（后续迭代可用 libmdns 等实现跨网段/Wi-Fi 感知发现）。
//! - 轮询遍历（poll）本期留空占位。

use chrono::Utc;
use log::{debug, error, info, warn};
use std::collections::HashMap;
use std::net::{SocketAddr, UdpSocket};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};
use std::thread;
use std::time::{Duration, Instant};

use crate::models::{
    Device, SyncDirection, SyncEventType, SyncLogEntry, SyncProtocol, SyncStatus,
};
use crate::storage::SyncDb;

pub type SharedDb = Arc<Mutex<SyncDb>>;

/// 广播消息的前缀标记，用于快速识别身份（避免与随机 UDP 包混淆）
const MAGIC: &str = "AW-SYNC/1.0";

/// UDP 广播发现常量
pub const DEFAULT_UDP_PORT: u16 = 46000;

/// 本机设备描述（供广播 / 列表展示）；data_dir 用于在广播线程内写同步日志
pub struct SelfInfo {
    pub device: Device,
    pub data_dir: PathBuf,
}

/// 同类日志的去抖窗口（秒）：避免每 5 秒的周期报文刷爆同步日志
const LOG_DEDUP_SECS: u64 = 60;

fn should_log(key: &str) -> bool {
    static DEDUP: OnceLock<Mutex<HashMap<String, Instant>>> = OnceLock::new();
    let m = DEDUP.get_or_init(|| Mutex::new(HashMap::new()));
    let mut map = match m.lock() {
        Ok(m) => m,
        Err(poisoned) => poisoned.into_inner(),
    };
    let now = Instant::now();
    let fresh = match map.get(key) {
        Some(t) => now.duration_since(*t).as_secs() < LOG_DEDUP_SECS,
        None => false,
    };
    if !fresh {
        map.insert(key.to_string(), now);
    }
    !fresh
}

/// 根据本机 IP 计算子网定向广播地址。
/// 例如 IP = 192.168.1.10 网关掩码 255.255.255.0 → 192.168.1.255
/// 在 Android 等环境中，255.255.255.255 可能无法穿透路由器，需要
/// 发送到子网定向广播地址才能真正到达局域网内的其他设备。
pub fn subnet_broadcast(local_ip: &str) -> Option<String> {
    let ip: std::net::Ipv4Addr = local_ip.parse().ok()?;
    // 默认假设 C 类网络 255.255.255.0（最常见家庭路由器情形）
    let mask: std::net::Ipv4Addr = "255.255.255.0".parse().unwrap();
    let octets = ip.octets();
    let mask_octets = mask.octets();
    let broadcast = std::net::Ipv4Addr::from([
        octets[0] | !mask_octets[0],
        octets[1] | !mask_octets[1],
        octets[2] | !mask_octets[2],
        octets[3] | !mask_octets[3],
    ]);
    // 跳过 0.0.0.0 / 127.x.x.x 等无效情况
    if broadcast.is_unspecified() || broadcast.is_loopback() {
        None
    } else {
        Some(broadcast.to_string())
    }
}

/// 在固定 UDP 端口周期广播本机信息。
/// 阻塞线程运行，返回后调用方需 join（一般放入常驻线程）。
pub fn broadcast_loop(info: SelfInfo, udp_port: u16, interval: Duration) {
    let device = info.device;
    // 强制从本机 Wi-Fi 网卡（device.ip）发包：把套接字绑定到该 IP，使广播报文从该网卡 egress、
    // 源地址固定为本机真实 Wi-Fi 地址（避免走 VPN 默认路由）。绑定失败则回退 0.0.0.0（全部网卡）。
    let bind_addr: SocketAddr = match format!("{}:0", device.ip).parse() {
        Ok(a) => a,
        Err(_) => "0.0.0.0:0".parse().unwrap(),
    };
    let socket = match UdpSocket::bind(bind_addr) {
        Ok(s) => s,
        Err(_) => match UdpSocket::bind("0.0.0.0:0".parse::<SocketAddr>().unwrap()) {
            Ok(s2) => s2,
            Err(e) => {
                error!("[aw-sync][discovery] 无法绑定 UDP 广播套接字: {e}");
                return;
            }
        },
    };
    let _ = socket.set_broadcast(true);
    crate::dbglog::info(format!(
        "[discovery] UDP 广播套接字绑定到 {}（本机地址 {}）",
        bind_addr, device.ip
    ));

    // 未获取到真实局域网 IP 时不广播假地址：否则多台设备都会宣称同一个回环/空地址，
    // 既互相无法区分，配对后又会错误地同步回本机。
    if device.ip.is_empty() || device.ip == "127.0.0.1" || device.ip == "localhost" {
        crate::dbglog::warn(format!(
            "[discovery] 本机未获取到局域网 IP（当前 {}），暂停 UDP 广播宣告（请检查 Wi-Fi 连接）",
            device.ip
        ));
        return;
    }

    // 广播目标：子网定向广播地址 + 端口
    // 同时发送到 255.255.255.255（有限广播）作为后备，某些局域网环境下这种方式更可靠
    let mut targets: Vec<SocketAddr> = Vec::new();
    if let Some(subnet_bcast) = subnet_broadcast(&device.ip) {
        if let Ok(a) = format!("{}:{udp_port}", subnet_bcast).parse() {
            targets.push(a);
        }
    }
    if let Ok(a) = format!("255.255.255.255:{udp_port}").parse() {
        targets.push(a);
    }
    if targets.is_empty() {
        return;
    }

    let payload = format!("{}\n{}", MAGIC, serde_json::to_string(&device).unwrap_or_default());
    crate::dbglog::info(format!(
        "[discovery] UDP 广播启动: 本机={}({}) {}:{} → 目标={} 端口 {}",
        device.name, device.id, device.ip, device.port,
        targets.iter().map(|t| t.ip().to_string()).collect::<Vec<_>>().join(", "),
        udp_port
    ));

    // 广播线程内独立的 sync.db 连接（把「发出广播宣告」写入同步报文信息）
    let out_db = SyncDb::open(Path::new(&info.data_dir)).ok();

    loop {
        for tgt in &targets {
            let _ = socket.send_to(payload.as_bytes(), *tgt);
        }
        // 出站广播报文：60 秒去抖，避免周期包刷屏
        if should_log(&format!("out-{}", device.id)) {
            crate::dbglog::info(format!(
                "[discovery] 发出广播宣告 {}:{} (udp:{})",
                device.ip, device.port, udp_port
            ));
            if let Some(db) = &out_db {
                let _ = db.add_log(&SyncLogEntry {
                    id: None,
                    timestamp: Utc::now(),
                    direction: SyncDirection::Out,
                    protocol: SyncProtocol::UdpBroadcast,
                    peer_id: None,
                    event_type: SyncEventType::Discovery,
                    status: SyncStatus::Success,
                    message: Some(format!(
                        "发出广播宣告 {}:{} (id:{} udp:{})",
                        device.ip, device.port, device.id, udp_port
                    )),
                    data_size: Some(payload.len() as u64),
                });
            }
        }
        thread::sleep(interval);
        // mDNS 预留：_aw-sync._tcp.local 注册/刷新
        debug!("[aw-sync] 广播自我信息到 {}", targets[0]);
    }
}

/// 在固定 UDP 端口监听广播，把发现的设备持久化进信任列表。
/// 成功找到未知设备时自动 join（无需手动配对）。
pub fn listener_loop(db: SharedDb, udp_port: u16, self_id: String) {
    let addr: SocketAddr = match format!("0.0.0.0:{udp_port}").parse() {
        Ok(a) => a,
        Err(e) => {
            error!("[aw-sync] 无效监听地址: {e}");
            return;
        }
    };
    let socket = match UdpSocket::bind(addr) {
        Ok(s) => s,
        Err(e) => {
            // 端口被其它进程占用属常见情况，仅警告不崩溃
            crate::dbglog::warn(format!("[discovery] 监听 UDP 端口 {udp_port} 失败: {e}"));
            return;
        }
    };
    let mut buf = [0u8; 4096];
    loop {
        match socket.recv_from(&mut buf) {
            Ok((n, src)) => {
                let text = String::from_utf8_lossy(&buf[..n]).to_string();
                if let Some(device) = parse_device(&text) {
                    if device.id == self_id {
                        continue; // 忽略自己
                    }
                    // 对端广播里携带自身的 is_self=true / paired 信息，在本地解析后必须固化：
                    // - is_self: 对端当然不是本机，强制 false，否则前端 is_self 过滤会在两个列表都藏掉该设备
                    // - paired:  由 upsert_discovered 决定——新发现保持未配对，已存在的保留其配对状态
                    let mut dev = device;
                    dev.is_self = false;
                    dev.paired = false;
                    dev.last_seen_at = Some(chrono::Utc::now());
                    // 关键：用真正收到包的源 IP 作为该对端的同步地址。
                    // 自报的 dev.ip 可能因本机 IP 探测出错而填错（如填成 VPN 网关/其它网卡），
                    // 但 UDP 包的源地址一定是当前网络下对方真正可达的地址，优先用它。
                    let src_ip = src.ip().to_string();
                    if !src_ip.is_empty()
                        && src_ip != "127.0.0.1"
                        && src_ip != "localhost"
                        && src_ip != "0.0.0.0"
                        && src_ip != "::1"
                    {
                        crate::dbglog::info(format!(
                            "[discovery] 收到 {} 的广播，源地址={}，采用源地址作为同步地址（自报 ip={}）",
                            dev.name, src_ip, dev.ip
                        ));
                        dev.ip = src_ip;
                    }
                    crate::dbglog::info(format!(
                        "[discovery] 发现设备 {}({}) {}:{}，已写入信任列表",
                        dev.name, dev.id, dev.ip, dev.port
                    ));
                    if let Ok(mut db) = db.lock() {
                        match db.upsert_discovered(&dev) {
                            Ok(()) => crate::dbglog::info(format!("[discovery] upsert_discovered 成功: {}", dev.id)),
                            Err(e) => crate::dbglog::error(format!("[discovery] upsert_discovered 失败: {} err={}", dev.id, e)),
                        }
                        if !should_log(&format!("in-{}", dev.id)) {
                            continue; // 去抖窗口内不重复写发现日志
                        }
                        let entry = SyncLogEntry {
                            id: None,
                            timestamp: chrono::Utc::now(),
                            direction: SyncDirection::In,
                            protocol: SyncProtocol::UdpBroadcast,
                            peer_id: Some(dev.id.clone()),
                            event_type: SyncEventType::Discovery,
                            status: SyncStatus::Success,
                            message: Some(format!(
                                "收到 {} 的广播报文 ({}:{} id:{})，已加入信任列表",
                                dev.name, dev.ip, dev.port, dev.id
                            )),
                            data_size: None,
                        };
                        db.add_log(&entry)
                            .map_err(|e| crate::dbglog::error(format!("[discovery] add_log failed: {}", e)))
                            .ok();
                    }
                    info!("[aw-sync>discovery] 已把设备 '{}' 加入信任列表", dev.name);
                }
            }
            Err(_) => {}
        }
    }
}

/// 解析广播文本为 Device（带 MAGIC 前缀或以 JSON 直接承载）
pub fn parse_device(text: &str) -> Option<Device> {
    // 支持两种格式：纯 JSON(device 对象) 或带 MAGIC 前缀的 JSON
    let json = text.strip_prefix(MAGIC).map(str::trim).unwrap_or(text.trim());
    if json.is_empty() {
        return None;
    }
    let mut dev: Device = serde_json::from_str(json).ok()?;
    if dev.id.is_empty() || dev.ip.is_empty() {
        return None;
    }
    // 防御性固化：任何从网络解析出的设备都不可能是“本机”或“已配对”，
    // 避免把对端广播里自带的 is_self=true / paired 状态污染进本地存储。
    dev.is_self = false;
    dev.paired = false;
    Some(dev)
}

// ================= 轮询遍历（本期留空） =================

/// 轮询遍历发现（在局域网扫指定 IP 段以找出运行中的对端）。
/// 本期仅保留类型与接口，逻辑留待后续迭代。
pub fn poll_loop(_db: SharedDb, _port: u16, _interval: Duration) {
    // TODO(后续迭代)：遍历局域网网段，向每个候选 IP 的 listen_port 发起 /api/0/sync/info
    // 握手，确认对端存在后加入信任列表。
    warn!("[aw-sync>discovery] 轮询遍历发现尚未实现，已跳过（示意占位）。");
}

// ================= mDNS（预留） =================

/// mDNS 服务解析：为对端提供查询本机 _aw-sync._tcp.local 的能力。
/// 本期以 UDP 广播为主，暂未启用真正 mDNS。后续可用 libmdns / mdns-sd 扩展，
/// 以便支持跨 VLAN / 单播查询。
pub fn mdns_available() -> bool {
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_raw_json() {
        let d = Device {
            id: "abc".into(),
            name: "Phone".into(),
            device_kind: crate::models::DeviceKind::Android,
            ip: "192.168.1.9".into(),
            port: 56001,
            paired_at: chrono::Utc::now(),
            last_sync_at: None,
            is_online: true,
            is_self: false,
            paired: false,
            alias: None,
        };
        let text = serde_json::to_string(&d).unwrap();
        let parsed = parse_device(&text).unwrap();
        assert_eq!(parsed.id, "abc");
        assert_eq!(parsed.ip, "192.168.1.9");
    }

    #[test]
    fn test_parse_with_magic() {
        let text = format!("{}\n{}", MAGIC, serde_json::to_string(&Device {
                id: "x".into(),
                name: "PC".into(),
                device_kind: crate::models::DeviceKind::Linux,
                ip: "10.0.0.2".into(),
                port: 56001,
                paired_at: chrono::Utc::now(),
                last_sync_at: None,
                                is_online: false,
                is_self: false,
                paired: false,
                alias: None,
            })
            .unwrap()
        );
        assert!(parse_device(&text).is_some());
    }
}