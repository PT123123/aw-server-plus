//! 设备发现：两条路径共用同一套入库/仲裁逻辑（见 record_peer）。
//!
//! - **首选 mDNS/DNS-SD**（`crate::mdns`）：服务类型 `_activitywatch._tcp.local.`，
//!   SRV 带端口、A 记录带地址，不依赖子网掩码，地址变更与下线有标准语义。
//! - **备选 UDP 广播**（本文件的 broadcast_loop / listener_loop）：固定端口 46000，
//!   周期发送自身信息。在组播被 AP 吞掉的环境里它反而更可靠，因此保留。
//! - 监听到的对端信息**自动加入本地永久信任列表**并刷新在线状态，
//!   下次同一局域网内无需重复配对。
//! - 轮询遍历（poll）本期留空占位。

use chrono::{DateTime, Utc};
use log::{debug, error, info};
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

/// 一条对端宣告的来源。mDNS 为首选，UDP 广播为备选。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PeerSource {
    Mdns,
    UdpBroadcast,
}

impl PeerSource {
    /// devices.seen_via 列的取值
    pub fn as_str(&self) -> &'static str {
        match self {
            PeerSource::Mdns => "mdns",
            PeerSource::UdpBroadcast => "udp",
        }
    }

    fn protocol(&self) -> SyncProtocol {
        match self {
            PeerSource::Mdns => SyncProtocol::Mdns,
            PeerSource::UdpBroadcast => SyncProtocol::UdpBroadcast,
        }
    }
}

/// mDNS 信息被视为权威的窗口（秒）。
///
/// 只要这台设备最近还持续被 mDNS 解析到，迟到的 UDP 广播就不许改写它的 ip/port：
/// 广播里的自报地址可能来自错误的网卡探测，而 mDNS 的 A 记录 + SRV 端口是权威的。
/// 超出该窗口说明 mDNS 已经哑了（组播不通 / 对端换网），此时放行广播兜底。
pub const MDNS_FRESH_SECS: i64 = 45;

/// 来源仲裁：本次宣告能否覆盖已有记录里的 ip/port。
///
/// 只有一种组合需要压制：已有记录是 mDNS 报的且仍在新鲜期内，而这次来自 UDP 广播。
pub fn source_wins(
    existing: Option<(String, Option<DateTime<Utc>>)>,
    incoming: PeerSource,
    now: DateTime<Utc>,
) -> bool {
    let (via, seen) = match existing {
        Some(e) => e,
        None => return true,
    };
    if incoming == PeerSource::UdpBroadcast && via == "mdns" {
        return match seen {
            Some(t) => (now - t).num_seconds() >= MDNS_FRESH_SECS,
            None => true,
        };
    }
    true
}

/// 把一条对端宣告写进信任列表：来源仲裁 → 落库 → 记发现日志。
///
/// 返回 false 表示本次信息被更高优先级来源压制（只刷新在线状态，未改写可达地址）。
pub fn record_peer(db: &SharedDb, mut dev: Device, via: PeerSource, note: String) -> bool {
    // 从网络解析出的设备绝不可能是「本机」或「已配对」，在此固化，
    // 避免对端自带的 is_self/paired 状态污染本地存储。
    dev.is_self = false;
    dev.paired = false;
    let now = Utc::now();
    dev.last_seen_at = Some(now);

    let guard = match db.lock() {
        Ok(g) => g,
        Err(poisoned) => poisoned.into_inner(),
    };

    if !source_wins(guard.device_seen_source(&dev.id).ok().flatten(), via, now) {
        let _ = guard.touch_seen(&dev.id);
        debug!("[discovery] {via:?} 落败：{note}");
        return false;
    }

    if let Err(e) = guard.upsert_discovered(&dev, via.as_str()) {
        error!("[discovery] upsert_discovered 失败: {} err={}", dev.id, e);
        return false;
    }
    info!("[aw-sync>discovery] 已把设备 '{}' 加入信任列表", dev.name);
    // 同类日志 60 秒去抖：宣告是周期性的，不去抖会刷爆同步日志
    if should_log(&format!("in-{}-{}", via.as_str(), dev.id)) {
        let entry = SyncLogEntry {
            id: None,
            timestamp: now,
            direction: SyncDirection::In,
            protocol: via.protocol(),
            peer_id: Some(dev.id.clone()),
            event_type: SyncEventType::Discovery,
            status: SyncStatus::Success,
            message: Some(note),
            data_size: None,
            details: None,
        };
        if let Err(e) = guard.add_log(&entry) {
            error!("[discovery] add_log failed: {e}");
        }
    }
    true
}


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

/// 构造 UDP 宣告报文（`MAGIC\n{json}`）。
///
/// 单独抽出来，是因为「什么能进被动广播」是一条安全边界而不是格式细节：
/// 同网段任何人无需配对就能抓到这些包，所以本机信息在出网前统一脱敏——
/// name 换成设备 id 的哈希别名（真实主机名往往是 Zhang-PC 这类可识别信息），
/// 密钥与装机指纹一律清空。真实名字只在配对后的 HTTP 报文里交换；装机指纹
/// 只随用户主动发起的配对请求走一次（manager::with_outgoing_secret）。
pub fn announce_payload(device: &Device) -> String {
    let mut announce = device.clone();
    announce.name = crate::crypto::hashed_alias(&announce.id);
    announce.device_secret = None;
    announce.machine_uid = None;
    format!("{}\n{}", MAGIC, serde_json::to_string(&announce).unwrap_or_default())
}

/// 在固定 UDP 端口周期广播本机信息。
/// 进程级开关 discovery_active() 由「进入/离开局域网同步界面」驱动：未开启时空转不发送。
/// 每轮重新解析本机局域网 IP（Wi-Fi 重连/换网后自动切换新地址）；无有效 IP 时暂停宣告。
pub fn broadcast_loop(info: SelfInfo, udp_port: u16, interval: Duration) {
    let mut device = info.device;
    // 广播线程内独立的 sync.db 连接（把「发出广播宣告」写入同步报文信息）
    let out_db = SyncDb::open(Path::new(&info.data_dir)).ok();
    let mut bound_ip = String::new();
    let mut socket: Option<UdpSocket> = None;

    loop {
        // 未进入局域网同步界面：不广播
        if !crate::manager::discovery_active() {
            thread::sleep(Duration::from_millis(500));
            continue;
        }

        // 每轮重解析本机局域网 IP。未获取到真实局域网 IP 时不广播假地址：
        // 否则多台设备都会宣称同一个回环/空地址，既互相无法区分，配对后又会错误地同步回本机。
        let ip = crate::manager::current_local_ip();
        if ip.is_empty() || ip == "127.0.0.1" || ip == "localhost" {
            if !bound_ip.is_empty() {
                crate::dbglog::warn(format!(
                    "[discovery] 本机失去局域网 IP（原 {bound_ip}），暂停 UDP 广播宣告（请检查 Wi-Fi 连接）"
                ));
                bound_ip.clear();
                socket = None;
            }
            thread::sleep(interval.max(Duration::from_secs(2)));
            continue;
        }

        // IP 变化（首次或 Wi-Fi 重连）：重建套接字绑定到新地址，强制从该网卡发包
        // （源地址固定为本机真实 Wi-Fi 地址，避免走 VPN 默认路由；绑定失败回退 0.0.0.0）。
        if socket.is_none() || ip != bound_ip {
            let bind_addr: SocketAddr = match format!("{}:0", ip).parse() {
                Ok(a) => a,
                Err(_) => "0.0.0.0:0".parse().unwrap(),
            };
            let s = match UdpSocket::bind(bind_addr) {
                Ok(s) => s,
                Err(_) => match UdpSocket::bind("0.0.0.0:0".parse::<SocketAddr>().unwrap()) {
                    Ok(s2) => s2,
                    Err(e) => {
                        error!("[aw-sync][discovery] 无法绑定 UDP 广播套接字: {e}");
                        thread::sleep(interval);
                        continue;
                    }
                },
            };
            let _ = s.set_broadcast(true);
            crate::dbglog::info(format!(
                "[discovery] UDP 广播套接字绑定到 {}（本机地址 {ip}）",
                bind_addr
            ));
            socket = Some(s);
            bound_ip = ip.clone();
            device.ip = ip.clone();
        }
        let sock = match &socket {
            Some(s) => s,
            None => continue,
        };

        // 广播目标：子网定向广播地址 + 端口；同时发 255.255.255.255（有限广播）作为后备，
        // 某些局域网环境下这种方式更可靠
        let mut targets: Vec<SocketAddr> = Vec::new();
        if let Some(subnet_bcast) = subnet_broadcast(&bound_ip) {
            if let Ok(a) = format!("{}:{udp_port}", subnet_bcast).parse() {
                targets.push(a);
            }
        }
        if let Ok(a) = format!("255.255.255.255:{udp_port}").parse() {
            targets.push(a);
        }
        if targets.is_empty() {
            thread::sleep(interval);
            continue;
        }

        let payload = announce_payload(&device);
        for tgt in &targets {
            let _ = sock.send_to(payload.as_bytes(), *tgt);
        }
        debug!("[aw-sync] 广播自我信息到 {}", targets[0]);
        // 出站广播报文：60 秒去抖，避免周期包刷屏
        log_announce(
            out_db.as_ref(),
            &format!("out-udp-{}", device.id),
            PeerSource::UdpBroadcast,
            format!(
                "发出广播宣告 {}:{} (id:{} udp:{})",
                device.ip, device.port, device.id, udp_port
            ),
            Some(payload.len() as u64),
        );
        thread::sleep(interval);
    }
}

/// 出站宣告日志（UDP 广播与 mDNS 注册共用）：按 key 去抖，避免周期性报文刷爆同步日志。
pub(crate) fn log_announce(
    db: Option<&SyncDb>,
    dedup_key: &str,
    via: PeerSource,
    msg: String,
    data_size: Option<u64>,
) {
    if !should_log(dedup_key) {
        return;
    }
    crate::dbglog::info(format!("[discovery] {msg}"));
    if let Some(db) = db {
        let _ = db.add_log(&SyncLogEntry {
            id: None,
            timestamp: Utc::now(),
            direction: SyncDirection::Out,
            protocol: via.protocol(),
            peer_id: None,
            event_type: SyncEventType::Discovery,
            status: SyncStatus::Success,
            message: Some(msg),
            data_size,
            details: None,
        });
    }
}

/// 可作为对端同步端点的地址：回环/未指定地址一律不算（写进库就是死地址）。
pub(crate) fn is_usable_ip(s: &str) -> bool {
    !s.is_empty()
        && s != "127.0.0.1"
        && s != "localhost"
        && s != "0.0.0.0"
        && s != "::1"
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
    // 绑定失败不再直接退出线程：端口可能被同一进程里早先残留的套接字或另一个实例
    // 短暂占用。线程一旦退出，本进程的「发现」就永久哑掉（广播发得出去、却再也收不到
    // 任何设备），而且表面毫无异常 —— 只能靠 5 秒一次的重试把它救回来。
    let socket = loop {
        match UdpSocket::bind(addr) {
            Ok(s) => break s,
            Err(e) => {
                crate::dbglog::warn(format!(
                    "[discovery] 监听 UDP 端口 {udp_port} 失败: {e}，5 秒后重试"
                ));
                thread::sleep(Duration::from_secs(5));
            }
        }
    };
    let mut buf = [0u8; 4096];
    // 读取超时：让循环能周期检查 discovery_active 开关
    let _ = socket.set_read_timeout(Some(Duration::from_secs(2)));
    loop {
        // 发现未开启（Android 端离开同步界面、或同步总开关关闭）：不处理广播
        // （内核接收缓冲满后自动丢弃新包）。桌面端发现常驻，这里长期为 true。
        if !crate::manager::discovery_active() {
            thread::sleep(Duration::from_millis(500));
            continue;
        }
        match socket.recv_from(&mut buf) {
            Ok((n, src)) => {
                let text = String::from_utf8_lossy(&buf[..n]).to_string();
                if let Some(device) = parse_device(&text) {
                    if device.id == self_id {
                        continue; // 忽略自己
                    }
                    // is_self / paired 的固化、在线状态、来源仲裁与日志都在 record_peer 里做，
                    // mDNS 路径走的是同一个入口，两条路径因此不会各写各的。
                    let mut dev = device;
                    // 关键：用真正收到包的源 IP 作为该对端的同步地址。
                    // 自报的 dev.ip 可能因本机 IP 探测出错而填错（如填成 VPN 网关/其它网卡），
                    // 但 UDP 包的源地址一定是当前网络下对方真正可达的地址，优先用它。
                    let src_ip = src.ip().to_string();
                    if is_usable_ip(&src_ip) && dev.ip != src_ip {
                        crate::dbglog::info(format!(
                            "[discovery] 收到 {} 的广播，源地址={}\
                             ，改用源地址（自报 ip={})",
                            dev.name, src_ip, dev.ip
                        ));
                        dev.ip = src_ip;
                    }
                    let note = format!(
                        "收到 {} 的广播报文 ({}:{} id:{})",
                        dev.name, dev.ip, dev.port, dev.id
                    );
                    record_peer(&db, dev, PeerSource::UdpBroadcast, note);
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

// ================= mDNS（首选路径） =================

/// 首选路径是否可用（mDNS daemon 已就绪）。UDP 广播始终在跑，所以这里 false 只代表
/// 「暂时退到备选路径」，不代表设备发现整体失效。
pub fn mdns_available() -> bool {
    crate::mdns::daemon_ready()
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
        last_seen_at: None,
            is_online: true,
            is_self: false,
            paired: false,
            alias: None,
            device_secret: None,
            machine_uid: None,
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
        last_seen_at: None,
                                is_online: false,
                is_self: false,
                paired: false,
                alias: None,
            device_secret: None,
            machine_uid: None,
            })
            .unwrap()
        );
        assert!(parse_device(&text).is_some());
    }

    // ---- 来源仲裁：mDNS 首选、UDP 广播兜底 ----

    fn peer(id: &str, ip: &str) -> Device {
        Device {
            id: id.into(),
            name: crate::crypto::hashed_alias(id),
            device_kind: crate::models::DeviceKind::Windows,
            ip: ip.into(),
            port: crate::DEFAULT_SYNC_PORT,
            paired_at: chrono::Utc::now(),
            last_sync_at: None,
            last_seen_at: None,
            is_online: true,
            // 故意带上错误状态：从网络解析出的设备既不是本机也不可能已配对
            is_self: true,
            paired: true,
            alias: None,
            device_secret: None,
            machine_uid: None,
        }
    }

    fn at(secs_ago: i64) -> DateTime<Utc> {
        chrono::Utc::now() - chrono::Duration::seconds(secs_ago)
    }

    #[test]
    fn source_wins_matrix() {
        let now = chrono::Utc::now();
        let udp = PeerSource::UdpBroadcast;
        let mdns = PeerSource::Mdns;
        // 从没见过的设备：谁来都能写
        assert!(source_wins(None, udp, now));
        // 老库/配对登记（seen_via 为空）或时间无法解析：不锁死更新
        assert!(source_wins(Some(("".into(), Some(now))), udp, now));
        assert!(source_wins(Some(("mdns".into(), None)), udp, now));
        // 只有「新鲜 mDNS 记录 + 迟到的广播」这一种组合会被压制
        assert!(source_wins(Some(("mdns".into(), Some(at(MDNS_FRESH_SECS - 5)))), udp, now) == false);
        assert!(source_wins(Some(("mdns".into(), Some(at(MDNS_FRESH_SECS + 5)))), udp, now));
        assert!(source_wins(Some(("udp".into(), Some(at(0)))), mdns, now), "首选路径永远压过备选");
        assert!(source_wins(Some(("udp".into(), Some(at(0)))), udp, now), "备选路径之间照常覆盖");
    }

    #[test]
    fn fresh_mdns_record_suppresses_broadcast_address() {
        let dir = tempfile::TempDir::new().unwrap();
        let db = Arc::new(Mutex::new(SyncDb::open(dir.path()).unwrap()));
        assert!(record_peer(&db, peer("B", "10.1.2.3"), PeerSource::Mdns, "解析到 B".into()));

        let mut late = peer("B", "192.168.5.99");
        late.paired_at = at(99999);
        assert!(
            !record_peer(&db, late, PeerSource::UdpBroadcast, "广播 B".into()),
            "新鲜期内广播不得改写可达地址"
        );

        let g = db.lock().unwrap();
        let b = g.get_device("B").unwrap().unwrap();
        assert_eq!(b.ip, "10.1.2.3");
        assert_eq!(b.port, crate::DEFAULT_SYNC_PORT);
        assert_eq!(g.device_seen_source("B").unwrap().unwrap().0, "mdns");
        assert!(!b.is_self && !b.paired, "宣告自带的 is_self/paired 必须被固化掉");
    }

    #[test]
    fn broadcast_takes_over_once_mdns_goes_silent() {
        let dir = tempfile::TempDir::new().unwrap();
        let db = Arc::new(Mutex::new(SyncDb::open(dir.path()).unwrap()));
        // 一条超出新鲜期的 mDNS 记录：组播被 AP 吞掉 / daemon 起不来时的真实形态
        let mut stale = peer("B", "10.1.2.3");
        stale.last_seen_at = Some(at(MDNS_FRESH_SECS + 10));
        db.lock().unwrap().upsert_discovered(&stale, "mdns").unwrap();

        assert!(
            record_peer(&db, peer("B", "192.168.5.99"), PeerSource::UdpBroadcast, "广播 B".into()),
            "mDNS 哑了之后广播要能兜底"
        );
        let g = db.lock().unwrap();
        assert_eq!(g.get_device("B").unwrap().unwrap().ip, "192.168.5.99");
        assert_eq!(g.device_seen_source("B").unwrap().unwrap().0, "udp");
    }

    /// 关键回归：落败的广播只保留在线状态，不能顺手把 mDNS 的新鲜期续上。
    ///
    /// 若新鲜期跟着 last_seen_at 走，每 5 秒一次的广播会把 45 秒窗口无限延长，
    /// mDNS 真的哑掉后备选路径永远补不进来。
    #[test]
    fn losing_broadcast_does_not_extend_mdns_window() {
        let dir = tempfile::TempDir::new().unwrap();
        let db = Arc::new(Mutex::new(SyncDb::open(dir.path()).unwrap()));
        let mut seen = peer("B", "10.1.2.3");
        seen.last_seen_at = Some(at(30));
        db.lock().unwrap().upsert_discovered(&seen, "mdns").unwrap();

        assert!(!record_peer(&db, peer("B", "192.168.5.99"), PeerSource::UdpBroadcast, "广播 B".into()));
        let g = db.lock().unwrap();
        let (_, window_start) = g.device_seen_source("B").unwrap().unwrap();
        let last_seen = g.get_device("B").unwrap().unwrap().last_seen_at;
        assert!(
            (chrono::Utc::now() - window_start.unwrap()).num_seconds() >= 25,
            "仲裁窗口仍是那次 mDNS 解析的时刻，未被广播刷新"
        );
        assert!(last_seen.unwrap() > window_start.unwrap(), "在线状态该被广播刷新");
        assert_eq!(g.device_seen_source("B").unwrap().unwrap().0, "mdns");
    }
}