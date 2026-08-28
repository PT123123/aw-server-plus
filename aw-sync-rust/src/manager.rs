//! SyncManager：面向 aw-server 的业务门面。

//! 聚合 sync.db 持久化、配对、目标库导出/导入、HTTP 推送、设备发现。



use std::path::Path;

use std::sync::{Arc, Mutex, OnceLock};



use chrono::Utc;

use std::collections::HashMap;



use crate::models::{
    ConflictSummary, Device, DeviceKind, DeviceSyncStats, PairCode, SyncConfig, SyncDirection,
    SyncEventType, SyncLogEntry, SyncProtocol, SyncSnapshot, SyncStatus,
};

use crate::paircode::{PairError, PairingManager};

use crate::serialize::{export_activity, export_inbox, import_activity, import_inbox};

use crate::storage::{LogFilter, SyncDb};

use crate::{conflict, discovery};



pub type SharedManager = Arc<Mutex<SyncManager>>;



pub struct SyncManager {

    /// 数据目录（sqlite.db / inbox.db / sync.db 所在目录）

    data_dir: std::path::PathBuf,

    /// 本机设备 ID（由上层 aw-server 注入,保证与主库 device_id 一致）

    self_id: String,

    db: Arc<Mutex<SyncDb>>,

    /// 收方待确认的配对请求：对方设备 id -> 对方 Device 信息（内存态,重启后需重新发起）

    pub inbound_pair_requests: Mutex<HashMap<String, Device>>,

}



impl SyncManager {

    pub fn new(data_dir: &Path, self_id: String) -> Result<SharedManager, String> {

        let db = SyncDb::open(data_dir).map_err(|e| e.to_string())?;

        // 强制将 listen_port 同步为实际服务器端口，防止旧数据库中保存的端口与实际不符
        {
            let mut cfg = db.get_config();
            if cfg.listen_port != crate::DEFAULT_SYNC_PORT {
                log::info!(
                    "[aw-sync] listen_port 从 {} 修正为 {}",
                    cfg.listen_port,
                    crate::DEFAULT_SYNC_PORT
                );
                cfg.listen_port = crate::DEFAULT_SYNC_PORT;
                let _ = db.set_config(&cfg);
            }
        }

        crate::dbglog::info(format!(

            "[server] SyncManager 初始化完成: device_id={}, data_dir={}",

            self_id,

            data_dir.display()

        ));

        Ok(Arc::new(Mutex::new(SyncManager {

            data_dir: data_dir.to_path_buf(),

            self_id,

            db: Arc::new(Mutex::new(db)),

            inbound_pair_requests: Mutex::new(HashMap::new()),

        })))

    }



    pub fn self_id(&self) -> &str {

        &self.self_id

    }

    fn db(&self) -> std::sync::MutexGuard<'_, SyncDb> {
        self.db.lock().unwrap()
    }



    // ---- 配置 ----



    pub fn get_config(&self) -> SyncConfig {

        self.db().get_config()

    }



    pub fn set_config(&self, cfg: &SyncConfig) -> Result<(), String> {

        self.db().set_config(cfg).map_err(|e| e.to_string())

    }



    // ---- 配对 ----



    pub fn create_pair_code(&self) -> Result<PairCode, PairError> {

        PairingManager::new(&self.db()).create_pair_code()

    }



    pub fn join_with_code(&self, code: &str, device: Device) -> Result<Device, PairError> {

        PairingManager::new(&self.db()).join_with_code(code, device)

    }



    // ---- 设备 ----



    pub fn list_devices(&self) -> Result<Vec<Device>, String> {

        self.db().get_devices().map_err(|e| e.to_string())

    }



    pub fn get_device(&self, id: &str) -> Result<Option<Device>, String> {

        self.db().get_device(id).map_err(|e| e.to_string())

    }



    pub fn delete_device(&self, id: &str) -> Result<bool, String> {

        self.db().delete_device(id).map_err(|e| e.to_string())

    }

    /// 清空所有配对/已发现设备（保留本机记录与同步设置），并清除内存态待确认配对请求。
    /// 对应前端「清空所有配对信息」按钮。
    pub fn clear_all_devices(&self) -> Result<usize, String> {
        let n = self.db().delete_all_devices().map_err(|e| e.to_string())?;
        // 同时清空内存态的待确认配对请求，避免删除后仍有「接受配对」按钮残留
        if let Ok(mut m) = self.inbound_pair_requests.lock() {
            m.clear();
        }
        Ok(n)
    }



        pub fn save_device(&self, device: &Device) -> Result<(), String> {

        self.db().upsert_device(device).map_err(|e| e.to_string())

    }



    pub fn update_device_alias(&self, id: &str, alias: Option<&str>) -> Result<bool, String> {

        self.db().update_alias(id, alias).map_err(|e| e.to_string())

    }



    // ---- 日志 ----



    pub fn list_logs(&self, f: &LogFilter) -> Result<Vec<SyncLogEntry>, String> {

        self.db().get_logs(f).map_err(|e| e.to_string())

    }



    pub fn log_count(&self) -> Result<u64, String> {

        self.db().log_count().map_err(|e| e.to_string())

    }



    pub fn add_log(&self, e: &SyncLogEntry) -> Result<i64, String> {

        self.db().add_log(e).map_err(|e| e.to_string())

    }



    pub fn truncate_logs(&self, keep: u64) -> Result<(), String> {

        self.db().truncate_logs(keep).map_err(|e| e.to_string())

    }



    // ---- 目标库导出 / 导入 ----



    /// 组装本机数据快照（按配置决定是否含 activity / inbox）。

    pub fn export(&self, sn: &mut SyncSnapshot) {

        let cfg = self.get_config();

        if cfg.sync_activity {

            let p = self.data_dir.join("sqlite.db");

            sn.activity = export_activity(p.as_path()).ok();

        }

        if cfg.sync_inbox {

            let p = self.data_dir.join("inbox.db");

            sn.inbox = export_inbox(p.as_path()).ok();

        }

    }



    /// 接收并写入本地目标库（幂等 upsert）。返回应用记录条数。

    pub fn apply_snapshot(&self, snap: &SyncSnapshot) -> Result<usize, String> {

        let src = snap

            .source_device

            .as_ref()

            .map(|d| format!("{}({})", d.name, d.id))

            .unwrap_or_else(|| "未知设备".into());

        crate::dbglog::info(format!("[sync] 收到来自 {} 的同步快照", src));

        let mut applied = 0usize;

        // 冲突处理占位：见 conflict.rs,本期采用幂等合并。

        let _action = conflict::resolve_conflict(snap, None, None);

        if let Some(activity) = &snap.activity {

            applied += import_activity(self.data_dir.join("sqlite.db").as_path(), activity)

                .unwrap_or(0);

        }

        if let Some(inbox) = &snap.inbox {

            applied += import_inbox(self.data_dir.join("inbox.db").as_path(), inbox)

                .unwrap_or(0);

        }

        crate::dbglog::info(format!("[sync] 快照应用完成: 来源 {}, 应用记录数 {}", src, applied));

        Ok(applied)

    }



    /// 立即向某设备推送一次同步。

    pub fn sync_to(&self, peer_id: &str) -> Result<usize, String> {

        let peer = self.get_device(peer_id)?.ok_or("未找到目标设备")?;

        let mut snap = SyncSnapshot {

            source_device: Some(self.self_device_info()),

            ..Default::default()

        };

        self.export(&mut snap);

        crate::dbglog::info(format!(

            "[sync] 向 {}({}) {}:{} 发起同步推送...",

            peer.name, peer.id, peer.ip, peer.port

        ));

        let applied = crate::transport::push_snapshot(&peer, &snap)?;

        self.db().mark_synced(&peer.id, Utc::now()).map_err(|e| e.to_string())?;

        let size = snap

            .activity

            .as_ref()

            .map_or(0, |s| s.len() as u64)

            + snap.inbox.as_ref().map_or(0, |s| s.len() as u64);

        self.add_log(&SyncLogEntry {
            id: None,
            timestamp: Utc::now(),
            direction: SyncDirection::Out,
            protocol: SyncProtocol::Http,
            peer_id: Some(peer.id.clone()),
            event_type: SyncEventType::Sync,
            status: SyncStatus::Success,
            message: Some(format!(
                "本机({}) 已向 {}({}) 同步 {} 条数据",
                self.self_id, peer.name, peer.id, applied
            )),
            data_size: Some(size),
        })?;

        Ok(applied)

    }



    // ---- 配对（基于已发现设备发起的 HTTP 请求/确认） ----



    /// 发起配对请求：向目标设备发送本机信息,等待对方确认。

    /// 返回 true 表示成功向对方发出了请求（对方会出现在其「已发现未配对」并可接受）。

    pub fn initiate_pair(&self, peer_id: &str) -> Result<serde_json::Value, String> {

        let peer = self.get_device(peer_id)?.ok_or("未找到目标设备")?;

        let me = self.self_device_info();

        let url = format!("{}/pair/request", peer.endpoint());

        let payload = serde_json::to_string(&me).unwrap_or_default();

        log::info!("[aw-sync] initiate_pair: 目标设备={}, ip={}, port={}", peer.name, peer.ip, peer.port);

        let detail = format!(
            "本机({}) 向 {}({}) 发起配对请求\n目标: {} ({}:{})\nURL: {}\n请求体: {}",
            self.self_id, peer.name, peer.id, peer.name, peer.ip, peer.port, url, payload
        );

        match self.add_log(&SyncLogEntry {
            id: None,
            timestamp: Utc::now(),
            direction: SyncDirection::Out,
            protocol: SyncProtocol::Http,
            peer_id: Some(peer.id.clone()),
            event_type: SyncEventType::Pairing,
            status: SyncStatus::Success,
            message: Some(detail.clone()),
            data_size: Some(payload.len() as u64),
        }) {
            Ok(id) => log::info!("[aw-sync] initiate_pair: add_log 成功, id={}", id),
            Err(e) => log::error!("[aw-sync] initiate_pair: add_log 失败: {}", e),
        }

        let resp = crate::transport::send_pair_request(&peer, &me)?;

        crate::dbglog::info(format!("[pair] 已向 {} 发起配对请求", peer.name));

        Ok(resp)

    }



    /// 收到对方发来的配对请求：记录到待确认列表。

    pub fn record_inbound_pair_request(&self, from: Device) -> Result<(), String> {
        let id = from.id.clone();
        let name = from.name.clone();
        // 记录到同步日志（显示报文）
        let log_entry = SyncLogEntry {
            id: None,
            timestamp: Utc::now(),
            direction: SyncDirection::In,
            protocol: SyncProtocol::Http,
            peer_id: Some(id.clone()),
            event_type: SyncEventType::Pairing,
            status: SyncStatus::Success,
            message: Some(format!(
                "本机({}) 收到配对请求: {}({}:{} - {})",
                self.self_id, name, from.id, from.ip, from.port
            )),
            data_size: None,
        };
        self.add_log(&log_entry)
.map_err(|e| crate::dbglog::error(format!("[pair] add_log failed: {}", e)))
            .ok();
        self.inbound_pair_requests
            .lock()
            .map_err(|e| format!("锁定配对请求状态失败: {e}"))?
            .insert(id, from);
        crate::dbglog::info(format!(
            "[pair] 收到来自 {} 的配对请求，等待本机确认",
            name
        ));
        Ok(())
    }
    pub fn confirm_pair_with(&self, peer_id: &str) -> Result<serde_json::Value, String> {
        let peer = self.get_device(peer_id)?.ok_or("未找到目标设备")?;
        let me = self.self_device_info();
        let url = format!("{}/pair/confirm", peer.endpoint());
        let payload = serde_json::to_string(&me).unwrap_or_default();
        self.add_log(&SyncLogEntry {
            id: None,
            timestamp: Utc::now(),
            direction: SyncDirection::Out,
            protocol: SyncProtocol::Http,
            peer_id: Some(peer.id.clone()),
            event_type: SyncEventType::Pairing,
            status: SyncStatus::Success,
            message: Some(format!(
                "本机({}) 确认与 {}({}) 配对\nURL: {}\n请求体: {}",
                self.self_id, peer.name, peer.id, url, payload
            )),
            data_size: Some(payload.len() as u64),
        }).ok();
        let resp = crate::transport::confirm_pair(&peer, &me)?;
        self.mark_paired(peer_id, true)?;
        // 清除待确认记录
        if let Ok(mut m) = self.inbound_pair_requests.lock() {
            m.remove(peer_id);
        }
        crate::dbglog::info(format!("[pair] 已与 {} 完成配对", peer.name));
        Ok(resp)
    }


    /// 被对方确认配对：把对方标记为已配对。

    pub fn mark_paired(&self, peer_id: &str, paired: bool) -> Result<(), String> {

        self.db().set_paired(peer_id, paired).map_err(|e| e.to_string())?;

        Ok(())

    }



    /// 是否有待本机确认的配对请求

    pub fn has_inbound_pair_request(&self, peer_id: &str) -> bool {

        self.inbound_pair_requests

            .lock()

            .map(|m| m.contains_key(peer_id))

            .unwrap_or(false)

    }



    /// 待本机确认的配对请求设备 id 列表

    pub fn inbound_pair_device_ids(&self) -> Vec<String> {

        self.inbound_pair_requests

            .lock()

            .map(|m| m.keys().cloned().collect())

            .unwrap_or_default()

    }



    /// 在线探测线程：遍历所有已配对设备,按配置间隔探测其在线状态并更新 is_online。

    /// 进程内用原子标志保证只启动一次,返回值恒为（空线程句柄或实际句柄）。

    pub fn spawn_probe(&self) -> std::thread::JoinHandle<()> {

        use std::sync::atomic::{AtomicBool, Ordering};

        static PROBE_STARTED: AtomicBool = AtomicBool::new(false);

        if PROBE_STARTED.swap(true, Ordering::SeqCst) {

            return std::thread::spawn(|| {});

        }

        let data_dir = self.data_dir.clone();

        std::thread::Builder::new()

            .name("aw-sync-probe".into())

            .spawn(move || loop {

                if let Ok(db) = SyncDb::open(&data_dir) {

                    if let Ok(devices) = db.get_devices() {

                        for d in devices {

                            // 探测所有非本机设备（包括已配对和仅发现的设备）
                            if !d.is_self {

                                let online = crate::transport::probe_online(&d).is_ok();

                                if let Err(e) = db.touch_online(&d.id, online) {
                                    crate::dbglog::error(format!("[probe] touch_online failed for {}: {}", d.id, e));
                                    continue;
                                }
                            }

                        }

                    }

                    // 读取探测间隔（一旦失败回退 10s）

                    let interval = db.get_config().probe_interval.max(2) as u64;

                    std::thread::sleep(std::time::Duration::from_secs(interval));

                } else {

                    std::thread::sleep(std::time::Duration::from_secs(10));

                }

            })

            .unwrap_or_else(|_| {

                // 若无法启动线程,返回一个已结束的句柄

                std::thread::spawn(|| {})

            })

    }



    /// 根据当前配置启动后台发现（广播宣告 + 监听并入信任列表）。

    /// 返回已启动的线程句柄。配置关闭或为非 broadcast 模式时不启动。

    /// 进程内只允许成功启动一次,避免用户反复保存设置导致线程累积。

    pub fn spawn_discovery(&self) -> Vec<std::thread::JoinHandle<()>> {

        if discovery_running() {

            return Vec::new();

        }

        let cfg = self.get_config();

        if !cfg.enabled || cfg.discovery_method != "broadcast" {

            return Vec::new();

        }

        set_discovery_running(true);

        let mut handles = Vec::new();

        let udp = cfg.udp_port;

        let self_device = self.self_device_info();



        // 周期广播自己的信息

        let dev = self_device.clone();

        let data_dir = self.data_dir.clone();

        if let Ok(h) = std::thread::Builder::new()

            .name("aw-sync-announce".into())

            .spawn(move || {

                discovery::broadcast_loop(

                    discovery::SelfInfo { device: dev, data_dir },

                    udp,

                    std::time::Duration::from_secs(5),

                )

            })

        {

            handles.push(h);

        }



        // 监听广播并把发现的设备写入信任列表

        let db: discovery::SharedDb = Arc::clone(&self.db);

        let sid = self_device.id.clone();

        if let Ok(h) = std::thread::Builder::new()

            .name("aw-sync-listen".into())

            .spawn(move || discovery::listener_loop(db, udp, sid))

        {

            handles.push(h);

        }

        handles

    }



    /// 获取设备同步统计信息
    pub fn get_device_sync_stats(&self, device_id: &str) -> Result<DeviceSyncStats, String> {
        self.db().get_device_sync_stats(device_id).map_err(|e| e.to_string())
    }

    /// 获取设备冲突列表
    pub fn get_device_conflicts(&self, device_id: &str) -> Result<Vec<ConflictSummary>, String> {
        self.db().get_device_conflicts(device_id).map_err(|e| e.to_string())
    }

    /// 本机 Device（用于展示与广播）。

    pub fn self_device_info(&self) -> Device {

        let cfg = self.get_config();

        // 本机名字可读性：① 设置里的别名优先；② 否则用主机名（排除 localhost 等无意义值）；
        // ③ 否则用「设备类型-短id」这类可读默认名，避免写死成 localhost。
        let raw_host = gethostname::gethostname().to_string_lossy().to_string();
        let name = if !cfg.self_alias.is_empty() {
            cfg.self_alias.clone()
        } else if !raw_host.is_empty()
            && raw_host != "localhost"
            && raw_host != "localhost.localdomain"
            && raw_host != "(none)"
        {
            raw_host
        } else {
            let short = self.self_id.replace('-', "");
            let short = &short[..short.len().min(6)];
            format!("{}-{}", device_kind_label(current_kind()), short)
        };

                Device {

            id: self.self_id.clone(),

            name,

            device_kind: current_kind(),

            // IP 优先用 Android 侧注入的 Wi-Fi 真地址（绕过 VPN），
            // 否则退回枚举网卡结果；都拿不到则留空（前端提示未获取到，广播跳过）。
            ip: local_ip_override().or_else(local_ip).unwrap_or_default(),

            port: cfg.listen_port,

            paired_at: Utc::now(),

            last_sync_at: None,

            last_seen_at: Some(Utc::now()),

            is_online: true,

            is_self: true,

            paired: false,

            alias: if cfg.self_alias.is_empty() {

                None

            } else {

                Some(cfg.self_alias.clone())

            },

        }

    }

}



fn current_kind() -> DeviceKind {

    if cfg!(target_os = "android") {

        DeviceKind::Android

    } else if cfg!(target_os = "windows") {

        DeviceKind::Windows

    } else if cfg!(target_os = "macos") {

        DeviceKind::Macos

    } else if cfg!(target_os = "linux") {

        DeviceKind::Linux

    } else {

        DeviceKind::Unknown

    }

}

/// 设备类型的中文/英文可读标签（用于默认本机名「类型-短id」）。
fn device_kind_label(kind: DeviceKind) -> &'static str {

    match kind {

        DeviceKind::Android => "Android",

        DeviceKind::Windows => "Windows",

        DeviceKind::Linux => "Linux",

        DeviceKind::Macos => "MacOS",

        DeviceKind::Ios => "iOS",

        DeviceKind::Unknown => "Device",

    }

}



/// 本机 IP 的权威覆盖位（由 Android 侧读取 Wi-Fi 链路地址后注入，绕过 VPN）。
/// 为空时退回 `local_ip()` 的枚举结果。
static LOCAL_IP_OVERRIDE: OnceLock<Mutex<Option<String>>> = OnceLock::new();

/// 最近一次枚举网卡选中时记录下来的接口名（如 `wlan0`），用于前端/日志透明展示。
static LOCAL_IP_IFACE: OnceLock<Mutex<Option<String>>> = OnceLock::new();

/// 由 Android Java 侧调用：注入从 Wi-Fi 链路直接读取到的本机 IP（不受 VPN 影响）。
pub fn set_local_ip_override(ip: String) {
    let clean = ip.trim().to_string();
    let valid = !clean.is_empty()
        && !clean.starts_with("127.")
        && clean != "localhost"
        && !clean.parse::<std::net::IpAddr>().map(|a| a.is_loopback() || a.is_unspecified()).unwrap_or(true);
    let slot = LOCAL_IP_OVERRIDE.get_or_init(|| Mutex::new(None));
    if let Ok(mut g) = slot.lock() {
        *g = if valid { Some(clean) } else { None };
    }
}

/// 返回注入的覆盖 IP（已校验非空且非回环），无效时返回 None。
pub fn local_ip_override() -> Option<String> {
    let guard = LOCAL_IP_OVERRIDE.get()?;
    let g = guard.lock().ok()?;
    g.clone().filter(|s| !s.is_empty())
}

/// 返回本机地址对应的接口来源，用于前端/日志透明展示：
/// - 若由 Android 注入 Wi-Fi 真地址，返回 "wifi(Android注入)"；
/// - 否则返回最近一次枚举选中的网卡名（如 `wlan0`）；没有则 None。
pub fn local_ip_iface() -> Option<String> {
    if LOCAL_IP_OVERRIDE.get().map(|m| m.lock().ok().map(|g| g.is_some())).flatten().unwrap_or(false) {
        return Some("wifi(Android注入)".to_string());
    }
    let guard = LOCAL_IP_IFACE.get()?;
    let g = guard.lock().ok()?;
    g.clone().filter(|s| !s.is_empty())
}

/// 探测本机非回环 IPv4：**枚举所有网卡接口**，挑出本机真实地址。
/// 不再用「UDP connect 外网取出口地址」——那在 Android 开 VPN 时会选中隧道接口
/// （如 tun0 的 172.19.0.1），导致多台设备拿到同一个网关地址。
/// 这里直接遍历接口、排除 VPN/隧道、优先 Wi-Fi/以太网，得到每台设备自己的 IP。
/// 仅当确实拿到真实局域网地址时返回 Some；失败返回 None（广播线程会跳过假地址）。

fn local_ip() -> Option<String> {

    // 直接调用 POSIX getifaddrs（linux / android 均可用），不依赖第三方枚举库，
    // 避免其 Android 分支在新工具链下 CStr::from_ptr 签名不兼容导致编译失败。

    #[cfg(not(any(target_os = "linux", target_os = "android")))]
    {
        return None;
    }

    #[cfg(any(target_os = "linux", target_os = "android"))]
    unsafe {
        use std::net::Ipv4Addr;

        let mut ifap: *mut libc::ifaddrs = std::ptr::null_mut();
        if libc::getifaddrs(&mut ifap) != 0 {
            return None;
        }

        let mut preferred: Vec<(Ipv4Addr, String)> = Vec::new();
        let mut others: Vec<(Ipv4Addr, String)> = Vec::new();

        let mut cur = ifap;
        while !cur.is_null() {
            let ifa = &*cur;

            if !ifa.ifa_addr.is_null()
                && (*ifa.ifa_addr).sa_family as i32 == libc::AF_INET as i32
            {
                let sin = ifa.ifa_addr as *const libc::sockaddr_in;
                // s_addr 按网络字节序存放，from_be 后转成标准库 Ipv4Addr（与主机字节序无关）。
                let ip = Ipv4Addr::from(u32::from_be((*sin).sin_addr.s_addr));

                // 跳过回环 / 未指定 / 链路本地(169.254.x)
                if ip.is_loopback() || ip.is_unspecified() || ip.is_link_local() {
                    cur = ifa.ifa_next;
                    continue;
                }

                // 读取接口名（手动遍历字节，避免 CStr::from_ptr 在新工具链的类型不匹配）
                let name = {
                    let mut n = 0usize;
                    while *ifa.ifa_name.add(n) != 0 {
                        n += 1;
                    }
                    let bytes = std::slice::from_raw_parts(ifa.ifa_name as *const u8, n);
                    String::from_utf8_lossy(bytes).to_string().to_ascii_lowercase()
                };

                // 跳过 VPN / 隧道接口
                if name.starts_with("tun")
                    || name.starts_with("ppp")
                    || name.starts_with("tap")
                    || name.starts_with("utun")
                    || name.contains("vpn")
                {
                    cur = ifa.ifa_next;
                    continue;
                }

                // 优先 Wi-Fi / 以太网接口
                if name.starts_with("wlan")
                    || name.starts_with("eth")
                    || name.starts_with("en")
                    || name.contains("wifi")
                {
                    preferred.push((ip, name));
                } else {
                    others.push((ip, name));
                }
            }

            cur = ifa.ifa_next;
        }

        libc::freeifaddrs(ifap);

        // 私网段优先级：192.168 > 10 > 172.16-31 > 其它私网
        let rank = |ip: &Ipv4Addr| -> u8 {
            let o = ip.octets();
            if o[0] == 192 && o[1] == 168 {
                3
            } else if o[0] == 10 {
                2
            } else if o[0] == 172 && (o[1] >= 16 && o[1] <= 31) {
                1
            } else if ip.is_private() {
                1
            } else {
                0
            }
        };

        let pick = |list: &mut Vec<(Ipv4Addr, String)>| -> Option<(String, String)> {
            list.sort_by(|a, b| rank(&b.0).cmp(&rank(&a.0)));
            list.first().map(|(ip, name)| (ip.to_string(), name.clone()))
        };

        let chosen = pick(&mut preferred).or_else(|| pick(&mut others));
        if let Some((ip, iface)) = chosen {
            if let Some(slot) = LOCAL_IP_IFACE.get() {
                if let Ok(mut g) = slot.lock() {
                    *g = Some(iface);
                }
            }
            Some(ip)
        } else {
            None
        }
    }
}

// ---- 进程内发现状态 ----



use std::sync::atomic::{AtomicBool, Ordering};



static DISCOVERY_RUNNING: AtomicBool = AtomicBool::new(false);



/// 广播发现线程是否已在运行（供 /api/0/sync/status 查询）

pub fn discovery_running() -> bool {

    DISCOVERY_RUNNING.load(Ordering::SeqCst)

}



fn set_discovery_running(v: bool) {

    DISCOVERY_RUNNING.store(v, Ordering::SeqCst);

}



/// 测试钩子：同一进程内起多台“虚拟设备”时,允许第二台也启动自己的广播线程。

#[doc(hidden)]

pub fn reset_discovery_started_for_testing() {

    set_discovery_running(false);

}
