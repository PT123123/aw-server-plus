//! mDNS / DNS-SD 设备发现：局域网内发现对端的**首选路径**（备选见 [`crate::discovery`]）。
//!
//! 服务类型 `_activitywatch._tcp.local.`：
//! - 实例名 = `hashed_alias(device id)`（8 位十六进制）。既不给整条链路送出
//!   `Zhang-PC` 这类主机名，又保证同一台设备在任何一侧看到的名字都稳定；
//!   哈希撞名概率可忽略，所以关掉 RFC 6762 的名称探测（probe），省一轮组播往来。
//! - TXT 只带 `v`（协议版本）/ `id`（映射回 devices 表主键）/ `kind`；
//!   可达地址与同步端口取 A / SRV 记录，不必像广播报文那样自报。
//!
//! 解析出的对端统一交给 [`crate::discovery::record_peer`]，与 UDP 广播走同一个入库
//! 入口：能不能改写一台设备的 ip/port，由那边的来源仲裁决定（mDNS 新鲜期内 UDP 广播落败）。

use std::collections::HashSet;
use std::net::{IpAddr, Ipv4Addr};
use std::sync::{Arc, Mutex, OnceLock};
use std::thread;
use std::time::{Duration, Instant};

use chrono::Utc;
use mdns_sd::{
    DaemonEvent, IfKind, Receiver, RecvTimeoutError, ResolvedService, ServiceDaemon, ServiceEvent,
    ServiceInfo,
};

use crate::dbglog;
use crate::discovery::{self, PeerSource, SelfInfo, SharedDb};
use crate::models::Device;
use crate::storage::SyncDb;

/// 服务类型（与 aw-qtui/src/config.h 的 kMdnsServiceType 一致）
pub const SERVICE_TYPE: &str = "_activitywatch._tcp.local.";

/// TXT：协议版本 / 设备 id / 设备类型
pub const TXT_VERSION: &str = "v";
pub const TXT_DEVICE_ID: &str = "id";
pub const TXT_DEVICE_KIND: &str = "kind";
/// 当前宣告协议版本；对端不认的一律不认领
pub const PROTO_VERSION: &str = "1";

/// 已注册后轮询本机 IP 的间隔：IP 变了（Wi-Fi 重连/换网）要撤旧服务、注册新服务
const ANNOUNCE_POLL: Duration = Duration::from_secs(3);
/// daemon 创建失败后的退避间隔（mDNS 起不来不影响 UDP 备选路径继续跑）
const DAEMON_RETRY: Duration = Duration::from_secs(30);
/// 浏览接收端的一次等待片长：让循环能周期检查 discovery_active 开关
const BROWSE_TICK: Duration = Duration::from_secs(2);
/// 撤服务后等 daemon 确认的上限：不等住，紧随其后的重注册会被当成重名服务
const UNREGISTER_WAIT: Duration = Duration::from_secs(2);

/// 进程内唯一的 mDNS daemon（注册与浏览共用：一个 daemon = 一个 5353 套接字）
struct DaemonSlot {
    daemon: Option<Arc<ServiceDaemon>>,
    last_try: Instant,
}

fn slot() -> &'static Mutex<DaemonSlot> {
    static SLOT: OnceLock<Mutex<DaemonSlot>> = OnceLock::new();
    SLOT.get_or_init(|| Mutex::new(DaemonSlot {
        daemon: None,
        last_try: Instant::now() - DAEMON_RETRY,
    }))
}

/// mDNS 是否已就绪（供 discovery::mdns_available 查询）
pub fn daemon_ready() -> bool {
    slot().lock().ok().map(|g| g.daemon.is_some()).unwrap_or(false)
}

/// 取（必要时创建）进程内唯一的 mDNS daemon；拿不到返回 None，由调用方退回备选路径。
///
/// 失败按 DAEMON_RETRY 退避重试：5353 可能被别的实现占用，也可能被安全软件拦下，
/// 但「mDNS 一时起不来」绝不该让整台设备的发现能力消失。
fn daemon() -> Option<Arc<ServiceDaemon>> {
    let mut g = slot().lock().ok()?;
    if g.daemon.is_some() {
        return g.daemon.clone();
    }
    if g.last_try.elapsed() < DAEMON_RETRY {
        return None;
    }
    g.last_try = Instant::now();
    match ServiceDaemon::new() {
        Err(e) => {
            dbglog::warn(format!(
                "[discovery] mDNS 不可用（{e}），本轮只用 UDP 广播发现"
            ));
            None
        }
        Ok(d) => {
            let d = Arc::new(d);
            // 浏览留在所有 IPv4 接口上：VPN / 本机热点接口同样能发现对端。
            // 宣告则由 register_service 限定在当前局域网那块网卡，避免把 tun0 地址广播出去。
            let _ = d.disable_interface(IfKind::IPv6);
            let _ = d.disable_interface(IfKind::LoopbackV4);
            g.daemon = Some(d.clone());
            dbglog::info(format!("[discovery] mDNS daemon 已启动（{SERVICE_TYPE}）"));
            Some(d)
        }
    }
}

/// 本机 mDNS 宣告线程主体：注册服务、跟随本机 IP 变化重注册、停止发现时撤服务。
pub fn announce_loop(info: SelfInfo) {
    let alias = crate::crypto::hashed_alias(&info.device.id);
    let out_db = SyncDb::open(&info.data_dir).ok();
    let mut cur_ip = String::new();
    let mut fullname: Option<String> = None;
    let mut monitor: Option<Receiver<DaemonEvent>> = None;

    loop {
        if !crate::manager::discovery_active() {
            unregister(&mut fullname);
            monitor = None;
            thread::sleep(Duration::from_millis(500));
            continue;
        }

        let ip = crate::manager::current_local_ip();
        if ip.parse::<Ipv4Addr>().is_err() {
            // 本机没有可用 IPv4（没连 Wi-Fi / 只剩回环）：撤掉旧宣告。
            // 留着它，对端就会一直照着链路里一个已经死掉的地址来同步。
            if unregister(&mut fullname) {
                dbglog::warn("[discovery] 本机失去局域网 IPv4，已撤销 mDNS 宣告");
            }
            cur_ip.clear();
            thread::sleep(ANNOUNCE_POLL);
            continue;
        }

        if fullname.is_some() && ip == cur_ip {
            drain_monitor(&mut monitor);
            thread::sleep(ANNOUNCE_POLL);
            continue;
        }

        // 首次注册或 IP 变化：先撤旧的，再注册新的
        unregister(&mut fullname);
        match register_service(&alias, &ip, &info) {
            Ok(name) => {
                fullname = Some(name);
                cur_ip = ip.clone();
                let port = info.device.port;
                discovery::log_announce(
                    out_db.as_ref(),
                    &format!("out-mdns-{}", info.device.id),
                    PeerSource::Mdns,
                    format!("注册 mDNS 服务 {alias} → {ip}:{port} (id:{})", info.device.id),
                    None,
                );
            }
            Err(e) => {
                cur_ip.clear();
                dbglog::warn(format!("[discovery] mDNS 注册失败（{e}），只用 UDP 广播宣告"));
                thread::sleep(DAEMON_RETRY.min(Duration::from_secs(5)));
            }
        }
    }
}

/// 本机 mDNS 浏览线程主体：解析到的对端统一经 record_peer 入库。
pub fn browse_loop(db: SharedDb, self_id: String) {
    let mut events: Option<Receiver<ServiceEvent>> = None;
    loop {
        if !crate::manager::discovery_active() {
            // 停止浏览：Android 端离开同步界面后不该继续收组播
            if events.take().is_some() {
                if let Some(d) = daemon() {
                    let _ = d.stop_browse(SERVICE_TYPE);
                }
            }
            thread::sleep(Duration::from_millis(500));
            continue;
        }

        let Some(d) = daemon() else {
            thread::sleep(Duration::from_secs(5));
            continue;
        };

        if events.is_none() {
            match d.browse(SERVICE_TYPE) {
                Ok(rx) => events = Some(rx),
                Err(e) => {
                    dbglog::warn(format!(
                        "[discovery] mDNS 浏览启动失败（{e}），只用 UDP 广播发现"
                    ));
                    thread::sleep(Duration::from_secs(30));
                    continue;
                }
            }
        }

        let Some(rx) = events.as_ref() else { continue };
        match rx.recv_timeout(BROWSE_TICK) {
            Ok(ServiceEvent::ServiceResolved(rs)) => {
                if let Some(dev) = device_from_service(&rs, &self_id) {
                    let note = format!(
                        "mDNS 解析到 {} ({}:{} id:{})",
                        dev.name, dev.ip, dev.port, dev.id
                    );
                    discovery::record_peer(&db, dev, PeerSource::Mdns, note);
                }
            }
            Ok(ServiceEvent::ServiceRemoved(_ty, name)) => {
                // 只记调试日志：在线状态由 record_peer 的 last_seen 超时与 HTTP 探活负责
                dbglog::info(format!("[discovery] mDNS 服务下线：{name}"));
            }
            Ok(_) => {}
            Err(RecvTimeoutError::Timeout) => {}
            Err(RecvTimeoutError::Disconnected) => {
                events = None;
                thread::sleep(Duration::from_millis(500));
            }
        }
    }
}

/// 注册本机服务，返回服务全名（撤注册时要用）。
fn register_service(alias: &str, ip: &str, info: &SelfInfo) -> Result<String, String> {
    let d = daemon().ok_or("mDNS daemon 未就绪")?;
    let dev = &info.device;
    let props: [(&str, &str); 3] = [
        (TXT_VERSION, PROTO_VERSION),
        (TXT_DEVICE_ID, &dev.id),
        (TXT_DEVICE_KIND, dev.device_kind.as_str()),
    ];
    // host_name 同样只用哈希别名：真实主机名一旦进了 A/SRV 记录就等于广播给整条链路。
    // 结尾的点不能省：mdns-sd 要求 host_name 以 ".local." 结尾，否则注册直接报错。
    let host_name = format!("{alias}.local.");
    let mut si = ServiceInfo::new(SERVICE_TYPE, alias, &host_name, ip, dev.port, &props[..])
        .map_err(|e| e.to_string())?;
    // 宣告只走本机局域网这块网卡（与 UDP 广播绑定源地址的理由相同）
    let intf = ip
        .parse::<Ipv4Addr>()
        .map_err(|e| format!("本机 IP 无效: {e}"))?;
    si.set_interfaces(vec![IfKind::Addr(IpAddr::V4(intf))]);
    // 实例名是设备 id 的哈希，撞名概率可忽略：跳过 probe，少一轮组播往来
    si.set_requires_probe(false);
    let fullname = si.get_fullname().to_string();
    d.register(si).map_err(|e| e.to_string())?;
    Ok(fullname)
}

/// 撤销 mDNS 服务（向链路发 goodbye，对端不必等 TTL 才摘掉）。返回是否真的撤了一个。
fn unregister(fullname: &mut Option<String>) -> bool {
    let Some(name) = fullname.take() else {
        return false;
    };
    if let Some(d) = daemon() {
        if let Ok(rx) = d.unregister(&name) {
            // 等 daemon 处理完：不等住，紧随其后的重注册会和这个尚未撤销的服务撞名
            let _ = rx.recv_timeout(UNREGISTER_WAIT);
        }
    }
    dbglog::info(format!("[discovery] 已撤销 mDNS 服务 {name}"));
    true
}

/// 把 daemon 的异步事件（多为套接字绑定失败）导出来记日志，不留队列堆积。
fn drain_monitor(monitor: &mut Option<Receiver<DaemonEvent>>) {
    let d = match daemon() {
        Some(d) => d,
        None => return,
    };
    if monitor.is_none() {
        *monitor = d.monitor().ok();
    }
    let Some(rx) = monitor.as_ref() else { return };
    while let Ok(ev) = rx.try_recv() {
        match ev {
            DaemonEvent::Error(e) => dbglog::warn(format!("[discovery] mDNS 运行错误：{e}")),
            DaemonEvent::NameChange(chg) => {
                // 撞名被改名不影响识别：对端是按 TXT 里的设备 id 归并的，不是按实例名
                dbglog::warn(format!("[discovery] mDNS 服务名冲突，已改用 {}", chg.new_name))
            }
            _ => {}
        }
    }
}

/// 把一条已解析的 mDNS 服务转成待入库的 Device；不是我们的服务则返回 None。
pub fn device_from_service(rs: &ResolvedService, self_id: &str) -> Option<Device> {
    // 版本不符 / 没有 id 的一律不认领：同服务类型的注册可能来自别的实现
    if rs.get_property_val_str(TXT_VERSION)? != PROTO_VERSION {
        return None;
    }
    let id = rs.get_property_val_str(TXT_DEVICE_ID)?;
    if id.is_empty() || id == self_id {
        return None;
    }
    let ip = pick_ipv4(&rs.get_addresses_v4())?.to_string();
    let kind = crate::storage::parse_device_kind(rs.get_property_val_str(TXT_DEVICE_KIND).unwrap_or(""));
    Some(Device {
        id: id.to_string(),
        // 与 UDP 广播一致：未配对前对外只有哈希别名，真名要等配对后的 HTTP 交换
        name: crate::crypto::hashed_alias(id),
        device_kind: kind,
        ip,
        port: rs.get_port(),
        paired_at: Utc::now(),
        last_sync_at: None,
        last_seen_at: Some(Utc::now()),
        is_online: true,
        is_self: false,
        paired: false,
        alias: None,
        device_secret: None,
        machine_uid: None,
    })
}

/// 从解析结果里挑一个可用来同步的 IPv4。
///
/// 多地址时取数值最小者：HashSet 的迭代顺序不稳定，不固定下来的话同一台设备
/// 每轮可能换一个地址，同步日志里看不出到底连的是哪块网卡。
pub fn pick_ipv4(addrs: &HashSet<Ipv4Addr>) -> Option<Ipv4Addr> {
    addrs
        .iter()
        .filter(|a| !a.is_loopback() && !a.is_unspecified() && !a.is_link_local())
        .copied()
        .min()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 造一条「已解析」的服务记录。
    ///
    /// 走 ServiceInfo → as_resolved_service，而不是手搓 ResolvedService：
    /// TXT 的线上编码由 crate 负责，手搓字段等于测了一个真收不到的形状。
    fn resolved(ip: &str, port: u16, props: &[(&str, &str)]) -> ResolvedService {
        ServiceInfo::new(SERVICE_TYPE, "aabbccdd", "aabbccdd.local.", ip, port, props)
            .expect("构造 ServiceInfo")
            .as_resolved_service()
    }

    fn ours(id: &'static str) -> Vec<(&'static str, &'static str)> {
        vec![
            (TXT_VERSION, PROTO_VERSION),
            (TXT_DEVICE_ID, id),
            (TXT_DEVICE_KIND, "windows"),
        ]
    }

    fn addrs(list: &[&str]) -> HashSet<Ipv4Addr> {
        list.iter().map(|s| s.parse().unwrap()).collect()
    }

    #[test]
    fn resolved_service_becomes_device() {
        let rs = resolved("192.168.5.34", 5600, &ours("device-B"));
        let d = device_from_service(&rs, "device-A").expect("应认领同服务类型的对端");
        assert_eq!(d.id, "device-B");
        // 地址取 A 记录、端口取 SRV，不再依赖广播里的自报字段
        assert_eq!(d.ip, "192.168.5.34");
        assert_eq!(d.port, 5600);
        assert_eq!(
            d.name,
            crate::crypto::hashed_alias("device-B"),
            "未配对前对外只有哈希别名，不能泄漏主机名"
        );
        assert_eq!(d.device_kind, crate::models::DeviceKind::Windows);
        assert!(!d.is_self && !d.paired);
    }

    #[test]
    fn foreign_or_self_services_are_not_claimed() {
        let ip = "192.168.5.34";
        // 版本不符：同服务类型可能有别的实现注册
        let mut v2 = ours("device-B");
        v2[0] = (TXT_VERSION, "999");
        assert!(device_from_service(&resolved(ip, 5600, &v2), "device-A").is_none());
        // 没有 id：无法映射回 devices 表主键
        assert!(device_from_service(
            &resolved(ip, 5600, &[(TXT_VERSION, PROTO_VERSION)]),
            "device-A"
        )
        .is_none());
        // 自己：daemon 会把自己宣告的服务也回送给浏览线程
        assert!(device_from_service(&resolved(ip, 5600, &ours("device-A")), "device-A").is_none());
        // 只有 IPv6 / 回环地址：没有可用同步端点
        assert!(device_from_service(&resolved("127.0.0.1", 5600, &ours("device-B")), "device-A")
            .is_none());
    }

    #[test]
    fn pick_ipv4_skips_unusable_and_is_deterministic() {
        assert_eq!(pick_ipv4(&addrs(&[])), None);
        assert_eq!(pick_ipv4(&addrs(&["127.0.0.1"])), None);
        assert_eq!(pick_ipv4(&addrs(&["0.0.0.0"])), None);
        assert_eq!(
            pick_ipv4(&addrs(&["169.254.1.2"])),
            None,
            "链路本地地址过不了 AP，写进库就是死地址"
        );
        // 多网卡：固定取数值最小者，否则同一台设备每轮换一个地址
        assert_eq!(
            pick_ipv4(&addrs(&["192.168.5.34", "10.8.0.3", "172.16.0.9"])),
            Some("10.8.0.3".parse().unwrap())
        );
        assert_eq!(
            pick_ipv4(&addrs(&["192.168.5.34", "10.8.0.3"])),
            pick_ipv4(&addrs(&["10.8.0.3", "192.168.5.34"]))
        );
    }
}
