//! mDNS（首选发现路径）端到端：起两个真实进程，各自跑一套完整发现逻辑，互相发现。
//!
//! 为什么不用同进程模拟两台设备：mdns-sd 的注册与浏览共用一个 daemon 套接字，
//! 同进程里自己发出的应答会被自己按「不是外部网络来的包」处理掉，实测只能单向解析——
//! 那是测试台架的假象，不是实现的性质。跨进程（等价于两台设备）才是真实的网络关系。

use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use aw_sync_rust::models::{SyncDirection, SyncEventType, SyncProtocol};
use aw_sync_rust::storage::{LogFilter, SyncDb};
use tempfile::TempDir;

const A_ID: &str = "mdns-device-A";
const B_ID: &str = "mdns-device-B";
/// 一个节点跑够这么久：注册 + 对端浏览解析 + 落库留有余量
const NODE_SECONDS: u64 = 40;
const WAIT_TIMEOUT: Duration = Duration::from_secs(35);

fn node_binary() -> PathBuf {
    // 测试可执行文件在 <target>/<profile>/deps/，example 在同级的 examples/ 下
    let exe = std::env::current_exe().expect("无法定位测试可执行文件");
    let deps = exe.parent().expect("测试可执行文件无父目录");
    let target = deps.parent().expect("测试可执行文件无上级目录");
    let name = if cfg!(windows) { "mdns_node.exe" } else { "mdns_node" };
    target.join("examples").join(name)
}

/// 某台设备的库里是否已出现「对端由 mDNS 报上来」的发现日志
fn mdns_discovered(dir: &Path, peer_id: &str) -> bool {
    let Ok(db) = SyncDb::open(dir) else {
        return false;
    };
    db.get_logs(&LogFilter {
        direction: Some(SyncDirection::In),
        protocol: Some(SyncProtocol::Mdns),
        event_type: Some(SyncEventType::Discovery),
        limit: 50,
        offset: 0,
    })
    .unwrap_or_default()
    .iter()
    .any(|l| l.message.as_deref().unwrap_or("").contains(peer_id))
}

#[test]
#[ignore = "真组播测试：会往局域网注册 _aw-sync._tcp，跑之前请先停掉真实服务端（cargo test -- --ignored 手动执行）"]
fn mdns_peers_discover_each_other() {
    // 本机没有可用局域网 IPv4（未联网 / 纯容器网络）时不会有任何宣告，属预期而非缺陷
    let probe_dir = TempDir::new().unwrap();
    let probe = aw_sync_rust::SyncManager::new(probe_dir.path(), "mdns-probe".into()).unwrap();
    let ip = probe.lock().unwrap().self_device_info().ip;
    if ip.is_empty() || ip == "127.0.0.1" {
        eprintln!("跳过：本机无可用局域网 IPv4（当前 {ip:?}），mDNS 无法宣告");
        return;
    }

    let bin = node_binary();
    assert!(
        bin.exists(),
        "缺少诊断节点可执行文件 {}（先跑 cargo build -p aw-sync-rust --example mdns_node）",
        bin.display()
    );

    let dir_a = TempDir::new().unwrap();
    let dir_b = TempDir::new().unwrap();
    let mut kids: Vec<Child> = Vec::new();
    let spawn = |id: &str, dir: &TempDir| {
        Command::new(&bin)
            .arg(dir.path())
            .arg(id)
            .arg(NODE_SECONDS.to_string())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("mDNS 诊断节点启动失败")
    };
    kids.push(spawn(A_ID, &dir_a));
    kids.push(spawn(B_ID, &dir_b));

    let started = Instant::now();
    let (mut ra, mut rb) = (false, false);
    while started.elapsed() < WAIT_TIMEOUT {
        ra = mdns_discovered(dir_a.path(), B_ID);
        rb = mdns_discovered(dir_b.path(), A_ID);
        if ra && rb {
            break;
        }
        std::thread::sleep(Duration::from_millis(500));
    }
    for k in kids.iter_mut() {
        let _ = k.kill();
    }
    assert!(
        ra && rb,
        "mDNS 双向发现超时：A 看到 B={ra}，B 看到 A={rb}（本机 IP {ip}）"
    );

    // 可达端口取自 SRV 记录（标准承载位）。今天它仍等于 5600：SyncManager::new 会把
    // listen_port 修正为实际服务端口，两条路径共用同一个值。
    for (dir, peer) in [(dir_a.path(), B_ID), (dir_b.path(), A_ID)] {
        let db = SyncDb::open(dir).unwrap();
        let dev = db
            .get_devices()
            .unwrap()
            .into_iter()
            .find(|d| d.id == peer)
            .expect("对端设备应已进信任列表");
        assert_eq!(dev.ip, ip, "对端地址应取 A 记录解析结果");
        assert_eq!(dev.port, aw_sync_rust::DEFAULT_SYNC_PORT);
        assert!(!dev.paired, "仅被发现不等于已配对");
        // 未配对前对外只有设备 id 的哈希别名：主机名不得出现在宣告里
        assert_eq!(dev.name, aw_sync_rust::crypto::hashed_alias(peer));
        assert_ne!(dev.name, peer, "未配对前对外只有哈希别名，不能是真实设备 id/主机名");
    }
}
