//! mDNS 发现的手动验证/诊断入口：一个「局域网节点」进程。
//!
//! 用法（两个进程互相发现，等价于两台设备的网络关系）：
//! ```sh
//! cargo run -p aw-sync-rust --example mdns_node -- /tmp/aw-a device-A 40
//! cargo run -p aw-sync-rust --example mdns_node -- /tmp/aw-b device-B 40
//! ```
//! 第二个进程启动后约 1~3 秒，两边都应打印出对方的 id 与地址，并在
//! `mdns_logs` 里带上 mDNS 入向发现记录（没有该日志却看到 peers，说明是 UDP 备选路径报的）。
//!
//! 本机没有可用局域网 IPv4 时（未联网 / 纯容器网络）不会有任何宣告，
//! 这是预期行为，不是缺陷。

use std::path::Path;
use std::time::{Duration, Instant};

use aw_sync_rust::models::SyncDirection;
use aw_sync_rust::storage::{LogFilter, SyncDb};

fn main() {
    let mut args = std::env::args().skip(1);
    let dir = args.next().expect("需要一个数据目录参数");
    let id = args.next().expect("需要设备 id 参数");
    let secs: u64 = args.next().map(|s| s.parse().unwrap_or(30)).unwrap_or(30);
    let dir = Path::new(&dir);
    std::fs::create_dir_all(dir).expect("数据目录创建失败");

    let mgr = aw_sync_rust::SyncManager::new(dir, id.clone()).expect("SyncManager 初始化失败");
    {
        let g = mgr.lock().unwrap();
        println!("[{id}] 本机宣告地址 = {}:{}", g.self_device_info().ip, g.self_device_info().port);
        g.start_discovery();
    }

    let started = Instant::now();
    let mut last = String::new();
    let mut seq = 0u64;
    while started.elapsed() < Duration::from_secs(secs) {
        // 先把内部发现日志倒出来：daemon 是否起来、注册/浏览是否成功都在这里
        for e in aw_sync_rust::dbglog::snapshot_after(seq) {
            seq = e.seq;
            println!("[{id}] {} {}", e.level, e.msg);
        }

        let g = mgr.lock().unwrap();
        let peers: Vec<String> = g
            .list_devices()
            .unwrap_or_default()
            .iter()
            .filter(|d| d.id != id)
            .map(|d| format!("{}@{}:{} online={}", d.id, d.ip, d.port, d.is_online))
            .collect();
        drop(g);
        let mdns_logs: Vec<String> = SyncDb::open(dir)
            .map(|db| {
                db.get_logs(&LogFilter {
                    direction: Some(SyncDirection::In),
                    protocol: Some(aw_sync_rust::models::SyncProtocol::Mdns),
                    event_type: Some(aw_sync_rust::models::SyncEventType::Discovery),
                    limit: 5,
                    offset: 0,
                })
                .unwrap_or_default()
                .into_iter()
                .filter_map(|l| l.message)
                .collect()
            })
            .unwrap_or_default();
        let line = format!("peers=[{}] mdns_logs={:?}", peers.join(" | "), mdns_logs);
        if line != last {
            println!("[{id}] {line}");
            last = line;
        }
        std::thread::sleep(Duration::from_millis(1000));
    }
    println!("[{id}] 结束");
}
