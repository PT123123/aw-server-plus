//! 端到端集成测试：在本机起两个真实 HTTP 服务（模拟两台机器），
//! 完整走通「配置 → 创建配对码 → 加入配对 → 双向登记 → 数据双向同步 → 日志校验 → 删除设备」。

use aw_sync_rust::endpoints::mount_rocket;
use aw_sync_rust::models::{Device, SyncSnapshot};
use aw_sync_rust::serialize::export_inbox;
use rocket::data::ToByteUnit;
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};
use tempfile::TempDir;

/// 发现广播端口：验证 UDP 自动发现的用例共用它（两台机器必须同端口才能互见）。
///
/// 这里**故意不用** production 默认端口 `discovery::DEFAULT_UDP_PORT`（46000）：
/// 台架与真实服务端同机运行时，同端口是双向污染的——测试发的广播会被用户真实
/// 服务端的监听线程收进 sync.db（变成信任列表里的幽灵设备），真实服务端周期
/// 广播的通告也会被测试进程写进临时库。两台“虚拟机”只要求**彼此同端口**，
/// 用哪个端口不影响被测性质，因此选一个 production 不会用到的私有端口。
const UDP_SHARED: u16 = 47611;

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

/// 取一个空闲 UDP 端口：测试里每台机器各占一个，避免互相听到广播。
fn free_udp_port() -> u16 {
    std::net::UdpSocket::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn wait_port(port: u16) {
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        if std::net::TcpStream::connect(("127.0.0.1", port)).is_ok() {
            return;
        }
        assert!(Instant::now() < deadline, "server on {} not ready", port);
        std::thread::sleep(Duration::from_millis(100));
    }
}

/// 起一台「虚拟机」：独立数据目录 + 独立 HTTP/UDP 端口。
///
/// `udp_port` 决定发现报文收发端口：同一测试里的两台机器要用**不同**端口，
/// 否则一台的广播会把另一台记录里的 ip:port 改写掉（广播通告的是网卡地址，
/// 不是 127.0.0.1，测试用的 loopback 服务反而连不上）。两机器真要验证广播时传同一个值。
fn spawn_server(dir: PathBuf, device_id: &str, port: u16, udp_port: u16) -> std::thread::JoinHandle<()> {
    let mgr = aw_sync_rust::SyncManager::new(&dir, device_id.to_string()).unwrap();
    // 广播通告的端口取自配置：不写成实际监听端口，对端记录会被改到一个根本没在监听的地址上。
    {
        let g = mgr.lock().unwrap();
        let mut cfg = g.get_config();
        cfg.listen_port = port;
        cfg.udp_port = udp_port;
        // 台架只验证 UDP 通路。mDNS 的注册/浏览走系统 daemon 套接字，注册成功的
        // `_aw-sync._tcp` 服务会被同一局域网内真实运行中的服务端浏览到并落库，
        // 同理是污染；mDNS 本身由 tests/mdns_discovery.rs 单独验证。
        cfg.discovery_method = "udp_only".to_string();
        g.set_config(&cfg).unwrap();
    }
    std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async move {
            // 与 aw-server 的 AWConfig::to_rocket_config 保持一致：/push 收原始字节，
            // rocket 默认 bytes 上限只有 8KiB，不放开大快照会直接 413。
            let limits = rocket::data::Limits::default()
                .limit("json", 1000u64.megabytes())
                .limit("bytes", 1000u64.megabytes());
            let figment = rocket::Config::figment()
                .merge(("address", "127.0.0.1"))
                .merge(("port", port))
                .merge(("limits", limits));
            let rocket = mount_rocket(rocket::custom(figment), mgr);
            let ignited = rocket.ignite().await.expect("rocket ignite failed");
            let _ = ignited.launch().await;
        });
    })
}

fn http() -> reqwest::blocking::Client {
    reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .unwrap()
}

/// 造一个 inbox.db 夹具。列必须与 aw-inbox-rust 的 migrate() 同形：
/// 导出按 `id,uuid,content,tags,created_at,updated_at,version,device_id,deleted,synced_at`
/// 取列，缺列会直接把 export_inbox 打成 "no such column: uuid"。
fn make_inbox_db(dir: &Path, content: &str) {
    let conn = rusqlite::Connection::open(dir.join("inbox.db")).unwrap();
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS notes (
            id INTEGER PRIMARY KEY AUTOINCREMENT, content TEXT NOT NULL,
            tags TEXT DEFAULT '[]', created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
            version INTEGER NOT NULL DEFAULT 1, device_id TEXT,
            deleted INTEGER NOT NULL DEFAULT 0, synced_at TEXT, uuid TEXT);
         CREATE TABLE IF NOT EXISTS note_relations (
            id INTEGER PRIMARY KEY AUTOINCREMENT, source_note_id INTEGER NOT NULL,
            target_note_id INTEGER NOT NULL, relation_type TEXT NOT NULL, created_at TEXT NOT NULL);",
    )
    .unwrap();
    // 合并按 uuid 认逻辑键，夹具必须带 uuid，否则两条对端笔记会被当成同一条
    conn.execute(
        "INSERT INTO notes (content,tags,created_at,updated_at,version,device_id,deleted,uuid)
         VALUES (?1,'[]','2026-08-25T00:00:00Z','2026-08-25T00:00:00Z',1,'fixture',0,?2)",
        rusqlite::params![content, uuid::Uuid::new_v4().to_string()],
    )
    .unwrap();
}

fn inbox_contains(dir: &Path, needle: &str) -> bool {
    match rusqlite::Connection::open(dir.join("inbox.db")) {
        Ok(conn) => {
            let n: i64 = conn
                .query_row(
                    "SELECT COUNT(*) FROM notes WHERE content LIKE ?1",
                    rusqlite::params![format!("%{}%", needle)],
                    |r| r.get(0),
                )
                .unwrap_or(0);
            n > 0
        }
        Err(_) => false,
    }
}

#[test]
fn two_machines_pair_and_sync_end_to_end() {
    let dir_a = TempDir::new().unwrap();
    let dir_b = TempDir::new().unwrap();

    let port_a = free_port();
    let port_b = free_port();
    let ha = spawn_server(dir_a.path().to_path_buf(), "machine-A", port_a, UDP_SHARED);
    let hb = spawn_server(dir_b.path().to_path_buf(), "machine-B", port_b, UDP_SHARED);
    wait_port(port_a);
    wait_port(port_b);

    let c = http();
    let ba = format!("http://127.0.0.1:{}", port_a);
    let bb = format!("http://127.0.0.1:{}", port_b);

    // 0) 两台机器开启同步，并把同步端口配置为各自实际监听端口
    //    （同一进程内模拟两台“虚拟机”，需在第二台启动前重置一次性发现标志）
    fn put_config(c: &reqwest::blocking::Client, base: &str, port: u16) {
        let mut cfg: serde_json::Value = c
            .get(format!("{}/api/0/sync/config", base))
            .send()
            .unwrap()
            .json()
            .unwrap();
        cfg["enabled"] = serde_json::json!(true);
        cfg["listen_port"] = serde_json::json!(port);
        let resp = c.put(format!("{}/api/0/sync/config", base)).json(&cfg).send().unwrap();
        assert!(resp.status().is_success());
        let after: serde_json::Value = resp.json().unwrap();
        assert_eq!(after["enabled"], true);
        assert_eq!(after["listen_port"], port);
    }
    put_config(&c, &ba, port_a);
    aw_sync_rust::manager::reset_discovery_started_for_testing();
    put_config(&c, &bb, port_b);

    // 1) A 创建配对码（6 位数字）
    let pc: serde_json::Value = c
        .post(format!("{}/api/0/sync/paircode", ba))
        .send()
        .unwrap()
        .json()
        .unwrap();
    let code = pc["code"].as_str().unwrap().to_string();
    assert_eq!(code.len(), 6);
    assert!(code.chars().all(|ch| ch.is_ascii_digit()));

    // 2) B 取本机信息 → 改写为可达地址 → 到 A 处加入配对
    let mut dev_b: serde_json::Value = c
        .get(format!("{}/api/0/sync/info", bb))
        .send()
        .unwrap()
        .json()
        .unwrap();
    assert_eq!(dev_b["id"], "machine-B");
    dev_b["ip"] = serde_json::json!("127.0.0.1");
    dev_b["port"] = serde_json::json!(port_b);
    dev_b["is_self"] = serde_json::json!(false);

    let join_resp: serde_json::Value = c
        .post(format!("{}/api/0/sync/join", ba))
        .json(&serde_json::json!({ "code": code, "device": dev_b }))
        .send()
        .unwrap()
        .json()
        .unwrap();
    assert_eq!(join_resp["device"]["id"], "machine-B");
    assert_eq!(join_resp["peer"]["id"], "machine-A");

    // 3) B 把 A 登记进自己的信任列表（双向互见）
    let mut peer_a = join_resp["peer"].clone();
    peer_a["is_self"] = serde_json::json!(false);
    let saved: serde_json::Value = c
        .post(format!("{}/api/0/sync/devices", bb))
        .json(&peer_a)
        .send()
        .unwrap()
        .json()
        .unwrap();
    assert_eq!(saved["saved"], true);

    // 4) 双方设备列表互含对方
    let devs_a: Vec<serde_json::Value> = c
        .get(format!("{}/api/0/sync/devices", ba))
        .send()
        .unwrap()
        .json()
        .unwrap();
    assert!(devs_a.iter().any(|d| d["id"] == "machine-B" && d["is_self"] == false));
    let devs_b: Vec<serde_json::Value> = c
        .get(format!("{}/api/0/sync/devices", bb))
        .send()
        .unwrap()
        .json()
        .unwrap();
    assert!(devs_b.iter().any(|d| d["id"] == "machine-A"));

    // 5) 数据同步 B→A：加入方客户端要像真实客户端那样保存 A 回传的密钥，
    //    之后带钥封装快照推送，A 侧解开并合并。
    let secret_ab = join_resp["device_secret"]
        .as_str()
        .expect("/join 必须回传本次协商好的 device_secret")
        .to_string();
    assert!(
        aw_sync_rust::crypto::secret_ok(&secret_ab),
        "回传密钥必须是 32 字节 hex：{}",
        secret_ab
    );
    make_inbox_db(dir_b.path(), "note-from-machine-B");
    let inbox_json_b = export_inbox(dir_b.path().join("inbox.db").as_path()).unwrap();
    let snap_b = SyncSnapshot {
        source_device: Some(serde_json::from_value::<Device>(dev_b.clone()).unwrap()),
        activity: None,
        inbox: Some(inbox_json_b),
        todo: None,
    };
    let mut a_for_b = join_resp["peer"].clone();
    a_for_b["ip"] = serde_json::json!("127.0.0.1");
    let target_a: Device = serde_json::from_value(a_for_b).unwrap();
    let applied_b = aw_sync_rust::transport::push_snapshot(&target_a, &snap_b, Some(&secret_ab))
        .expect("信封推送应被对端解开并应用");
    assert!(applied_b >= 1);
    assert!(inbox_contains(dir_a.path(), "note-from-machine-B"), "A 应收到 B 的笔记");

    // 5.5) 降级防护：A 已经与 machine-B 协商了密钥，此时同一条明文 /push 必须被拒
    let downgrade = c
        .post(format!("{}/api/0/sync/push", ba))
        .json(&snap_b)
        .send()
        .unwrap();
    assert!(
        !downgrade.status().is_success(),
        "已协商密钥的对端仍收明文 = 中间人可随意降级"
    );
    // 而且 /devices 响应里绝不能出现密钥本身（同网段任何人可读）
    let devs_raw: String = c
        .get(format!("{}/api/0/sync/devices", ba))
        .send()
        .unwrap()
        .text()
        .unwrap();
    assert!(!devs_raw.contains(&secret_ab), "/devices 泄漏了 device_secret");
    assert!(!devs_raw.contains("device_secret"), "/devices 不应序列化 device_secret 字段");
    // 装机指纹同理：/devices 对同网段任何人可读，机器标识不能被动读走
    assert!(
        !devs_raw.contains("machine_uid"),
        "/devices 不应序列化 machine_uid 字段：{devs_raw}"
    );

    // 6) 数据同步 A→B：走真实 HTTP 客户端 transport::push_snapshot。
    //    B 这一侧只是被 A 登记了（A 的库里存了密钥），B 并没有通过接口写入过密钥
    //    ——密钥不经接口注入是设计约束——所以这一方向仍是明文回退路径，必须照样可用。
    make_inbox_db(dir_a.path(), "note-from-machine-A");
    let inbox_json_a = export_inbox(dir_a.path().join("inbox.db").as_path()).unwrap();
    let info_a: serde_json::Value = c
        .get(format!("{}/api/0/sync/info", ba))
        .send()
        .unwrap()
        .json()
        .unwrap();
    let snap_a = SyncSnapshot {
        source_device: Some(serde_json::from_value::<Device>(info_a).unwrap()),
        activity: None,
        inbox: Some(inbox_json_a),
        todo: None,
    };
    let target_b: Device = serde_json::from_value(dev_b.clone()).unwrap();
    let applied = aw_sync_rust::transport::push_snapshot(&target_b, &snap_a, None).unwrap();
    assert!(applied >= 1);
    assert!(inbox_contains(dir_b.path(), "note-from-machine-A"), "B 应收到 A 的笔记");

    // 7) 双方都留下配对与同步日志
    let log_a: serde_json::Value = c.get(format!("{}/api/0/sync/log", ba)).send().unwrap().json().unwrap();
    let logs_a = log_a["logs"].as_array().unwrap();
    assert!(logs_a.iter().any(|l| l["event_type"] == "pairing"));
    assert!(logs_a.iter().any(|l| l["event_type"] == "sync" && l["direction"] == "in"));
    let log_b: serde_json::Value = c.get(format!("{}/api/0/sync/log", bb)).send().unwrap().json().unwrap();
    let logs_b = log_b["logs"].as_array().unwrap();
    assert!(logs_b.iter().any(|l| l["event_type"] == "sync" && l["direction"] == "in"));

    // 7.5) 调试日志通道：配对与同步动作应已在环形缓冲中留下痕迹（供 F12 拉取）
    let dbg: Vec<serde_json::Value> = c
        .get(format!("{}/api/0/sync/debuglog", ba))
        .query(&[("after", 0u64)])
        .send()
        .unwrap()
        .json()
        .unwrap();
    assert!(dbg.iter().any(|e| e["msg"].as_str().unwrap_or("").contains("/join")));
    assert!(dbg.iter().any(|e| e["msg"].as_str().unwrap_or("").contains("推送完成") || e["msg"].as_str().unwrap_or("").contains("收到来自")));

    // 8) 配对码一次性：复用旧码再次加入必须失败
    let reuse = c
        .post(format!("{}/api/0/sync/join", ba))
        .json(&serde_json::json!({ "code": code, "device": dev_b }))
        .send()
        .unwrap();
    assert_eq!(reuse.status(), reqwest::StatusCode::BAD_REQUEST);

    // 8.5) 发现状态可见 + 广播报文应写入同步日志
    let st: serde_json::Value = c
        .get(format!("{}/api/0/sync/status", ba))
        .send()
        .unwrap()
        .json()
        .unwrap();
    assert_eq!(st["enabled"], true);
    assert_eq!(st["discovery_running"], true);
    assert_eq!(st["udp_port"], UDP_SHARED);

    // 等待 A 的监听器收到 B 的 UDP 广播（去抖后写入同步日志），最多 ~15s
    let mut udp_seen = false;
    for _ in 0..30 {
        let lg: serde_json::Value = c
            .get(format!("{}/api/0/sync/log", ba))
            .query(&[("protocol", "udp_broadcast")])
            .send()
            .unwrap()
            .json()
            .unwrap();
        if lg["logs"].as_array().map(|a| !a.is_empty()).unwrap_or(false) {
            udp_seen = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(500));
    }
    assert!(udp_seen, "同步日志中应出现 UDP 广播报文记录");

    // 8.6) 关键回归：UDP 广播自动发现后，A 的信任列表里必须出现 B，且 is_self=false。
    //   曾经误用 upsert_device 会把对端广播自带的 is_self=true 原样入库，
    //   导致前端 `!is_self && !paired` 过滤把 B 从「已发现未配对」列表藏掉。
    //   （本环节 B 已在前面靠配对码完成配对，故 paired=true 属正常；关键是 is_self 必须为 false）
    let mut b_not_self = false;
    for _ in 0..20 {
        let devs: Vec<serde_json::Value> = c
            .get(format!("{}/api/0/sync/devices", ba))
            .send()
            .unwrap()
            .json()
            .unwrap();
        if let Some(b) = devs.iter().find(|d| d["id"] == "machine-B") {
            if b["is_self"] == false {
                b_not_self = true;
                break;
            }
        }
        std::thread::sleep(Duration::from_millis(500));
    }
    assert!(
        b_not_self,
        "A 的信任列表中必须显示 B 且 is_self=false（不得将对端广播自带的 is_self=true 入库）"
    );

    // 9) 删除设备后列表不再包含
    let del: serde_json::Value = c
        .delete(format!("{}/api/0/sync/devices/machine-B", ba))
        .send()
        .unwrap()
        .json()
        .unwrap();
    assert_eq!(del["deleted"], true);
    let devs_a2: Vec<serde_json::Value> = c
        .get(format!("{}/api/0/sync/devices", ba))
        .send()
        .unwrap()
        .json()
        .unwrap();
    assert!(!devs_a2.iter().any(|d| d["id"] == "machine-B"));

    // 收尾（server 线程随进程退出；显式 join 以便失败时暴露 panic）
    let _ = (ha.is_finished(), hb.is_finished());
}

/// 回归：模拟 webui 前端「加入配对」时手工拼装的设备载荷。
/// - paired_at 提供合法 ISO 时间戳 → 配对成功（修复过 null 导致 422 的问题）
/// - paired_at 为 null → 服务端必须拒绝（422），且不得消费配对码
#[test]
fn join_accepts_frontend_payload_and_rejects_null_paired_at() {
    let dir_a = TempDir::new().unwrap();
    let port = free_port();
    let h = spawn_server(dir_a.path().to_path_buf(), "machine-A", port, free_udp_port());
    wait_port(port);

    let c = http();
    let ba = format!("http://127.0.0.1:{}", port);

    // A 生成有效配对码
    let pc: serde_json::Value = c
        .post(format!("{}/api/0/sync/paircode", ba))
        .send()
        .unwrap()
        .json()
        .unwrap();
    let code = pc["code"].as_str().unwrap().to_string();

    // 1) 负例：paired_at 为 null（旧版前端缺陷）→ 必须 422 且不消费配对码
    let bad = serde_json::json!({
        "code": code,
        "device": {
            "id": "phone-b", "name": "Phone B", "device_kind": "android",
            "ip": "192.168.1.23", "port": 56001,
            "paired_at": serde_json::Value::Null,
            "last_sync_at": serde_json::Value::Null,
            "is_online": true, "is_self": false
        }
    });
    let resp_bad = c.post(format!("{}/api/0/sync/join", ba)).json(&bad).send().unwrap();
    assert_eq!(resp_bad.status(), reqwest::StatusCode::UNPROCESSABLE_ENTITY);

    // 2) 正例：前端同款载荷，paired_at 为当前时间 ISO 字符串 → 成功
    let good = serde_json::json!({
        "code": code,
        "device": {
            "id": "phone-b", "name": "Phone B", "device_kind": "android",
            "ip": "192.168.1.23", "port": 56001,
            "paired_at": chrono::Utc::now().to_rfc3339(),
            "last_sync_at": serde_json::Value::Null,
            "is_online": true, "is_self": false
        }
    });
    let resp_ok = c.post(format!("{}/api/0/sync/join", ba)).json(&good).send().unwrap();
    assert!(resp_ok.status().is_success());
    let body: serde_json::Value = resp_ok.json().unwrap();
    assert_eq!(body["device"]["id"], "phone-b");
    assert_eq!(body["peer"]["id"], "machine-A");

    // 设备已登记
    let devs: Vec<serde_json::Value> = c
        .get(format!("{}/api/0/sync/devices", ba))
        .send()
        .unwrap()
        .json()
        .unwrap();
    assert!(devs.iter().any(|d| d["id"] == "phone-b"));

    let _ = h.is_finished();
}

/// 回归：配对码只在创建方设备的 sync.db 中有效。
/// 若把请求发到另一台未生成该码的设备（旧版前端误发给本机），应得到 400 而非 500。
#[test]
fn join_with_foreign_code_returns_400_not_500() {
    // 两台互不相干的设备 A / B，各自独立的 sync.db
    let dir_a = TempDir::new().unwrap();
    let dir_b = TempDir::new().unwrap();

    let port_a = free_port();
    let port_b = free_port();
    let ha = spawn_server(dir_a.path().to_path_buf(), "machine-A", port_a, UDP_SHARED);
    let hb = spawn_server(dir_b.path().to_path_buf(), "machine-B", port_b, UDP_SHARED);
    wait_port(port_a);
    wait_port(port_b);

    let c = http();
    let ba = format!("http://127.0.0.1:{}", port_a);
    let bb = format!("http://127.0.0.1:{}", port_b);

    // A 创建配对码（只存在于 A 的库中）
    let pc: serde_json::Value = c
        .post(format!("{}/api/0/sync/paircode", ba))
        .send()
        .unwrap()
        .json()
        .unwrap();
    let code = pc["code"].as_str().unwrap().to_string();

    // B 的本机信息（模拟前端 payload）
    let dev_b: serde_json::Value = c
        .get(format!("{}/api/0/sync/info", bb))
        .send()
        .unwrap()
        .json()
        .unwrap();

    // 把 B 的 join 请求错误地发给 B 自己（旧版前端行为）：码在 B 处不存在 → 必须 400
    let wrong_target = c
        .post(format!("{}/api/0/sync/join", bb))
        .json(&serde_json::json!({ "code": code, "device": dev_b }))
        .send()
        .unwrap();
    assert_eq!(wrong_target.status(), reqwest::StatusCode::BAD_REQUEST);
    let body: serde_json::Value = wrong_target.json().unwrap();
    assert_eq!(body["error"], "invalid_or_expired_code");

    // 正确目标（A）则成功
    let right_target = c
        .post(format!("{}/api/0/sync/join", ba))
        .json(&serde_json::json!({
            "code": code,
            "device": {
                "id": "phone-b", "name": "Phone B", "device_kind": "android",
                "ip": "127.0.0.1", "port": port_b,
                "paired_at": chrono::Utc::now().to_rfc3339(),
                "last_sync_at": serde_json::Value::Null,
                "is_online": true, "is_self": false
            }
        }))
        .send()
        .unwrap();
    assert!(right_target.status().is_success());

    let _ = (ha.is_finished(), hb.is_finished());
}


/// 配对握手全流程：A 发起 → B 收到请求（incoming_pair_request）→ B 接受 → 双方 paired=true。
/// 对应 UI「已发现未配对的设备」上的 发起配对 / 接受配对 按钮。
#[test]
fn pair_flow_initiate_accept_confirm() {
    let dir_a = TempDir::new().unwrap();
    let dir_b = TempDir::new().unwrap();
    let port_a = free_port();
    let port_b = free_port();
    let ha = spawn_server(dir_a.path().to_path_buf(), "pair-A", port_a, free_udp_port());
    let hb = spawn_server(dir_b.path().to_path_buf(), "pair-B", port_b, free_udp_port());
    wait_port(port_a);
    wait_port(port_b);
    let c = http();
    let ba = format!("http://127.0.0.1:{}", port_a);
    let bb = format!("http://127.0.0.1:{}", port_b);

    // 1) A、B 各自登记对方（模拟广播发现后彼此出现在「已发现未配对」）
    //    A 登记 B
    let dev_b: serde_json::Value = c.get(format!("{}/api/0/sync/info", bb)).send().unwrap().json().unwrap();
    let mut b_for_a = dev_b.clone();
    b_for_a["ip"] = serde_json::json!("127.0.0.1");
    b_for_a["port"] = serde_json::json!(port_b);
    b_for_a["is_self"] = serde_json::json!(false);
    c.post(format!("{}/api/0/sync/devices", ba)).json(&b_for_a).send().unwrap();
    //    B 登记 A
    let dev_a: serde_json::Value = c.get(format!("{}/api/0/sync/info", ba)).send().unwrap().json().unwrap();
    let mut a_for_b = dev_a.clone();
    a_for_b["ip"] = serde_json::json!("127.0.0.1");
    a_for_b["port"] = serde_json::json!(port_a);
    a_for_b["is_self"] = serde_json::json!(false);
    c.post(format!("{}/api/0/sync/devices", bb)).json(&a_for_b).send().unwrap();

    // 2) A 发起配对
    let init: serde_json::Value = c
        .post(format!("{}/api/0/sync/pair/initiate", ba))
        .json(&serde_json::json!({ "device_id": "pair-B" }))
        .send()
        .unwrap()
        .json()
        .unwrap();
    assert_eq!(init["ok"], true);

    // 3) B 的设备列表中 A 应标记 incoming_pair_request=true（前端据此显示「接受配对」）
    let mut inbound_seen = false;
    for _ in 0..10 {
        let devs: Vec<serde_json::Value> = c.get(format!("{}/api/0/sync/devices", bb)).send().unwrap().json().unwrap();
        if let Some(a) = devs.iter().find(|d| d["id"] == "pair-A") {
            if a["incoming_pair_request"] == true {
                inbound_seen = true;
                break;
            }
        }
        std::thread::sleep(Duration::from_millis(300));
    }
    assert!(inbound_seen, "B 应看到来自 A 的配对请求（incoming_pair_request=true）");

    // 4) B 接受配对
    let acc: serde_json::Value = c
        .post(format!("{}/api/0/sync/pair/accept", bb))
        .json(&serde_json::json!({ "device_id": "pair-A" }))
        .send()
        .unwrap()
        .json()
        .unwrap();
    assert_eq!(acc["ok"], true);

    // 5) 双方设备彼此 paired=true（前端据此把对方移入「已配对设备」）
    let mut a_paired = false;
    let mut b_paired = false;
    for _ in 0..10 {
        let devs_a: Vec<serde_json::Value> = c.get(format!("{}/api/0/sync/devices", ba)).send().unwrap().json().unwrap();
        let devs_b: Vec<serde_json::Value> = c.get(format!("{}/api/0/sync/devices", bb)).send().unwrap().json().unwrap();
        a_paired = devs_a.iter().find(|d| d["id"] == "pair-B").map(|d| d["paired"] == true).unwrap_or(false);
        b_paired = devs_b.iter().find(|d| d["id"] == "pair-A").map(|d| d["paired"] == true).unwrap_or(false);
        if a_paired && b_paired {
            break;
        }
        std::thread::sleep(Duration::from_millis(300));
    }
    assert!(a_paired, "A 侧应将 B 标记为已配对");
    assert!(b_paired, "B 侧应将 A 标记为已配对");

    // 6) 安全码：配对两端各自算出的 4 位码必须一致（人工比对才有意义），
    //    且 /devices 只暴露指纹、不暴露密钥。
    let devs_a: Vec<serde_json::Value> = c.get(format!("{}/api/0/sync/devices", ba)).send().unwrap().json().unwrap();
    let devs_b: Vec<serde_json::Value> = c.get(format!("{}/api/0/sync/devices", bb)).send().unwrap().json().unwrap();
    let fp_a = devs_a.iter().find(|d| d["id"] == "pair-B").and_then(|d| d["fingerprint"].as_str()).unwrap_or("");
    let fp_b = devs_b.iter().find(|d| d["id"] == "pair-A").and_then(|d| d["fingerprint"].as_str()).unwrap_or("");
    assert_eq!(fp_a.len(), 4, "A 侧应看到与 B 的安全码");
    assert_eq!(fp_a, fp_b, "两端安全码必须相同（不同 = 中间人已替换密钥）");
    assert_eq!(
        devs_a.iter().find(|d| d["id"] == "pair-B").and_then(|d| d["encrypted"].as_bool()),
        Some(true)
    );

    // 6.5) 握手会用对端自报的网卡地址刷新记录（生产里是对的：换 IP 后要靠它刷新；
    //      但同一台机器上跑两个 loopback 测试服务，那个地址根本连不通），钉回 127.0.0.1。
    for (local, remote, remote_id, remote_port) in
        [(&ba, &bb, "pair-B", port_b), (&bb, &ba, "pair-A", port_a)]
    {
        let mut dev: serde_json::Value = c
            .get(format!("{remote}/api/0/sync/info"))
            .send()
            .unwrap()
            .json()
            .unwrap();
        assert_eq!(dev["id"], remote_id);
        dev["ip"] = serde_json::json!("127.0.0.1");
        dev["port"] = serde_json::json!(remote_port);
        dev["is_self"] = serde_json::json!(false);
        // /info 里的 paired 是「对端眼里的自己」，直接落库会把本机侧的配对状态冲掉
        dev["paired"] = serde_json::json!(true);
        assert!(c
            .post(format!("{local}/api/0/sync/devices"))
            .json(&dev)
            .send()
            .unwrap()
            .status()
            .is_success());
    }

    // 6.6) 握手交换过的密钥不得出现在设备列表里（同网段任何人都能读它）
    let raw_devices = c
        .get(format!("{}/api/0/sync/devices", ba))
        .send()
        .unwrap()
        .text()
        .unwrap();
    assert!(!raw_devices.contains("device_secret"), "设备列表不得带出密钥字段");

    // 7) 双向真实同步：拉 + 推都走信封，两端各自用库里那把密钥解
    make_inbox_db(dir_a.path(), "note-pair-A");
    make_inbox_db(dir_b.path(), "note-pair-B");
    let resp = c
        .post(format!("{}/api/0/sync/devices/pair-B/sync", ba))
        .send()
        .unwrap();
    let status = resp.status();
    let text = resp.text().unwrap();
    assert!(status.is_success(), "同步接口返回 {status}: {text}");
    let sync: serde_json::Value = serde_json::from_str(&text).unwrap();
    assert!(
        sync["result"]["errors"].as_array().map(|e| e.is_empty()).unwrap_or(false),
        "信封同步不应报错: {sync:?}"
    );
    assert!(inbox_contains(dir_b.path(), "note-pair-A"), "A 的笔记应经加密推送到达 B");
    assert!(inbox_contains(dir_a.path(), "note-pair-B"), "B 的笔记应经加密拉取到达 A");

    // 8) WiFi 热点回程：客户端不碰密钥，POST 本机 /push-to 代发代封
    //    （旧写法是客户端直推对端 /push 明文，配对后会被对端的降级防护拒掉）
    make_inbox_db(dir_a.path(), "note-hotspot-A");
    let push: serde_json::Value = c
        .post(format!("{}/api/0/sync/push-to", ba))
        .json(&serde_json::json!({ "ip": "127.0.0.1", "port": port_b, "device_id": "pair-B" }))
        .send()
        .unwrap()
        .json()
        .unwrap();
    assert_eq!(push["encrypted"], true, "已配对 → /push-to 必须信封化");
    assert!(push["applied"].as_u64().unwrap() >= 1, "热点推送应落数据: {push:?}");
    assert!(inbox_contains(dir_b.path(), "note-hotspot-A"), "A 的笔记应经 /push-to 到达 B");

    let _ = (ha.is_finished(), hb.is_finished());
}

/// 平板卸载重装（或换签名键）后 device_id 会换，于是码主的列表里躺着两行同一台物理机。
/// 装机指纹让码主**认得出**这件事，但行为刻意止步于此：只提示，合并要用户点，
/// 安全码照样得比（uid 不是凭据，同机双实例的 uid 也完全相同）。
#[test]
fn reinstall_offers_an_explicit_merge_but_changes_nothing_by_itself() {
    // 前提自证：本机读得到 OS 级稳定标识，否则这条用例的因果根本不成立
    let uid = aw_sync_rust::machine_uid::machine_uid()
        .expect("本机读不到装机指纹（Windows MachineGuid / Linux machine-id），用例前提不成立");
    assert_eq!(uid.len(), 16, "指纹应为 16 个 hex");

    let dir_a = TempDir::new().unwrap();
    let dir_old = TempDir::new().unwrap();
    let dir_new = TempDir::new().unwrap();
    let (port_a, port_old, port_new) = (free_port(), free_port(), free_port());
    // 三台“机器”各用自己的 UDP 端口：本用例验的是配对与列表，不该互相广播干扰
    let ha = spawn_server(dir_a.path().to_path_buf(), "host-A", port_a, free_udp_port());
    let ho = spawn_server(dir_old.path().to_path_buf(), "tablet-old", port_old, free_udp_port());
    let hn = spawn_server(dir_new.path().to_path_buf(), "tablet-new", port_new, free_udp_port());
    wait_port(port_a);
    wait_port(port_old);
    wait_port(port_new);

    let c = http();
    let a = format!("http://127.0.0.1:{port_a}");
    let old = format!("http://127.0.0.1:{port_old}");
    let new = format!("http://127.0.0.1:{port_new}");

    // 走真实的「加入方」路径：/join-remote 由本机管理器发起出站 /join，
    // 出站报文里才会附上本机装机指纹（手工拼 body 等于自己把答案抄进去）。
    fn pair_by_code(c: &reqwest::blocking::Client, joiner: &str, host: &str, host_port: u16, code: &str) {
        let mut host_dev: serde_json::Value = c
            .get(format!("{host}/api/0/sync/info"))
            .send()
            .unwrap()
            .json()
            .unwrap();
        host_dev["ip"] = serde_json::json!("127.0.0.1");
        host_dev["port"] = serde_json::json!(host_port);
        host_dev["is_self"] = serde_json::json!(false);
        let host_id = host_dev["id"].as_str().unwrap().to_string();
        assert!(c
            .post(format!("{joiner}/api/0/sync/devices"))
            .json(&host_dev)
            .send()
            .unwrap()
            .status()
            .is_success());
        let resp: serde_json::Value = c
            .post(format!("{joiner}/api/0/sync/join-remote"))
            .json(&serde_json::json!({ "device_id": host_id, "code": code }))
            .send()
            .unwrap()
            .json()
            .unwrap();
        assert_eq!(resp["ok"], true, "配对码加入应成功: {resp:?}");
    }

    fn code_at(c: &reqwest::blocking::Client, base: &str) -> String {
        let pc: serde_json::Value = c
            .post(format!("{base}/api/0/sync/paircode"))
            .send()
            .unwrap()
            .json()
            .unwrap();
        pc["code"].as_str().unwrap().to_string()
    }

    // 1) 老平板（重装前的 id）配一次
    let code1 = code_at(&c, &a);
    pair_by_code(&c, &old, &a, port_a, &code1);

    // 2) 同一台物理机重装后换了 id，再配一次
    let code2 = code_at(&c, &a);
    pair_by_code(&c, &new, &a, port_a, &code2);

    // 3) 码主的列表里：两行都在，都没被自动折叠；新那行带一个「疑似同一台机器」的候选
    let devs_raw = c.get(format!("{a}/api/0/sync/devices")).send().unwrap().text().unwrap();
    let devs: Vec<serde_json::Value> = serde_json::from_str(&devs_raw).unwrap();
    let row = |id: &str| devs.iter().find(|d| d["id"] == id).cloned();
    assert!(row("tablet-old").is_some() && row("tablet-new").is_some(), "系统不得自动折叠任何一行: {devs_raw}");
    assert_eq!(devs.iter().filter(|d| d["is_self"] == false).count(), 2);
    let cand = row("tablet-new").unwrap()["merge_candidate"].clone();
    assert_eq!(cand["id"], "tablet-old", "换了 id 的那行应被提示为同一台机器的重装");
    assert_eq!(cand["uid_hint"], &uid[..8], "提示只给 8 个 hex，不给完整机器标识");
    // 提示只挂一次：更早配对的那行不该反过来指向新行（界面上不该冒出两个合并按钮）
    assert!(row("tablet-old").unwrap().get("merge_candidate").is_none());

    // 4) 纪律「不被动出机」在 HTTP 层的两面：
    //    /info 任何人可 GET → 不得带指纹；配对握手的响应（用户主动发起）→ 必须带，
    //    否则另一侧永远记不下这台的指纹，下次重装就认不出来了。
    let info_raw = c.get(format!("{a}/api/0/sync/info")).send().unwrap().text().unwrap();
    assert!(!info_raw.contains("machine_uid"), "/info 是被动可读端点，不得带出指纹: {info_raw}");
    let code3 = code_at(&c, &a);
    let mut probe: serde_json::Value = c.get(format!("{old}/api/0/sync/info")).send().unwrap().json().unwrap();
    probe["id"] = serde_json::json!("probe-x");
    probe["ip"] = serde_json::json!("127.0.0.1");
    probe["port"] = serde_json::json!(port_old);
    probe["is_self"] = serde_json::json!(false);
    let join_resp: serde_json::Value = c
        .post(format!("{a}/api/0/sync/join"))
        .json(&serde_json::json!({ "code": code3, "device": probe }))
        .send()
        .unwrap()
        .json()
        .unwrap();
    assert_eq!(
        join_resp["peer"]["machine_uid"], uid,
        "配对响应必须回上本机指纹（四个配对方向都要记得到对方的指纹）"
    );

    // 5) 列表整体仍然干净：密钥与完整指纹都不在里面
    assert!(!devs_raw.contains("machine_uid"), "/devices 不得序列化 machine_uid");
    assert!(!devs_raw.contains(&uid), "/devices 不得带出完整装机指纹");

    // 6) 用户点「合并」：旧行退场、新行留下，且新行的密钥/安全码不受影响
    let before_fp = row("tablet-new").unwrap()["fingerprint"].clone();
    let merged: serde_json::Value = c
        .post(format!("{a}/api/0/sync/merge"))
        .json(&serde_json::json!({ "from": "tablet-old", "to": "tablet-new" }))
        .send()
        .unwrap()
        .json()
        .unwrap();
    assert_eq!(merged["ok"], true, "合并应成功: {merged:?}");
    let devs2: Vec<serde_json::Value> = c
        .get(format!("{a}/api/0/sync/devices"))
        .send()
        .unwrap()
        .json()
        .unwrap();
    let row2 = |id: &str| devs2.iter().find(|d| d["id"] == id).cloned();
    assert!(row2("tablet-old").is_none(), "旧行应从列表退场");
    let kept = row2("tablet-new").expect("活着的这一行必须留下");
    assert_eq!(kept["paired"], true);
    assert_eq!(kept["fingerprint"], before_fp, "归并不动密钥：安全码得跟合并前一致");
    assert!(kept.get("merge_candidate").is_none(), "合并完不该还挂着提示");

    // 7) 合并方向不可逆地保护历史：反着并（把活行并进死行）必须被拒
    let wrong_way = c
        .post(format!("{a}/api/0/sync/merge"))
        .json(&serde_json::json!({ "from": "tablet-new", "to": "tablet-old" }))
        .send()
        .unwrap();
    assert!(
        !wrong_way.status().is_success(),
        "已被归并的行不得再作为 from/to 参与归并"
    );
    // 拒绝原因必须进响应体：两端 UI 的失败提示直接读它，裸 500 只会显示「服务器错误 (500)」
    let why = wrong_way.text().unwrap_or_default();
    assert!(
        why.contains("error") && !why.contains("Internal Server Error"),
        "归并拒绝应带可读原因，实际响应: {why}"
    );

    // 8) 「一键清理」：30 天阈值下这三行都还年轻，一行都不该动
    let purge: serde_json::Value = c
        .post(format!("{a}/api/0/sync/devices/purge"))
        .json(&serde_json::json!({ "stale_days": 30 }))
        .send()
        .unwrap()
        .json()
        .unwrap();
    assert_eq!(purge["ok"], true);
    assert_eq!(purge["discovered_removed"], 0);
    assert_eq!(purge["paired_removed"], 0, "刚配好的设备不得被一键清理误删: {purge:?}");

    let _ = (ha.is_finished(), ho.is_finished(), hn.is_finished());
}
