//! 端到端集成测试：在本机起两个真实 HTTP 服务（模拟两台机器），
//! 完整走通「配置 → 创建配对码 → 加入配对 → 双向登记 → 数据双向同步 → 日志校验 → 删除设备」。

use aw_sync_rust::endpoints::mount_rocket;
use aw_sync_rust::models::{Device, SyncSnapshot};
use aw_sync_rust::serialize::export_inbox;
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};
use tempfile::TempDir;

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
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

fn spawn_server(dir: PathBuf, device_id: &str, port: u16) -> std::thread::JoinHandle<()> {
    let mgr = aw_sync_rust::SyncManager::new(&dir, device_id.to_string()).unwrap();
    std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async move {
            let figment = rocket::Config::figment()
                .merge(("address", "127.0.0.1"))
                .merge(("port", port));
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

fn make_inbox_db(dir: &Path, content: &str) {
    let conn = rusqlite::Connection::open(dir.join("inbox.db")).unwrap();
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS notes (
            id INTEGER PRIMARY KEY AUTOINCREMENT, content TEXT NOT NULL,
            tags TEXT DEFAULT '[]', created_at TEXT NOT NULL, updated_at TEXT NOT NULL);
         CREATE TABLE IF NOT EXISTS note_relations (
            id INTEGER PRIMARY KEY AUTOINCREMENT, source_note_id INTEGER NOT NULL,
            target_note_id INTEGER NOT NULL, relation_type TEXT NOT NULL, created_at TEXT NOT NULL);",
    )
    .unwrap();
    conn.execute(
        "INSERT INTO notes (content,tags,created_at,updated_at) VALUES (?1,'[]','2026-08-25T00:00:00Z','2026-08-25T00:00:00Z')",
        rusqlite::params![content],
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
    let ha = spawn_server(dir_a.path().to_path_buf(), "machine-A", port_a);
    let hb = spawn_server(dir_b.path().to_path_buf(), "machine-B", port_b);
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

    // 1) A 创建配对码（4 位数字）
    let pc: serde_json::Value = c
        .post(format!("{}/api/0/sync/paircode", ba))
        .send()
        .unwrap()
        .json()
        .unwrap();
    let code = pc["code"].as_str().unwrap().to_string();
    assert_eq!(code.len(), 4);
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

    // 5) 数据同步 B→A：B 的 inbox 写入笔记，构造快照推给 A 的 /push
    make_inbox_db(dir_b.path(), "note-from-machine-B");
    let inbox_json_b = export_inbox(dir_b.path().join("inbox.db").as_path()).unwrap();
    let snap_b = SyncSnapshot {
        source_device: Some(serde_json::from_value::<Device>(dev_b.clone()).unwrap()),
        activity: None,
        inbox: Some(inbox_json_b),
    };
    let push_resp: serde_json::Value = c
        .post(format!("{}/api/0/sync/push", ba))
        .json(&snap_b)
        .send()
        .unwrap()
        .json()
        .unwrap();
    assert!(push_resp["applied"].as_u64().unwrap() >= 1);
    assert!(inbox_contains(dir_a.path(), "note-from-machine-B"), "A 应收到 B 的笔记");

    // 6) 数据同步 A→B：走真实 HTTP 客户端 transport::push_snapshot
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
    };
    let target_b: Device = serde_json::from_value(dev_b.clone()).unwrap();
    let applied = aw_sync_rust::transport::push_snapshot(&target_b, &snap_a).unwrap();
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
    assert_eq!(st["udp_port"], 46000);

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
    let h = spawn_server(dir_a.path().to_path_buf(), "machine-A", port);
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
    let ha = spawn_server(dir_a.path().to_path_buf(), "machine-A", port_a);
    let hb = spawn_server(dir_b.path().to_path_buf(), "machine-B", port_b);
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
    let ha = spawn_server(dir_a.path().to_path_buf(), "pair-A", port_a);
    let hb = spawn_server(dir_b.path().to_path_buf(), "pair-B", port_b);
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

    let _ = (ha.is_finished(), hb.is_finished());
}
