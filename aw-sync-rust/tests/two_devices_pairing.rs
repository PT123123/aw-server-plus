//! 模拟两台设备（A/B，各自独立的 sync.db 数据目录）之间的配对流程与异常路径。
//! 覆盖：配对码生成规则、完整配对、双向登记、错误码/过期码拒绝、一次性消费、删除设备、日志筛选。

use aw_sync_rust::manager::SharedManager;
use aw_sync_rust::models::{Device, DeviceKind, PairCode};
use aw_sync_rust::paircode::PairError;
use aw_sync_rust::storage::{LogFilter, SyncDb};
use chrono::{Duration as ChronoDuration, Utc};
use tempfile::TempDir;

const A_ID: &str = "device-A";
const B_ID: &str = "device-B";

/// 每台“机器”拥有独立的数据目录与 SyncManager（守卫必须存活，否则目录被删导致只读）
fn make_manager(id: &str) -> (TempDir, SharedManager) {
    let dir = TempDir::new().unwrap();
    let m = aw_sync_rust::SyncManager::new(dir.path(), id.to_string()).unwrap();
    (dir, m)
}

fn fake_device(id: &str, name: &str, ip: &str) -> Device {
    Device {
        id: id.into(),
        name: name.into(),
        device_kind: DeviceKind::Linux,
        ip: ip.into(),
        port: 56001,
        paired_at: Utc::now(),
        last_sync_at: None,
                is_online: true,
        is_self: false,
        paired: false,
        alias: None,
    }
}

#[test]
fn paircode_is_four_digits() {
    let (_d, a) = make_manager(A_ID);
    let g = a.lock().unwrap();
    for _ in 0..50 {
        let pc = g.create_pair_code().unwrap();
        assert_eq!(pc.code.len(), 4, "应为4位");
        assert!(pc.code.chars().all(|c| c.is_ascii_digit()), "应为纯数字: {}", pc.code);
    }
}

#[test]
fn two_devices_full_pairing_flow() {
    let (_da, a) = make_manager(A_ID);
    let (_db, b) = make_manager(B_ID);

    // 1) A 生成配对码
    let code = a.lock().unwrap().create_pair_code().unwrap().code;

    // 2) B 凭码在 A 处加入（A 校验并登记 B）
    let joined = {
        let g = a.lock().unwrap();
        g.join_with_code(&code, fake_device(B_ID, "Phone-B", "192.168.1.23")).unwrap()
    };
    assert_eq!(joined.id, B_ID);

    // 3) A 的信任列表包含 B
    {
        let g = a.lock().unwrap();
        let devs = g.list_devices().unwrap();
        assert_eq!(devs.len(), 1);
        assert_eq!(devs[0].id, B_ID);
        assert!(!devs[0].is_self);
    }

    // 4) join 响应会携带 A 的本机信息 → B 把它登记进自己的信任列表（双向互见）
    {
        let peer_a = a.lock().unwrap().self_device_info();
        let mut to_save = peer_a;
        to_save.is_self = false;
        b.lock().unwrap().save_device(&to_save).unwrap();
    }
    {
        let devs = b.lock().unwrap().list_devices().unwrap();
        assert!(devs.iter().any(|d| d.id == A_ID), "B 应能看到 A");
        assert!(devs.iter().all(|d| !d.is_self), "B 列表中不应有本机标记");
    }

    // 5) 配对码一次性：复用同一码再次加入必须失败
    let again = {
        let g = a.lock().unwrap();
        g.join_with_code(&code, fake_device("C", "c", "1.2.3.4"))
    };
    assert!(matches!(again, Err(PairError::InvalidOrExpiredCode)));
    // C 未被登记
    assert_eq!(a.lock().unwrap().list_devices().unwrap().len(), 1);
}

#[test]
fn wrong_code_is_rejected_and_no_device_added() {
    let (_d, a) = make_manager(A_ID);
    let g = a.lock().unwrap();
    let r = g.join_with_code("9999", fake_device(B_ID, "b", "10.0.0.9"));
    assert!(matches!(r, Err(PairError::InvalidOrExpiredCode)));
    assert!(g.list_devices().unwrap().is_empty());
}

#[test]
fn expired_code_cannot_be_used() {
    let dir = TempDir::new().unwrap();
    let db = SyncDb::open(dir.path()).unwrap();
    db.store_pair_code(&PairCode {
        code: "0000".into(),
        created_at: Utc::now() - ChronoDuration::minutes(10),
        expires_at: Utc::now() - ChronoDuration::minutes(5),
    })
    .unwrap();
    assert!(!db.validate_pair_code("0000").unwrap());
    // 过期码清理后彻底消失
    db.cleanup_expired_codes().unwrap();
    assert!(!db.validate_pair_code("0000").unwrap());
}

#[test]
fn delete_device_removes_from_trust_list() {
    let (_d, a) = make_manager(A_ID);
    let code = a.lock().unwrap().create_pair_code().unwrap().code;
    a.lock()
        .unwrap()
        .join_with_code(&code, fake_device(B_ID, "b", "ip"))
        .unwrap();
    assert!(a.lock().unwrap().delete_device(B_ID).unwrap());
    assert!(a.lock().unwrap().list_devices().unwrap().is_empty());
    // 再删除不存在的设备返回 false
    assert!(!a.lock().unwrap().delete_device(B_ID).unwrap());
}

#[test]
fn sync_log_direction_protocol_filtering_and_paging() {
    use aw_sync_rust::models::{SyncDirection, SyncEventType, SyncLogEntry, SyncProtocol, SyncStatus};
    let dir = TempDir::new().unwrap();
    let db = SyncDb::open(dir.path()).unwrap();
    let mk = |direction: SyncDirection, protocol: SyncProtocol, event_type: SyncEventType| SyncLogEntry {
        id: None,
        timestamp: Utc::now(),
        direction,
        protocol,
        peer_id: Some(B_ID.into()),
        event_type,
        status: SyncStatus::Success,
        message: None,
        data_size: Some(128),
    };
    db.add_log(&mk(SyncDirection::Out, SyncProtocol::Http, SyncEventType::Sync)).unwrap();
    db.add_log(&mk(SyncDirection::In, SyncProtocol::Http, SyncEventType::Sync)).unwrap();
    db.add_log(&mk(SyncDirection::Out, SyncProtocol::UdpBroadcast, SyncEventType::Discovery)).unwrap();
    assert_eq!(db.log_count().unwrap(), 3);

    let f = |direction, protocol| LogFilter { direction, protocol, event_type: None, limit: 100, offset: 0 };
    let out_http = db.get_logs(&f(Some(SyncDirection::Out), Some(SyncProtocol::Http))).unwrap();
    assert_eq!(out_http.len(), 1);
    let udp = db.get_logs(&f(None, Some(SyncProtocol::UdpBroadcast))).unwrap();
    assert_eq!(udp.len(), 1);
    let all = db.get_logs(&f(None, None)).unwrap();
    assert_eq!(all.len(), 3);

    // 按报文阶段（event_type）筛选
    let discovery_only = db.get_logs(&LogFilter { direction: None, protocol: None, event_type: Some(SyncEventType::Discovery), limit: 100, offset: 0 }).unwrap();
    assert_eq!(discovery_only.len(), 1);
    assert_eq!(discovery_only[0].event_type, SyncEventType::Discovery);
    let sync_only = db.get_logs(&LogFilter { direction: None, protocol: None, event_type: Some(SyncEventType::Sync), limit: 100, offset: 0 }).unwrap();
    assert_eq!(sync_only.len(), 2);
    // 阶段 + 方向组合筛选
    let combo = db.get_logs(&LogFilter { direction: Some(SyncDirection::Out), protocol: None, event_type: Some(SyncEventType::Sync), limit: 100, offset: 0 }).unwrap();
    assert_eq!(combo.len(), 1);

    // 分页
    let page = db.get_logs(&LogFilter { direction: None, protocol: None, event_type: None, limit: 2, offset: 0 }).unwrap();
    assert_eq!(page.len(), 2);
    let page2 = db.get_logs(&LogFilter { direction: None, protocol: None, event_type: None, limit: 2, offset: 2 }).unwrap();
    assert_eq!(page2.len(), 1);

    // 截断保留最近 N 条
    db.truncate_logs(2).unwrap();
    assert_eq!(db.log_count().unwrap(), 2);
}
