//! 设备身份：在线判定、未配对行淘汰、装机指纹与「同一台机器换 id」的归并。
//!
//! 对应设计文《设备身份归并》的 A / B2 / C 三段：
//! - A：`is_online` 必须按真实时间判定（曾经拿 RFC3339 串和 SQLite 的
//!   `datetime('now',…)` 串混比，字符串序上 `'T' > ' '`，条件恒真 → 一台设备只要上线过
//!   就永远显示在线）；未配对的纯发现行要能自动退场。
//! - B2：装机指纹（machine_uid）的两条纪律——不是凭据、不被动出机。
//! - C：同指纹的两行只作**提示**，合并由用户点；合并改的是显示与历史归因，不碰密钥信任。
//!
//! 全部直连 SyncDb / SyncManager，不起 HTTP、不占网络端口（台架隔离见 sync_e2e_http.rs 顶部）。
//! 需要同步库内部字段的断言，另开一个 `SyncDb::open(同一目录)` 句柄来读：sync.db 是 WAL，
//! 读得到已提交内容，也不必为测试把 manager 的私有连接暴露出去。

use std::sync::{Arc, Mutex};

use aw_sync_rust::crypto::hashed_alias;
use aw_sync_rust::discovery::{announce_payload, parse_device, record_peer, PeerSource};
use aw_sync_rust::models::{Device, DeviceKind};
use aw_sync_rust::storage::{SyncDb, ONLINE_GRACE_SECS};
use chrono::{Duration, Utc};
use tempfile::TempDir;

/// `storage` 的私有淘汰策略常量，测试侧按约定值写死：改那里要同步改这里。
const DISCOVERED_MAX_AGE_DAYS: i64 = 7;
const DISCOVERED_KEEP: usize = 50;

fn dev(id: &str) -> Device {
    Device {
        id: id.into(),
        name: format!("{id}-主机名"),
        device_kind: DeviceKind::Windows,
        ip: "192.168.5.40".into(),
        port: 56001,
        paired_at: Utc::now(),
        last_sync_at: None,
        last_seen_at: Some(Utc::now()),
        is_online: true,
        is_self: false,
        paired: false,
        alias: None,
        device_secret: None,
        machine_uid: None,
    }
}

fn ids(list: &[Device]) -> Vec<String> {
    let mut v: Vec<String> = list.iter().map(|d| d.id.clone()).collect();
    v.sort();
    v
}

fn secret(tag: &str) -> String {
    format!("{tag}{tag}").repeat(32 / 2)
}

// ---- A：在线判定 ----

#[test]
fn online_flag_follows_real_time_not_string_order() {
    let dir = TempDir::new().unwrap();
    let db = SyncDb::open(dir.path()).unwrap();

    // 刚被发现线程刷过的设备：探活失败也不许判离线（广播 5 秒一轮，抖一下不该闪断）
    db.upsert_device(&dev("fresh")).unwrap();
    db.touch_online("fresh", false).unwrap();
    assert!(
        db.get_device("fresh").unwrap().unwrap().is_online,
        "宽限期内（{ONLINE_GRACE_SECS}s）收到离线上报应被忽略"
    );

    // 真的很久没出现了：离线上报必须写进去。曾经这一句恒为「还在播报」，
    // 因为 last_seen_at 的 RFC3339 串在字符串序上恒大于 SQLite 的 datetime 串。
    let mut stale = dev("stale");
    stale.last_seen_at = Some(Utc::now() - Duration::seconds(ONLINE_GRACE_SECS + 60));
    db.upsert_device(&stale).unwrap();
    db.touch_online("stale", false).unwrap();
    assert!(
        !db.get_device("stale").unwrap().unwrap().is_online,
        "超出宽限期的历史时间必须按探活结论标成离线，否则设备一旦上线过就永远显示在线"
    );

    // 上线方向不受宽限期约束（否则在线状态再也回不来）
    db.touch_online("stale", true).unwrap();
    assert!(db.get_device("stale").unwrap().unwrap().is_online);

    // 发现路径自己刷的时间（touch_seen）同样要让设备重新获得宽限期
    db.touch_seen("stale").unwrap();
    db.touch_online("stale", false).unwrap();
    assert!(db.get_device("stale").unwrap().unwrap().is_online, "touch_seen 应刷新 last_seen_epoch");
}

// ---- A：未配对发现行的自动退场 ----

#[test]
fn discovered_rows_expire_but_paired_and_manual_ones_stay() {
    let dir = TempDir::new().unwrap();
    let db = SyncDb::open(dir.path()).unwrap();
    let overdue = Duration::days(DISCOVERED_MAX_AGE_DAYS + 1);

    // 静默超龄的发现行：该退场
    let mut ghost = dev("passer-by");
    ghost.last_seen_at = Some(Utc::now() - overdue);
    db.upsert_discovered(&ghost, "udp").unwrap();
    // 最近还在播报的发现行：留下
    db.upsert_discovered(&dev("recent"), "mdns").unwrap();
    // 同样超龄，但已配对 / 是手动登记的（seen_via 为空）：都不归自动淘汰管
    let mut old_paired = dev("old-paired");
    old_paired.last_seen_at = Some(Utc::now() - overdue);
    db.upsert_discovered(&old_paired, "udp").unwrap();
    db.set_paired("old-paired", true).unwrap();
    let mut manual = dev("manual");
    manual.last_seen_at = Some(Utc::now() - Duration::days(30));
    db.upsert_device(&manual).unwrap();

    assert_eq!(db.purge_discovered().unwrap(), 1, "只应淘汰那一条超龄未配对行");
    assert_eq!(
        ids(&db.get_devices().unwrap()),
        ["manual", "old-paired", "recent"],
        "已配对行与手动登记行不得被自动清理动到"
    );
}

#[test]
fn discovered_list_is_capped_keeping_the_newest() {
    let dir = TempDir::new().unwrap();
    let db = SyncDb::open(dir.path()).unwrap();

    // 一次性冒出来一堆（换网段、访客机器、旧版本各自为政）：条数上限兜住，
    // 丢掉最久没出现的那些，而不是把列表变成考古现场。i 越小 = 最近才见过。
    let total = DISCOVERED_KEEP + 7;
    for i in 0..total {
        let mut d = dev(&format!("floody-{i:03}"));
        d.last_seen_at = Some(Utc::now() - Duration::minutes(i as i64));
        db.upsert_discovered(&d, "udp").unwrap();
    }
    assert_eq!(db.get_devices().unwrap().len(), total);
    assert_eq!(db.purge_discovered().unwrap(), total - DISCOVERED_KEEP);

    let left = ids(&db.get_devices().unwrap());
    assert_eq!(left.len(), DISCOVERED_KEEP);
    assert!(left.iter().any(|s| s == "floody-000"), "最近见过的那条必须留下");
    assert!(left.contains(&format!("floody-{:03}", DISCOVERED_KEEP - 1)));
    assert!(
        !left.contains(&format!("floody-{DISCOVERED_KEEP:03}")),
        "超出上限的旧行应被挤掉，且挤掉的是最久没出现的"
    );
    assert!(!left.iter().any(|s| s == "floody-056"));
}

// ---- B2：装机指纹绝不被动出机 ----

#[test]
fn announce_payload_carries_neither_secret_nor_fingerprint() {
    let uid = "a1b2c3d4e5f60718";
    let key = secret("ab");
    let mut d = dev("dev-x");
    d.name = "Ted-Workstation".into();
    d.machine_uid = Some(uid.into());
    d.device_secret = Some(key.clone());

    let payload = announce_payload(&d);
    assert!(payload.starts_with("AW-SYNC/1.0\n"), "报文前缀是对端识别依据，别改：{payload}");
    assert!(!payload.contains(uid), "装机指纹不得进被动广播");
    assert!(!payload.contains(&key), "密钥不得进被动广播");
    // 连字段名都不该出现：出现即说明序列化没 skip，同网段任何人能看出这里有什么可偷
    assert!(!payload.contains("machine_uid"));
    assert!(!payload.contains("device_secret"));
    assert!(!payload.contains("Ted-Workstation"), "未配对前真实主机名不得出机");
    assert!(payload.contains(&hashed_alias("dev-x")));

    // 对端仍能正常解析
    let parsed = parse_device(&payload).expect("宣告必须可解析");
    assert_eq!(parsed.id, "dev-x");
    assert_eq!(parsed.name, hashed_alias("dev-x"));
}

#[test]
fn passive_paths_cannot_write_a_fingerprint() {
    let dir = TempDir::new().unwrap();
    let db: Arc<Mutex<SyncDb>> = Arc::new(Mutex::new(SyncDb::open(dir.path()).unwrap()));

    // 有人（哪怕是伪造的宣告）把指纹塞进被动路径：报文里解析得到，但落不了库
    let mut liar = dev("liar");
    liar.machine_uid = Some("deadbeefdeadbeef".into());
    assert!(record_peer(&db, liar, PeerSource::UdpBroadcast, "测试宣告".into()));
    assert!(
        db.lock().unwrap().machine_uid_of("liar").unwrap().is_none(),
        "被动发现路径写不进装机指纹，否则任何人都能给我塞一个假的归并目标"
    );

    // 信任列表的序列化结果里永远没有 machine_uid（row_to_device 恒填 None）
    let json = serde_json::to_string(&db.lock().unwrap().get_devices().unwrap()).unwrap();
    assert!(!json.contains("machine_uid"), "/devices 会原样序列化这个响应: {json}");
}

// ---- B2 / C：归并提示 ----

/// 同一台物理机上跑两个实例时指纹完全相同，所以只提示、绝不自动折叠（用户选定）。
#[test]
fn merge_hint_only_points_at_the_paired_twin_and_shows_once() {
    let dir = TempDir::new().unwrap();
    let m = aw_sync_rust::SyncManager::new(dir.path(), "host-A".into()).unwrap();
    let uid = "0123456789abcdef";

    {
        let g = m.lock().unwrap();
        // 旧行：10 天前配好的那台机器
        let mut old = dev("old-tablet-id");
        old.paired = true;
        old.paired_at = Utc::now() - Duration::days(10);
        old.machine_uid = Some(uid.into());
        g.save_device(&old).unwrap();
        // 新行：同一台机器重装之后换了 id
        let mut fresh = dev("new-tablet-id");
        fresh.paired = true;
        fresh.machine_uid = Some(uid.into());
        g.save_device(&fresh).unwrap();
        // 不相干的第三台
        let mut other = dev("laptop");
        other.paired = true;
        other.machine_uid = Some("ffffffffffffffff".into());
        g.save_device(&other).unwrap();

        let cand = g.merge_candidate_for("new-tablet-id").expect("换了 id 的行应命中旧行");
        assert_eq!(cand["id"], "old-tablet-id");
        // 只回前 8 个 hex：/devices 同网段任何人可读，不给可跟踪的完整机器标识
        assert_eq!(cand["uid_hint"], "01234567");
        assert_eq!(cand["since_paired_days"], 10);
        assert!(g.merge_candidate_for("old-tablet-id").is_none(), "更早配对的那行不该再提示");
        assert!(g.merge_candidate_for("laptop").is_none(), "没有同指纹对端就不该提示");
        // 列表里根本没这台设备（未配对、无指纹）→ 一次查询都不该发
        assert!(g.merge_candidate_for("nobody").is_none());
        assert!(g.list_devices().unwrap().iter().all(|d| d.machine_uid.is_none()));
    }

    // 指纹确实按行存在库里，只是永不出接口
    let db = SyncDb::open(dir.path()).unwrap();
    assert_eq!(db.machine_uid_of("old-tablet-id").unwrap().as_deref(), Some(uid));
}

#[test]
fn an_unpaired_discovered_twin_is_not_a_merge_target() {
    let dir = TempDir::new().unwrap();
    let db = SyncDb::open(dir.path()).unwrap();
    let uid = "abcabcabcabcabcc";

    let mut twin = dev("discovered-twin");
    twin.machine_uid = Some(uid.into());
    db.upsert_device(&twin).unwrap(); // paired = false
    let mut me = dev("mine");
    me.paired = true;
    me.machine_uid = Some(uid.into());
    db.upsert_device(&me).unwrap();

    assert!(
        db.find_merge_candidate(uid, "mine").unwrap().is_none(),
        "未配对的孪生行本来就归 purge_discovered 管，拿它当合并目标没有意义"
    );
    db.set_paired("discovered-twin", true).unwrap();
    assert_eq!(db.find_merge_candidate(uid, "mine").unwrap().map(|d| d.id).as_deref(), Some("discovered-twin"));
}

#[test]
fn merge_voids_the_dead_id_but_keeps_history_and_keys() {
    let dir = TempDir::new().unwrap();
    let m = aw_sync_rust::SyncManager::new(dir.path(), "host-A".into()).unwrap();
    let uid = "1234123412341234";

    {
        let db = SyncDb::open(dir.path()).unwrap();
        let mut old = dev("old-id");
        old.paired = true;
        old.paired_at = Utc::now() - Duration::days(30);
        old.alias = Some("客厅那台".into());
        old.machine_uid = Some(uid.into());
        db.upsert_device(&old).unwrap();
        db.set_device_secret("old-id", &secret("o1")).unwrap();

        let mut fresh = dev("new-id");
        fresh.paired = true;
        fresh.machine_uid = Some(uid.into());
        db.upsert_device(&fresh).unwrap();
        db.set_device_secret("new-id", &secret("n1")).unwrap();
    }

    m.lock().unwrap().merge_devices("old-id", "new-id").unwrap();

    let g = m.lock().unwrap();
    // 列表里只剩活着的这一行；旧行只为历史归因保留
    assert_eq!(ids(&g.list_devices().unwrap()), ["new-id"]);
    let all = SyncDb::open(dir.path())
        .unwrap()
        .get_devices_including_superseded()
        .unwrap();
    assert_eq!(ids(&all), ["new-id", "old-id"]);
    assert!(!all.iter().find(|d| d.id == "old-id").unwrap().is_online, "旧行不会再播报，不该显示在线");

    // 旧 id 作废：它自己的密钥没了；新 id 那把原封不动（归并不改密钥信任）
    assert!(g.device_secret("old-id").is_none(), "归并后旧 id 不该还能签名");
    assert_eq!(g.device_secret("new-id").unwrap(), secret("n1"));

    // 历史归因：写着旧 id 的已同步数据还能解析到现行设备
    let db = SyncDb::open(dir.path()).unwrap();
    assert_eq!(db.resolve_device_id("old-id"), "new-id");
    assert_eq!(db.resolve_device_id("new-id"), "new-id", "没被归并过的 id 原样返回");
    // 旧行的别名与最早配对时间迁到新行
    let kept = db.get_device("new-id").unwrap().unwrap();
    assert_eq!(kept.alias.as_deref(), Some("客厅那台"));
    assert!(kept.paired_at <= Utc::now() - Duration::days(30), "配对时间应取两者最早");
    assert!(kept.paired);
    assert_eq!(db.machine_uid_of("new-id").unwrap().as_deref(), Some(uid));

    // 护栏：并过的行不能再当端点，自己不能和自己并
    assert!(g.merge_devices("old-id", "new-id").is_err(), "重复归并必须被拒");
    assert!(g.merge_devices("new-id", "new-id").is_err());
}

#[test]
fn merge_rejects_anything_that_would_break_attribution() {
    let dir = TempDir::new().unwrap();
    let db = SyncDb::open(dir.path()).unwrap();
    let mut me = dev("self-row");
    me.is_self = true;
    me.paired = true;
    db.upsert_device(&me).unwrap();
    db.upsert_device(&dev("plain")).unwrap();
    db.set_paired("plain", true).unwrap();

    assert!(db.merge_device("self-row", "plain").is_err(), "本机行不得被并进别处");
    assert!(db.merge_device("plain", "self-row").is_err(), "也不许把别的行并进本机行");
    assert!(db.merge_device("missing", "plain").is_err(), "不存在的行不该被静默当作成功");

    // 两边都没配对：没有归并意义
    db.upsert_device(&dev("a")).unwrap();
    db.upsert_device(&dev("b")).unwrap();
    assert!(db.merge_device("a", "b").is_err());
    // 失败路径不留半成品：既没打标记也没写别名
    assert_eq!(db.resolve_device_id("a"), "a");
    assert_eq!(db.get_devices().unwrap().len(), 4, "被拒的归并不得让任何一行从列表消失");
}

// ---- C：一键清理 ----

#[test]
fn one_click_purge_drops_silent_pairs_and_keeps_recent_ones() {
    let dir = TempDir::new().unwrap();
    let m = aw_sync_rust::SyncManager::new(dir.path(), "host-A".into()).unwrap();

    {
        let g = m.lock().unwrap();
        let mut dead = dev("dead-phone");
        dead.paired = true;
        g.save_device(&dead).unwrap();
        let mut alive = dev("alive-phone");
        alive.paired = true;
        g.save_device(&alive).unwrap();
        // 配过但从没同步成功：last_sync_at 为空，一律不动（刚点的配对被自动删掉会很莫名）
        g.save_device(&{
            let mut n = dev("never-synced");
            n.paired = true;
            n
        })
        .unwrap();
    }

    let db = SyncDb::open(dir.path()).unwrap();
    db.mark_synced("dead-phone", Utc::now() - Duration::days(60)).unwrap();
    db.set_device_secret("dead-phone", &secret("d1")).unwrap();
    db.mark_synced("alive-phone", Utc::now() - Duration::days(3)).unwrap();
    let mut ghost = dev("passer-by");
    ghost.last_seen_at = Some(Utc::now() - Duration::days(DISCOVERED_MAX_AGE_DAYS + 2));
    db.upsert_discovered(&ghost, "udp").unwrap();

    let (discovered, paired) = m.lock().unwrap().purge_stale_devices(30).unwrap();
    assert_eq!((discovered, paired), (1, 1), "只应删掉超龄发现行与静默旧配对各一条");
    let g = m.lock().unwrap();
    assert_eq!(ids(&g.list_devices().unwrap()), ["alive-phone", "never-synced"]);
    assert!(g.device_secret("dead-phone").is_none(), "删配对必须连带作废密钥");
}
