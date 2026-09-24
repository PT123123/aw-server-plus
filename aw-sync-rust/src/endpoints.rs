//! 局域网同步 REST API（挂载于 /api/0/sync），供 aw-webui 调用与对端互操作。

use log::info;
use rocket::form::FromForm;
use rocket::http::Status;
use rocket::serde::json::Json;
use rocket::{delete, get, post, put, routes, Build, Rocket, State};
use serde::{Deserialize, Serialize};

use crate::manager::{SharedManager, SyncManager};
use crate::models::{
    Device, SyncDirection, SyncEventType, SyncLogEntry, SyncProtocol, SyncSnapshot, SyncStatus,
};
use crate::storage::LogFilter;
use chrono::Utc;

// ---- Cloudflare D1 云同步 ----

#[post("/d1/test")]
async fn d1_test(state: &State<SharedManager>) -> Res {
    run(state, |m| {
        let result = m.d1_test()?;
        Ok(serde_json::to_value(result).unwrap_or(serde_json::Value::Null))
    })
    .await
}

#[get("/d1/status")]
async fn d1_status(state: &State<SharedManager>) -> Res {
    run(state, |m| {
        let result = m.d1_status()?;
        Ok(serde_json::to_value(result).unwrap_or(serde_json::Value::Null))
    })
    .await
}

#[post("/d1/sync")]
async fn d1_sync_now(state: &State<SharedManager>) -> Res {
    run(state, |m| {
        let result = m.d1_sync_now()?;
        Ok(serde_json::to_value(result).unwrap_or(serde_json::Value::Null))
    })
    .await
}

#[post("/d1/full_sync")]
async fn d1_full_sync(state: &State<SharedManager>) -> Res {
    run(state, |m| {
        let result = m.d1_full_sync()?;
        Ok(serde_json::to_value(result).unwrap_or(serde_json::Value::Null))
    })
    .await
}

#[post("/d1/reset")]
async fn d1_reset(state: &State<SharedManager>) -> Res {
    run(state, |m| {
        m.d1_clear_checkpoint()?;
        Ok(serde_json::json!({ "ok": true }))
    })
    .await
}

/// D1 同步历史日志：过滤 protocol=d1 的 sync_log 条目。
#[get("/d1/logs?<query..>")]
async fn d1_logs(query: LogQuery, state: &State<SharedManager>) -> Res {
    run(state, move |m| {
        let filter = LogFilter {
            direction: None,
            protocol: Some(SyncProtocol::D1),
            event_type: None,
            limit: query.limit.unwrap_or(20).max(1).min(100) as u64,
            offset: query.offset.unwrap_or(0) as u64,
        };
        let list = m.list_logs(&filter).map_err(|e| e.to_string())?;
        Ok(serde_json::to_value(list).unwrap_or(serde_json::Value::Null))
    }).await
}

type Res = Result<Json<serde_json::Value>, Status>;

/// 在阻塞线程中执行同步管理器操作，统一返回 Json<Value>。
async fn run<F>(state: &State<SharedManager>, f: F) -> Res
where
    F: FnOnce(&SyncManager) -> Result<serde_json::Value, String> + Send + 'static,
{
    let mgr = state.inner().clone();
    tokio::task::spawn_blocking(move || {
        let guard = mgr.lock().map_err(|_| Status::InternalServerError)?;
        f(&guard).map_err(|e| {
            log::error!("[aw-sync] handler error: {e}");
            Status::InternalServerError
        })
    })
    .await
    .map_err(|_| Status::InternalServerError)?
    .map(Json)
}

#[get("/")]
fn root() -> &'static str {
    "aw-sync-rust 同步服务已就绪"
}

#[derive(Serialize, Deserialize)]
struct JoinRequest {
    code: String,
    device: Device,
}

/// 配对码响应
#[derive(Serialize)]
struct PairResp {
    code: String,
    expires_at: String,
}

/// 返回本机设备信息（对端握手 / 广播校验）
#[get("/info")]
async fn info(state: &State<SharedManager>) -> Res {
    let mgr = state.inner().clone();
    tokio::task::spawn_blocking(move || {
        let g = mgr.lock().map_err(|_| Status::InternalServerError)?;
        let dev = g.self_device_info();
        let mut v = serde_json::to_value(dev).unwrap_or(serde_json::Value::Null);
        if let serde_json::Value::Object(ref mut map) = v {
            map.insert(
                "ip_iface".to_string(),
                serde_json::to_value(crate::manager::local_ip_iface()).unwrap_or(serde_json::Value::Null),
            );
        }
        Ok(v)
    })
    .await
    .map_err(|_| Status::InternalServerError)?
    .map(Json)
}

// ---- 设置 ----

#[get("/config")]
async fn config(state: &State<SharedManager>) -> Res {
    run(state, |m| {
        Ok(serde_json::to_value(m.get_config()).unwrap_or(serde_json::Value::Null))
    })
    .await
}

#[put("/config", data = "<cfg>", format = "json")]
async fn config_save(state: &State<SharedManager>, cfg: Json<crate::models::SyncConfig>) -> Res {
    let cfg = cfg.into_inner();
    run(state, move |m| {
        m.set_config(&cfg)?;
        crate::dbglog::info(format!(
            "[config] 同步设置已更新: enabled={}, discovery_method={}, listen_port={}, udp_port={}",
            cfg.enabled, cfg.discovery_method, cfg.listen_port, cfg.udp_port
        ));
        // 若此刻开启了同步，立即启动在线探测后台线程（无需重启服务）。
        m.spawn_probe();
        // 注意：这里曾经调用 reset_discovery_started_for_testing() 来「重建发现线程」，
        // 但那会让下一次 discovery/start 在同一进程里再 spawn 一个 listener 去抢同一个
        // UDP 端口，bind 必然 10048 失败 → 该进程的发现监听彻底哑掉（只发得出去、
        // 收不到任何设备）。现在发现线程只拉起一次并常驻，udp_port 变更需重启进程生效。
        //
        // 是否恢复广播：
        // - 桌面端：发现常驻（discovery_persistent），配置开启即回到广播状态；
        // - Android 端：仍只由「进入局域网同步界面」驱动（discovery/start），
        //   否则 Wi-Fi 自动开启 enabled 时会在后台偷偷广播。
        //
        // 不再按 discovery_method 过滤：该字段只决定「mDNS 首选是否参与」（udp_only = 排障时
        // 关掉 mDNS），UDP 广播始终作为兜底在跑。曾经写死 == "broadcast" 时，默认值改成
        // "mdns" 后这条分支永远不进，桌面端配置开启后 discovery_running 一直是 false。
        if crate::manager::discovery_persistent() && cfg.enabled {
            m.start_discovery();
        }
        Ok(serde_json::to_value(m.get_config()).unwrap_or(serde_json::Value::Null))
    })
    .await
}

// ---- 发现广播开关（进入/离开「局域网同步」界面时由客户端调用） ----

/// 进入界面：开始 UDP 广播宣告与监听处理（不进入界面绝不广播）
#[post("/discovery/start")]
async fn discovery_start(state: &State<SharedManager>) -> Res {
    run(state, |m| {
        m.start_discovery();
        Ok(serde_json::json!({
            "discovery_running": crate::manager::discovery_running()
        }))
    })
    .await
}

/// 离开界面：停止广播与监听处理
#[post("/discovery/stop")]
async fn discovery_stop(state: &State<SharedManager>) -> Res {
    run(state, |m| {
        m.stop_discovery();
        Ok(serde_json::json!({
            "discovery_running": crate::manager::discovery_running()
        }))
    })
    .await
}

/// 后台自愈重发现窗口（秒）：不依赖「是否停留在同步界面」，短暂开一轮 UDP 广播+监听，
/// 让对端广播把设备记录里的 IP 刷新成当前真实地址。secs=0 表示立刻关闭。
///
/// 调用方：① Android 侧回到 Wi-Fi / 本机 IP 变化后；② auto_sync 探测失败后自己开。
#[post("/discovery/burst?<secs>")]
async fn discovery_burst(state: &State<SharedManager>, secs: Option<u64>) -> Res {
    run(state, move |m| {
        let secs = secs.unwrap_or(5);
        if secs == 0 {
            m.stop_discovery_burst();
            Ok(serde_json::json!({ "burst_secs": 0 }))
        } else {
            let secs = secs.clamp(1, 30);
            m.start_discovery_burst(secs);
            Ok(serde_json::json!({ "burst_secs": secs }))
        }
    })
    .await
}

// ---- 配对 ----

#[post("/paircode")]
async fn create_paircode(state: &State<SharedManager>) -> Res {
    run(state, move |m| {
        let pc = m.create_pair_code().map_err(|e| e.to_string())?;
        Ok(serde_json::to_value(PairResp {
            code: pc.code,
            expires_at: pc.expires_at.to_rfc3339(),
        })
        .unwrap())
    })
    .await
}

/// join 的错误需要区分「用户输入错误(400)」与「内部故障(500)」。
type JoinResult = Result<Json<serde_json::Value>, (Status, Json<serde_json::Value>)>;

#[post("/join", data = "<req>", format = "json")]
async fn join(state: &State<SharedManager>, req: Json<JoinRequest>) -> JoinResult {
    let req = req.into_inner();
    let mgr = state.inner().clone();
    // 配对码的具体数字不进任何日志（/log 同网段可读）；密钥同样只在报文中出现一次
    crate::dbglog::info("[pair] /join 收到加入请求");
    // 统一错误载体：(HTTP 状态码, 错误 JSON)
    let joined = tokio::task::spawn_blocking(move || {
            let offered = req.device.device_secret.clone();
            let g = mgr.lock().map_err(|_| {
                (500u16, serde_json::json!({"error": "internal"}))
            })?;
            match g.join_with_code(&req.code, req.device) {
                Ok(mut dev) => {
                    // 密钥协商：采纳加入方带来的密钥，没有则由本机生成，随响应回传
                    let secret = g.adopt_incoming_secret(&dev.id, offered.as_deref()).map_err(|e| {
                        crate::dbglog::error(format!("[pair] join 密钥协商失败: {e}"));
                        (500u16, serde_json::json!({"error": "secret_exchange_failed"}))
                    })?;
                    dev.device_secret = None;
                    let _ = g.add_log(&SyncLogEntry {
                        id: None,
                        timestamp: chrono::Utc::now(),
                        direction: SyncDirection::In,
                        protocol: SyncProtocol::Http,
                        peer_id: Some(dev.id.clone()),
                        event_type: SyncEventType::Pairing,
                        status: SyncStatus::Success,
                        message: Some(format!("已与设备配对: {}", dev.name)),
                        data_size: None,
                        details: None,
                    });
                    crate::dbglog::info(format!(
                        "[pair] /join 成功: 已登记 {}({}), 并返回本机信息+密钥给对方",
                        dev.name, dev.id
                    ));
                    g.save_device(&dev).map_err(|e| {
                        crate::dbglog::error(format!("[pair] join save_device 失败: {e}"));
                        (500u16, serde_json::json!({"error": "internal"}))
                    })?;
                    let me = g.with_outgoing_secret(&g.self_device_info());
                    Ok(serde_json::json!({
                        "device": serde_json::to_value(dev).unwrap_or(serde_json::Value::Null),
                        // 本机信息：加入方收到后把它存进自己的信任列表，实现双向互见
                        "peer": serde_json::to_value(me).unwrap_or(serde_json::Value::Null),
                        "device_secret": secret,
                    }))
                }
                Err(crate::paircode::PairError::InvalidOrExpiredCode) => {
                    // 用户输入错误：配对码无效或已过期 → 400（而非 500）
                    crate::dbglog::warn("[pair] /join 返回 400: 配对码无效或已过期");
                    Err((
                        400u16,
                        serde_json::json!({
                            "error": "invalid_or_expired_code",
                            "message": "配对码无效或已过期，请在发起方重新创建"
                        }),
                    ))
                }
                Err(crate::paircode::PairError::Db(e)) => {
                    crate::dbglog::error(format!("[pair] join 数据库错误: {e}"));
                    Err((500u16, serde_json::json!({"error": "internal"})))
                }
            }
        })
        .await;

    // JoinError（任务 panic 等）也归一为 500
    let result: Result<serde_json::Value, (u16, serde_json::Value)> =
        joined.unwrap_or_else(|_| Err((500u16, serde_json::json!({"error": "internal"}))));

    match result {
        Ok(v) => Ok(Json(v)),
        Err((code, body)) => Err((
            Status::from_code(code).unwrap_or(Status::InternalServerError),
            Json(body),
        )),
    }
}

/// 「使用配对码配对」——加入方入口：本机把对端的配对码提交给**对端**服务器。
/// 与上面的 /join 是一对：/join 由码主接收，/join-remote 由加入方发起。
#[post("/join-remote", data = "<body>", format = "json")]
async fn join_remote(state: &State<SharedManager>, body: Json<serde_json::Value>) -> Res {
    let device_id = body.get("device_id").and_then(|v| v.as_str()).unwrap_or("").to_string();
    let code = body.get("code").and_then(|v| v.as_str()).unwrap_or("").to_string();
    run(state, move |m| {
        let resp = m.join_remote(&device_id, &code)?;
        Ok(serde_json::json!({ "ok": true, "peer": resp }))
    })
    .await
}

/// 手动保存/更新一台对端设备到本地信任列表（配对反向登记用）。
#[post("/devices", data = "<dev>", format = "json")]
async fn add_device(state: &State<SharedManager>, dev: Json<Device>) -> Res {
    let mut d = dev.into_inner();
    d.is_self = false; // 强制非本机
    d.device_secret = None; // 密钥只能经配对握手协商，不接受外部塞入
    // 装机指纹同理：它决定「提示用户把两行并成一行」，能被外部塞入就等于能伪造
    // 「这是你那台旧平板」，把用户往错误的合并目标上引。只有握手路径可以登记它。
    d.machine_uid = None;
    run(state, move |m| {
        m.save_device(&d)?;
        Ok(serde_json::json!({ "saved": true, "id": d.id }))
    })
    .await
}

// ---- 配对（已发现设备 —— 发起/接受/确认） ----

/// 发起配对：本机向目标设备发出配对请求（由本机前端调用）。
#[post("/pair/initiate", data = "<body>", format = "json")]
async fn pair_initiate(state: &State<SharedManager>, body: Json<serde_json::Value>) -> Res {
    let device_id = body.get("device_id").and_then(|v| v.as_str()).unwrap_or("").to_string();
    log::info!("[aw-sync] POST /pair/initiate 收到请求: device_id={}", device_id);
    run(state, move |m| {
        log::info!("[aw-sync] pair_initiate 开始执行: device_id={}", device_id);
        match m.initiate_pair(&device_id) {
            Ok(resp) => {
                log::info!("[aw-sync] pair_initiate 成功: device_id={}", device_id);
                Ok(serde_json::json!({ "ok": true, "peer": resp }))
            }
            Err(e) => {
                log::error!("[aw-sync] pair_initiate 失败: device_id={}, error={}", device_id, e);
                Err(e)
            }
        }
    })
    .await
}

/// 接受配对：本机确认接受目标设备的配对请求（由本机前端调用）。
/// 本机向对方发出 confirm，并把对方标记为已配对。
#[post("/pair/accept", data = "<body>", format = "json")]
async fn pair_accept(state: &State<SharedManager>, body: Json<serde_json::Value>) -> Res {
    let device_id = body.get("device_id").and_then(|v| v.as_str()).unwrap_or("").to_string();
    log::info!("[aw-sync] POST /pair/accept 收到请求: device_id={}", device_id);
    run(state, move |m| {
        log::info!("[aw-sync] pair_accept 开始执行: device_id={}", device_id);
        match m.confirm_pair_with(&device_id) {
            Ok(resp) => {
                log::info!("[aw-sync] pair_accept 成功: device_id={}", device_id);
                Ok(serde_json::json!({ "ok": true, "peer": resp }))
            }
            Err(e) => {
                log::error!("[aw-sync] pair_accept 失败: device_id={}, error={}", device_id, e);
                Err(e)
            }
        }
    })
    .await
}

/// 设备间内部端点：收到对方发来的配对请求，记录到待确认列表。body = 对方 Device。
///
/// 顺带完成密钥协商：报文里带了合法密钥就采纳（发起方决定），否则本机新生成一把；
/// 生效的密钥随响应回传给发起方，两端由此收敛到同一把（日志/接口都不暴露它）。
#[post("/pair/request", data = "<dev>", format = "json")]
async fn pair_request(state: &State<SharedManager>, dev: Json<Device>) -> Res {
    let mut from = dev.into_inner();
    let offered = from.device_secret.take();
    let peer_id = from.id.clone();
    run(state, move |m| {
        m.record_inbound_pair_request(from)?;
        let secret = m.adopt_incoming_secret(&peer_id, offered.as_deref())?;
        // 自报信息带装机指纹：发起方靠它认出「这台被我配过的机器重装了」。
        // 只在用户主动发起的配对握手往返里带，/info 与广播一律不带。
        let me = m.with_outgoing_secret(&m.self_device_info());
        Ok(serde_json::json!({
            "ok": true,
            "me": serde_json::to_value(me).unwrap_or(serde_json::Value::Null),
            "device_secret": secret,
        }))
    })
    .await
}

/// 设备间内部端点：收到对方确认配对，把对方标记为已配对。body = 对方 Device。
#[post("/pair/confirm", data = "<dev>", format = "json")]
async fn pair_confirm(state: &State<SharedManager>, dev: Json<Device>) -> Res {
    let mut peer = dev.into_inner();
    let offered = peer.device_secret.take();
    let peer_id = peer.id.clone();
    run(state, move |m| {
        // 登记/刷新对方：广播里只有哈希别名，配对报文的真名与可达地址要落到信任列表
        let mut p = peer.clone();
        p.is_self = false;
        m.refresh_peer_from_handshake(&p)?;
        // 密钥对齐：以本机已有/新生成的为准，回传后双方一致
        let secret = m.adopt_incoming_secret(&peer_id, offered.as_deref())?;
        // 记录到同步日志（显示报文） - 接收的确认
        let log_entry = SyncLogEntry {
            id: None,
            timestamp: Utc::now(),
            direction: SyncDirection::In,
            protocol: SyncProtocol::Http,
            peer_id: Some(peer.id.clone()),
            event_type: SyncEventType::Pairing,
            status: SyncStatus::Success,
            message: Some(format!("收到来自 {} 的配对确认", peer.name)),
            data_size: None,
            details: None,
        };
        m.add_log(&log_entry)
            .map_err(|e| crate::dbglog::error(format!("[pair] add_log failed: {}", e)))
            .ok();
        // 标记对方为已配对
        m.mark_paired(&peer.id, true)?;
        // 清除待确认记录（如果有的话）
        if let Ok(mut m) = m.inbound_pair_requests.lock() {
            let _ = m.remove(&peer.id);
        }
        // 对方（发起方）也想知道本机是谁：回一份自报信息，双方名字才能对得上
        let me = m.with_outgoing_secret(&m.self_device_info());
        Ok(serde_json::json!({
            "ok": true,
            "me": serde_json::to_value(me).unwrap_or(serde_json::Value::Null),
            "device_secret": secret,
        }))
    })
    .await
}

// ---- 设备 -
#[get("/devices")]
async fn devices(state: &State<SharedManager>) -> Res {
    run(state, |m| {
        let list = m.list_devices().map_err(|e| e.to_string())?;
        // 安全码表：只有已交换过密钥的对端才有值。密钥本身绝不进这个响应
        // （桌面端监听 0.0.0.0，同网段任何人都能读 /devices）。
        let fps = m.security_fingerprints();
        let out: Vec<serde_json::Value> = list
            .iter()
            .map(|d| {
                let mut v = serde_json::to_value(d).unwrap_or(serde_json::Value::Null);
                if let serde_json::Value::Object(ref mut map) = v {
                    map.insert(
                        "incoming_pair_request".into(),
                        serde_json::json!(m.has_inbound_pair_request(&d.id)),
                    );
                    map.insert(
                        "encrypted".into(),
                        serde_json::json!(fps.contains_key(&d.id)),
                    );
                    // 装机指纹相同 = 疑似同一台机器换了 device_id（升级/重装）。两种时刻
                    // 都要提示：它正带着指纹请求配对；或列表里已经躺着两行同指纹的设备。
                    // 没有指纹的行（未配对、纯被发现）在 merge_candidate_for 里一次索引
                    // 查询都不做就返回 None。命中也只给提示，合并与否由用户点。
                    if let Some(c) = m.merge_candidate_for(&d.id) {
                        map.insert("merge_candidate".into(), c);
                    }
                    if let Some(fp) = fps.get(&d.id) {
                        map.insert("fingerprint".into(), serde_json::json!(fp));
                    }
                }
                v
            })
            .collect();
        Ok(serde_json::to_value(out).unwrap_or(serde_json::Value::Null))
    })
    .await
}

#[post("/devices/<id>/sync")]
async fn sync_now(state: &State<SharedManager>, id: String) -> Res {
    // 分阶段加锁版本：网络传输期间不持 manager 锁，同步页轮询不被大快照传输饿死
    let mgr = state.inner().clone();
    let dev_id = id.clone();
    let result = tokio::task::spawn_blocking(move || {
        crate::manager::SyncManager::sync_to_unlocked(&mgr, &dev_id, true)
    })
    .await
    .map_err(|_| Status::InternalServerError)?;
    match result {
        Ok(applied) => Ok(Json(serde_json::json!({
            "device_id": id, "applied": applied.applied, "result": applied
        }))),
        Err(e) => {
            log::error!("[aw-sync] handler error: {e}");
            Err(Status::InternalServerError)
        }
    }
}

#[delete("/devices/<id>")]
async fn device_delete(state: &State<SharedManager>, id: String) -> Res {
    run(state, move |m| {
        let removed = m.delete_device(&id)?;
        let _ = m.add_log(&SyncLogEntry {
            id: None,
            timestamp: Utc::now(),
            direction: SyncDirection::In,
            protocol: SyncProtocol::Http,
            peer_id: Some(id.clone()),
            event_type: SyncEventType::Pairing,
            status: SyncStatus::Success,
            message: Some(format!("已删除设备 {id}")),
            data_size: None,
            details: None,
        });
        Ok(serde_json::json!({ "deleted": removed }))
    })
    .await
}

/// 与 [`run`] 同构，但把失败原因写进响应体。
///
/// 只给归并/清理这类「用户点一下」的端点用：它们的拒绝几乎都是条件不成立
/// （这一行已被归并过、候选刚刚消失），裸 500 会让两端 toast 显示成「服务器错误 (500)」，
/// 用户不知道刚才那一下到底成没成。内部故障仍回 500，只是同样带上可读的文案。
type VerboseRes = std::result::Result<Json<serde_json::Value>, (Status, Json<serde_json::Value>)>;

fn err_with(status: Status, msg: impl ToString) -> (Status, Json<serde_json::Value>) {
    (status, Json(serde_json::json!({ "error": msg.to_string() })))
}

async fn run_verbose<F>(state: &State<SharedManager>, f: F) -> VerboseRes
where
    F: FnOnce(&SyncManager) -> Result<serde_json::Value, String> + Send + 'static,
{
    let mgr = state.inner().clone();
    tokio::task::spawn_blocking(move || match mgr.lock() {
        Err(_) => Err(err_with(
            Status::InternalServerError,
            "同步管理器暂不可用，请重试",
        )),
        Ok(guard) => f(&guard).map(Json).map_err(|e| {
            log::error!("[aw-sync] handler error: {e}");
            err_with(Status::BadRequest, e)
        }),
    })
    .await
    .map_err(|_| err_with(Status::InternalServerError, "内部错误"))?
}

/// 把一行归并进另一行（界面上「这台设备和旧记录疑似同一台机器，合并？」点了「合并」）。
///
/// 方向固定 `from` = 换 id 之前的旧行、`to` = 现在活着的行：旧行那个 device_id 已经
/// 不会再有任何报文上来，留下只会一直显示离线。
/// 只改显示与归因：旧行打 superseded_by、旧 id 的密钥作废、记一条 old→new 别名。
/// 配对本身与密钥一律各归各的，安全码该比还是要比（见 machine_uid 模块的两条纪律）。
#[post("/merge", data = "<body>", format = "json")]
async fn device_merge(state: &State<SharedManager>, body: Json<serde_json::Value>) -> VerboseRes {
    let from = body.get("from").and_then(|v| v.as_str()).unwrap_or("").to_string();
    let to = body.get("to").and_then(|v| v.as_str()).unwrap_or("").to_string();
    run_verbose(state, move |m| {
        if from.is_empty() || to.is_empty() {
            return Err("需要 from 与 to 两个设备 id".to_string());
        }
        m.merge_devices(&from, &to)?;
        Ok(serde_json::json!({ "ok": true, "merged_from": from, "merged_into": to }))
    })
    .await
}

/// 「一键清理」：淘汰静默已久的未配对发现行 + 删掉 N 天没同步成功过的旧配对。
/// body 可选 `{"stale_days": 30}`；未配对行的静默阈值由服务端常量决定，不受此参数影响。
#[post("/devices/purge", data = "<body>", format = "json")]
async fn devices_purge(state: &State<SharedManager>, body: Json<serde_json::Value>) -> VerboseRes {
    let days = body.get("stale_days").and_then(|v| v.as_i64()).unwrap_or(30);
    run_verbose(state, move |m| {
        let (discovered, paired) = m.purge_stale_devices(days)?;
        Ok(serde_json::json!({
            "ok": true, "discovered_removed": discovered, "paired_removed": paired,
        }))
    })
    .await
}

/// 清空所有配对/已发现设备（保留本机记录与同步设置）。对应前端「清空所有配对信息」。
#[delete("/devices/all")]
async fn devices_clear_all(state: &State<SharedManager>) -> Res {
    run(state, move |m| {
        let cleared = m.clear_all_devices()?;
        let _ = m.add_log(&SyncLogEntry {
            id: None,
            timestamp: Utc::now(),
            direction: SyncDirection::In,
            protocol: SyncProtocol::Http,
            peer_id: None,
            event_type: SyncEventType::Pairing,
            status: SyncStatus::Success,
            message: Some(format!("已清空所有配对信息（移除 {cleared} 台设备）")),
            data_size: None,
            details: None,
        });
        Ok(serde_json::json!({ "cleared": cleared }))
    })
    .await
}

#[derive(Deserialize)]
struct AliasBody {
    alias: Option<String>,
}

#[put("/devices/<id>/alias", data = "<body>")]
async fn device_alias(state: &State<SharedManager>, id: String, body: Json<AliasBody>) -> Res {
    let alias_opt = body.into_inner().alias;
    run(state, move |m| {
        let updated = m.update_device_alias(&id, alias_opt.as_deref())?;
        if !updated {
            return Err(format!("设备不存在: {id}").into());
        }
        let msg = if alias_opt.as_ref().map(|a| a.is_empty()).unwrap_or(true) {
            format!("已为设备 {id} 清空别名")
        } else {
            format!("已为设备 {id} 设置别名: {}", alias_opt.as_deref().unwrap_or(""))
        };
        let _ = m.add_log(&SyncLogEntry {
            id: None,
            timestamp: chrono::Utc::now(),
            direction: SyncDirection::In,
            protocol: SyncProtocol::Http,
            peer_id: Some(id.clone()),
            event_type: SyncEventType::Pairing,
            status: SyncStatus::Success,
            message: Some(msg),
            data_size: None,
        details: None,
        });
        Ok(serde_json::json!({ "updated": true, "id": id }))
    })
    .await
}

// ---- 设备同步统计 ----

#[get("/devices/<id>/stats")]
async fn device_stats(state: &State<SharedManager>, id: String) -> Res {
    run(state, move |m| {
        let stats = m.get_device_sync_stats(&id)?;
        Ok(serde_json::to_value(stats).unwrap_or(serde_json::Value::Null))
    })
    .await
}

#[get("/devices/<id>/conflicts")]
async fn device_conflicts(state: &State<SharedManager>, id: String) -> Res {
    run(state, move |m| {
        let conflicts = m.get_device_conflicts(&id)?;
        Ok(serde_json::json!({ "conflicts": conflicts }))
    })
    .await
}

// ---- 同步日志 ----

#[derive(Deserialize, FromForm)]
struct LogQuery {
    direction: Option<String>,
    protocol: Option<String>,
    event_type: Option<String>,
    limit: Option<i64>,
    offset: Option<i64>,
}

#[get("/log?<query..>")]
async fn logs(state: &State<SharedManager>, query: LogQuery) -> Res {
    log::info!(
        "[aw-sync] GET /log 收到请求: direction={:?}, protocol={:?}, event_type={:?}, limit={:?}, offset={:?}",
        query.direction, query.protocol, query.event_type, query.limit, query.offset
    );
    run(state, move |m| {
        // Helper to treat empty string as None
        let empty_as_none = |s: Option<String>| s.filter(|s| !s.is_empty());
        
        let filter = LogFilter {
            direction: empty_as_none(query.direction).as_deref().map(|s| {
                if s == "out" {
                    SyncDirection::Out
                } else {
                    SyncDirection::In
                }
            }),
            protocol: empty_as_none(query.protocol).as_deref().map(|s| match s {
                "udp_broadcast" => SyncProtocol::UdpBroadcast,
                "mdns" => SyncProtocol::Mdns,
                "d1" => SyncProtocol::D1,
                _ => SyncProtocol::Http,
            }),
            event_type: empty_as_none(query.event_type).as_deref().map(|s| match s {
                "discovery" => SyncEventType::Discovery,
                "pairing" => SyncEventType::Pairing,
                "conflict" => SyncEventType::Conflict,
                _ => SyncEventType::Sync,
            }),
            limit: query.limit.unwrap_or(200) as u64,
            offset: query.offset.unwrap_or(0) as u64,
        };
        log::info!("[aw-sync] /log 查询过滤条件: {:?}", filter);
        let list = m.list_logs(&filter).map_err(|e| {
            log::error!("[aw-sync] /log list_logs 失败: {e}");
            e.to_string()
        })?;
        let total = m.log_count().map_err(|e| {
            log::error!("[aw-sync] /log log_count 失败: {e}");
            e.to_string()
        })?;
        log::info!("[aw-sync] /log 返回结果: {} 条记录, total={}", list.len(), total);
        Ok(serde_json::json!({ "logs": list, "total": total }))
    })
    .await
}

/// 清空全部同步报文日志（保留设备与同步设置），供前端「清空日志」按钮调用。
#[delete("/log")]
async fn log_clear(state: &State<SharedManager>) -> Res {
    run(state, move |m| {
        m.truncate_logs(0)?;
        Ok(serde_json::json!({ "cleared": true }))
    })
    .await
}

// ---- 对端写入（供其它设备推送） ----

/// 对端推送入口。body 直接收原始字节：明文快照可能有几十 MB，
/// 而 Rocket 的 `String`/`json` 之外的数据上限默认只有 8KiB（bytes/string 档）。
#[post("/push", data = "<body>", format = "json")]
async fn push(state: &State<SharedManager>, body: rocket::data::Capped<Vec<u8>>) -> Res {
    let Ok(raw) = std::str::from_utf8(&body.value) else {
        return Err(Status::BadRequest);
    };
    let raw = raw.to_string();
    run(state, move |m| {
        // body 可能是明文快照（旧端 / 未配对），也可能是 {v,kid,ts,n,ct} 信封
        let snap = m.decode_snapshot_body(&raw, crate::crypto::PATH_PUSH)?;
        let applied = m.apply_snapshot(&snap)?;
        crate::dbglog::info(format!("[push] /push 处理完成: 应用记录数 {}", applied.applied));
        let peer_id = snap.source_device.as_ref().map(|d| d.id.clone());
        let peer_id_str = peer_id.clone().unwrap_or_default();
        let peer_name = snap
            .source_device
            .as_ref()
            .map(|d| d.name.clone())
            .unwrap_or_default();
        // 若对端尚未在信任列表，自动加入
        if let Some(dev) = &snap.source_device {
            if let Ok(existing) = m.get_device(&dev.id) {
                if existing.is_none() {
                    let _ = m.save_device(dev);
                }
            }
        }
        let details = if applied.records.is_empty() { None } else { Some(applied.records.clone()) };
        let _ = m.add_log(&SyncLogEntry {
            id: None,
            timestamp: chrono::Utc::now(),
            direction: SyncDirection::In,
            protocol: SyncProtocol::Http,
            peer_id,
            event_type: SyncEventType::Sync,
            status: SyncStatus::Success,
            message: Some(format!(
                "本机({}) 收到来自 {}({}) 的同步，应用记录数: {}",
                m.self_id(),
                peer_name,
                peer_id_str,
                applied.applied
            )),
            data_size: Some(
                snap.activity.as_ref().map_or(0, |s| s.len() as u64)
                    + snap.inbox.as_ref().map_or(0, |s| s.len() as u64)
                    + snap.todo.as_ref().map_or(0, |s| s.len() as u64),
            ),
            details,
        });
        Ok(serde_json::json!({ "applied": applied.applied, "result": applied }))
    })
    .await
}

// ---- WiFi 热点传输（实验性）：快照导出 / 拉取合并 ----
// 由扫码方（传送方）在本机与对端之间中转数据：
//   1. GET  /snapshot：导出本机快照（含 source_device）；
//   2. POST /apply  ：把「从对端拉来的快照」合并进本机（与 /push 复用同一 apply_snapshot）。

/// 导出本机快照。`from` = 请求方的设备 id（谁在拉我）：
/// 本机与该 id 有共享密钥时，响应体整个换成信封；没有则仍返回明文（旧端/热点直连）。
/// `from` 无需可信：它只用来**选**密钥，选错就解不开，拉取端会直接判定失败。
#[get("/snapshot?<from>")]
async fn snapshot(state: &State<SharedManager>, from: Option<String>) -> Res {
    run(state, move |m| {
        let mut snap = SyncSnapshot {
            source_device: Some(m.self_device_info()),
            ..Default::default()
        };
        m.export(&mut snap);
        let body = m.encode_snapshot_body(from.as_deref(), crate::crypto::PATH_SNAPSHOT, &snap)?;
        let value: serde_json::Value =
            serde_json::from_str(&body).map_err(|e| format!("快照序列化失败: {e}"))?;
        Ok(value)
    })
    .await
}

#[post("/apply", data = "<snap>", format = "json")]
async fn apply(state: &State<SharedManager>, snap: Json<SyncSnapshot>) -> Res {
    let snap = snap.into_inner();
    run(state, move |m| {
        let applied = m.apply_snapshot(&snap)?;
        let peer = snap.source_device.clone().unwrap_or_else(|| Device {
            id: String::new(),
            name: "未知设备".into(),
            device_kind: crate::models::DeviceKind::Unknown,
            ip: String::new(),
            port: 0,
            paired_at: Utc::now(),
            last_sync_at: None,
            last_seen_at: None,
            is_online: false,
            is_self: false,
            paired: false,
            alias: None,
            device_secret: None,
            machine_uid: None,
        });
        // 若对端尚未在信任列表，自动加入
        if !peer.id.is_empty() {
            if let Ok(existing) = m.get_device(&peer.id) {
                if existing.is_none() {
                    let _ = m.save_device(&peer);
                }
            }
        }
        crate::dbglog::info(format!("[wifi] /apply 处理完成: 应用记录数 {}", applied.applied));
        let details = if applied.records.is_empty() { None } else { Some(applied.records.clone()) };
        let _ = m.add_log(&SyncLogEntry {
            id: None,
            timestamp: chrono::Utc::now(),
            direction: SyncDirection::In,
            protocol: SyncProtocol::Http,
            peer_id: Some(peer.id.clone()),
            event_type: SyncEventType::Sync,
            status: SyncStatus::Success,
            message: Some(format!(
                "WiFi 传输：本机({}) 已合并来自 {}({}) 的数据，应用记录数: {}",
                m.self_id(),
                peer.name,
                peer.id,
                applied.applied
            )),
            data_size: Some(
                snap.activity.as_ref().map_or(0, |s| s.len() as u64)
                    + snap.inbox.as_ref().map_or(0, |s| s.len() as u64)
                    + snap.todo.as_ref().map_or(0, |s| s.len() as u64),
            ),
            details,
        });
        Ok(serde_json::json!({ "applied": applied.applied, "result": applied }))
    })
    .await
}

/// 热点传输的回程推送：客户端只说「把本机快照推到 ip:port」，导出 + 信封化 + HTTP 全在 Rust 做。
///
/// 为什么不给 Kotlin/Qt 自己 POST /push：`/push` 现在拒收「已协商密钥却仍是明文」的降级报文，
/// 而密钥按设计从不出服务端接口，手搓 HTTP 的客户端根本拿不到。让本机代发，
/// 两端就能继续复用同一套策略：已配对 → 信封，未配对 → 明文（与旧行为一致）。
#[derive(Deserialize)]
struct PushToRequest {
    ip: String,
    port: u16,
    /// 对端设备 id（热点场景可从对端 /snapshot 的 source_device.id 取到）；
    /// 只用于**选**密钥，选不到就退回明文，选错则对端解不开并报错。
    #[serde(default)]
    device_id: Option<String>,
    #[serde(default)]
    name: Option<String>,
}

#[post("/push-to", data = "<req>", format = "json")]
async fn push_to(state: &State<SharedManager>, req: Json<PushToRequest>) -> Res {
    let Json(req) = req;
    if req.ip.trim().is_empty() || req.port == 0 {
        return Err(Status::BadRequest);
    }
    let mgr = state.inner().clone();
    // 快照可能有几十 MB，推送又是 60s 级阻塞请求：学 sync_now 分阶段拿锁，别把同步接口饿死
    tokio::task::spawn_blocking(move || {
        let target_id = req.device_id.clone().unwrap_or_default();
        let target_name = req.name.clone().unwrap_or_default();
        let (snap, secret) = {
            let g = mgr.lock().map_err(|_| Status::InternalServerError)?;
            let mut snap = SyncSnapshot {
                source_device: Some(g.self_device_info()),
                ..Default::default()
            };
            g.export(&mut snap);
            let secret = if target_id.is_empty() { None } else { g.device_secret(&target_id) };
            (snap, secret)
        };
        let target = Device {
            id: if target_id.is_empty() { format!("hotspot-{}", req.ip) } else { target_id },
            name: if target_name.is_empty() { req.ip.clone() } else { target_name },
            device_kind: crate::models::DeviceKind::Unknown,
            ip: req.ip,
            port: req.port,
            paired_at: Utc::now(),
            last_sync_at: None,
            last_seen_at: None,
            is_online: true,
            is_self: false,
            paired: false,
            alias: None,
            device_secret: None,
            machine_uid: None,
        };
        let applied = crate::transport::push_snapshot(&target, &snap, secret.as_deref())
            .map_err(|e| {
                log::error!("[aw-sync] /push-to 推送失败: {e}");
                Status::InternalServerError
            })?;
        Ok(Json(serde_json::json!({ "applied": applied, "encrypted": secret.is_some() })))
    })
    .await
    .map_err(|_| Status::InternalServerError)?
}

// ---- 回收站（trash，P0）----

#[derive(FromForm)]
struct TrashQuery {
    kind: Option<String>,
}

#[get("/trash?<query..>")]
async fn trash_list(state: &State<SharedManager>, query: TrashQuery) -> Res {
    run(state, move |m| {
        let list = m.list_trash(query.kind.as_deref()).map_err(|e| e.to_string())?;
        let count = m.trash_count().map_err(|e| e.to_string())?;
        Ok(serde_json::json!({ "trash": list, "count": count }))
    })
    .await
}

#[post("/trash/<id>/restore")]
async fn trash_restore(state: &State<SharedManager>, id: i64) -> Res {
    run(state, move |m| {
        let restored = m.restore_trash(id).map_err(|e| e.to_string())?;
        Ok(serde_json::json!({ "restored": restored, "id": id }))
    })
    .await
}

#[delete("/trash/<id>")]
async fn trash_delete(state: &State<SharedManager>, id: i64) -> Res {
    run(state, move |m| {
        let deleted = m.delete_trash(id).map_err(|e| e.to_string())?;
        Ok(serde_json::json!({ "deleted": deleted, "id": id }))
    })
    .await
}

/// 手动清空回收站（全部删除）。
#[delete("/trash")]
async fn trash_clear_all(state: &State<SharedManager>) -> Res {
    run(state, move |m| {
        let mut deleted = 0usize;
        let list = m.list_trash(None).map_err(|e| e.to_string())?;
        for t in &list {
            if m.delete_trash(t.id).map_err(|e| e.to_string())? {
                deleted += 1;
            }
        }
        Ok(serde_json::json!({ "cleared": deleted }))
    })
    .await
}

/// 创建 SyncManager、挂载同步路由，并按需启动后台发现线程。
/// 供 aw-server 的桌面入口(main.rs)与 Android 入口(android/mod.rs)复用。
pub fn install_sync(
    rocket: Rocket<Build>,
    data_dir: &std::path::Path,
    device_id: String,
    start_discovery: bool,
) -> Result<Rocket<Build>, String> {
    let mgr = SyncManager::new(data_dir, device_id)?;
    if start_discovery {
        if let Ok(g) = mgr.lock() {
            let _ = g.spawn_discovery();
        }
    }
    Ok(mount_rocket(rocket, mgr))
}

/// 发现状态：前端据此显示「广播发现运行中 / 未开启」状态条
#[get("/status")]
async fn status(state: &State<SharedManager>) -> Res {
    run(state, |m| {
        let cfg = m.get_config();
        let me = m.self_device_info();
        Ok(serde_json::json!({
            "enabled": cfg.enabled,
            "http_enabled": cfg.http_enabled,
            "discovery_method": cfg.discovery_method,
            "discovery_running": crate::manager::discovery_running(),
            "udp_port": cfg.udp_port,
            "listen_port": cfg.listen_port,
            "self_device": serde_json::to_value(me).unwrap_or(serde_json::Value::Null),
        }))
    })
    .await
}

/// 数据修订号：客户端低频轮询，值变了说明本地业务库被远端改动过 → 静默刷新列表。
/// 比轮询 sync_log 可靠：无变更的自动同步轮刻意不写日志，日志出现断档并不代表没在同步。
#[get("/revision")]
async fn revision() -> Json<serde_json::Value> {
    Json(serde_json::json!({ "revision": crate::manager::data_revision() }))
}

#[derive(FromForm)]
struct DebugLogQuery {
    after: Option<u64>,
}

/// 浏览器 F12 调试日志：增量返回 Rust 侧同步日志（前端轮询后 console.log）。
#[get("/debuglog?<query..>")]
async fn debug_log(query: DebugLogQuery) -> Json<Vec<crate::dbglog::DebugEntry>> {
    Json(crate::dbglog::snapshot_after(query.after.unwrap_or(0)))
}

/// 挂载同步路由（需已创建 SyncManager）。
pub fn mount_rocket(rocket: Rocket<Build>, mgr: SharedManager) -> Rocket<Build> {
    info!("[aw-sync] 注册同步路由到 /api/0/sync");
    // 通道自证：只要服务挂载成功，环形缓冲必有条目，前端 F12 可立即验证通道
    crate::dbglog::info(
        "[server] aw-sync-rust 同步服务已挂载，debuglog 通道就绪 (GET /api/0/sync/debuglog?after=0)",
    );
    rocket
        .manage(mgr)
        .mount(
            "/api/0/sync",
            routes![
                root, info, config, config_save, discovery_start, discovery_stop, discovery_burst,
                create_paircode, join, join_remote,
                pair_initiate, pair_accept, pair_request, pair_confirm,
                devices, add_device,
                sync_now, device_delete, device_alias, devices_clear_all,
                device_merge, devices_purge,
                device_stats, device_conflicts,
                logs, log_clear, push, push_to, apply, snapshot, debug_log, status, revision,
                trash_list, trash_restore, trash_delete, trash_clear_all,
                d1_test, d1_status, d1_sync_now, d1_full_sync, d1_reset, d1_logs
            ],
        )
}