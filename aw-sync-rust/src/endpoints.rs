//! 局域网同步 REST API（挂载于 /api/0/sync），供 aw-webui 调用与对端互操作。

use log::info;
use rocket::form::FromForm;
use rocket::http::Status;
use rocket::serde::json::Json;
use rocket::{delete, get, post, put, routes, Build, Rocket, State};
use serde::{Deserialize, Serialize};

use crate::manager::{SharedManager, SyncManager};
use crate::models::{
    Device, SyncConfig, SyncDirection, SyncEventType, SyncLogEntry, SyncProtocol, SyncSnapshot,
    SyncStatus,
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
        // 若此刻开启了同步，立即启动广播发现与在线探测后台线程（无需重启服务）
        m.spawn_discovery();
        m.spawn_probe();
        Ok(serde_json::to_value(m.get_config()).unwrap_or(serde_json::Value::Null))
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
    // 统一错误载体：(HTTP 状态码, 错误 JSON)
    let joined = tokio::task::spawn_blocking(move || {
            let req_code_for_log = req.code.clone();
            crate::dbglog::info(format!("[pair] /join 收到请求: code={}", req.code));
            let g = mgr.lock().map_err(|_| {
                (500u16, serde_json::json!({"error": "internal"}))
            })?;
            match g.join_with_code(&req.code, req.device) {
                Ok(dev) => {
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
                        "[pair] /join 成功: 已登记 {}({}), 并返回本机信息给对方",
                        dev.name, dev.id
                    ));
                    g.save_device(&dev).map_err(|e| {
                        crate::dbglog::error(format!("[pair] join save_device 失败: {e}"));
                        (500u16, serde_json::json!({"error": "internal"}))
                    })?;
                    let me = g.self_device_info();
                    Ok(serde_json::json!({
                        "device": serde_json::to_value(dev).unwrap_or(serde_json::Value::Null),
                        // 本机信息：加入方收到后把它存进自己的信任列表，实现双向互见
                        "peer": serde_json::to_value(me).unwrap_or(serde_json::Value::Null),
                    }))
                }
                Err(crate::paircode::PairError::InvalidOrExpiredCode) => {
                    // 用户输入错误：配对码无效或已过期 → 400（而非 500）
                    crate::dbglog::warn(format!(
                        "[pair] /join 返回 400: 配对码无效或已过期 (code={})",
                        req_code_for_log
                    ));
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

/// 手动保存/更新一台对端设备到本地信任列表（配对反向登记用）。
#[post("/devices", data = "<dev>", format = "json")]
async fn add_device(state: &State<SharedManager>, dev: Json<Device>) -> Res {
    let mut d = dev.into_inner();
    d.is_self = false; // 强制非本机
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
#[post("/pair/request", data = "<dev>", format = "json")]
async fn pair_request(state: &State<SharedManager>, dev: Json<Device>) -> Res {
    let from = dev.into_inner();
    run(state, move |m| {
        m.record_inbound_pair_request(from)?;
        let me = m.self_device_info();
        Ok(serde_json::json!({ "ok": true, "me": serde_json::to_value(me).unwrap_or(serde_json::Value::Null) }))
    })
    .await
}

/// 设备间内部端点：收到对方确认配对，把对方标记为已配对。body = 对方 Device。
#[post("/pair/confirm", data = "<dev>", format = "json")]
async fn pair_confirm(state: &State<SharedManager>, dev: Json<Device>) -> Res {
    let peer = dev.into_inner();
    run(state, move |m| {
        // 确保对方已在信任列表（若尚未发现则补登记）
        if m.get_device(&peer.id)?.is_none() {
            let mut p = peer.clone();
            p.is_self = false;
            m.save_device(&p)?;
        }
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
        Ok(serde_json::json!({ "ok": true }))
    })
    .await
}

// ---- 设备 -

#[get("/devices")]
async fn devices(state: &State<SharedManager>) -> Res {
    run(state, |m| {
        let list = m.list_devices().map_err(|e| e.to_string())?;
        // 为每个设备附带「是否有待本机确认的配对请求」，供前端显示「接受配对」按钮
        let out: Vec<serde_json::Value> = list
            .iter()
            .map(|d| {
                let mut v = serde_json::to_value(d).unwrap_or(serde_json::Value::Null);
                if let serde_json::Value::Object(ref mut map) = v {
                    map.insert(
                        "incoming_pair_request".into(),
                        serde_json::json!(m.has_inbound_pair_request(&d.id)),
                    );
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
    run(state, move |m| {
        let applied = m.sync_to(&id)?;
        Ok(serde_json::json!({ "device_id": id, "applied": applied.applied, "result": applied }))
    })
    .await
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

#[post("/push", data = "<snap>", format = "json")]
async fn push(state: &State<SharedManager>, snap: Json<SyncSnapshot>) -> Res {
    let snap = snap.into_inner();
    run(state, move |m| {
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

#[get("/snapshot")]
async fn snapshot(state: &State<SharedManager>) -> Res {
    run(state, |m| {
        let mut snap = SyncSnapshot {
            source_device: Some(m.self_device_info()),
            ..Default::default()
        };
        m.export(&mut snap);
        Ok(serde_json::to_value(snap).unwrap_or(serde_json::Value::Null))
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
                root, info, config, config_save, create_paircode, join,
                pair_initiate, pair_accept, pair_request, pair_confirm,
                devices, add_device,
                sync_now, device_delete, device_alias, devices_clear_all,
                device_stats, device_conflicts,
                logs, log_clear, push, apply, snapshot, debug_log, status,
                trash_list, trash_restore, trash_delete, trash_clear_all,
                d1_test, d1_status, d1_sync_now
            ],
        )
}