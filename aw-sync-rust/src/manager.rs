//! SyncManager：面向 aw-server 的业务门面。

//! 聚合 sync.db 持久化、配对、目标库导出/导入、HTTP 推送、设备发现。



use std::path::Path;

use std::sync::{Arc, Mutex, OnceLock};



use chrono::Utc;

use std::collections::HashMap;



use crate::models::{
    ApplyResult, ConflictSummary, Device, DeviceKind, DeviceSyncStats, PairCode, SyncConfig,
    SyncDirection, SyncEventType, SyncLogEntry, SyncProtocol, SyncSnapshot, SyncStatus, TrashEntry,
};

use crate::paircode::{PairError, PairingManager};

use crate::serialize::{export_activity, export_inbox, export_todo, import_activity, import_inbox, import_todo};

use crate::storage::{LogFilter, SyncDb};

use crate::discovery;



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
        if cfg.sync_todo {
            let p = self.data_dir.join("todo.db");
            sn.todo = export_todo(p.as_path()).ok();
        }
    }



    /// 接收并写入本地目标库（幂等 upsert）。返回应用记录条数。

    /// 接收并写入本地目标库（按 uuid 逻辑键 + rev 仲裁合并，P0 起启用）。
    /// 返回结构化合并结果；被覆盖方归档进回收站、冲突记录写入 sync_conflicts。
    pub fn apply_snapshot(&self, snap: &SyncSnapshot) -> Result<ApplyResult, String> {
        let src = snap
            .source_device
            .as_ref()
            .map(|d| format!("{}({})", d.name, d.id))
            .unwrap_or_else(|| "未知设备".into());
        let src_id = snap.source_device.as_ref().map(|d| d.id.clone());
        crate::dbglog::info(format!("[sync] 收到来自 {} 的同步快照", src));

        let mut result = ApplyResult::default();
        let db = self.db();

        if let Some(activity) = &snap.activity {
            match import_activity(self.data_dir.join("sqlite.db").as_path(), activity) {
                Ok(out) => persist_outcome(&mut result, out, src_id.as_deref(), &db),
                Err(e) => result.errors.push(format!("activity: {e}")),
            }
        }
        if let Some(inbox) = &snap.inbox {
            match import_inbox(self.data_dir.join("inbox.db").as_path(), inbox) {
                Ok(out) => persist_outcome(&mut result, out, src_id.as_deref(), &db),
                Err(e) => result.errors.push(format!("inbox: {e}")),
            }
        }
        if let Some(todo) = &snap.todo {
            match import_todo(self.data_dir.join("todo.db").as_path(), todo) {
                Ok(out) => persist_outcome(&mut result, out, src_id.as_deref(), &db),
                Err(e) => result.errors.push(format!("todo: {e}")),
            }
        }

        if result.archived > 0 {
            let _ = self.add_log(&SyncLogEntry {
                id: None,
                timestamp: Utc::now(),
                direction: SyncDirection::In,
                protocol: SyncProtocol::Http,
                peer_id: src_id.clone(),
                event_type: SyncEventType::Conflict,
                status: SyncStatus::Success,
                message: Some(format!(
                    "合并来自 {} 的同步：{} 条进入回收站（自动仲裁，无需人工处理）",
                    src, result.archived
                )),
                data_size: None,
                details: None,
            });
        }
        crate::dbglog::info(format!(
            "[sync] 快照应用完成: 来源 {}, applied={} archived={} errors={}",
            src, result.applied, result.archived, result.errors.len()
        ));
        Ok(result)
    }




    /// 与某设备执行一次「拉-合-推」双向同步（分阶段加锁，不要求调用方持锁）。
    /// ① 短锁取对端 → ② 网络拉快照（不持锁）→ ③ 短锁合并入本地（冲突自动仲裁、被覆盖方进回收站）
    /// → ④ 短锁导出本地 → ⑤ 网络推送（不持锁）→ ⑥ 短锁登记与日志。
    /// 网络传输期间不持有 manager 锁：大快照/慢链路传输不再饿死同步页等其余接口。
    pub fn sync_to_unlocked(
        mgr: &SharedManager,
        peer_id: &str,
        log_noop: bool,
    ) -> Result<ApplyResult, String> {
        let lock_err = || "同步管理器锁不可用".to_string();

        // ① 取对端（短锁）
        let peer = {
            let g = mgr.lock().map_err(|_| lock_err())?;
            g.get_device(peer_id)?.ok_or("未找到目标设备")?
        };

        // ② 拉取对端快照（不持锁）
        let remote = crate::transport::fetch_snapshot(&peer)?;

        // ③ 合并进本地（短锁）
        let (applied, self_id) = {
            let g = mgr.lock().map_err(|_| lock_err())?;
            let applied = g.apply_snapshot(&remote)?;
            (applied, g.self_id.clone())
        };

        // ④ 导出本地（短锁，含新合并内容；对端侧合并为幂等，可安全重推）
        let snap = {
            let g = mgr.lock().map_err(|_| lock_err())?;
            let mut snap = SyncSnapshot {
                source_device: Some(g.self_device_info()),
                ..Default::default()
            };
            g.export(&mut snap);
            snap
        };

        // ⑤ 推送给对端（不持锁）
        let pushed = crate::transport::push_snapshot(&peer, &snap).unwrap_or(0);

        // ⑥ 登记与日志（短锁）
        let size = snap
            .activity
            .as_ref()
            .map_or(0, |s| s.len() as u64)
            + snap.inbox.as_ref().map_or(0, |s| s.len() as u64)
            + snap.todo.as_ref().map_or(0, |s| s.len() as u64);
        {
            let g = mgr.lock().map_err(|_| lock_err())?;
            g.db()
                .mark_synced(&peer.id, Utc::now())
                .map_err(|e| e.to_string())?;
            // 无实际变更且为周期自动同步时跳过成功日志（静默模式，避免刷爆同步报文）
            let noop = applied.applied == 0 && pushed == 0 && applied.archived == 0;
            if log_noop || !noop {
                g.add_log(&SyncLogEntry {
                    id: None,
                    timestamp: Utc::now(),
                    direction: SyncDirection::Out,
                    protocol: SyncProtocol::Http,
                    peer_id: Some(peer.id.clone()),
                    event_type: SyncEventType::Sync,
                    status: SyncStatus::Success,
                    message: Some(format!(
                        "本机({}) 与 {}({}) 双向同步完成: 拉取应用 {} 条(新增{} 更新{} 删除{})，推送 {} 条，归档 {} 条",
                        self_id,
                        peer.name,
                        peer.id,
                        applied.applied,
                        applied.created,
                        applied.updated,
                        applied.deleted,
                        pushed,
                        applied.archived
                    )),
                    data_size: Some(size),
                    details: if applied.records.is_empty() { None } else { Some(applied.records.clone()) },
                })?;
            }
        }

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
        details: None,
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
            details: None,
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
            details: None,
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



    /// D1 云同步后台线程：按配置间隔定期触发 D1 双向同步。
    /// 进程内用原子标志保证只启动一次,返回值恒为（空线程句柄或实际句柄）。
    pub fn spawn_d1_sync(&self) -> std::thread::JoinHandle<()> {
        use std::sync::atomic::{AtomicBool, Ordering};

        static D1_SYNC_STARTED: AtomicBool = AtomicBool::new(false);

        if D1_SYNC_STARTED.swap(true, Ordering::SeqCst) {
            return std::thread::spawn(|| {});
        }

        let data_dir = self.data_dir.clone();
        let self_id = self.self_id.clone();

        std::thread::Builder::new()
            .name("aw-sync-d1".into())
            .spawn(move || loop {
                let (enabled, interval_secs) = {
                    if let Ok(db) = SyncDb::open(&data_dir) {
                        let cfg = db.get_config();
                        (cfg.d1_enabled, cfg.d1_sync_interval.max(10) as u64)
                    } else {
                        (false, 300)
                    }
                };

                if enabled {
                    let cfg = SyncDb::open(&data_dir)
                        .map(|db| db.get_config())
                        .unwrap_or_default();
                    if cfg.d1_account_id.trim().is_empty()
                        || cfg.d1_database_id.trim().is_empty()
                        || cfg.d1_api_token.trim().is_empty()
                    {
                        crate::dbglog::warn(
                            "[d1] D1 未配置完整，跳过本次自动同步".to_string(),
                        );
                    } else {
                        crate::dbglog::info(
                            "[d1] 后台自动同步触发...".to_string(),
                        );
                        match crate::d1_sync::d1_sync_now(&data_dir, &self_id, &cfg) {
                            Ok(result) => {
                                crate::dbglog::info(format!(
                                    "[d1] 后台同步完成: ok={}, pushed={}n/{}t, pulled={}n/{}t, conflicts={}, errors={}",
                                    result.ok,
                                    result.pushed_notes,
                                    result.pushed_todos,
                                    result.pulled_notes,
                                    result.pulled_todos,
                                    result.conflicts,
                                    result.errors.len()
                                ));
                                // 写入 sync_log
                                if let Ok(db) = SyncDb::open(&data_dir) {
                                    let log_entry = SyncLogEntry {
                                        id: None,
                                        timestamp: Utc::now(),
                                        direction: SyncDirection::Out,
                                        protocol: SyncProtocol::D1,
                                        peer_id: None,
                                        event_type: SyncEventType::Sync,
                                        status: if result.ok { SyncStatus::Success } else { SyncStatus::Failed },
                                        message: Some(format!(
                                            "推送 {} 笔记 / {} TODO · 拉取 {} 笔记 / {} TODO · 冲突 {}",
                                            result.pushed_notes, result.pushed_todos,
                                            result.pulled_notes, result.pulled_todos,
                                            result.conflicts
                                        )),
                                        data_size: None,
                                        details: None,
                                    };
                                    let _ = db.add_log(&log_entry);
                                }
                                if result.ok {
                                    let now = Utc::now().to_rfc3339();
                                    if let Ok(db) = SyncDb::open(&data_dir) {
                                        let _ = db.set_d1_last_sync(&now);
                                    }
                                }
                            }
                            Err(e) => {
                                crate::dbglog::error(format!("[d1] 后台同步失败: {e}"));
                            }
                        }
                    }
                }

                std::thread::sleep(std::time::Duration::from_secs(interval_secs));
            })
            .unwrap_or_else(|_| std::thread::spawn(|| {}))
    }

    /// 局域网自动同步后台线程：enabled 时按 sync_interval 周期对所有已配对设备
    /// 执行「拉-合-推」双向同步（与手动同步共用 sync_to）。
    /// 与 spawn_d1_sync 同模式：进程内只启动一次、常驻循环、每轮重读配置。
    /// 因需调用 sync_to（&self 方法），此处以 SharedManager 为参而非 &self。
    /// 三档模式即 sync_interval 的预设：狂暴 10 / 平和 300 / 静默 1800（秒）。
    pub fn spawn_auto_sync(mgr: &SharedManager) -> std::thread::JoinHandle<()> {
        use std::sync::atomic::{AtomicBool, Ordering};

        static AUTO_SYNC_STARTED: AtomicBool = AtomicBool::new(false);

        if AUTO_SYNC_STARTED.swap(true, Ordering::SeqCst) {
            return std::thread::spawn(|| {});
        }

        let data_dir = match mgr.lock() {
            Ok(g) => g.data_dir.clone(),
            Err(_) => return std::thread::spawn(|| {}),
        };
        let mgr = Arc::clone(mgr);

        std::thread::Builder::new()
            .name("aw-sync-auto".into())
            .spawn(move || {
                let mut prev_enabled = false;
                loop {
                    let (enabled, interval_secs) = match SyncDb::open(&data_dir) {
                        Ok(db) => {
                            let cfg = db.get_config();
                            (cfg.enabled, cfg.sync_interval.max(5))
                        }
                        Err(_) => (false, 10u64),
                    };

                    if enabled {
                        // 关→开（如刚连上 Wi-Fi 自动开启）：不管在线状态立即尝试一轮
                        let force = !prev_enabled;
                        if force {
                            crate::dbglog::info(
                                "[auto] 局域网同步已开启，立即同步一轮".to_string(),
                            );
                        }
                        if let Ok(db) = SyncDb::open(&data_dir) {
                            let targets: Vec<crate::models::Device> = match db.get_devices() {
                                Ok(devices) => devices
                                    .into_iter()
                                    .filter(|d| d.paired && !d.is_self && (force || d.is_online))
                                    .collect(),
                                Err(_) => Vec::new(),
                            };
                            for d in targets {
                                // 先做轻量可达性探测（不持锁，连接 2s/读取 3s）：对端离网时
                                // 快速跳过，避免拿住全局锁等满 HTTP 总超时、饿死其余接口
                                if crate::transport::probe_online(&d).is_err() {
                                    crate::dbglog::info(format!(
                                        "[auto] 设备 {}({}) 不可达，本轮跳过同步",
                                        d.name, d.id
                                    ));
                                    continue;
                                }
                                // 分阶段加锁同步：网络传输不持锁，UI 请求只与短小的本地阶段竞争
                                if let Err(e) = SyncManager::sync_to_unlocked(&mgr, &d.id, false) {
                                    crate::dbglog::warn(format!(
                                        "[auto] 与设备 {}({}) 自动同步失败: {e}",
                                        d.name, d.id
                                    ));
                                }
                            }
                        }
                    }
                    prev_enabled = enabled;

                    // 5 秒步进睡眠：修改频率配置后最迟 5 秒生效
                    let mut waited = 0u64;
                    while waited < interval_secs {
                        let step = 5u64.min(interval_secs - waited);
                        std::thread::sleep(std::time::Duration::from_secs(step));
                        waited += step;
                    }
                }
            })
            .unwrap_or_else(|_| std::thread::spawn(|| {}))
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

                // 未开启时空转：enabled 由 Android 侧按 Wi-Fi 状态自动驱动
                let lan_enabled = SyncDb::open(&data_dir)

                    .map(|db| db.get_config().enabled)

                    .unwrap_or(false);

                if !lan_enabled {

                    std::thread::sleep(std::time::Duration::from_secs(5));

                    continue;

                }

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



    /// 兼容入口（桌面启动 / install_sync）：确保发现线程已拉起；配置开启时立即开始广播。
    /// Android 端改用 start_discovery / stop_discovery（由「进入/离开局域网同步界面」驱动）。
    pub fn spawn_discovery(&self) -> Vec<std::thread::JoinHandle<()>> {

        self.ensure_discovery_threads();

        let cfg = self.get_config();

        if cfg.enabled && cfg.discovery_method == "broadcast" {

            set_discovery_active(true);

        }

        Vec::new()

    }



    /// 进入「局域网同步」界面：开始广播宣告与监听处理。
    /// 线程进程内常驻（只拉起一次），实际收发由 DISCOVERY_ACTIVE 开关逐轮控制。

    pub fn start_discovery(&self) {

        self.ensure_discovery_threads();

        set_discovery_active(true);

    }



    /// 离开「局域网同步」界面：停止广播与监听处理（不进入界面绝不广播）。

    pub fn stop_discovery(&self) {

        set_discovery_active(false);

    }



    fn ensure_discovery_threads(&self) {

        let cfg = self.get_config();

        if cfg.discovery_method != "broadcast" {

            return;

        }

        if DISCOVERY_THREADS_STARTED.swap(true, Ordering::SeqCst) {

            return;

        }

        let udp = cfg.udp_port;

        let self_device = self.self_device_info();



        // 周期广播自己的信息（循环内每轮检查 discovery_active，并重解析本机 IP）

        let dev = self_device.clone();

        let data_dir = self.data_dir.clone();

        let _ = std::thread::Builder::new()

            .name("aw-sync-announce".into())

            .spawn(move || {

                discovery::broadcast_loop(

                    discovery::SelfInfo { device: dev, data_dir },

                    udp,

                    std::time::Duration::from_secs(5),

                )

            });



        // 监听广播并把发现的设备写入信任列表

        let db: discovery::SharedDb = Arc::clone(&self.db);

        let sid = self_device.id.clone();

        let _ = std::thread::Builder::new()

            .name("aw-sync-listen".into())

            .spawn(move || discovery::listener_loop(db, udp, sid));

    }



    /// 获取设备同步统计信息
    pub fn get_device_sync_stats(&self, device_id: &str) -> Result<DeviceSyncStats, String> {
        self.db().get_device_sync_stats(device_id).map_err(|e| e.to_string())
    }

    /// 获取设备冲突列表
    pub fn get_device_conflicts(&self, device_id: &str) -> Result<Vec<ConflictSummary>, String> {
        self.db().get_device_conflicts(device_id).map_err(|e| e.to_string())
    }

    /// 回收站列表（kind 可为 "note"/"todo"，空表示全部）。
    pub fn list_trash(&self, kind: Option<&str>) -> Result<Vec<TrashEntry>, String> {
        self.db().list_trash(kind).map_err(|e| e.to_string())
    }

    /// 从回收站恢复一条归档（写回业务库；安全语义：不覆盖当前胜出方）。
    pub fn restore_trash(&self, id: i64) -> Result<bool, String> {
        let entry = self
            .db()
            .get_trash(id)
            .map_err(|e| e.to_string())?
            .ok_or_else(|| "回收站条目不存在".to_string())?;
        if entry.restored {
            return Ok(false); // 已恢复过
        }
        let ok = match entry.kind.as_str() {
            "note" => crate::serialize::restore_note(
                self.data_dir.join("inbox.db").as_path(),
                &entry.archived,
            )?,
            "todo" => crate::serialize::restore_todo(
                self.data_dir.join("todo.db").as_path(),
                &entry.archived,
            )?,
            _ => return Err(format!("未知归档类型 {}", entry.kind)),
        };
        if ok {
            let _ = self.db().mark_trash_restored(id);
            let _ = self.add_log(&SyncLogEntry {
                id: None,
                timestamp: Utc::now(),
                direction: SyncDirection::In,
                protocol: SyncProtocol::Http,
                peer_id: entry.source_device.clone(),
                event_type: SyncEventType::Conflict,
                status: SyncStatus::Success,
                message: Some(format!(
                    "已从回收站恢复 {}（逻辑键 {}）",
                    entry.kind, entry.logical_key
                )),
                data_size: None,
                details: None,
            });
        }
        Ok(ok)
    }

    /// 从回收站永久删除一条归档。
    pub fn delete_trash(&self, id: i64) -> Result<bool, String> {
        self.db().delete_trash(id).map_err(|e| e.to_string())
    }

    /// 未恢复归档总数（供前端角标）。
    pub fn trash_count(&self) -> Result<i64, String> {
        self.db().count_trash().map_err(|e| e.to_string())
    }

    // ---- Cloudflare D1 云同步 ----

    /// 测试 D1 连接。
    pub fn d1_test(&self) -> Result<crate::d1_sync::D1TestResult, String> {
        let cfg = self.get_config();
        crate::d1_sync::d1_test(&cfg)
    }

    /// 获取 D1 同步状态。
    pub fn d1_status(&self) -> Result<crate::d1_sync::D1Status, String> {
        let cfg = self.get_config();
        let last_sync = self.db().get_d1_last_sync();
        Ok(crate::d1_sync::d1_status(&cfg, last_sync))
    }

    /// 触发一次 D1 双向同步。成功后更新 d1_last_sync 时间戳并写入 sync_log。
    pub fn d1_sync_now(&self) -> Result<crate::d1_sync::D1SyncResult, String> {
        let cfg = self.get_config();
        let result = crate::d1_sync::d1_sync_now(&self.data_dir, &self.self_id, &cfg)?;

        // 写入 sync_log，供同步详情展示
        let log_entry = SyncLogEntry {
            id: None,
            timestamp: Utc::now(),
            direction: SyncDirection::Out,
            protocol: SyncProtocol::D1,
            peer_id: None,
            event_type: SyncEventType::Sync,
            status: if result.ok { SyncStatus::Success } else { SyncStatus::Failed },
            message: Some(format!(
                "推送 {} 笔记 / {} TODO · 拉取 {} 笔记 / {} TODO · 冲突 {}",
                result.pushed_notes, result.pushed_todos,
                result.pulled_notes, result.pulled_todos,
                result.conflicts
            )),
            data_size: None,
            details: None,
        };
        if let Err(e) = self.add_log(&log_entry) {
            crate::dbglog::warn(format!("[d1] 写入 sync_log 失败: {e}"));
        }

        if result.ok {
            let now = Utc::now().to_rfc3339();
            if let Err(e) = self.db().set_d1_last_sync(&now) {
                crate::dbglog::warn(format!("[d1] 更新 d1_last_sync 失败: {e}"));
            }
        }
        Ok(result)
    }

    /// 触发一次强制全量 D1 同步（清空 checkpoint 后全量拉取）。
    pub fn d1_full_sync(&self) -> Result<crate::d1_sync::D1SyncResult, String> {
        let cfg = self.get_config();
        let result = crate::d1_sync::d1_sync_now_full(&self.data_dir, &self.self_id, &cfg)?;

        // 写入 sync_log
        let log_entry = SyncLogEntry {
            id: None,
            timestamp: Utc::now(),
            direction: SyncDirection::Out,
            protocol: SyncProtocol::D1,
            peer_id: None,
            event_type: SyncEventType::Sync,
            status: if result.ok { SyncStatus::Success } else { SyncStatus::Failed },
            message: Some(format!(
                "[全量] 推送 {} 笔记 / {} TODO · 拉取 {} 笔记 / {} TODO · 冲突 {}",
                result.pushed_notes, result.pushed_todos,
                result.pulled_notes, result.pulled_todos,
                result.conflicts
            )),
            data_size: None,
            details: None,
        };
        if let Err(e) = self.add_log(&log_entry) {
            crate::dbglog::warn(format!("[d1] 写入 sync_log 失败: {e}"));
        }


        if result.ok {
            let now = Utc::now().to_rfc3339();
            if let Err(e) = self.db().set_d1_last_sync(&now) {
                crate::dbglog::warn(format!("[d1] 更新 d1_last_sync 失败: {e}"));
            }
        }
        Ok(result)
    }

    /// 清除 D1 上的本机 checkpoint（重置同步状态）。
    pub fn d1_clear_checkpoint(&self) -> Result<(), String> {
        let cfg = self.get_config();
        crate::d1_sync::d1_clear_checkpoint(
            &cfg.d1_account_id,
            &cfg.d1_database_id,
            &cfg.d1_api_token,
            &self.self_id,
        )
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
            ip: current_local_ip(),

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

/// 当前本机局域网 IP：Android 注入的 Wi-Fi 地址优先，否则枚举网卡；拿不到为空串。
/// 供广播线程每轮重解析（Wi-Fi 重连/换网后地址会变化）。
pub(crate) fn current_local_ip() -> String {
    local_ip_override().or_else(local_ip).unwrap_or_default()
}

/// 探测本机非回环 IPv4：**枚举所有网卡接口**，挑出本机真实地址。
/// 不再用「UDP connect 外网取出口地址」——那在 Android 开 VPN 时会选中隧道接口
/// （如 tun0 的 172.19.0.1），导致多台设备拿到同一个网关地址。
/// 这里直接遍历接口、排除 VPN/隧道、优先 Wi-Fi/以太网，得到每台设备自己的 IP。
/// 仅当确实拿到真实局域网地址时返回 Some；失败返回 None（广播线程会跳过假地址）。

fn local_ip() -> Option<String> {
    // 直接调用 POSIX getifaddrs（linux / android 均可用），不依赖第三方枚举库，
    // 避免其 Android 分支在新工具链下 CStr::from_ptr 签名不兼容导致编译失败。

    #[cfg(not(any(target_os = "linux", target_os = "android", target_os = "windows")))]
    {
        return None;
    }

    #[cfg(target_os = "windows")]
    {
        local_ip_windows()
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

/// Windows 平台：用 GetAdaptersAddresses 枚举网卡，排除 VPN/隧道，
/// 优先选择 Wi-Fi / 以太网接口的 IPv4 地址。
#[cfg(target_os = "windows")]
fn local_ip_windows() -> Option<String> {
    use std::net::Ipv4Addr;
    use std::ptr;
    use windows_sys::Win32::NetworkManagement::IpHelper::GetAdaptersAddresses;
    use windows_sys::Win32::NetworkManagement::IpHelper::GAA_FLAG_INCLUDE_PREFIX;
    use windows_sys::Win32::Networking::WinSock::AF_INET;

    unsafe {
        let family = AF_INET as u32;
        let flags: u32 = GAA_FLAG_INCLUDE_PREFIX;
        let mut size: u32 = 0;

        let mut adapters_buffer: Vec<u8> = Vec::with_capacity(15000);

        let mut ret = GetAdaptersAddresses(
            family,
            flags,
            ptr::null_mut(),
            adapters_buffer.as_mut_ptr() as *mut _,
            &mut size,
        );

        if ret == 111 {
            // ERROR_BUFFER_OVERFLOW - 缓冲区不足，按返回的 size 重新分配
            adapters_buffer = Vec::with_capacity(size as usize);
            ret = GetAdaptersAddresses(
                family,
                flags,
                ptr::null_mut(),
                adapters_buffer.as_mut_ptr() as *mut _,
                &mut size,
            );
        }

        if ret != 0 {
            return None;
        }

        let mut preferred: Vec<(Ipv4Addr, String)> = Vec::new();
        let mut others: Vec<(Ipv4Addr, String)> = Vec::new();

        let mut adapter = adapters_buffer.as_ptr() as *const windows_sys::Win32::NetworkManagement::IpHelper::IP_ADAPTER_ADDRESSES_LH;

        while !adapter.is_null() {
            let ifa = &*adapter;

            // 跳过环回、未连接、虚拟接口
            let if_type = ifa.IfType;
            // IF_TYPE_SOFTWARE_LOOPBACK = 24, IF_TYPE_TUNNEL = 13
            if if_type == 24 || if_type == 13 {
                adapter = ifa.Next;
                continue;
            }

            // 检查接口是否已连接（IfOperStatusUp = 1）
            if ifa.OperStatus != 1 {
                adapter = ifa.Next;
                continue;
            }

            // 获取单播地址列表
            let mut unicast = ifa.FirstUnicastAddress;
            while !unicast.is_null() {
                let addr = &*unicast;
                let sockaddr = addr.Address.lpSockaddr;

                if !sockaddr.is_null() {
                    let family = (*sockaddr).sa_family;
                    if family == AF_INET as u16 {
                        let sin = sockaddr as *const windows_sys::Win32::Networking::WinSock::SOCKADDR_IN;
                        let s_addr = (*sin).sin_addr.S_un.S_addr;
                        // Windows stores in network byte order
                        let ip = Ipv4Addr::from(u32::from_be(s_addr));

                        // 跳过回环 / 未指定 / 链路本地
                        if !ip.is_loopback() && !ip.is_unspecified() && !ip.is_link_local() {
                            // 获取接口友好名称（Windows FriendlyName 是 UTF-16 宽字符串）
                            let name = {
                                let len = (0..).take_while(|&i| ifa.FriendlyName.add(i).read() != 0).count();
                                let slice = std::slice::from_raw_parts(ifa.FriendlyName, len);
                                String::from_utf16_lossy(slice).to_ascii_lowercase()
                            };

                            // 跳过 VPN / 隧道接口
                            let is_vpn = name.contains("vpn")
                                || name.contains("tap")
                                || name.contains("tunnel")
                                || name.contains("virtual")
                                || name.contains("hyper-v")
                                || name.contains("virtualbox")
                                || name.contains("vmware");

                            if is_vpn {
                                unicast = addr.Next;
                                continue;
                            }

                            // 优先 Wi-Fi / 以太网接口
                            let is_preferred = name.contains("wi-fi")
                                || name.contains("wifi")
                                || name.contains("wlan")
                                || name.contains("ethernet")
                                || name.contains("eth")
                                || name.contains("local area connection");

                            if is_preferred {
                                preferred.push((ip, name));
                            } else {
                                others.push((ip, name));
                            }
                        }
                    }
                }
                unicast = addr.Next;
            }
            adapter = ifa.Next;
        }

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



static DISCOVERY_ACTIVE: AtomicBool = AtomicBool::new(false);



/// 发现线程是否已拉起（进程内只 spawn 一次，之后由 DISCOVERY_ACTIVE 控制实际收发）

static DISCOVERY_THREADS_STARTED: AtomicBool = AtomicBool::new(false);



/// 广播发现是否正在进行（供 /api/0/sync/status 查询）

pub fn discovery_running() -> bool {

    DISCOVERY_ACTIVE.load(Ordering::SeqCst)

}



/// 发现循环每轮检查的开关（discovery.rs 调用）：未开启时广播/监听空转

pub(crate) fn discovery_active() -> bool {

    DISCOVERY_ACTIVE.load(Ordering::SeqCst)

}



pub(crate) fn set_discovery_active(v: bool) {

    DISCOVERY_ACTIVE.store(v, Ordering::SeqCst);

}



/// 测试钩子：同一进程内起多台“虚拟设备”时,允许第二台也启动自己的广播线程。

#[doc(hidden)]

pub fn reset_discovery_started_for_testing() {

    DISCOVERY_THREADS_STARTED.store(false, Ordering::SeqCst);

    set_discovery_active(false);

}


// ---- 合并结果落库：写回收站 + sync_conflicts ----

fn persist_outcome(
    result: &mut ApplyResult,
    out: crate::serialize::ImportOutcome,
    src_id: Option<&str>,
    db: &SyncDb,
) {
    result.applied += out.applied();
    result.created += out.created;
    result.updated += out.updated;
    result.deleted += out.deleted;
    result.ignored += out.ignored_stale + out.ignored_dup;
    result.conflicts += out.archived.len();
    result.archived += out.archived.len();
    result.errors.extend(out.errors);
    result.records.extend(out.records);

    for ar in out.archived {
        let trash = TrashEntry {
            id: 0,
            kind: ar.kind.clone(),
            logical_key: ar.logical_key.clone(),
            archived: ar.archived_json,
            winner_rev: ar.winner_rev.clone(),
            reason: ar.reason.clone(),
            source_device: src_id.map(|s| s.to_string()),
            archived_at: Utc::now().to_rfc3339(),
            restored: false,
        };
        if let Ok(tid) = db.insert_trash(&trash) {
            let _ = db.insert_conflict(
                src_id.unwrap_or("unknown"),
                &ar.kind,
                &ar.logical_key,
                None,
                ar.winner_rev.as_deref(),
                &ar.reason,
                Some(tid),
            );
        }
    }
}
