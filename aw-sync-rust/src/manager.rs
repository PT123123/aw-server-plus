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


    // ---- 信封加密密钥（B 方案，详见 crypto 模块）----

    /// 本机与某对端当前生效的共享密钥；None = 从未交换过（旧端）→ 报文体走明文回退。
    pub fn device_secret(&self, peer_id: &str) -> Option<String> {
        self.db().get_device_secret(peer_id).ok().flatten()
    }

    /// 处理入站配对/加入报文携带的密钥：报文里有合法密钥就采纳（发起方决定），
    /// 没有或不可用则本机新生成一把；结果落库并原样回传给对方。
    /// 两端由此收敛到同一把密钥，重新配对即轮换。
    pub fn adopt_incoming_secret(&self, peer_id: &str, offered: Option<&str>) -> Result<String, String> {
        let secret = match offered {
            Some(s) if crate::crypto::secret_ok(s) => s.to_string(),
            _ => crate::crypto::generate_secret(),
        };
        self.db()
            .set_device_secret(peer_id, &secret)
            .map_err(|e| e.to_string())?;
        Ok(secret)
    }

    /// 采纳对端响应里回传的密钥（以对端为准，双方才会一致）。
    /// 旧端响应没有 device_secret 字段：保持现状（可能已有密钥，也可能继续明文）。
    pub fn absorb_secret_from(&self, peer_id: &str, resp: &serde_json::Value) {
        let Some(s) = resp.get("device_secret").and_then(|v| v.as_str()) else {
            return;
        };
        if !crate::crypto::secret_ok(s) {
            crate::dbglog::warn(format!("[crypto] 对端 {peer_id} 回传的密钥格式非法，已忽略"));
            return;
        }
        let owned = s.to_string();
        let _ = self.db().set_device_secret(peer_id, &owned).map_err(|e| {
            crate::dbglog::error(format!("[crypto] 保存对端密钥失败 {peer_id}: {e}"));
            e
        });
    }

    /// 用配对握手里对端自报的信息刷新本地登记（真实设备名/可达地址）。
    ///
    /// 为什么必要：UDP 广播出于被动抓包考虑只带哈希别名，配对前设备列表里就是一串
    /// 十六进制；配对的 HTTP 报文才带真名。保留本机侧的 alias（用户自己设的别名
    /// 不能被对端自报名覆盖）与 paired / last_sync_at。
    pub fn refresh_peer_from_handshake(&self, dev: &Device) -> Result<(), String> {
        let mut d = dev.clone();
        d.is_self = false;
        d.device_secret = None;
        if let Some(existing) = self.get_device(&d.id).map_err(|e| e.to_string())? {
            d.paired = existing.paired;
            d.alias = existing.alias;
            if d.last_sync_at.is_none() {
                d.last_sync_at = existing.last_sync_at;
            }
        }
        self.save_device(&d)
    }

    /// 从配对/加入响应里取出对端自报的本机信息（/pair/request 用 "me"、/join 用 "peer"）
    /// 并登记到信任列表。
    pub fn absorb_peer_from(&self, resp: &serde_json::Value) {
        for key in ["me", "peer"] {
            let Ok(dev) = serde_json::from_value::<Device>(match resp.get(key) {
                Some(v) => v.clone(),
                None => continue,
            }) else {
                continue;
            };
            if dev.id.is_empty() || dev.id == self.self_id {
                continue;
            }
            if let Err(e) = self.refresh_peer_from_handshake(&dev) {
                crate::dbglog::warn(format!("[pair] 登记对端自报信息失败 {}: {e}", dev.id));
            }
        }
    }

    /// 出站配对报文：附上本机的装机指纹。
    ///
    /// 这是 machine_uid **唯一的出站口子**，调用点全在用户主动发起的配对握手上来回：
    /// 请求侧三处（initiate / confirm / join_remote），响应侧三处（/join、/pair/request、
    /// /pair/confirm 回的自报信息）。少了响应侧那三处，四个配对方向里只有「码主记下
    /// 加入方」这一条能落到库，另一台机器的旧行就永远等不到归并提示。
    /// 广播走 discovery::announce_payload 的脱敏（抹掉 name/密钥/指纹），/devices 与
    /// /info 走 storage::row_to_device / self_device_info（指纹恒为 None），
    /// 两条被动路径都刻意不给它出机的机会。
    ///
    /// 关于 device_secret 这一行要说实话：`d` 传进来的始终是**本机**自报信息，
    /// 于是这里查的是 `device_secrets[本机 id]` —— 那张表按对端 id 存，所以恒为 None。
    /// 即「发起方带上已有密钥」这件事从来没有发生过，密钥一律由收方现生成
    /// （adopt_incoming_secret 走 set_device_secret 覆盖，所以每次重新配对必然轮换）。
    /// 行为本身自洽（两端仍收敛到同一把），只是和早期注释的设想不同；改它要先定
    /// 「重配对该不该保留旧密钥」，不在本次归并改动范围内。
    pub fn with_outgoing_secret(&self, peer: &Device) -> Device {
        let mut d = peer.clone();
        d.device_secret = self.device_secret(&d.id);
        d.machine_uid = crate::machine_uid::machine_uid();
        d
    }

    /// 这台对端是否「疑似某台已配设备的重装」。命中有两种时刻：
    /// ① 它正带着指纹请求配对（接受之前就能提示）；② 列表里已经躺着两行同指纹的设备
    /// （升级/重装后各配了一次，正是用户抱怨的那种乱）。
    ///
    /// 命中只产出一条 UI 提示（含旧行的短指纹片段，供人眼对照），**不改变配对结果、
    /// 不省掉安全码比对**。同机双实例的指纹完全相同，所以自动折叠是错的，必须人点。
    pub fn merge_candidate_for(&self, peer_id: &str) -> Option<serde_json::Value> {
        // 指纹来源：① 内存里那份待确认请求（还没入库）；② 库里这一行自己的
        // （只有配对过的行才会被登记指纹，见 with_outgoing_secret 的四条握手路径）。
        let uid = self
            .inbound_pair_requests
            .lock()
            .ok()
            .and_then(|m| m.get(peer_id).and_then(|d| d.machine_uid.clone()))
            .or_else(|| self.db().machine_uid_of(peer_id).ok().flatten())?;
        let old = self.db().find_merge_candidate(&uid, peer_id).ok().flatten()?;
        // 提示只挂在「配对较晚的那一行」上：两行互为候选会让界面上冒出两个合并按钮，
        // 而用户要做的决定其实只有一个——旧行并进来，活着的这个 id 留下。
        if let Some(me) = self.db().get_device(peer_id).ok().flatten().filter(|d| d.paired) {
            if me.paired_at <= old.paired_at {
                return None;
            }
        }
        // 只回前 8 个 hex：界面要的是「这两行是不是同一个」，不是可跟踪的完整标识
        //（/devices 同网段任何人可读）。
        Some(serde_json::json!({
            "id": old.id,
            "name": old.name,
            "alias": old.alias,
            "uid_hint": &uid[..uid.len().min(8)],
            "paired_at": old.paired_at.to_rfc3339(),
            "last_sync_at": old.last_sync_at.map(|t| t.to_rfc3339()),
            "since_paired_days": (Utc::now() - old.paired_at).num_days(),
        }))
    }

    /// 安全码表：peer_id → 4 位十六进制。只暴露指纹，密钥永不出接口。
    pub fn security_fingerprints(&self) -> std::collections::HashMap<String, String> {
        self.db().fingerprints(&self.self_id)
    }

    /// 解一个快照类请求体（/push）：信封按 kid 查密钥解开；明文则要求本机与该发送方
    /// 从未交换过密钥（否则视为降级攻击）。成功时顺带写一条传输方式日志。
    pub fn decode_snapshot_body(&self, raw: &str, path: &str) -> Result<SyncSnapshot, String> {
        let kid = crate::crypto::envelope_kid(raw);
        let secret = kid.as_ref().and_then(|k| self.device_secret(k));
        let plain = match &kid {
            Some(_) => crate::crypto::open_body(secret.as_deref(), path, raw)
                .map_err(|e| format!("信封报文处理失败: {e}"))?,
            // 明文：先解析出发送方，再判断是否允许明文
            None => {
                let snap: SyncSnapshot =
                    serde_json::from_str(raw).map_err(|e| format!("快照 JSON 解析失败: {e}"))?;
                if let Some(dev) = &snap.source_device {
                    if self.device_secret(&dev.id).is_some() {
                        return Err(format!(
                            "已与 {} 协商加密密钥，拒收明文报文（防止中间人降级）",
                            dev.id
                        ));
                    }
                }
                raw.to_string()
            }
        };
        serde_json::from_str(&plain).map_err(|e| format!("解密后快照解析失败: {e}"))
    }

    /// 出站快照响应（/snapshot）：知道请求方是谁（?from=）且与之有共享密钥就封成信封，
    /// 否则返回明文 —— 拉取端会做同样的对称校验。
    pub fn encode_snapshot_body(
        &self,
        requester_id: Option<&str>,
        path: &str,
        snap: &SyncSnapshot,
    ) -> Result<String, String> {
        let plain = serde_json::to_string(snap).map_err(|e| e.to_string())?;
        let secret = requester_id.and_then(|id| self.device_secret(id));
        crate::crypto::seal_body(secret.as_deref(), path, &self.self_id, &plain)
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

    /// 把 from 行归并进 to 行（用户在「疑似同一台机器」提示里点了「合并」）。
    /// 落库语义与前置校验见 storage::merge_device；这里只补一句日志。
    pub fn merge_devices(&self, from: &str, to: &str) -> Result<(), String> {
        self.db().merge_device(from, to)?;
        // 归并会少一行，必须在同步日志里留痕，否则用户只会问「我的设备怎么不见了」。
        let _ = self.add_log(&SyncLogEntry {
            id: None,
            timestamp: Utc::now(),
            direction: SyncDirection::In,
            protocol: SyncProtocol::Http,
            peer_id: Some(to.to_string()),
            event_type: SyncEventType::Pairing,
            status: SyncStatus::Success,
            message: Some(format!("已把 {from} 归并进 {to}（装机指纹相同，判定为同一台机器）")),
            data_size: None,
            details: None,
        });
        crate::dbglog::info(format!("[pair] 已把 {from} 归并进 {to}（旧行保留用于历史归因）"));
        Ok(())
    }

    /// 「一键清理」：两件事一次做完，返回 (清掉的发现行数, 清掉的旧配对数)。
    ///
    /// 未配对行按服务端常量淘汰（周期性的探活循环本来就在做，这里只是让界面能立刻
    /// 给出反馈）；旧配对按用户指定的天数删，`last_sync_at` 为空的不动。
    pub fn purge_stale_devices(&self, days: i64) -> Result<(usize, usize), String> {
        let db = self.db();
        let discovered = db.purge_discovered().map_err(|e| e.to_string())?;
        let paired = db.purge_stale_paired(days).map_err(|e| e.to_string())?;
        crate::dbglog::info(format!(
            "[sync] 清理：淘汰 {discovered} 条静默发现行，删除 {paired} 条 {days} 天未同步的旧配对"
        ));
        Ok((discovered, paired))
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
            // 修复自死锁：此处 db 锁（MutexGuard）仍被本函数持有，
            // 不能经 self.add_log() 再次 lock 同一个不可重入的 std::sync::Mutex，
            // 必须直接用已持有的 guard 调用 SyncDb::add_log。
            let _ = db.add_log(&SyncLogEntry {
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
        // 只有真的改动了本地业务库才递增修订号（无变更的自动轮不打扰客户端）
        if result.applied > 0 || result.archived > 0 {
            bump_data_revision();
        }
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

        // ① 取对端 + 本机与之的共享密钥（短锁）。没有密钥 = 旧端/尚未交换 → 明文回退。
        let (peer, secret, self_id) = {
            let g = mgr.lock().map_err(|_| lock_err())?;
            let peer = g.get_device(peer_id)?.ok_or("未找到目标设备")?;
            let secret = g.device_secret(&peer.id);
            (peer, secret, g.self_id.clone())
        };

        // ② 拉取对端快照（不持锁）
        let remote = crate::transport::fetch_snapshot(&peer, secret.as_deref(), &self_id)?;

        // ③ 合并进本地（短锁）
        let applied = {
            let g = mgr.lock().map_err(|_| lock_err())?;
            g.apply_snapshot(&remote)?
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
        let pushed = crate::transport::push_snapshot(&peer, &snap, secret.as_deref()).unwrap_or(0);

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

        // 首次配对本机没有密钥：由对端生成并随响应回传；再次配对则带上已有密钥（轮换）
        let me_wire = self.with_outgoing_secret(&me);

        let url = format!("{}/pair/request", peer.endpoint());

        let payload = serde_json::to_string(&me).unwrap_or_default();

        log::info!("[aw-sync] initiate_pair: 目标设备={}, ip={}, port={}", peer.name, peer.ip, peer.port);

        let detail = format!(
            "本机({}) 向 {}({}) 发起配对请求\n目标: {} ({}:{})\nURL: {}\n请求体: {}{}",
            self.self_id,
            peer.name,
            peer.id,
            peer.name,
            peer.ip,
            peer.port,
            url,
            payload,
            if me_wire.device_secret.is_some() { "\n密钥: 随报文附带（日志不落密钥）" } else { "\n密钥: 本次由对端生成后交换" }
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

        let resp = crate::transport::send_pair_request(&peer, &me_wire)?;

        // 对端回传它采纳/生成的那把密钥：以对端为准存下，两端由此一致
        self.absorb_secret_from(peer_id, &resp);
        self.absorb_peer_from(&resp);
        let mut resp = resp;
        if let Some(code) = self.security_fingerprints().get(peer_id).cloned() {
            if !resp.is_object() {
                resp = serde_json::json!({});
            }
            resp["fingerprint"] = serde_json::json!(code);
        }

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
        // 本机可能已握着密钥（对方发起配对时交换的），带上让对方对齐；没有则让对方生成
        let me_wire = self.with_outgoing_secret(&me);
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
                "本机({}) 确认与 {}({}) 配对\nURL: {}\n请求体: {}{}",
                self.self_id,
                peer.name,
                peer.id,
                url,
                payload,
                if me_wire.device_secret.is_some() { "\n密钥: 随报文附带（日志不落密钥）" } else { "\n密钥: 本次由对端生成后交换" }
            )),
            data_size: Some(payload.len() as u64),
            details: None,
        }).ok();
        let resp = crate::transport::confirm_pair(&peer, &me_wire)?;
        self.absorb_secret_from(peer_id, &resp);
        self.absorb_peer_from(&resp);
        self.mark_paired(peer_id, true)?;
        // 清除待确认记录
        if let Ok(mut m) = self.inbound_pair_requests.lock() {
            m.remove(peer_id);
        }
        let fp = self.security_fingerprints().get(peer_id).cloned();
        crate::dbglog::info(format!(
            "[pair] 已与 {} 完成配对；安全码 {}（请与对端屏幕上显示的核对一致）",
            peer.name,
            fp.as_deref().unwrap_or("（尚无密钥，未加密同步）")
        ));
        // 安全码回给本机前端，同步页据此提示人工比对
        let mut resp = resp;
        if let Some(code) = fp {
            if !resp.is_object() {
                resp = serde_json::json!({});
            }
            resp["fingerprint"] = serde_json::json!(code);
        }
        Ok(resp)
    }


    /// 「使用配对码配对」的加入方一侧：把配对码提交给**对端**服务器。
    ///
    /// 注意必须打到对端的 /join：配对码存在创建方（码主）的 sync.db 里，
    /// 打给本机只会得到一个「配对码无效」——码主永远收不到你。
    /// 成功响应含 `{device, peer, device_secret}`：peer 是码主自报的信息，
    /// 登记进本机信任列表并与之共享这把新密钥。
    pub fn join_remote(&self, peer_id: &str, code: &str) -> Result<serde_json::Value, String> {
        let peer = self
            .get_device(peer_id)?
            .ok_or("未找到目标设备，请先在局域网中让它出现在设备列表里")?;
        let me = self.with_outgoing_secret(&self.self_device_info());
        let resp = crate::transport::join_with_code(&peer, code.trim(), &me)?;
        self.absorb_secret_from(&peer.id, &resp);
        self.absorb_peer_from(&resp);
        // 码主已把我们登记为已配对；本机这边对等地补上，两端状态才对称
        let _ = self.mark_paired(&peer.id, true);
        let fp = self.security_fingerprints().get(&peer.id).cloned();
        crate::dbglog::info(format!(
            "[pair] 已用配对码与 {} 完成配对；安全码 {}（请与对端屏幕上显示的核对一致）",
            peer.name,
            fp.clone().unwrap_or_else(|| "（未取得密钥，仍为明文同步）".to_string())
        ));
        let mut resp = resp;
        if let Some(code) = fp {
            if !resp.is_object() {
                resp = serde_json::json!({});
            }
            resp["fingerprint"] = serde_json::json!(code);
        }
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
    ///
    /// 除周期轮询外还有两个「不等下一轮」的触发点：
    /// - 同步开关 false→true：立即全量尝试一轮（不管在线标志）；
    /// - 设备在线标志 0→1（对端刚回到局域网）：立即同步该设备。
    /// 另外离线设备每 OFFLINE_RETRY_SECS 补探一次，避免 is_online 卡在 0 时被永久静默跳过。
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
                // 上一轮各设备的在线状态（id → is_online），用于识别「刚回到局域网」
                let mut prev_online: HashMap<String, bool> = HashMap::new();
                // 离线设备的最近一次补探时刻：即使探测线程失灵，自动同步也能自己把设备探回来
                let mut offline_probe_at: HashMap<String, std::time::Instant> = HashMap::new();
                // 离线设备补探间隔（秒）
                const OFFLINE_RETRY_SECS: u64 = 30;

                // 探测失败后的后台重发现窗口与等待时长（等对端广播刷新记录里的 IP）
                const REDISCOVER_WINDOW_SECS: u64 = 6;
                const REDISCOVER_WAIT_MS: u64 = 3000;
                // 重发现限流：对端只是关机/离网时探测失败也会走到自愈分支，若每轮都开
                // 广播窗口，在狂暴档（间隔 10s）下几乎全程都在发 UDP —— 白耗电还招系统清理。
                // 离线设备本身已有 OFFLINE_RETRY_SECS 的补探门限，两者取同一量级即可。
                const REDISCOVER_MIN_INTERVAL_SECS: u64 = 30;
                // 最近一次开重发现窗口的时刻（全局限流用）
                let mut last_rediscover: Option<std::time::Instant> = None;

                loop {
                    // enabled=false 或 sync_interval=0（仅手动）时都不做自动轮询；
                    // 手动同步与事件触发推送仍可调用 sync_to
                    let (enabled, interval_secs) = match SyncDb::open(&data_dir) {
                        Ok(db) => {
                            let cfg = db.get_config();
                            (cfg.enabled && cfg.sync_interval > 0, cfg.sync_interval.max(5))
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
                            let devices = db.get_devices().unwrap_or_default();
                            for d in devices {
                                if !d.paired || d.is_self {
                                    continue;
                                }

                                // 在线状态翻转（0→1）：对端刚回到局域网，不等下一轮，立刻同步。
                                // 这是「手机连上 Wi-Fi 就该自动传」的关键触发点。
                                let was_online =
                                    prev_online.insert(d.id.clone(), d.is_online).unwrap_or(false);
                                if d.is_online && !was_online {
                                    crate::dbglog::info(format!(
                                        "[auto] 设备 {}({}) 已回到局域网，立即同步一轮",
                                        d.name, d.id
                                    ));
                                } else if was_online && !d.is_online {
                                    crate::dbglog::info(format!(
                                        "[auto] 设备 {}({}) 已离线，暂停自动同步",
                                        d.name, d.id
                                    ));
                                }

                                // 离线设备按 OFFLINE_RETRY_SECS 补探一次：
                                // 只信 is_online 会形成死结 —— 一旦该标志没能被刷回 1，
                                // 这里就每轮静默跳过（连日志都没有），用户只能手动点同步。
                                if !d.is_online {
                                    let due = offline_probe_at
                                        .get(&d.id)
                                        .map_or(true, |t| t.elapsed().as_secs() >= OFFLINE_RETRY_SECS);
                                    if !due && !force {
                                        continue;
                                    }
                                    offline_probe_at.insert(d.id.clone(), std::time::Instant::now());
                                } else {
                                    offline_probe_at.remove(&d.id);
                                }

                                // 先做轻量可达性探测（不持锁，连接 2s/读取 3s）：对端离网时
                                // 快速跳过，避免拿住全局锁等满 HTTP 总超时、饿死其余接口
                                if crate::transport::probe_online(&d).is_err() {
                                    // 探测失败不再直接放弃：记录里的 IP 可能已经过期（对端 DHCP
                                    // 换了地址），这时再怎么重试旧地址都是白搭。开一个短暂的后台
                                    // 重发现窗口，让对端广播把 IP 刷成当前真实地址后重试一次。
                                    // 刚开启的那一轮必开窗口（最需要自愈的时刻），其余按限流走。
                                    let may_rediscover = force
                                        || last_rediscover.map_or(true, |t| {
                                            t.elapsed().as_secs()
                                                >= REDISCOVER_MIN_INTERVAL_SECS
                                        });
                                    if !may_rediscover {
                                        crate::dbglog::info(format!(
                                            "[auto] 设备 {}({}) 不可达，本轮跳过同步（距上次重发现不足 {REDISCOVER_MIN_INTERVAL_SECS}s，未重复开广播）",
                                            d.name, d.id
                                        ));
                                        continue;
                                    }
                                    last_rediscover = Some(std::time::Instant::now());
                                    if let Ok(g) = mgr.lock() {
                                        g.start_discovery_burst(REDISCOVER_WINDOW_SECS);
                                    }
                                    std::thread::sleep(std::time::Duration::from_millis(
                                        REDISCOVER_WAIT_MS,
                                    ));
                                    // 重读该设备：广播可能刚把 ip/port 刷新过
                                    let refreshed = SyncDb::open(&data_dir)
                                        .ok()
                                        .and_then(|db| db.get_devices().ok())
                                        .and_then(|vs| vs.into_iter().find(|x| x.id == d.id))
                                        .unwrap_or_else(|| d.clone());
                                    if crate::transport::probe_online(&refreshed).is_err() {
                                        crate::dbglog::info(format!(
                                            "[auto] 设备 {}({}) 不可达，本轮跳过同步（已尝试后台重发现）",
                                            d.name, d.id
                                        ));
                                        continue;
                                    }
                                    crate::dbglog::info(format!(
                                        "[auto] 设备 {}({}) 后台重发现后恢复可达 {}:{}，继续同步",
                                        d.name, d.id, refreshed.ip, refreshed.port
                                    ));
                                }
                                if !d.is_online {
                                    // 补探成功：设备实际可达但标志位还是 0，说明探测线程没跟上，
                                    // 这里直接按「刚回到局域网」处理，立即补一轮
                                    crate::dbglog::info(format!(
                                        "[auto] 设备 {}({}) 恢复可达，立即补一轮同步",
                                        d.name, d.id
                                    ));
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

                    // 淘汰堆积的纯发现行：每轮探活顺手做一次（一条 DELETE，行数量级）。
                    // 老库里 last_seen_epoch 为 NULL 的行会被算成「很旧」而先删掉——真在
                    // 场的设备下一条宣告（广播 5s / mDNS 刷新）就会带着 epoch 重新落库。
                    match db.purge_discovered() {
                        Ok(0) => {}
                        Ok(n) => crate::dbglog::info(format!("[probe] 淘汰 {n} 条未配对且已静默的发现行")),
                        Err(e) => crate::dbglog::error(format!("[probe] purge_discovered failed: {e}")),
                    }

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

        if cfg.enabled {

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
    ///
    /// 桌面端为 no-op：桌面端发现常驻（见 `discovery_persistent`），
    /// 否则离开页面就等于关掉设备发现 —— 对端换 IP / 换设备 id / 上下线全部失明，
    /// 只剩 HTTP 探活，正是「手机回到局域网却不自动同步」的根因之一。
    pub fn stop_discovery(&self) {
        if discovery_persistent() {
            return;
        }
        set_discovery_active(false);
    }



    /// 后台自愈发现窗口：不依赖是否停留在同步界面，短暂开一轮 UDP 广播+监听，
    /// 让对端广播把设备记录里的 IP 刷新成当前真实地址。用在两处：
    /// ① Android 侧检测到 Wi-Fi / 本机 IP 变化后主动调一次；
    /// ② spawn_auto_sync 探测失败时自己开一次，重读记录再试一遍。
    pub fn start_discovery_burst(&self, secs: u64) {
        let secs = secs.clamp(1, 30);
        // 线程进程内常驻（只拉起一次），这里只保证已拉起
        self.ensure_discovery_threads();
        let until = now_ms() + secs.saturating_mul(1000);
        DISCOVERY_BURST_UNTIL_MS.store(until, Ordering::SeqCst);
        crate::dbglog::info(format!(
            "[discovery] 开启后台重发现窗口 {secs}s（用于刷新对端 IP）"
        ));
    }

    /// 立刻结束后台自愈窗口（不影响由界面驱动的常开发现）
    pub fn stop_discovery_burst(&self) {
        DISCOVERY_BURST_UNTIL_MS.store(0, Ordering::SeqCst);
    }



    /// 台架隔离开关：`AW_SYNC_NO_DISCOVERY` 非空且非 "0" 时，本进程不许起任何发现线程。
    ///
    /// 刻意每次调用都重新读环境变量、不加 OnceLock 缓存：测试进程里各用例是并发跑的，
    /// 谁先触发都会把「关」或「开」的状态固化给后面所有用例。读一次几微秒，起错线程
    /// 的代价是污染用户真机信任库。
    fn discovery_forbidden() -> bool {
        std::env::var_os("AW_SYNC_NO_DISCOVERY")
            .map(|v| !v.is_empty() && v != "0")
            .unwrap_or(false)
    }

    fn ensure_discovery_threads(&self) {

        let cfg = self.get_config();

        // 台架隔离：设了 AW_SYNC_NO_DISCOVERY 就一个发现线程都不拉（ announce / listen /
        // mDNS 全跳）。开发机上跑测试时，假设备（machine-A、mdns-device-B…）一旦开始
        // 广播，就会被**同一台机上正在运行的真实服务端**收下并永久写进它的信任列表——
        // 发现即入库，测试污染的是真数据。需要真组播的那条测试自己 #[ignore] 了。
        if Self::discovery_forbidden() {
            log::info!("[aw-sync] AW_SYNC_NO_DISCOVERY 已设置：跳过全部发现线程");
            return;
        }

        // 默认「mDNS 首选 + UDP 广播兜底」：两条路径的线程都拉起，先把对端报上来的那个
        // 写库，冲突时由 discovery::record_peer 按来源仲裁（mDNS 新鲜期内广播不得改写
        // ip/port）。只有显式把 discovery_method 设成 "udp_only" 才关掉 mDNS 用于排障。
        let mdns_enabled = cfg.discovery_method != "udp_only";

        if DISCOVERY_THREADS_STARTED.swap(true, Ordering::SeqCst) {

            return;

        }

        // udp_port=0（配置未填写/旧版客户端写入）时回退默认端口，
        // 否则监听会绑到临时端口、广播会发往 0 号端口，发现功能整体失效。
        let udp = if cfg.udp_port == 0 { discovery::DEFAULT_UDP_PORT } else { cfg.udp_port };
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

        // mDNS（首选路径）：注册本机服务 + 浏览对端服务，同样由 discovery_active 逐轮门控

        if mdns_enabled {
            let dev = self_device.clone();
            let data_dir = self.data_dir.clone();
            let _ = std::thread::Builder::new()
                .name("aw-sync-mdns-announce".into())
                .spawn(move || crate::mdns::announce_loop(discovery::SelfInfo {
                    device: dev,
                    data_dir,
                }));

            let db: discovery::SharedDb = Arc::clone(&self.db);
            let sid = self_device.id.clone();
            let _ = std::thread::Builder::new()
                .name("aw-sync-mdns-browse".into())
                .spawn(move || crate::mdns::browse_loop(db, sid));
        }
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

            // 本机信息会出现在 /info 响应与 UDP 广播里（同网段任何人都读得到），
            // 密钥一律由配对流程在出站前单独附上，这里永远留空。
            device_secret: None,
            machine_uid: None,

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



/// 后台自愈发现窗口的截止时刻（Unix 毫秒），0 = 未开启。
///
/// 与「是否停留在同步界面」解耦：探测失败时短暂开一轮广播+监听，让对端广播把
/// 设备记录里的 IP 刷成当前真实地址，解决「对端换了 IP（DHCP 重分配）之后本机
/// 永远按旧地址探测失败」——只靠 HTTP 探活是猜不出来的。
///
/// Android 端尤其需要：那边 discovery_persistent() 为 false，离开同步界面就没有
/// 任何广播/监听，后台期间对端换地址后自动同步会一直静默失败。

static DISCOVERY_BURST_UNTIL_MS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);



/// 发现线程是否已拉起（进程内只 spawn 一次，之后由 DISCOVERY_ACTIVE 控制实际收发）

static DISCOVERY_THREADS_STARTED: AtomicBool = AtomicBool::new(false);



/// 广播发现是否正在进行（供 /api/0/sync/status 查询）

pub fn discovery_running() -> bool {

    DISCOVERY_ACTIVE.load(Ordering::SeqCst)

}



/// 发现循环每轮检查的开关（discovery.rs 调用）：未开启时广播/监听空转。
/// 两个来源：① discovery/start 打开的常开标志；② 后台自愈窗口（burst）。

pub(crate) fn discovery_active() -> bool {

    DISCOVERY_ACTIVE.load(Ordering::SeqCst) || discovery_burst_pending()

}



/// 后台自愈窗口是否仍未过期

pub(crate) fn discovery_burst_pending() -> bool {

    let until = DISCOVERY_BURST_UNTIL_MS.load(Ordering::SeqCst);

    until != 0 && now_ms() < until

}



fn now_ms() -> u64 {

    std::time::SystemTime::now()

        .duration_since(std::time::UNIX_EPOCH)

        .map(|d| d.as_millis() as u64)

        .unwrap_or(0)

}



pub(crate) fn set_discovery_active(v: bool) {

    DISCOVERY_ACTIVE.store(v, Ordering::SeqCst);

    // 注意：这里刻意不清 burst。Android 端离开同步界面会调 stop_discovery()，
    // 若顺带清掉自愈窗口，就会把后台刚开的那一轮重发现掐死；而 burst 自带截止
    // 时刻（最长 30s），不清理也不会泄漏。

}



/// 测试钩子：同一进程内起多台“虚拟设备”时,允许第二台也启动自己的广播线程。

#[doc(hidden)]

pub fn reset_discovery_started_for_testing() {

    DISCOVERY_THREADS_STARTED.store(false, Ordering::SeqCst);

    set_discovery_active(false);

}


/// 发现广播是否「常驻」。
///
/// 桌面端（Windows/Linux/macOS）常驻：设备发现是「手机一回到局域网就自动同步」的前提，
/// 桌面端也不缺那点电 —— 把广播绑在 UI 页面上意味着页面外永远发现不到对端
/// （换 IP / 换 device id / 上下线全都感知不到，只剩 HTTP 探活硬猜）。
///
/// Android 端保持「进入局域网同步界面才广播」的省电语义不变。
#[cfg(target_os = "android")]
pub const fn discovery_persistent() -> bool {
    false
}

#[cfg(not(target_os = "android"))]
pub const fn discovery_persistent() -> bool {
    true
}

// ---- 数据修订号（远端变更 → 客户端刷新界面的信号） ----

/// 数据修订号：每当远端数据真正落地（快照合并有新应用或新归档）就 +1。
///
/// 客户端无法从「同步完成」推断界面该不该刷新（无变更的自动轮询刻意不写日志），
/// 所以单独暴露一个单调递增的计数：客户端低频轮询 `GET /api/0/sync/revision`，
/// 值变了说明本地业务库被远端改动过 → 静默刷新列表。
/// 进程内计数即可：客户端轮询的就是同一个进程。
static DATA_REVISION: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// 当前数据修订号（供 /api/0/sync/revision 查询）
pub fn data_revision() -> u64 {
    DATA_REVISION.load(Ordering::SeqCst)
}

/// 远端数据落地后递增修订号（apply_snapshot 内调用）
fn bump_data_revision() {
    DATA_REVISION.fetch_add(1, Ordering::SeqCst);
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
