//! 同步状态持久化（sync.db）：设备、配对码、同步日志、同步设置、冲突记录、回收站。
//! 独立于 aw-server 主库，逻辑隔离。

use std::path::Path;

use chrono::{DateTime, Utc};
use rusqlite::{params, Connection, OptionalExtension, Result};

use crate::models::{
    ConflictSummary, Device, DeviceKind, DeviceSyncStats, PairCode, SyncConfig, SyncDirection,
    SyncEventType, SyncLogEntry, SyncProtocol, SyncStatus, TrashEntry,
};

pub struct SyncDb {
    conn: Connection,
}

/// 「还在播报」的宽限期：这段时间内不许把设备判成离线。
/// 取 6 个广播周期（广播 5s 一轮）再多一点余量，抖动一下不至于让设备在界面上闪断。
pub const ONLINE_GRACE_SECS: i64 = 30;

/// 未配对纯发现行的存活策略（见 [`SyncDb::purge_discovered`]）。
/// 天数取宽（换网/关机几天很正常），条数取小（真正常驻局域网的设备远少于此）。
const DISCOVERED_MAX_AGE_DAYS: i64 = 7;
const DISCOVERED_KEEP: i64 = 50;

/// 日志分页查询过滤条件
#[derive(Debug, Default, Clone)]
pub struct LogFilter {
    pub direction: Option<SyncDirection>,
    pub protocol: Option<SyncProtocol>,
    pub event_type: Option<SyncEventType>,
    pub limit: u64,
    pub offset: u64,
}

impl SyncDb {
    pub fn open(data_dir: &Path) -> Result<SyncDb> {
        let conn = Connection::open(data_dir.join("sync.db"))?;
        conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA busy_timeout=5000;")?;
        let db = SyncDb { conn };
        db.migrate()?;
        Ok(db)
    }

    fn migrate(&self) -> Result<()> {
        self.conn.execute_batch(
            "BEGIN;
            CREATE TABLE IF NOT EXISTS devices (
                id TEXT PRIMARY KEY, name TEXT NOT NULL, device_kind TEXT NOT NULL,
                ip TEXT NOT NULL, port INTEGER NOT NULL, paired_at TEXT NOT NULL,
                last_sync_at TEXT, last_seen_at TEXT, is_online INTEGER NOT NULL DEFAULT 0, is_self INTEGER NOT NULL DEFAULT 0);
            CREATE TABLE IF NOT EXISTS pairing_codes (
                code TEXT PRIMARY KEY, created_at TEXT NOT NULL, expires_at TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS sync_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT, timestamp TEXT NOT NULL,
                direction TEXT NOT NULL, protocol TEXT NOT NULL, peer_id TEXT,
                event_type TEXT NOT NULL, status TEXT NOT NULL, message TEXT, data_size INTEGER,
                details TEXT);
            CREATE TABLE IF NOT EXISTS sync_config (key TEXT PRIMARY KEY, value TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS device_secrets (
                device_id TEXT PRIMARY KEY, secret TEXT NOT NULL, updated_at TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS sync_conflicts (
                id INTEGER PRIMARY KEY AUTOINCREMENT, device_id TEXT NOT NULL,
                kind TEXT NOT NULL, logical_key TEXT NOT NULL,
                local_rev TEXT, remote_rev TEXT, resolution TEXT NOT NULL,
                archived_id INTEGER, created_at TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS trash (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                kind TEXT NOT NULL, logical_key TEXT NOT NULL,
                archived TEXT NOT NULL, winner_rev TEXT, reason TEXT NOT NULL,
                source_device TEXT, archived_at TEXT NOT NULL,
                restored INTEGER NOT NULL DEFAULT 0);
            CREATE INDEX IF NOT EXISTS idx_sync_log_filter
                ON sync_log(direction, protocol, event_type, id);
            COMMIT;",
        )?;
        // 幂等加列（老库升级）
        self.ensure_column("devices", "paired", "ALTER TABLE devices ADD COLUMN paired INTEGER NOT NULL DEFAULT 0");
        self.ensure_column("devices", "alias", "ALTER TABLE devices ADD COLUMN alias TEXT");
        self.ensure_column("devices", "last_seen_at", "ALTER TABLE devices ADD COLUMN last_seen_at TEXT");
        // 最近一次「是谁把这台设备报上来的」：mdns / udp。NULL = 老库或配对/手动登记的行。
        // 只用于内部仲裁（mDNS 首选、UDP 广播兜底），不进 Device 模型，免得前端跟着加字段。
        self.ensure_column("devices", "seen_via", "ALTER TABLE devices ADD COLUMN seen_via TEXT");
        // 那个「是谁」发生的时刻。不能复用 last_seen_at：备选路径在仲裁中落败时也会
        // 刷新 last_seen_at（为了保留在线状态），若拿它算新鲜期，每 5 秒一次的广播会把
        // mDNS 的 45 秒窗口无限续期，mDNS 真正哑掉后 UDP 也永远补不上去。
        self.ensure_column("devices", "seen_via_at", "ALTER TABLE devices ADD COLUMN seen_via_at TEXT");
        // 在线判定与淘汰的时间载体。last_seen_at 是 RFC3339 串，而 SQLite 的
        // datetime('now','-30 seconds') 返回 'YYYY-MM-DD HH:MM:SS'，两者做字符串比较时
        // 'T'(0x54) > ' '(0x20)，任何 RFC3339 串都「大于」SQLite 串 —— 于是
        // touch_online 里那句「最近还见过就别判离线」恒为真，历史行永远显示在线。
        // 时间大小比较一律走 unix 秒，别再依赖两种格式混比的字符串。
        self.ensure_column("devices", "last_seen_epoch", "ALTER TABLE devices ADD COLUMN last_seen_epoch INTEGER");
        // 装机指纹（见 machine_uid 模块）：认「重装之后的同一台机器」，只做归并提示与
        // 历史归因，不参与密钥信任。和 device_secret 一样**不从 row_to_device 出来**——
        // /devices 对同网段任何人可读，机器标识不该被动读走。
        self.ensure_column("devices", "machine_uid", "ALTER TABLE devices ADD COLUMN machine_uid TEXT");
        // 非空 = 这一行已被并进哪一行。旧行不删：已同步过来的数据仍按旧 device_id 归因，
        // 删掉就把历史打掉了。列表查询默认只出 superseded_by IS NULL 的行。
        self.ensure_column("devices", "superseded_by", "ALTER TABLE devices ADD COLUMN superseded_by TEXT");
        self.conn
            .execute_batch("CREATE INDEX IF NOT EXISTS idx_devices_muid ON devices(machine_uid);")
            .ok();
        // 旧 device_id → 现行 device_id：重装换 id 后，inbox/todo 里写着旧 id 的历史行
        // 还要能解析出设备名（见 device_name / resolve_device_id）。
        self.conn
            .execute_batch(
                "CREATE TABLE IF NOT EXISTS device_id_alias (
                    old_id TEXT PRIMARY KEY, new_id TEXT NOT NULL, merged_at TEXT NOT NULL);",
            )
            .ok();
        self.ensure_column("sync_log", "details", "ALTER TABLE sync_log ADD COLUMN details TEXT");
        // 老库瘦身：sync_log 从不自动截断会膨胀到数万行，拖垮 /log 查询；保留最近 500 条
        if self.log_count().unwrap_or(0) > 500 {
            if let Err(e) = self.truncate_logs(500) {
                log::warn!("[aw-sync] migrate 截断 sync_log 失败: {e}");
            }
        }
        Ok(())
    }

    fn ensure_column(&self, table: &str, col: &str, ddl: &str) {
        let sql = format!(
            "SELECT COUNT(*) FROM pragma_table_info('{}') WHERE name='{}'",
            table, col
        );
        let exists: i64 = self
            .conn
            .query_row(&sql, [], |r| r.get(0))
            .unwrap_or(0);
        if exists == 0 {
            if let Err(e) = self.conn.execute_batch(ddl) {
                log::warn!("[aw-sync] add column {}.{} failed: {}", table, col, e);
            }
        }
    }

    // ---- Devices ----

    pub fn upsert_device(&self, d: &Device) -> Result<()> {
        let last_seen = d.last_seen_at.map(|dt| dt.to_rfc3339());
        let last_seen_epoch = d.last_seen_at.map(|dt| dt.timestamp());
        self.conn.execute(
            "INSERT INTO devices (id,name,device_kind,ip,port,paired_at,last_sync_at,last_seen_at,last_seen_epoch,is_online,is_self,paired,alias,machine_uid)
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14)
             ON CONFLICT(id) DO UPDATE SET name=excluded.name, device_kind=excluded.device_kind,
               ip=excluded.ip, port=excluded.port, last_sync_at=excluded.last_sync_at,
               last_seen_at=excluded.last_seen_at, last_seen_epoch=excluded.last_seen_epoch,
               is_online=excluded.is_online, is_self=excluded.is_self,
               paired=excluded.paired, alias=excluded.alias,
               machine_uid=COALESCE(excluded.machine_uid, devices.machine_uid)",
            params![
                d.id, d.name, d.device_kind.as_str(), d.ip, d.port as i64,
                d.paired_at.to_rfc3339(), d.last_sync_at.map(|t| t.to_rfc3339()), last_seen,
                last_seen_epoch,
                d.is_online as i64, d.is_self as i64, d.paired as i64, d.alias,
                d.machine_uid,
            ],
        )?;
        Ok(())
    }

    fn row_to_device(r: &rusqlite::Row) -> rusqlite::Result<Device> {
        Ok(Device {
            id: r.get(0)?,
            name: r.get(1)?,
            device_kind: parse_device_kind(&r.get::<_, String>(2)?),
            ip: r.get(3)?,
            port: r.get(4)?,
            paired_at: parse_dt(&r.get::<_, String>(5)?),
            last_sync_at: r.get::<_, Option<String>>(6)?.map(|s| parse_dt(&s)),
            last_seen_at: r.get::<_, Option<String>>(7)?.map(|s| parse_dt(&s)),
            is_online: r.get::<_, i64>(8)? != 0,
            is_self: r.get::<_, i64>(9)? != 0,
            paired: r.get::<_, i64>(10).unwrap_or(0) != 0,
            alias: r.get::<_, Option<String>>(11).ok().flatten(),
            // 密钥只活在 device_secrets 表与配对握手报文里，绝不随 Device 出接口
            device_secret: None,
            // 同理：装机指纹是机器标识，/devices 对同网段任何人可读，不能带出去。
            // 需要它做归并的内部逻辑走 machine_uid_of() 专用查询。
            machine_uid: None,
        })
    }

    /// 信任列表（界面与客户端读的这份）默认**不含被并掉的旧行**。
    /// 见 [`Self::merge_device`]：`superseded_by` 非空 = 这一行已归并进别的行，
    /// 留着只为历史归因，不该再出现在列表里。
    pub fn get_devices(&self) -> Result<Vec<Device>> {
        let mut stmt = self.conn.prepare(
            "SELECT id,name,device_kind,ip,port,paired_at,last_sync_at,last_seen_at,is_online,is_self,paired,alias FROM devices WHERE superseded_by IS NULL",
        )?;
        let rows = stmt.query_map([], Self::row_to_device)?;
        rows.collect()
    }

    /// 含被归并旧行的完整列表（排障与「显示历史」用）。
    pub fn get_devices_including_superseded(&self) -> Result<Vec<Device>> {
        let mut stmt = self.conn.prepare(
            "SELECT id,name,device_kind,ip,port,paired_at,last_sync_at,last_seen_at,is_online,is_self,paired,alias FROM devices",
        )?;
        let rows = stmt.query_map([], Self::row_to_device)?;
        rows.collect()
    }

    pub fn get_device(&self, id: &str) -> Result<Option<Device>> {
        let mut stmt = self.conn.prepare(
            "SELECT id,name,device_kind,ip,port,paired_at,last_sync_at,last_seen_at,is_online,is_self,paired,alias FROM devices WHERE id=?1",
        )?;
        let mut rows = stmt.query_map(params![id], Self::row_to_device)?;
        rows.next().transpose()
    }

    /// 广播发现 / mDNS 解析 / 推送自动登记：不存在则插入（未配对状态），
    /// 已存在则刷新可达信息：更新 last_seen_at 并标记 is_online=1，
    /// 保留 paired / alias / last_sync_at / paired_at。
    ///
    /// name 只在「尚未配对」时跟随宣告更新：宣告里带的是哈希别名（避免主机名被被动
    /// 抓包读走），已配对设备必须保留真实名字/别名，否则同步页会退化成一串十六进制。
    ///
    /// `seen_via` 记录这次是谁报上来的（"mdns" / "udp"），供 [`Self::device_seen_source`]
    /// 做优先级仲裁；调用方一律走 discovery::record_peer，不要绕过仲裁直接写库。
    pub fn upsert_discovered(&self, d: &Device, seen_via: &str) -> Result<()> {
        let last_seen = d.last_seen_at.map(|dt| dt.to_rfc3339());
        let last_seen_epoch = d.last_seen_at.map(|dt| dt.timestamp());
        self.conn.execute(
            "INSERT INTO devices (id,name,device_kind,ip,port,paired_at,last_sync_at,last_seen_at,last_seen_epoch,is_online,is_self,paired,alias,seen_via,seen_via_at)
             VALUES (?1,?2,?3,?4,?5,?6,NULL,?7,?9,0,0,0,NULL,?8,?7)
             ON CONFLICT(id) DO UPDATE SET
               name=CASE WHEN devices.paired=1 THEN devices.name ELSE excluded.name END,
               device_kind=excluded.device_kind,
               ip=excluded.ip, port=excluded.port, last_seen_at=excluded.last_seen_at,
               last_seen_epoch=excluded.last_seen_epoch,
               is_online=1, is_self=0, seen_via=excluded.seen_via, seen_via_at=excluded.seen_via_at",
            params![
                d.id, d.name, d.device_kind.as_str(), d.ip, d.port as i64,
                d.paired_at.to_rfc3339(), last_seen, seen_via, last_seen_epoch,
            ],
        )?;
        Ok(())
    }

    /// 某台设备最近一次是被谁发现的（"mdns" / "udp" / 空 = 未知），以及那个时刻。
    ///
    /// UDP 广播在写 ip/port 前要先问一次：mDNS 新鲜期内它是备选路径，
    /// 不能拿可能过期的广播地址盖掉 mDNS 解析出的权威地址。
    /// 时刻取 `seen_via_at`（那次宣告真正落库的瞬间）而不是 `last_seen_at`：
    /// 后者会被落败的广播刷新，新鲜期就永远过不去了。
    /// 时间解析失败按「过期」处理（宁可放行，也不要永久锁死一台设备的更新）。
    pub fn device_seen_source(&self, id: &str) -> Result<Option<(String, Option<DateTime<Utc>>)>> {
        match self.conn.query_row(
            "SELECT IFNULL(seen_via,''), seen_via_at FROM devices WHERE id=?1",
            params![id],
            |r| {
                let via: String = r.get(0)?;
                let seen: Option<String> = r.get(1)?;
                let seen = seen
                    .as_deref()
                    .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
                    .map(|d| d.with_timezone(&Utc));
                Ok((via, seen))
            },
        ) {
            Ok(row) => Ok(Some(row)),
            // 查不到 = 这台设备从没被发现过
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// 只刷新「还活着」，不动 ip/port：备选路径在仲裁中落败时用它保留在线状态。
    ///
    /// 同样不动 `seen_via_at`：落败的一方不该给自己续上首选路径的新鲜期。
    pub fn touch_seen(&self, id: &str) -> Result<()> {
        let now = Utc::now();
        self.conn.execute(
            "UPDATE devices SET last_seen_at=?2, last_seen_epoch=?3, is_online=1 WHERE id=?1 AND is_self=0",
            params![id, now.to_rfc3339(), now.timestamp()],
        )?;
        Ok(())
    }

    /// 更新设备别名（id 为本机时由上层改写 config.self_alias）
    pub fn update_alias(&self, id: &str, alias: Option<&str>) -> Result<bool> {
        let n = self.conn.execute(
            "UPDATE devices SET alias=?2 WHERE id=?1",
            params![id, alias],
        )?;
        Ok(n > 0)
    }

    pub fn delete_device(&self, id: &str) -> Result<bool> {
        let n = self.conn.execute("DELETE FROM devices WHERE id=?1", params![id])?;
        // 解除配对即作废密钥：残留旧密钥会让后续明文请求被错误拒收
        self.conn.execute("DELETE FROM device_secrets WHERE device_id=?1", params![id])?;
        Ok(n > 0)
    }

    /// 清空全部「非本机」设备（已配对 + 已发现），用于「清空所有配对信息」。
    /// 保留 is_self=1 的占位行（本机通常不落库）与同步设置。
    pub fn delete_all_devices(&self) -> Result<usize> {
        let n = self.conn.execute("DELETE FROM devices WHERE is_self = 0", [])?;
        self.conn.execute("DELETE FROM device_secrets", [])?;
        // 别名表不清的话，「清空所有配对信息」之后旧 id 仍会解析到已不存在的行
        self.conn.execute("DELETE FROM device_id_alias", [])?;
        Ok(n)
    }

    // ---- 装机指纹与归并（见 machine_uid 模块）----

    /// 某台设备登记的装机指纹（未登记 = None）。
    pub fn machine_uid_of(&self, id: &str) -> Result<Option<String>> {
        let raw: Option<String> = self
            .conn
            .query_row(
                "SELECT IFNULL(machine_uid, '') FROM devices WHERE id=?1",
                params![id],
                |r| r.get::<_, String>(0),
            )
            .optional()?;
        Ok(raw.filter(|s| !s.is_empty()))
    }

    /// 找「换了 device_id 但装机指纹相同」的另一行 —— 归并提示的唯一依据。
    ///
    /// 只认 `paired=1` 的行：未配对的旧行本来就归 `purge_discovered` 管，拿它当合并
    /// 目标没有意义。命中也不代表就是同一台机器（同机双实例的指纹完全相同），所以
    /// 调用方只把它当成**提示**，合并与否由用户点。
    ///
    /// 固定取配对最早的那行（`ORDER BY paired_at`）：三行同指纹时候选必须稳定，
    /// 否则两两互为候选，用户每次刷新看到的合并目标都在换。
    pub fn find_merge_candidate(&self, uid: &str, new_id: &str) -> Result<Option<Device>> {
        let mut stmt = self.conn.prepare(
            "SELECT id,name,device_kind,ip,port,paired_at,last_sync_at,last_seen_at,is_online,is_self,paired,alias
             FROM devices
             WHERE machine_uid=?1 AND id!=?2 AND paired=1 AND is_self=0 AND superseded_by IS NULL
             ORDER BY paired_at ASC LIMIT 1",
        )?;
        let mut rows = stmt.query_map(params![uid, new_id], Self::row_to_device)?;
        rows.next().transpose()
    }

    /// 旧 device_id → 现行 device_id（沿 device_id_alias 追，最多 8 跳防环）。
    /// 已同步过来的 inbox/todo 写着旧 id，解析设备名时要靠它落到还活着的行上。
    pub fn resolve_device_id(&self, id: &str) -> String {
        let mut cur = id.to_string();
        for _ in 0..8 {
            match self.conn.query_row(
                "SELECT new_id FROM device_id_alias WHERE old_id=?1",
                params![cur],
                |r| r.get::<_, String>(0),
            ) {
                Ok(next) if next != cur => cur = next,
                _ => break,
            }
        }
        cur
    }

    /// 把 from 行归并进 to 行（用户在「疑似同一台机器」的提示里点了「合并」）。
    ///
    /// 一个事务里四件事，任何一步失败整体回滚：
    /// 1. 作废旧 id 的密钥 —— 归并后旧 id 不该还能签名；
    /// 2. 旧行打 `superseded_by` 而**不删行**：已同步过来的数据仍按旧 device_id 归因；
    /// 3. 记一条 old→new 映射，供历史行解析设备名；
    /// 4. 旧行的别名与配对时间迁到新行（只在新行自己没设过别名/没配对时间时迁）。
    ///
    /// 刻意不碰 `device_secrets` 里 to 那行：配对是各自完成的，密钥各归各的。
    pub fn merge_device(&self, from: &str, to: &str) -> Result<(), String> {
        if from == to {
            return Err("不能与自己合并".into());
        }
        let check = |id: &str| -> Result<bool, String> {
            let row = self
                .conn
                .query_row(
                    "SELECT paired, is_self, superseded_by FROM devices WHERE id=?1",
                    params![id],
                    |r| {
                        Ok((
                            r.get::<_, i64>(0)? != 0,
                            r.get::<_, i64>(1)? != 0,
                            r.get::<_, Option<String>>(2)?,
                        ))
                    },
                )
                .optional()
                .map_err(|e| e.to_string())?;
            let (paired, is_self, superseded) = row.ok_or_else(|| format!("找不到设备 {id}"))?;
            if is_self {
                return Err(format!("{id} 是本机，不能参与归并"));
            }
            if superseded.is_some() {
                return Err(format!("{id} 已被归并过"));
            }
            Ok(paired)
        };
        let from_paired = check(from)?;
        let to_paired = check(to)?;
        if !to_paired && !from_paired {
            return Err("两边都没配对，没有归并的意义".into());
        }

        let tx = self
            .conn
            .unchecked_transaction()
            .map_err(|e| e.to_string())?;
        tx.execute("DELETE FROM device_secrets WHERE device_id=?1", params![from])
            .map_err(|e| e.to_string())?;
        tx.execute(
            "UPDATE devices SET superseded_by=?2, is_online=0 WHERE id=?1",
            params![from, to],
        )
        .map_err(|e| e.to_string())?;
        tx.execute(
            "INSERT INTO device_id_alias (old_id,new_id,merged_at) VALUES (?1,?2,?3)
             ON CONFLICT(old_id) DO UPDATE SET new_id=excluded.new_id, merged_at=excluded.merged_at",
            params![from, to, Utc::now().to_rfc3339()],
        )
        .map_err(|e| e.to_string())?;
        tx.execute(
            "UPDATE devices SET
               alias=COALESCE(alias, (SELECT alias FROM devices WHERE id=?2)),
               paired=MAX(paired, (SELECT paired FROM devices WHERE id=?2)),
               paired_at=MIN(paired_at, (SELECT paired_at FROM devices WHERE id=?2)),
               last_sync_at=COALESCE(last_sync_at, (SELECT last_sync_at FROM devices WHERE id=?2)),
               machine_uid=COALESCE(machine_uid, (SELECT machine_uid FROM devices WHERE id=?2))
             WHERE id=?1",
            params![to, from],
        )
        .map_err(|e| e.to_string())?;
        tx.commit().map_err(|e| e.to_string())?;
        Ok(())
    }

    /// 「一键清理」：删掉 N 天没同步成功过的旧配对行（连带密钥与别名映射）。
    ///
    /// 只管 `paired=1` 的行；未配对的交给 [`Self::purge_discovered`]。
    /// `last_sync_at` 为空的**不动**：配过但从没同步成功，多半是用户刚点的，
    /// 自动删掉会很莫名，得他自己在界面上确认。
    pub fn purge_stale_paired(&self, days: i64) -> Result<usize, String> {
        let cutoff = Utc::now().timestamp() - days.max(1) * 24 * 3600;
        let ids: Vec<String> = {
            let mut stmt = self
                .conn
                .prepare(
                    "SELECT id FROM devices
                     WHERE paired=1 AND is_self=0 AND superseded_by IS NULL
                       AND last_sync_at IS NOT NULL
                       AND CAST(strftime('%s', last_sync_at) AS INTEGER) < ?1",
                )
                .map_err(|e| e.to_string())?;
            let rows = stmt
                .query_map(params![cutoff], |r| r.get::<_, String>(0))
                .map_err(|e| e.to_string())?;
            rows.filter_map(|r| r.ok()).collect()
        };
        let mut n = 0;
        for id in &ids {
            n += self.delete_device(id).map_err(|e| e.to_string())? as usize;
            self.conn
                .execute("DELETE FROM device_id_alias WHERE old_id=?1 OR new_id=?1", params![id])
                .map_err(|e| e.to_string())?;
        }
        Ok(n)
    }

    /// 淘汰「纯发现、未配对、已不再播报」的行。
    ///
    /// 判据用 `seen_via IS NOT NULL`：那是发现路径（广播 / mDNS）自动登记的痕迹，
    /// 除了「有人喊过它」没有别的含义。手动登记与配对过的行要么 seen_via 为 NULL、
    /// 要么 paired=1，都不会被碰到；而「解除配对 → 退回发现池 → 静默若干天后消失」
    /// 正好是这个判据的自然结果，不需要另设状态。
    ///
    /// 两条规则各治一种膨胀：时间管「关机几天的正常设备」，条数管「扫过一堆一次性
    /// 设备」——只有时间规则的话，嘈杂网段照样能把列表堆到几百行。
    /// 返回值是删掉的总行数，供调用方记日志。
    pub fn purge_discovered(&self) -> Result<usize> {
        let cutoff = Utc::now().timestamp() - DISCOVERED_MAX_AGE_DAYS * 24 * 3600;
        let stale = self.conn.execute(
            "DELETE FROM devices
             WHERE paired=0 AND is_self=0 AND seen_via IS NOT NULL
               AND IFNULL(last_seen_epoch, 0) < ?1",
            params![cutoff],
        )?;
        let overflow = self.conn.execute(
            "DELETE FROM devices
             WHERE paired=0 AND is_self=0 AND seen_via IS NOT NULL
               AND id NOT IN (SELECT id FROM devices
                              WHERE paired=0 AND is_self=0 AND seen_via IS NOT NULL
                              ORDER BY IFNULL(last_seen_epoch, 0) DESC LIMIT ?1)",
            params![DISCOVERED_KEEP],
        )?;
        Ok(stale + overflow)
    }

    pub fn touch_online(&self, id: &str, online: bool) -> Result<()> {
        // 关键：如果设备最近还在播报（发现线程刚刷过 last_seen），不要覆盖 is_online=1。
        // 否则 probe_loop 的 HTTP 探活失败会反复把在线设备标记为离线。
        //
        // 比较一律用 last_seen_epoch（unix 秒）。曾经这里写的是
        // `last_seen_at > datetime('now','-30 seconds')`：last_seen_at 存的是 RFC3339 串，
        // SQLite 返回的是 'YYYY-MM-DD HH:MM:SS'，第 11 个字符 'T'(0x54) 恒大于 ' '(0x20)，
        // 所以那个条件对任何历史值都为真 —— 探活的「离线」结论永远写不进去，
        // 设备一旦上线过就永远显示在线。
        if !online {
            let recently_seen: bool = self.conn.query_row(
                "SELECT 1 FROM devices WHERE id=?1 AND last_seen_epoch > ?2",
                params![id, Utc::now().timestamp() - ONLINE_GRACE_SECS],
                |_| Ok(true),
            ).unwrap_or(false);
            if recently_seen {
                return Ok(());
            }
        }
        self.conn.execute(
            "UPDATE devices SET is_online=?2 WHERE id=?1",
            params![id, online as i64],
        )?;
        Ok(())
    }

    /// 设置设备的配对状态（配对成功置 true；解除/删除置 false）
    pub fn set_paired(&self, id: &str, paired: bool) -> Result<bool> {
        let n = self.conn.execute(
            "UPDATE devices SET paired=?2 WHERE id=?1",
            params![id, paired as i64],
        )?;
        Ok(n > 0)
    }

    pub fn mark_synced(&self, id: &str, at: DateTime<Utc>) -> Result<()> {
        self.conn.execute(
            "UPDATE devices SET last_sync_at=?2, is_online=1 WHERE id=?1",
            params![id, at.to_rfc3339()],
        )?;
        Ok(())
    }

    // ---- 信封加密密钥（B 方案）----
    //
    // 密钥刻意存在独立表而非 devices 表：桌面端 aw-server 监听 0.0.0.0，
    // /api/0/sync/devices 与 /log 对同网段任何人可读。密钥只要不进 Device 结构，
    // 就结构上不可能被接口序列化或日志打印带出去，而不依赖「记得过滤」这种自觉。

    /// 读取与某设备的共享密钥（hex）；未交换过密钥的对端返回 None（走明文回退）。
    pub fn get_device_secret(&self, id: &str) -> Result<Option<String>> {
        let mut stmt = self
            .conn
            .prepare("SELECT secret FROM device_secrets WHERE device_id=?1")?;
        let mut rows = stmt.query_map(params![id], |r| r.get::<_, String>(0))?;
        match rows.next() {
            Some(Ok(s)) => Ok(Some(s)),
            Some(Err(e)) => Err(e),
            None => Ok(None),
        }
    }

    /// 写入/更新共享密钥。已有值时覆盖（重新配对即轮换密钥）。
    pub fn set_device_secret(&self, id: &str, secret: &str) -> Result<()> {
        self.conn.execute(
            "INSERT INTO device_secrets (device_id, secret, updated_at) VALUES (?1,?2,?3)
             ON CONFLICT(device_id) DO UPDATE SET secret=excluded.secret, updated_at=excluded.updated_at",
            params![id, secret, Utc::now().to_rfc3339()],
        )?;
        Ok(())
    }

    /// 若尚无密钥则写入，返回最终生效的密钥。
    /// 用于「对端在 confirm 响应里回了密钥」的场景：先到先得，重复配对不覆盖已商定值。
    pub fn store_secret_if_absent(&self, id: &str, secret: &str) -> Result<Option<String>> {
        if let Some(existing) = self.get_device_secret(id)? {
            return Ok(Some(existing));
        }
        self.set_device_secret(id, secret)?;
        Ok(Some(secret.to_string()))
    }

    /// 列出所有已存密钥的对端 id（供 /devices 计算安全码，不返回密钥本身）
    pub fn list_secret_device_ids(&self) -> Result<Vec<String>> {
        let mut stmt = self
            .conn
            .prepare("SELECT device_id FROM device_secrets WHERE secret IS NOT NULL AND secret != ''")?;
        let rows = stmt.query_map([], |r| r.get::<_, String>(0))?;
        rows.collect()
    }

    /// 本机与各对端的安全码映射：`peer_id -> 4 位十六进制`。
    /// 两端各自算出同一值，人工对一眼即可确认中间人没有替换密钥。
    pub fn fingerprints(&self, self_id: &str) -> std::collections::HashMap<String, String> {
        let mut out = std::collections::HashMap::new();
        for id in self.list_secret_device_ids().unwrap_or_default() {
            let secret = match self.get_device_secret(&id) {
                Ok(Some(s)) => s,
                _ => continue,
            };
            if let Some(fp) = crate::crypto::fingerprint(&secret, self_id, &id) {
                out.insert(id, fp);
            }
        }
        out
    }

    pub fn delete_device_secret(&self, id: &str) -> Result<()> {
        self.conn
            .execute("DELETE FROM device_secrets WHERE device_id=?1", params![id])?;
        Ok(())
    }

    // ---- Pairing codes ----

    pub fn store_pair_code(&self, pc: &PairCode) -> Result<()> {
        self.conn.execute(
            "INSERT INTO pairing_codes (code,created_at,expires_at) VALUES (?1,?2,?3)
             ON CONFLICT(code) DO UPDATE SET created_at=excluded.created_at, expires_at=excluded.expires_at",
            params![pc.code, pc.created_at.to_rfc3339(), pc.expires_at.to_rfc3339()],
        )?;
        Ok(())
    }

    pub fn validate_pair_code(&self, code: &str) -> Result<bool> {
        let now = Utc::now();
        let mut stmt = self
            .conn
            .prepare("SELECT code,created_at,expires_at FROM pairing_codes WHERE code=?1")?;
        let mut rows = stmt.query_map(params![code], |r| {
            Ok(PairCode {
                code: r.get(0)?,
                created_at: parse_dt(&r.get::<_, String>(1)?),
                expires_at: parse_dt(&r.get::<_, String>(2)?),
            })
        })?;
        Ok(match rows.next() {
            Some(Ok(pc)) => now < pc.expires_at,
            _ => false,
        })
    }

    pub fn delete_pair_code(&self, code: &str) -> Result<()> {
        self.conn
            .execute("DELETE FROM pairing_codes WHERE code=?1", params![code])?;
        Ok(())
    }

    pub fn cleanup_expired_codes(&self) -> Result<()> {
        self.conn.execute(
            "DELETE FROM pairing_codes WHERE expires_at <= ?1",
            params![Utc::now().to_rfc3339()],
        )?;
        Ok(())
    }

    // ---- Sync log ----

    pub fn add_log(&self, e: &SyncLogEntry) -> Result<i64> {
        let details_json = e.details.as_ref().map(|d| serde_json::to_string(d).unwrap_or_default());
        self.conn.execute(
            "INSERT INTO sync_log (timestamp,direction,protocol,peer_id,event_type,status,message,data_size,details)
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9)",
            params![
                e.timestamp.to_rfc3339(), e.direction.as_str(), e.protocol.as_str(),
                e.peer_id, e.event_type.as_str(), e.status.as_str(), e.message,
                e.data_size.map(|s| s as i64), details_json,
            ],
        )?;
        let id = self.conn.last_insert_rowid();
        // 自动截断：每写 100 条检查一次，把 sync_log 稳定在 ~600 行内，
        // 避免 /log 的 COUNT(*) 与过滤查询随表膨胀击穿客户端读超时
        if id % 100 == 0 {
            if let Err(e) = self.truncate_logs(500) {
                log::warn!("[aw-sync] 自动截断 sync_log 失败: {e}");
            }
        }
        Ok(id)
    }

    pub fn get_logs(&self, f: &LogFilter) -> Result<Vec<SyncLogEntry>> {
        // 注意：SELECT 列表顺序与下方 r.get(N) 索引一一对应，details 必须在第 10 列
        let mut sql =
            String::from("SELECT id,timestamp,direction,protocol,peer_id,event_type,status,message,data_size,details FROM sync_log");
        let mut conds: Vec<String> = Vec::new();
        if let Some(d) = &f.direction {
            conds.push(format!("direction='{}'", d.as_str()));
        }
        if let Some(p) = &f.protocol {
            conds.push(format!("protocol='{}'", p.as_str()));
        }
        if let Some(e) = &f.event_type {
            conds.push(format!("event_type='{}'", e.as_str()));
        }
        if !conds.is_empty() {
            sql.push_str(" WHERE ");
            sql.push_str(&conds.join(" AND "));
        }
        sql.push_str(" ORDER BY id DESC LIMIT ?1 OFFSET ?2");
        let mut stmt = self.conn.prepare(&sql)?;
        let rows = stmt.query_map(params![f.limit.max(1) as i64, f.offset as i64], |r| {
            let details_str: Option<String> = r.get(9)?;
            let details = details_str.and_then(|s| serde_json::from_str::<Vec<crate::models::TransferRecord>>(&s).ok());
            Ok(SyncLogEntry {
                id: Some(r.get(0)?),
                timestamp: parse_dt(&r.get::<_, String>(1)?),
                direction: parse_direction(&r.get::<_, String>(2)?),
                protocol: parse_protocol(&r.get::<_, String>(3)?),
                peer_id: r.get::<_, Option<String>>(4)?,
                event_type: parse_event_type(&r.get::<_, String>(5)?),
                status: parse_status(&r.get::<_, String>(6)?),
                message: r.get(7)?,
                data_size: r.get::<_, Option<i64>>(8)?.map(|v| v as u64),
                details,
            })
        })?;
        rows.collect()
    }

    pub fn log_count(&self) -> Result<u64> {
        let n: i64 = self.conn.query_row("SELECT COUNT(*) FROM sync_log", [], |r| r.get(0))?;
        Ok(n as u64)
    }

    pub fn truncate_logs(&self, keep: u64) -> Result<()> {
        self.conn.execute(
            "DELETE FROM sync_log WHERE id NOT IN (SELECT id FROM sync_log ORDER BY id DESC LIMIT ?1)",
            params![keep as i64],
        )?;
        Ok(())
    }

    // ---- Config ----

    pub fn get_config(&self) -> SyncConfig {
        let mut cfg = SyncConfig::default();
        if let Ok(map) = self.get_all_config() {
            if let Some(v) = map.get("enabled") {
                cfg.enabled = v.as_bool().unwrap_or(false);
            }
            if let Some(v) = map.get("http_enabled") {
                cfg.http_enabled = v.as_bool().unwrap_or(true);
            }
            if let Some(v) = map.get("discovery_method") {
                cfg.discovery_method = v.as_str().unwrap_or("broadcast").to_string();
            }
            // 端口 0 视为「未配置」（旧版客户端写入的空值），回退默认端口。
            // 必须在这里兜底而不是只在使用处判断：self_device_info() 会把
            // listen_port 当作本机对外端点端口广播出去，0 会让对端拿到坏地址。
            if let Some(v) = map.get("listen_port") {
                let p = v.as_u64().unwrap_or(0) as u16;
                cfg.listen_port = if p == 0 { crate::models::DEFAULT_HTTP_PORT } else { p };
            }
            if let Some(v) = map.get("udp_port") {
                let p = v.as_u64().unwrap_or(0) as u16;
                cfg.udp_port = if p == 0 { crate::discovery::DEFAULT_UDP_PORT } else { p };
            }
            if let Some(v) = map.get("sync_inbox") {
                cfg.sync_inbox = v.as_bool().unwrap_or(true);
            }
            if let Some(v) = map.get("sync_activity") {
                cfg.sync_activity = v.as_bool().unwrap_or(true);
            }
            if let Some(v) = map.get("self_alias") {
                cfg.self_alias = v.as_str().unwrap_or("").to_string();
            }
            if let Some(v) = map.get("probe_interval") {
                cfg.probe_interval = v.as_u64().unwrap_or(10) as u16;
            }
            if let Some(v) = map.get("sync_interval") {
                cfg.sync_interval = v.as_u64().unwrap_or(10).max(5);
            }
            // Cloudflare D1 云同步
            if let Some(v) = map.get("d1_enabled") {
                cfg.d1_enabled = v.as_bool().unwrap_or(false);
            }
            if let Some(v) = map.get("d1_account_id") {
                cfg.d1_account_id = v.as_str().unwrap_or("").to_string();
            }
            if let Some(v) = map.get("d1_database_id") {
                cfg.d1_database_id = v.as_str().unwrap_or("").to_string();
            }
            if let Some(v) = map.get("d1_api_token") {
                cfg.d1_api_token = v.as_str().unwrap_or("").to_string();
            }
            if let Some(v) = map.get("d1_sync_interval") {
                cfg.d1_sync_interval = v.as_i64().unwrap_or(300);
            }
        }
        cfg
    }

    pub fn get_all_config(&self) -> Result<serde_json::Map<String, serde_json::Value>> {
        let mut stmt = self.conn.prepare("SELECT key,value FROM sync_config")?;
        let rows = stmt.query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)))?;
        let mut map = serde_json::Map::new();
        for row in rows {
            let (k, v) = row?;
            match k.as_str() {
                "enabled" | "http_enabled" | "sync_inbox" | "sync_activity" | "d1_enabled" => {
                    map.insert(k, serde_json::json!(v == "true"));
                }
                "listen_port" | "udp_port" | "probe_interval" => {
                    map.insert(k, serde_json::json!(v.parse::<u16>().unwrap_or(0)));
                }
                "sync_interval" => {
                    map.insert(k, serde_json::json!(v.parse::<u64>().unwrap_or(10)));
                }
                _ => {
                    map.insert(k, serde_json::json!(v));
                }
            }
        }
        Ok(map)
    }

    pub fn set_config(&self, cfg: &SyncConfig) -> Result<()> {
        let entries: Vec<(&str, String)> = vec![
            ("enabled", cfg.enabled.to_string()),
            ("http_enabled", cfg.http_enabled.to_string()),
            ("discovery_method", cfg.discovery_method.clone()),
            ("listen_port", cfg.listen_port.to_string()),
            ("udp_port", cfg.udp_port.to_string()),
            ("sync_inbox", cfg.sync_inbox.to_string()),
            ("sync_activity", cfg.sync_activity.to_string()),
            ("self_alias", cfg.self_alias.clone()),
            ("probe_interval", cfg.probe_interval.to_string()),
            ("sync_interval", cfg.sync_interval.max(5).to_string()),
            // Cloudflare D1 云同步
            ("d1_enabled", cfg.d1_enabled.to_string()),
            ("d1_account_id", cfg.d1_account_id.clone()),
            ("d1_database_id", cfg.d1_database_id.clone()),
            ("d1_api_token", cfg.d1_api_token.clone()),
            ("d1_sync_interval", cfg.d1_sync_interval.to_string()),
        ];
        let tx = self.conn.unchecked_transaction()?;
        for (k, v) in entries {
            tx.execute(
                "INSERT INTO sync_config (key,value) VALUES (?1,?2)
                 ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                params![k, v],
            )?;
        }
        tx.commit()?;
        Ok(())
    }

    // ---- D1 同步时间戳 ----

    /// 读取最近一次 D1 同步成功的时间戳（RFC3339），未同步过返回 None。
    pub fn get_d1_last_sync(&self) -> Option<String> {
        self.conn
            .query_row(
                "SELECT value FROM sync_config WHERE key='d1_last_sync'",
                [],
                |r| r.get::<_, String>(0),
            )
            .ok()
    }

    /// 写入 D1 同步成功的时间戳。
    pub fn set_d1_last_sync(&self, ts: &str) -> Result<(), String> {
        self.conn
            .execute(
                "INSERT OR REPLACE INTO sync_config (key,value) VALUES ('d1_last_sync', ?1)",
                params![ts],
            )
            .map_err(|e| e.to_string())?;
        Ok(())
    }
}

// ---- helpers ----

pub fn parse_device_kind(s: &str) -> DeviceKind {
    match s {
        "windows" => DeviceKind::Windows,
        "android" => DeviceKind::Android,
        "ios" => DeviceKind::Ios,
        "linux" => DeviceKind::Linux,
        "macos" => DeviceKind::Macos,
        _ => DeviceKind::Unknown,
    }
}

pub fn parse_direction(s: &str) -> SyncDirection {
    if s == "out" {
        SyncDirection::Out
    } else {
        SyncDirection::In
    }
}

pub fn parse_protocol(s: &str) -> SyncProtocol {
    match s {
        "udp_broadcast" => SyncProtocol::UdpBroadcast,
        "mdns" => SyncProtocol::Mdns,
        _ => SyncProtocol::Http,
    }
}

pub fn parse_event_type(s: &str) -> SyncEventType {
    match s {
        "discovery" => SyncEventType::Discovery,
        "sync" => SyncEventType::Sync,
        "conflict" => SyncEventType::Conflict,
        _ => SyncEventType::Pairing,
    }
}

pub fn parse_status(s: &str) -> SyncStatus {
    match s {
        "failed" => SyncStatus::Failed,
        "running" => SyncStatus::Running,
        _ => SyncStatus::Success,
    }
}

fn parse_dt(s: &str) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(s)
        .map(|d| d.with_timezone(&Utc))
        .unwrap_or_else(|_| Utc::now())
}

// ---- 设备同步统计 ----

impl SyncDb {
    /// 获取设备同步统计信息
    pub fn get_device_sync_stats(&self, device_id: &str) -> Result<DeviceSyncStats> {
        // 从 sync_log 表统计总同步条数和大小
        let sync_stats = self.conn.query_row(
            "SELECT 
                COALESCE(SUM(CASE WHEN event_type='sync' AND status='success' THEN 1 ELSE 0 END), 0),
                COALESCE(SUM(CASE WHEN event_type='sync' AND status='success' THEN data_size ELSE 0 END), 0)
             FROM sync_log WHERE peer_id=?1",
            params![device_id],
            |r| {
                Ok((
                    r.get::<_, i64>(0)? as i64,
                    r.get::<_, i64>(1)? as i64,
                ))
            },
        )?;

        // 获取上次同步时间
        let last_sync_at: Option<String> = self.conn.query_row(
            "SELECT last_sync_at FROM devices WHERE id=?1",
            params![device_id],
            |r| r.get(0),
        )?;

        // 获取上次全量同步时间（从 sync_log 中查找包含 "全量" 或 "full sync" 的记录）
        let last_full_sync_at: Option<String> = self.conn.query_row(
            "SELECT timestamp FROM sync_log 
             WHERE peer_id=?1 AND event_type='sync' AND status='success' 
             AND (message LIKE '%全量%' OR message LIKE '%full sync%' OR message LIKE '%Full%')
             ORDER BY timestamp DESC LIMIT 1",
            params![device_id],
            |r| r.get(0),
        ).ok();

        // 计算同步频率（最近 10 次同步的平均间隔）
        let sync_frequency_minutes = self.calculate_sync_frequency(device_id)?;

        // 获取最近错误信息
        let last_error_info: Option<(String, String)> = self.conn.query_row(
            "SELECT message, timestamp FROM sync_log 
             WHERE peer_id=?1 AND event_type='sync' AND status='failed'
             ORDER BY timestamp DESC LIMIT 1",
            params![device_id],
            |r| Ok((r.get(0)?, r.get(1)?)),
        ).ok();

        // 待同步/待解决冲突数：从 sync_conflicts 表统计未解决冲突
        let pending_conflict_count: i32 = self
            .conn
            .query_row(
                "SELECT COALESCE(COUNT(*),0) FROM sync_conflicts WHERE device_id=?1 AND resolution=''",
                params![device_id],
                |r| r.get::<_, i64>(0),
            )
            .unwrap_or(0) as i32;
        let pending_push_count: i32 = 0;

        // 获取本地笔记条数（从 inbox_notes 表中统计）
        let local_note_count = self.get_local_note_count();
        let remote_note_count = self.get_remote_note_count(device_id);

        Ok(DeviceSyncStats {
            device_id: device_id.to_string(),
            pending_push_count,
            pending_conflict_count,
            total_synced_count: sync_stats.0,
            total_synced_size: sync_stats.1,
            local_note_count,
            remote_note_count,
            last_sync_at,
            last_full_sync_at,
            sync_frequency_minutes,
            last_error: last_error_info.as_ref().map(|(m, _)| m.clone()),
            last_error_at: last_error_info.map(|(_, t)| t),
        })
    }

    /// 计算同步频率（最近 10 次同步的平均间隔，单位：分钟）
    fn calculate_sync_frequency(&self, device_id: &str) -> Result<Option<i32>> {
        let mut stmt = self.conn.prepare(
            "SELECT timestamp FROM sync_log 
             WHERE peer_id=?1 AND event_type='sync' AND status='success'
             ORDER BY timestamp DESC LIMIT 10",
        )?;

        let timestamps: Vec<DateTime<Utc>> = stmt
            .query_map(params![device_id], |r| {
                let ts: String = r.get(0)?;
                Ok(parse_dt(&ts))
            })?
            .filter_map(|r| r.ok())
            .collect();

        if timestamps.len() < 2 {
            return Ok(None);
        }

        let mut total_minutes = 0i64;
        for i in 0..timestamps.len() - 1 {
            let diff = timestamps[i] - timestamps[i + 1];
            total_minutes += diff.num_minutes();
        }

        let avg_minutes = (total_minutes / (timestamps.len() as i64 - 1)) as i32;
        Ok(Some(avg_minutes))
    }

    /// 获取本地笔记条数
    fn get_local_note_count(&self) -> i32 {
        // 尝试查询 inbox_notes 表，如果表不存在则返回 0
        self.conn
            .query_row("SELECT COUNT(*) FROM inbox_notes", [], |r| r.get(0))
            .unwrap_or(0)
    }

    /// 获取远端笔记条数（通过同步日志估算）
    fn get_remote_note_count(&self, device_id: &str) -> i32 {
        // 从同步日志中统计从该设备同步过来的笔记条数
        // 这是一个估算值，实际应该在数据库中存储
        self.conn
            .query_row(
                "SELECT COALESCE(SUM(CASE WHEN direction='in' AND event_type='sync' AND status='success' 
                 THEN CAST(SUBSTR(message, INSTR(message, ':') + 1) AS INTEGER) ELSE 0 END), 0)
                 FROM sync_log WHERE peer_id=?1",
                params![device_id],
                |r| r.get(0),
            )
            .unwrap_or(0)
    }

    /// 获取设备冲突列表（从 sync_conflicts 表读取；P0 起该表已真实落库）
    pub fn get_device_conflicts(&self, device_id: &str) -> Result<Vec<ConflictSummary>> {
        let mut stmt = self.conn.prepare(
            "SELECT id,kind,logical_key,created_at,resolution 
             FROM sync_conflicts WHERE device_id=?1 ORDER BY created_at DESC",
        )?;
        let rows = stmt.query_map(params![device_id], |r| {
            let key: String = r.get(2)?;
            let created: String = r.get(3)?;
            let resolution: Option<String> = r.get(4)?;
            let resolved = resolution
                .as_deref()
                .map(|s| !s.is_empty() && s != "unresolved")
                .unwrap_or(false);
            Ok(ConflictSummary {
                note_id: key.parse::<i64>().unwrap_or(0),
                note_title: key,
                detected_at: created,
                resolved,
                resolution,
            })
        })?;
        rows.collect()
    }
}

// ---- 冲突记录 + 回收站（P0）----

impl SyncDb {
    /// 写入一条冲突记录。resolution：overwritten_by_remote / deleted_by_remote /
    /// stale_remote_ignored / unresolved（人工处理中）。
    pub fn insert_conflict(
        &self,
        device_id: &str,
        kind: &str,
        logical_key: &str,
        local_rev: Option<&str>,
        remote_rev: Option<&str>,
        resolution: &str,
        archived_id: Option<i64>,
    ) -> Result<i64> {
        self.conn.execute(
            "INSERT INTO sync_conflicts (device_id,kind,logical_key,local_rev,remote_rev,resolution,archived_id,created_at)
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8)",
            params![
                device_id,
                kind,
                logical_key,
                local_rev,
                remote_rev,
                resolution,
                archived_id,
                Utc::now().to_rfc3339()
            ],
        )?;
        Ok(self.conn.last_insert_rowid())
    }

    /// 写入一条回收站归档，返回 trash id。
    pub fn insert_trash(&self, t: &TrashEntry) -> Result<i64> {
        self.conn.execute(
            "INSERT INTO trash (kind,logical_key,archived,winner_rev,reason,source_device,archived_at,restored)
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8)",
            params![
                t.kind,
                t.logical_key,
                t.archived,
                t.winner_rev,
                t.reason,
                t.source_device,
                t.archived_at,
                t.restored as i64
            ],
        )?;
        Ok(self.conn.last_insert_rowid())
    }

    fn row_to_trash(r: &rusqlite::Row) -> rusqlite::Result<TrashEntry> {
        Ok(TrashEntry {
            id: r.get(0)?,
            kind: r.get(1)?,
            logical_key: r.get(2)?,
            archived: r.get(3)?,
            winner_rev: r.get(4)?,
            reason: r.get(5)?,
            source_device: r.get(6)?,
            archived_at: r.get(7)?,
            restored: r.get::<_, i64>(8)? != 0,
        })
    }

    /// 列出回收站；kind 为空表示全部（"note"/"todo"）。
    pub fn list_trash(&self, kind: Option<&str>) -> Result<Vec<TrashEntry>> {
        let sql = match kind {
            Some(k) => {
                "SELECT id,kind,logical_key,archived,winner_rev,reason,source_device,archived_at,restored
                 FROM trash WHERE kind=?1 ORDER BY archived_at DESC"
            }
            None => {
                "SELECT id,kind,logical_key,archived,winner_rev,reason,source_device,archived_at,restored
                 FROM trash ORDER BY archived_at DESC"
            }
        };
        let mut stmt = self.conn.prepare(sql)?;
        let rows = if let Some(k) = kind {
            stmt.query_map(params![k], Self::row_to_trash)?
        } else {
            stmt.query_map([], Self::row_to_trash)?
        };
        rows.collect()
    }

    pub fn get_trash(&self, id: i64) -> Result<Option<TrashEntry>> {
        let mut stmt = self.conn.prepare(
            "SELECT id,kind,logical_key,archived,winner_rev,reason,source_device,archived_at,restored
             FROM trash WHERE id=?1",
        )?;
        let mut rows = stmt.query_map(params![id], Self::row_to_trash)?;
        rows.next().transpose()
    }

    /// 标记归档已恢复（restore 成功后），避免重复恢复。
    pub fn mark_trash_restored(&self, id: i64) -> Result<bool> {
        let n = self
            .conn
            .execute("UPDATE trash SET restored=1 WHERE id=?1", params![id])?;
        Ok(n > 0)
    }

    /// 从回收站永久删除一条（手动清空 / 恢复后清理）。
    pub fn delete_trash(&self, id: i64) -> Result<bool> {
        let n = self
            .conn
            .execute("DELETE FROM trash WHERE id=?1", params![id])?;
        Ok(n > 0)
    }

    /// 清理早于 cutoff 且未恢复的归档（默认 90 天自动清理）。
    pub fn purge_trash_before(&self, cutoff: &str) -> Result<usize> {
        let n = self.conn.execute(
            "DELETE FROM trash WHERE restored=0 AND archived_at < ?1",
            params![cutoff],
        )?;
        Ok(n)
    }

    /// 未恢复归档总数（供前端角标）。
    pub fn count_trash(&self) -> Result<i64> {
        self.conn
            .query_row("SELECT COUNT(*) FROM trash WHERE restored=0", [], |r| r.get(0))
    }

    /// 自动清理已恢复的归档副本（保留期结束后清理）。
    pub fn purge_restored_before(&self, cutoff: &str) -> Result<usize> {
        let n = self.conn.execute(
            "DELETE FROM trash WHERE restored=1 AND archived_at < ?1",
            params![cutoff],
        )?;
        Ok(n)
    }
}
