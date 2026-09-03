//! 同步状态持久化（sync.db）：设备、配对码、同步日志、同步设置、冲突记录、回收站。
//! 独立于 aw-server 主库，逻辑隔离。

use std::path::Path;

use chrono::{DateTime, Utc};
use rusqlite::{params, Connection, Result};

use crate::models::{
    ConflictSummary, Device, DeviceKind, DeviceSyncStats, PairCode, SyncConfig, SyncDirection,
    SyncEventType, SyncLogEntry, SyncProtocol, SyncStatus, TrashEntry,
};

pub struct SyncDb {
    conn: Connection,
}

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
            COMMIT;",
        )?;
        // 幂等加列（老库升级）
        self.ensure_column("devices", "paired", "ALTER TABLE devices ADD COLUMN paired INTEGER NOT NULL DEFAULT 0");
        self.ensure_column("devices", "alias", "ALTER TABLE devices ADD COLUMN alias TEXT");
        self.ensure_column("devices", "last_seen_at", "ALTER TABLE devices ADD COLUMN last_seen_at TEXT");
        self.ensure_column("sync_log", "details", "ALTER TABLE sync_log ADD COLUMN details TEXT");
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
        self.conn.execute(
            "INSERT INTO devices (id,name,device_kind,ip,port,paired_at,last_sync_at,last_seen_at,is_online,is_self,paired,alias)
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12)
             ON CONFLICT(id) DO UPDATE SET name=excluded.name, device_kind=excluded.device_kind,
               ip=excluded.ip, port=excluded.port, last_sync_at=excluded.last_sync_at,
               last_seen_at=excluded.last_seen_at, is_online=excluded.is_online, is_self=excluded.is_self,
               paired=excluded.paired, alias=excluded.alias",
            params![
                d.id, d.name, d.device_kind.as_str(), d.ip, d.port as i64,
                d.paired_at.to_rfc3339(), d.last_sync_at.map(|t| t.to_rfc3339()), last_seen,
                d.is_online as i64, d.is_self as i64, d.paired as i64, d.alias,
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
        })
    }

    pub fn get_devices(&self) -> Result<Vec<Device>> {
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

    /// 广播发现 / 推送自动登记：不存在则插入（未配对状态），
    /// 已存在则只刷新可达信息，**保留 paired / alias / last_sync_at / paired_at / last_seen_at**。
    pub fn upsert_discovered(&self, d: &Device) -> Result<()> {
        let last_seen = d.last_seen_at.map(|dt| dt.to_rfc3339());
        self.conn.execute(
            "INSERT INTO devices (id,name,device_kind,ip,port,paired_at,last_sync_at,last_seen_at,is_online,is_self,paired,alias)
             VALUES (?1,?2,?3,?4,?5,?6,NULL,?7,0,0,0,NULL)
             ON CONFLICT(id) DO UPDATE SET name=excluded.name, device_kind=excluded.device_kind,
               ip=excluded.ip, port=excluded.port, last_seen_at=excluded.last_seen_at, is_self=0",
            params![
                d.id, d.name, d.device_kind.as_str(), d.ip, d.port as i64,
                d.paired_at.to_rfc3339(), last_seen,
            ],
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
        Ok(n > 0)
    }

    /// 清空全部「非本机」设备（已配对 + 已发现），用于「清空所有配对信息」。
    /// 保留 is_self=1 的占位行（本机通常不落库）与同步设置。
    pub fn delete_all_devices(&self) -> Result<usize> {
        let n = self.conn.execute("DELETE FROM devices WHERE is_self = 0", [])?;
        Ok(n)
    }

    pub fn touch_online(&self, id: &str, online: bool) -> Result<()> {
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
        Ok(self.conn.last_insert_rowid())
    }

    pub fn get_logs(&self, f: &LogFilter) -> Result<Vec<SyncLogEntry>> {
        let mut sql =
            String::from("SELECT id,timestamp,direction,protocol,peer_id,event_type,status,message,data_size FROM sync_log");
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
            if let Some(v) = map.get("listen_port") {
                cfg.listen_port = v.as_u64().unwrap_or(5600) as u16;
            }
            if let Some(v) = map.get("udp_port") {
                cfg.udp_port = v.as_u64().unwrap_or(46000) as u16;
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
                "enabled" | "http_enabled" | "sync_inbox" | "sync_activity" => {
                    map.insert(k, serde_json::json!(v == "true"));
                }
                "listen_port" | "udp_port" | "probe_interval" => {
                    map.insert(k, serde_json::json!(v.parse::<u16>().unwrap_or(0)));
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
