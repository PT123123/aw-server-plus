//! 同步载荷序列化：把目标库（ActivityWatch 的 sqlite.db + Inbox 的 inbox.db + Todo 的 todo.db）
//! 用 SQL 读出为 JSON 文本，供局域网 HTTP 传输；接收端写回目标库。
//!
//! 合并策略（P0 起启用「自动仲裁 + 冲突进回收站」，见 docs/lan-sync-conflict-redesign.md）：
//! - Inbox/Todo：以 uuid 为逻辑键跨设备匹配，rev = (updated_at 毫秒, device_id) 字典序仲裁；
//!   对端更新则覆盖本地并把本地旧版本归档进回收站；本地更新则忽略对端并把对端版本归档；
//!   内容完全一致则去重忽略。删除状态随 deleted 列同步（event 删除不传播，bucket 不做删除传播）。
//! - Activity：bucket 按 name 合并（避免 name UNIQUE 冲突吞掉整批）；event 按
//!   (bucket_name, timestamp, duration, data) 内容指纹去重，追加式合并，绝不覆盖。

use std::collections::{HashMap, HashSet};
use std::path::Path;

use chrono::Utc;
use rusqlite::Connection;
use serde::{Deserialize, Serialize};

use crate::conflict;
use crate::models::TransferRecord;

/// 一条被归档的记录（冲突 / 删除导致被覆盖或忽略的旧版本，交由上层写入回收站表）。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArchivedRecord {
    /// 业务类型："note" | "todo"
    pub kind: String,
    /// 逻辑键（uuid，或旧数据回退的 "legacy:{id}"）
    pub logical_key: String,
    /// 被归档行的完整 JSON（可用于恢复）
    pub archived_json: String,
    /// 胜出方的 rev，形如 "2026-09-03T10:00:00.000Z@device-1"
    pub winner_rev: Option<String>,
    /// 归档原因：overwritten_by_remote / deleted_by_remote / stale_remote_ignored
    pub reason: String,
}

/// 一次导入的合并结果（结构化，供上层落 sync_conflicts / trash 统计）。
#[derive(Debug, Clone, Default)]
pub struct ImportOutcome {
    pub created: usize,
    pub updated: usize,
    pub deleted: usize,
    pub ignored_stale: usize,
    pub ignored_dup: usize,
    pub archived: Vec<ArchivedRecord>,
    pub errors: Vec<String>,
    /// 逐条传输明细（P1 起，供前端「同步详情」展示）
    pub records: Vec<TransferRecord>,
}

impl ImportOutcome {
    pub fn applied(&self) -> usize {
        self.created + self.updated + self.deleted
    }
}

/// 截断字符串到 max_chars 个字符（用于 title 字段）。
fn truncate_title(s: &str, max_chars: usize) -> String {
    if s.chars().count() <= max_chars {
        s.to_string()
    } else {
        s.chars().take(max_chars).collect::<String>() + "…"
    }
}

/// 从 NoteRow 构建一条传输明细。
fn transfer_from_note(n: &NoteRow, action: &str, reason: Option<&str>) -> TransferRecord {
    TransferRecord {
        kind: "note".into(),
        logical_key: logical_key(&n.uuid, n.id),
        title: truncate_title(&n.content, 40),
        action: action.into(),
        reason: reason.map(|s| s.to_string()),
    }
}

/// 从 TodoRow 构建一条传输明细。
fn transfer_from_todo(t: &TodoRow, action: &str, reason: Option<&str>) -> TransferRecord {
    TransferRecord {
        kind: "todo".into(),
        logical_key: logical_key(&t.uuid, t.id),
        title: truncate_title(&t.title, 40),
        action: action.into(),
        reason: reason.map(|s| s.to_string()),
    }
}

// ---- Activity ----

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ActivityExport {
    pub buckets: Vec<BucketRow>,
    pub events: Vec<EventRow>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BucketRow {
    pub id: i64,
    pub name: String,
    pub bucket_type: String,
    pub client: String,
    pub hostname: String,
    pub created: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventRow {
    pub id: i64,
    pub bucketrow: i64,
    /// 所属 bucket 的名称（发送方导出时 JOIN 得到，接收方据此映射本地 bucket id）
    #[serde(default)]
    pub bucket_name: String,
    /// 起始时间（unix 毫秒）
    pub timestamp: i64,
    /// 时长（毫秒）
    pub duration: i64,
    pub data: String,
}

/// 导出 ActivityWatch 主库(sqlite.db)为 JSON 文本
pub fn export_activity(db_path: &Path) -> Result<String, String> {
    let conn = Connection::open(db_path).map_err(|e| e.to_string())?;
    let mut buckets = Vec::new();
    let mut stmt = conn
        .prepare("SELECT id,name,type,client,hostname,created FROM buckets")
        .map_err(|e| e.to_string())?;
    let rows = stmt
        .query_map([], |r| {
            Ok(BucketRow {
                id: r.get(0)?,
                name: r.get(1)?,
                bucket_type: r.get(2)?,
                client: r.get(3)?,
                hostname: r.get(4)?,
                created: r.get(5)?,
            })
        })
        .map_err(|e| e.to_string())?;
    for row in rows {
        buckets.push(row.map_err(|e| e.to_string())?);
    }

    let mut events = Vec::new();
    let mut stmt = conn
        .prepare(
            "SELECT e.id,e.bucketrow,b.name,e.starttime,e.endtime,e.data
             FROM events e JOIN buckets b ON b.id = e.bucketrow",
        )
        .map_err(|e| e.to_string())?;
    let rows = stmt
        .query_map([], |r| {
            let start: i64 = r.get(4)?;
            let end: i64 = r.get(5)?;
            Ok(EventRow {
                id: r.get(0)?,
                bucketrow: r.get(1)?,
                bucket_name: r.get(2)?,
                timestamp: start,
                duration: (end - start).max(0),
                data: r.get(6)?,
            })
        })
        .map_err(|e| e.to_string())?;
    for row in rows {
        events.push(row.map_err(|e| e.to_string())?);
    }

    serde_json::to_string(&ActivityExport { buckets, events }).map_err(|e| e.to_string())
}

// ---- Inbox ----

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct InboxExport {
    pub notes: Vec<NoteRow>,
    pub relations: Vec<RelationRow>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NoteRow {
    pub id: i64,
    #[serde(default)]
    pub uuid: String,
    pub content: String,
    pub tags: String,
    pub created_at: String,
    pub updated_at: String,
    #[serde(default)]
    pub version: i64,
    #[serde(default)]
    pub device_id: Option<String>,
    #[serde(default)]
    pub deleted: i64,
    #[serde(default)]
    pub synced_at: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RelationRow {
    pub id: i64,
    pub source_note_id: i64,
    pub target_note_id: i64,
    pub relation_type: String,
    pub created_at: String,
}

/// 导出 Inbox 库(inbox.db)为 JSON 文本（含 uuid / version / device_id / deleted / synced_at）
pub fn export_inbox(db_path: &Path) -> Result<String, String> {
    let conn = Connection::open(db_path).map_err(|e| e.to_string())?;
    let mut notes = Vec::new();
    let mut stmt = conn
        .prepare(
            "SELECT id,uuid,content,tags,created_at,updated_at,version,device_id,deleted,synced_at
             FROM notes",
        )
        .map_err(|e| e.to_string())?;
    let rows = stmt
        .query_map([], |r| {
            Ok(NoteRow {
                id: r.get(0)?,
                uuid: r.get(1)?,
                content: r.get(2)?,
                tags: r.get(3)?,
                created_at: r.get(4)?,
                updated_at: r.get(5)?,
                version: r.get(6)?,
                device_id: r.get(7)?,
                deleted: r.get(8)?,
                synced_at: r.get(9)?,
            })
        })
        .map_err(|e| e.to_string())?;
    for row in rows {
        notes.push(row.map_err(|e| e.to_string())?);
    }

    let mut relations = Vec::new();
    let mut stmt = conn
        .prepare("SELECT id,source_note_id,target_note_id,relation_type,created_at FROM note_relations")
        .map_err(|e| e.to_string())?;
    let rows = stmt
        .query_map([], |r| {
            Ok(RelationRow {
                id: r.get(0)?,
                source_note_id: r.get(1)?,
                target_note_id: r.get(2)?,
                relation_type: r.get(3)?,
                created_at: r.get(4)?,
            })
        })
        .map_err(|e| e.to_string())?;
    for row in rows {
        relations.push(row.map_err(|e| e.to_string())?);
    }

    serde_json::to_string(&InboxExport { notes, relations }).map_err(|e| e.to_string())
}

// ---- Todo ----

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct TodoExport {
    pub todos: Vec<TodoRow>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TodoRow {
    pub id: i64,
    #[serde(default)]
    pub uuid: String,
    pub title: String,
    #[serde(default)]
    pub content: Option<String>,
    #[serde(default)]
    pub completed: i64,
    #[serde(default)]
    pub priority: Option<i64>,
    #[serde(default)]
    pub due_date: Option<String>,
    #[serde(default)]
    pub tags: String,
    pub created_at: String,
    pub updated_at: String,
    #[serde(default)]
    pub completed_at: Option<String>,
    #[serde(default)]
    pub version: i64,
    #[serde(default)]
    pub device_id: Option<String>,
    #[serde(default)]
    pub deleted: i64,
    #[serde(default)]
    pub synced_at: Option<String>,
}

/// 导出 Todo 库(todo.db)为 JSON 文本（含已软删除的行，保证删除状态可同步）
pub fn export_todo(db_path: &Path) -> Result<String, String> {
    let conn = Connection::open(db_path).map_err(|e| e.to_string())?;
    let mut stmt = conn
        .prepare(
            "SELECT id,uuid,title,content,completed,priority,due_date,tags,created_at,updated_at,
                    completed_at,version,device_id,deleted,synced_at FROM todos",
        )
        .map_err(|e| e.to_string())?;
    let rows = stmt
        .query_map([], |r| {
            Ok(TodoRow {
                id: r.get(0)?,
                uuid: r.get(1)?,
                title: r.get(2)?,
                content: r.get(3)?,
                completed: r.get(4)?,
                priority: r.get(5)?,
                due_date: r.get(6)?,
                tags: r.get(7)?,
                created_at: r.get(8)?,
                updated_at: r.get(9)?,
                completed_at: r.get(10)?,
                version: r.get(11)?,
                device_id: r.get(12)?,
                deleted: r.get(13)?,
                synced_at: r.get(14)?,
            })
        })
        .map_err(|e| e.to_string())?;
    let mut todos = Vec::new();
    for row in rows {
        todos.push(row.map_err(|e| e.to_string())?);
    }
    serde_json::to_string(&TodoExport { todos }).map_err(|e| e.to_string())
}

// ---- Schema helpers ----

/// 幂等补列（老库升级用），避免整表 DROP 重建。
fn ensure_column(conn: &Connection, table: &str, column: &str, decl: &str) -> Result<(), String> {
    let mut stmt = conn
        .prepare(&format!("PRAGMA table_info({table})"))
        .map_err(|e| e.to_string())?;
    let cols: Vec<String> = stmt
        .query_map([], |r| r.get::<_, String>(1))
        .map_err(|e| e.to_string())?
        .filter_map(|r| r.ok())
        .collect();
    if !cols.iter().any(|c| c == column) {
        conn.execute(
            &format!("ALTER TABLE {table} ADD COLUMN {column} {decl}"),
            [],
        )
        .map_err(|e| e.to_string())?;
    }
    Ok(())
}

/// 为缺少 uuid 的历史行补 uuid（每行不同，SQLite 对每行重新求值 randomblob）。
fn backfill_uuid(conn: &Connection, table: &str) -> Result<(), String> {
    conn.execute(
        &format!(
            "UPDATE {table} SET uuid = lower(hex(randomblob(16))) WHERE uuid IS NULL OR uuid = ''"
        ),
        [],
    )
    .map_err(|e| e.to_string())?;
    Ok(())
}

fn logical_key(uuid: &str, fallback_id: i64) -> String {
    if uuid.trim().is_empty() {
        format!("legacy:{fallback_id}")
    } else {
        uuid.to_string()
    }
}

/// 被归档记录的 rev 展示串。
fn rev_str(ts: &str, dev: &Option<String>) -> String {
    format!("{}@{}", ts, dev.as_deref().unwrap_or(""))
}

// ---- Import: Inbox ----

/// 把 Inbox JSON 写入 inbox.db（按 uuid 逻辑键 + rev 仲裁合并；被覆盖方进 archived）。
pub fn import_inbox(db_path: &Path, json: &str) -> Result<ImportOutcome, String> {
    let data: InboxExport = serde_json::from_str(json).map_err(|e| e.to_string())?;
    let conn = Connection::open(db_path).map_err(|e| e.to_string())?;
    // 首次接收同步的目标库可能尚不存在，先确保 schema 就位（与 aw-inbox-rust 对齐，含 uuid）
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
    .map_err(|e| format!("ensure inbox schema failed: {e}"))?;
    ensure_column(&conn, "notes", "uuid", "TEXT")?;
    backfill_uuid(&conn, "notes")?;

    // 本地索引：uuid -> 行
    let mut local: HashMap<String, NoteRow> = HashMap::new();
    {
        let mut stmt = conn
            .prepare(
                "SELECT id,uuid,content,tags,created_at,updated_at,version,device_id,deleted,synced_at
                 FROM notes",
            )
            .map_err(|e| e.to_string())?;
        let rows = stmt
            .query_map([], |r| {
                Ok(NoteRow {
                    id: r.get(0)?,
                    uuid: r.get(1)?,
                    content: r.get(2)?,
                    tags: r.get(3)?,
                    created_at: r.get(4)?,
                    updated_at: r.get(5)?,
                    version: r.get(6)?,
                    device_id: r.get(7)?,
                    deleted: r.get(8)?,
                    synced_at: r.get(9)?,
                })
            })
            .map_err(|e| e.to_string())?;
        for row in rows {
            let n = row.map_err(|e| e.to_string())?;
            local.insert(n.uuid.clone(), n);
        }
    }

    let tx = conn.unchecked_transaction().map_err(|e| e.to_string())?;
    let mut out = ImportOutcome::default();
    for n in &data.notes {
        let key = logical_key(&n.uuid, n.id);
        match local.get(&key) {
            Some(ln) => {
                let same =
                    ln.content == n.content && ln.tags == n.tags && ln.deleted == n.deleted;
                if same {
                    out.ignored_dup += 1;
                    out.records.push(transfer_from_note(n, "ignored_dup", None));
                    continue;
                }
                let newer = conflict::incoming_newer(
                    &n.updated_at,
                    n.device_id.as_deref(),
                    &ln.updated_at,
                    ln.device_id.as_deref(),
                );
                if newer {
                    out.archived.push(ArchivedRecord {
                        kind: "note".into(),
                        logical_key: key.clone(),
                        archived_json: serde_json::to_string(ln).unwrap_or_default(),
                        winner_rev: Some(rev_str(&n.updated_at, &n.device_id)),
                        reason: if n.deleted != 0 {
                            "deleted_by_remote".into()
                        } else {
                            "overwritten_by_remote".into()
                        },
                    });
                    tx.execute(
                        "UPDATE notes SET content=?1,tags=?2,created_at=?3,updated_at=?4,
                           version=?5,device_id=?6,deleted=?7,synced_at=?8 WHERE uuid=?9",
                        rusqlite::params![
                            n.content,
                            n.tags,
                            n.created_at,
                            n.updated_at,
                            n.version,
                            n.device_id,
                            n.deleted,
                            n.synced_at,
                            key
                        ],
                    )
                    .map_err(|e| e.to_string())?;
                    if n.deleted != 0 {
                        out.deleted += 1;
                        out.records.push(transfer_from_note(n, "deleted", Some("deleted_by_remote")));
                    } else {
                        out.updated += 1;
                        out.records.push(transfer_from_note(n, "updated", Some("overwritten_by_remote")));
                    }
                } else {
                    out.archived.push(ArchivedRecord {
                        kind: "note".into(),
                        logical_key: key.clone(),
                        archived_json: serde_json::to_string(n).unwrap_or_default(),
                        winner_rev: Some(rev_str(&ln.updated_at, &ln.device_id)),
                        reason: "stale_remote_ignored".into(),
                    });
                    out.ignored_stale += 1;
                    out.records.push(transfer_from_note(n, "ignored_stale", Some("stale_remote_ignored")));
                }
            }
            None => {
                tx.execute(
                    "INSERT INTO notes (uuid,content,tags,created_at,updated_at,version,device_id,deleted,synced_at)
                     VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9)",
                    rusqlite::params![
                        key,
                        n.content,
                        n.tags,
                        n.created_at,
                        n.updated_at,
                        n.version,
                        n.device_id,
                        n.deleted,
                        n.synced_at
                    ],
                )
                .map_err(|e| e.to_string())?;
                out.created += 1;
                out.records.push(transfer_from_note(n, "created", None));
            }
        }
    }

    // relations：按 (source_note_id, target_note_id, relation_type) 四元组去重，追加不覆盖。
    let mut rel_seen: HashSet<(i64, i64, String)> = HashSet::new();
    {
        let mut stmt = conn
            .prepare("SELECT source_note_id,target_note_id,relation_type FROM note_relations")
            .map_err(|e| e.to_string())?;
        let rows = stmt
            .query_map([], |r| {
                Ok((r.get::<_, i64>(0)?, r.get::<_, i64>(1)?, r.get::<_, String>(2)?))
            })
            .map_err(|e| e.to_string())?;
        for row in rows {
            if let Ok(t) = row {
                rel_seen.insert(t);
            }
        }
    }
    for r in &data.relations {
        let k = (r.source_note_id, r.target_note_id, r.relation_type.clone());
        if rel_seen.insert(k) {
            tx.execute(
                "INSERT INTO note_relations (source_note_id,target_note_id,relation_type,created_at)
                 VALUES (?1,?2,?3,?4)",
                rusqlite::params![r.source_note_id, r.target_note_id, r.relation_type, r.created_at],
            )
            .map_err(|e| e.to_string())?;
        }
    }

    tx.commit().map_err(|e| e.to_string())?;
    Ok(out)
}

// ---- Import: Todo ----

/// 把 Todo JSON 写入 todo.db（按 uuid 逻辑键 + rev 仲裁合并；被覆盖方进 archived）。
pub fn import_todo(db_path: &Path, json: &str) -> Result<ImportOutcome, String> {
    let data: TodoExport = serde_json::from_str(json).map_err(|e| e.to_string())?;
    let conn = Connection::open(db_path).map_err(|e| e.to_string())?;
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS todos (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT NOT NULL,
            content TEXT,
            completed INTEGER NOT NULL DEFAULT 0,
            priority INTEGER,
            due_date TEXT,
            tags TEXT DEFAULT '[]',
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            completed_at TEXT,
            version INTEGER NOT NULL DEFAULT 1,
            device_id TEXT,
            deleted INTEGER NOT NULL DEFAULT 0,
            synced_at TEXT,
            uuid TEXT);",
    )
    .map_err(|e| format!("ensure todo schema failed: {e}"))?;
    ensure_column(&conn, "todos", "uuid", "TEXT")?;
    backfill_uuid(&conn, "todos")?;

    let mut local: HashMap<String, TodoRow> = HashMap::new();
    {
        let mut stmt = conn
            .prepare(
                "SELECT id,uuid,title,content,completed,priority,due_date,tags,created_at,updated_at,
                        completed_at,version,device_id,deleted,synced_at FROM todos",
            )
            .map_err(|e| e.to_string())?;
        let rows = stmt
            .query_map([], |r| {
                Ok(TodoRow {
                    id: r.get(0)?,
                    uuid: r.get(1)?,
                    title: r.get(2)?,
                    content: r.get(3)?,
                    completed: r.get(4)?,
                    priority: r.get(5)?,
                    due_date: r.get(6)?,
                    tags: r.get(7)?,
                    created_at: r.get(8)?,
                    updated_at: r.get(9)?,
                    completed_at: r.get(10)?,
                    version: r.get(11)?,
                    device_id: r.get(12)?,
                    deleted: r.get(13)?,
                    synced_at: r.get(14)?,
                })
            })
            .map_err(|e| e.to_string())?;
        for row in rows {
            let t = row.map_err(|e| e.to_string())?;
            local.insert(t.uuid.clone(), t);
        }
    }

    let tx = conn.unchecked_transaction().map_err(|e| e.to_string())?;
    let mut out = ImportOutcome::default();
    for t in &data.todos {
        let key = logical_key(&t.uuid, t.id);
        match local.get(&key) {
            Some(lt) => {
                let same = lt.title == t.title
                    && lt.content == t.content
                    && lt.completed == t.completed
                    && lt.deleted == t.deleted
                    && lt.tags == t.tags;
                if same {
                    out.ignored_dup += 1;
                    out.records.push(transfer_from_todo(t, "ignored_dup", None));
                    continue;
                }
                let newer = conflict::incoming_newer(
                    &t.updated_at,
                    t.device_id.as_deref(),
                    &lt.updated_at,
                    lt.device_id.as_deref(),
                );
                if newer {
                    out.archived.push(ArchivedRecord {
                        kind: "todo".into(),
                        logical_key: key.clone(),
                        archived_json: serde_json::to_string(lt).unwrap_or_default(),
                        winner_rev: Some(rev_str(&t.updated_at, &t.device_id)),
                        reason: if t.deleted != 0 {
                            "deleted_by_remote".into()
                        } else {
                            "overwritten_by_remote".into()
                        },
                    });
                    tx.execute(
                        "UPDATE todos SET title=?1,content=?2,completed=?3,priority=?4,due_date=?5,
                           tags=?6,created_at=?7,updated_at=?8,completed_at=?9,version=?10,
                           device_id=?11,deleted=?12,synced_at=?13 WHERE uuid=?14",
                        rusqlite::params![
                            t.title,
                            t.content,
                            t.completed,
                            t.priority,
                            t.due_date,
                            t.tags,
                            t.created_at,
                            t.updated_at,
                            t.completed_at,
                            t.version,
                            t.device_id,
                            t.deleted,
                            t.synced_at,
                            key
                        ],
                    )
                    .map_err(|e| e.to_string())?;
                    if t.deleted != 0 {
                        out.deleted += 1;
                        out.records.push(transfer_from_todo(t, "deleted", Some("deleted_by_remote")));
                    } else {
                        out.updated += 1;
                        out.records.push(transfer_from_todo(t, "updated", Some("overwritten_by_remote")));
                    }
                } else {
                    out.archived.push(ArchivedRecord {
                        kind: "todo".into(),
                        logical_key: key.clone(),
                        archived_json: serde_json::to_string(t).unwrap_or_default(),
                        winner_rev: Some(rev_str(&lt.updated_at, &lt.device_id)),
                        reason: "stale_remote_ignored".into(),
                    });
                    out.ignored_stale += 1;
                    out.records.push(transfer_from_todo(t, "ignored_stale", Some("stale_remote_ignored")));
                }
            }
            None => {
                tx.execute(
                    "INSERT INTO todos (uuid,title,content,completed,priority,due_date,tags,
                                        created_at,updated_at,completed_at,version,device_id,deleted,synced_at)
                     VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14)",
                    rusqlite::params![
                        key,
                        t.title,
                        t.content,
                        t.completed,
                        t.priority,
                        t.due_date,
                        t.tags,
                        t.created_at,
                        t.updated_at,
                        t.completed_at,
                        t.version,
                        t.device_id,
                        t.deleted,
                        t.synced_at
                    ],
                )
                .map_err(|e| e.to_string())?;
                out.created += 1;
                out.records.push(transfer_from_todo(t, "created", None));
            }
        }
    }

    tx.commit().map_err(|e| e.to_string())?;
    Ok(out)
}

// ---- Import: Activity ----

/// event 内容指纹：相同 (bucket, 时间, 时长, 数据) 视为同一条事件，跨设备去重。
fn event_fingerprint(bucket_name: &str, timestamp: i64, duration: i64, data: &str) -> String {
    format!("{bucket_name}|{timestamp}|{duration}|{data}")
}

/// 把 ActivityWatch JSON 写入 sqlite.db：
/// - bucket 按 name 合并（同名更新元数据、异名插入，避免 name UNIQUE 冲突）；
/// - event 按内容指纹去重，追加式合并，绝不覆盖已有事件。
pub fn import_activity(db_path: &Path, json: &str) -> Result<ImportOutcome, String> {
    let data: ActivityExport = serde_json::from_str(json).map_err(|e| e.to_string())?;
    let conn = Connection::open(db_path).map_err(|e| e.to_string())?;
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS buckets (
            id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT UNIQUE NOT NULL,
            type TEXT NOT NULL, client TEXT NOT NULL, hostname TEXT NOT NULL, created TEXT NOT NULL);
         CREATE TABLE IF NOT EXISTS events (
            id INTEGER PRIMARY KEY AUTOINCREMENT, bucketrow INTEGER NOT NULL,
            starttime INTEGER NOT NULL, endtime INTEGER NOT NULL, data TEXT NOT NULL);",
    )
    .map_err(|e| format!("ensure activity schema failed: {e}"))?;

    // 本地 bucket：name -> id / 元数据
    let mut bucket_ids: HashMap<String, i64> = HashMap::new();
    {
        let mut stmt = conn
            .prepare("SELECT id,name,type,client,hostname,created FROM buckets")
            .map_err(|e| e.to_string())?;
        let rows = stmt
            .query_map([], |r| {
                Ok((
                    r.get::<_, String>(1)?,
                    (
                        r.get::<_, i64>(0)?,
                        r.get::<_, String>(2)?,
                        r.get::<_, String>(3)?,
                        r.get::<_, String>(4)?,
                        r.get::<_, String>(5)?,
                    ),
                ))
            })
            .map_err(|e| e.to_string())?;
        for row in rows {
            if let Ok((name, (id, _, _, _, _))) = row {
                bucket_ids.insert(name, id);
            }
        }
    }

    // 本地 event 指纹集合
    let mut local_fp: HashSet<String> = HashSet::new();
    {
        let mut stmt = conn
            .prepare(
                "SELECT e.id,e.bucketrow,b.name,e.starttime,e.endtime,e.data
                 FROM events e JOIN buckets b ON b.id = e.bucketrow",
            )
            .map_err(|e| e.to_string())?;
        let rows = stmt
            .query_map([], |r| {
                let start: i64 = r.get(3)?;
                let end: i64 = r.get(4)?;
                Ok((
                    r.get::<_, String>(2)?,
                    start,
                    (end - start).max(0),
                    r.get::<_, String>(5)?,
                ))
            })
            .map_err(|e| e.to_string())?;
        for row in rows {
            if let Ok((bn, start, dur, data)) = row {
                local_fp.insert(event_fingerprint(&bn, start, dur, &data));
            }
        }
    }

    let tx = conn.unchecked_transaction().map_err(|e| e.to_string())?;
    let mut out = ImportOutcome::default();

    // 1) buckets：按 name 合并
    for b in &data.buckets {
        if let Some(bid) = bucket_ids.get(&b.name) {
            tx.execute(
                "UPDATE buckets SET type=?1,client=?2,hostname=?3,created=?4 WHERE id=?5",
                rusqlite::params![b.bucket_type, b.client, b.hostname, b.created, bid],
            )
            .map_err(|e| e.to_string())?;
            out.updated += 1;
            out.records.push(TransferRecord {
                kind: "bucket".into(),
                logical_key: b.name.clone(),
                title: truncate_title(&b.name, 40),
                action: "updated".into(),
                reason: None,
            });
        } else {
            tx.execute(
                "INSERT INTO buckets (name,type,client,hostname,created) VALUES (?1,?2,?3,?4,?5)",
                rusqlite::params![b.name, b.bucket_type, b.client, b.hostname, b.created],
            )
            .map_err(|e| e.to_string())?;
            let bid = tx.last_insert_rowid();
            bucket_ids.insert(b.name.clone(), bid);
            out.created += 1;
            out.records.push(TransferRecord {
                kind: "bucket".into(),
                logical_key: b.name.clone(),
                title: truncate_title(&b.name, 40),
                action: "created".into(),
                reason: None,
            });
        }
    }

    // 2) events：指纹去重，追加式插入
    for e in &data.events {
        let fp = event_fingerprint(&e.bucket_name, e.timestamp, e.duration, &e.data);
        if local_fp.contains(&fp) {
            out.ignored_dup += 1;
            out.records.push(TransferRecord {
                kind: "event".into(),
                logical_key: fp.clone(),
                title: format!("{} @ {}", e.bucket_name, e.timestamp),
                action: "ignored_dup".into(),
                reason: None,
            });
            continue;
        }
        let fp_clone = fp.clone();
        match bucket_ids.get(&e.bucket_name) {
            Some(bid) => {
                tx.execute(
                    "INSERT INTO events (bucketrow,starttime,endtime,data) VALUES (?1,?2,?3,?4)",
                    rusqlite::params![bid, e.timestamp, e.timestamp + e.duration, e.data],
                )
                .map_err(|e| e.to_string())?;
                local_fp.insert(fp_clone);
                out.created += 1;
                out.records.push(TransferRecord {
                    kind: "event".into(),
                    logical_key: fp.clone(),
                    title: format!("{} @ {}", e.bucket_name, e.timestamp),
                    action: "created".into(),
                    reason: None,
                });
            }
            None => out
                .errors
                .push(format!("event references unknown bucket '{}'", e.bucket_name)),
        }
    }

    tx.commit().map_err(|e| e.to_string())?;
    Ok(out)
}

// ---- Restore（从回收站恢复）----

/// 从归档 JSON 恢复一条 note 到 inbox.db。
/// 语义（安全、不制造新冲突）：仅当逻辑键当前不存在、或当前处于软删除(deleted=1)时生效；
/// 若当前存在活动版本则跳过（不覆盖仲裁胜出方），返回 false。
pub fn restore_note(db_path: &Path, json: &str) -> Result<bool, String> {
    let n: NoteRow = serde_json::from_str(json).map_err(|e| e.to_string())?;
    let conn = Connection::open(db_path).map_err(|e| e.to_string())?;
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS notes (
            id INTEGER PRIMARY KEY AUTOINCREMENT, content TEXT NOT NULL,
            tags TEXT DEFAULT '[]', created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
            version INTEGER NOT NULL DEFAULT 1, device_id TEXT,
            deleted INTEGER NOT NULL DEFAULT 0, synced_at TEXT, uuid TEXT);",
    )
    .map_err(|e| format!("ensure inbox schema failed: {e}"))?;
    ensure_column(&conn, "notes", "uuid", "TEXT")?;
    backfill_uuid(&conn, "notes")?;
    let key = logical_key(&n.uuid, n.id);
    let exists: i64 = conn
        .query_row("SELECT COUNT(*) FROM notes WHERE uuid=?1", [&key], |r| r.get(0))
        .map_err(|e| e.to_string())?;
    if exists > 0 {
        // 存在：仅当软删除时恢复；否则跳过（不覆盖胜出方）
        let deleted: i64 = conn
            .query_row("SELECT deleted FROM notes WHERE uuid=?1", [&key], |r| r.get(0))
            .map_err(|e| e.to_string())?;
        if deleted != 0 {
            conn.execute(
                "UPDATE notes SET deleted=0, updated_at=?1 WHERE uuid=?2",
                rusqlite::params![Utc::now().to_rfc3339(), key],
            )
            .map_err(|e| e.to_string())?;
            return Ok(true);
        }
        return Ok(false);
    }
    conn.execute(
        "INSERT INTO notes (uuid,content,tags,created_at,updated_at,version,device_id,deleted,synced_at)
         VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9)",
        rusqlite::params![
            key,
            n.content,
            n.tags,
            n.created_at,
            n.updated_at,
            n.version,
            n.device_id,
            n.deleted,
            n.synced_at
        ],
    )
    .map_err(|e| e.to_string())?;
    Ok(true)
}

/// 从归档 JSON 恢复一条 todo 到 todo.db，语义同 restore_note。
pub fn restore_todo(db_path: &Path, json: &str) -> Result<bool, String> {
    let t: TodoRow = serde_json::from_str(json).map_err(|e| e.to_string())?;
    let conn = Connection::open(db_path).map_err(|e| e.to_string())?;
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS todos (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT NOT NULL, content TEXT,
            completed INTEGER NOT NULL DEFAULT 0, priority INTEGER,
            due_date TEXT, tags TEXT DEFAULT '[]',
            created_at TEXT NOT NULL, updated_at TEXT NOT NULL, completed_at TEXT,
            version INTEGER NOT NULL DEFAULT 1, device_id TEXT,
            deleted INTEGER NOT NULL DEFAULT 0, synced_at TEXT, uuid TEXT);",
    )
    .map_err(|e| format!("ensure todo schema failed: {e}"))?;
    ensure_column(&conn, "todos", "uuid", "TEXT")?;
    backfill_uuid(&conn, "todos")?;
    let key = logical_key(&t.uuid, t.id);
    let exists: i64 = conn
        .query_row("SELECT COUNT(*) FROM todos WHERE uuid=?1", [&key], |r| r.get(0))
        .map_err(|e| e.to_string())?;
    if exists > 0 {
        let deleted: i64 = conn
            .query_row("SELECT deleted FROM todos WHERE uuid=?1", [&key], |r| r.get(0))
            .map_err(|e| e.to_string())?;
        if deleted != 0 {
            conn.execute(
                "UPDATE todos SET deleted=0, updated_at=?1 WHERE uuid=?2",
                rusqlite::params![Utc::now().to_rfc3339(), key],
            )
            .map_err(|e| e.to_string())?;
            return Ok(true);
        }
        return Ok(false);
    }
    conn.execute(
        "INSERT INTO todos (uuid,title,content,completed,priority,due_date,tags,
                            created_at,updated_at,completed_at,version,device_id,deleted,synced_at)
         VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14)",
        rusqlite::params![
            key,
            t.title,
            t.content,
            t.completed,
            t.priority,
            t.due_date,
            t.tags,
            t.created_at,
            t.updated_at,
            t.completed_at,
            t.version,
            t.device_id,
            t.deleted,
            t.synced_at
        ],
    )
    .map_err(|e| e.to_string())?;
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn make_inbox_db(path: &Path) {
        let conn = Connection::open(path).unwrap();
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
        conn.execute(
            "INSERT INTO notes (uuid,content,tags,created_at,updated_at,version,device_id)
             VALUES (?1,?2,?3,?4,?4,?5,?6)",
            rusqlite::params![
                "u-note-1",
                "你好",
                "[\"work\"]",
                "2026-08-25T00:00:00Z",
                1,
                "dev-a"
            ],
        )
        .unwrap();
    }

    #[test]
    fn inbox_roundtrip() {
        let dir = tempdir().unwrap();
        let p = dir.path().join("inbox.db");
        make_inbox_db(&p);
        let json = export_inbox(&p).unwrap();
        assert!(json.contains("你好"));
        let conn = Connection::open(&p).unwrap();
        conn.execute("DELETE FROM notes", []).unwrap();
        drop(conn);
        let out = import_inbox(&p, &json).unwrap();
        assert_eq!(out.created, 1);
        let conn = Connection::open(&p).unwrap();
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM notes", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 1);
    }

    /// 双设备撞 id：A/B 各自建了 id=1 的笔记（不同 uuid、不同内容）。
    /// 按 uuid 逻辑键，B 导入 A 的数据时应作为两条不同笔记，而不是覆盖。
    #[test]
    fn inbox_merge_by_uuid_not_id() {
        let dir = tempdir().unwrap();
        let pa = dir.path().join("inbox_a.db");
        let pb = dir.path().join("inbox_b.db");
        make_inbox_db(&pa);

        // B 端：id=1 但不同 uuid、不同内容、不同时间
        let conn = Connection::open(&pb).unwrap();
        conn.execute_batch(
            "CREATE TABLE notes (
                id INTEGER PRIMARY KEY AUTOINCREMENT, content TEXT NOT NULL,
                tags TEXT DEFAULT '[]', created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
                version INTEGER NOT NULL DEFAULT 1, device_id TEXT,
                deleted INTEGER NOT NULL DEFAULT 0, synced_at TEXT, uuid TEXT);",
        )
        .unwrap();
        conn.execute(
            "INSERT INTO notes (id,uuid,content,tags,created_at,updated_at,version,device_id)
             VALUES (1,?1,?2,?3,?4,?5,?6,?7)",
            rusqlite::params![
                "u-note-b",
                "B 的笔记",
                "[]",
                "2026-08-25T00:00:00Z",
                "2026-08-25T08:00:00Z",
                1,
                "dev-b"
            ],
        )
        .unwrap();
        drop(conn);

        // B 导入 A 的导出
        let json_a = export_inbox(&pa).unwrap();
        let out = import_inbox(&pb, &json_a).unwrap();
        assert_eq!(out.created, 1); // A 的 u-note-1 作为新行插入
        assert_eq!(out.updated, 0);

        let conn = Connection::open(&pb).unwrap();
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM notes", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 2);
        // 内容互不覆盖
        let has_a: i64 = conn
            .query_row("SELECT COUNT(*) FROM notes WHERE content='你好'", [], |r| r.get(0))
            .unwrap();
        let has_b: i64 = conn
            .query_row("SELECT COUNT(*) FROM notes WHERE content='B 的笔记'", [], |r| r.get(0))
            .unwrap();
        assert_eq!(has_a, 1);
        assert_eq!(has_b, 1);
    }

    /// rev 仲裁：同一 uuid，对端更新更新 → 覆盖本地并归档本地旧版本。
    #[test]
    fn inbox_merge_remote_newer_archives_local() {
        let dir = tempdir().unwrap();
        let p = dir.path().join("inbox.db");
        make_inbox_db(&p);

        // 构造对端快照：同一 uuid，updated_at 更晚
        let incoming = serde_json::json!({
            "notes": [{
                "id": 99, "uuid": "u-note-1", "content": "远程新版",
                "tags": "[\"work\"]",
                "created_at": "2026-08-25T00:00:00Z",
                "updated_at": "2026-08-25T12:00:00Z",
                "version": 2, "device_id": "dev-b", "deleted": 0, "synced_at": null
            }],
            "relations": []
        });
        let out = import_inbox(&p, &incoming.to_string()).unwrap();
        assert_eq!(out.updated, 1);
        assert_eq!(out.archived.len(), 1);
        assert_eq!(out.archived[0].kind, "note");
        assert_eq!(out.archived[0].logical_key, "u-note-1");
        assert_eq!(out.archived[0].reason, "overwritten_by_remote");

        let conn = Connection::open(&p).unwrap();
        let content: String = conn
            .query_row("SELECT content FROM notes WHERE uuid='u-note-1'", [], |r| r.get(0))
            .unwrap();
        assert_eq!(content, "远程新版");
        let n: i64 = conn
            .query_row("SELECT COUNT(*) FROM notes", [], |r| r.get(0))
            .unwrap();
        assert_eq!(n, 1);
    }

    /// rev 仲裁：同一 uuid，本地更新更新 → 忽略对端并把对端版本归档。
    #[test]
    fn inbox_merge_local_newer_ignores_remote() {
        let dir = tempdir().unwrap();
        let p = dir.path().join("inbox.db");
        make_inbox_db(&p);

        let incoming = serde_json::json!({
            "notes": [{
                "id": 99, "uuid": "u-note-1", "content": "过期旧版",
                "tags": "[\"work\"]",
                "created_at": "2026-08-25T00:00:00Z",
                "updated_at": "2026-08-24T00:00:00Z",
                "version": 1, "device_id": "dev-b", "deleted": 0, "synced_at": null
            }],
            "relations": []
        });
        let out = import_inbox(&p, &incoming.to_string()).unwrap();
        assert_eq!(out.ignored_stale, 1);
        assert_eq!(out.archived.len(), 1);
        assert_eq!(out.archived[0].reason, "stale_remote_ignored");

        let conn = Connection::open(&p).unwrap();
        let content: String = conn
            .query_row("SELECT content FROM notes WHERE uuid='u-note-1'", [], |r| r.get(0))
            .unwrap();
        assert_eq!(content, "你好"); // 本地内容保持不变
    }

    /// 删除传播：对端删除了某条（deleted=1 且更新）→ 本地软删，归档本地旧版本。
    #[test]
    fn inbox_merge_remote_delete_propagates() {
        let dir = tempdir().unwrap();
        let p = dir.path().join("inbox.db");
        make_inbox_db(&p);

        let incoming = serde_json::json!({
            "notes": [{
                "id": 99, "uuid": "u-note-1", "content": "你好",
                "tags": "[\"work\"]",
                "created_at": "2026-08-25T00:00:00Z",
                "updated_at": "2026-08-25T12:00:00Z",
                "version": 2, "device_id": "dev-b", "deleted": 1, "synced_at": null
            }],
            "relations": []
        });
        let out = import_inbox(&p, &incoming.to_string()).unwrap();
        assert_eq!(out.deleted, 1);
        assert_eq!(out.archived.len(), 1);
        assert_eq!(out.archived[0].reason, "deleted_by_remote");

        let conn = Connection::open(&p).unwrap();
        let deleted: i64 = conn
            .query_row("SELECT deleted FROM notes WHERE uuid='u-note-1'", [], |r| r.get(0))
            .unwrap();
        assert_eq!(deleted, 1);
    }

    /// Activity：同名 bucket 不触发 UNIQUE 冲突；事件按指纹去重不重复插入。
    #[test]
    fn activity_merge_by_bucket_name_and_fingerprint() {
        let dir = tempdir().unwrap();
        let p = dir.path().join("sqlite.db");
        let conn = Connection::open(&p).unwrap();
        conn.execute_batch(
            "CREATE TABLE buckets (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT UNIQUE NOT NULL,
                type TEXT NOT NULL, client TEXT NOT NULL, hostname TEXT NOT NULL, created TEXT NOT NULL);
             CREATE TABLE events (id INTEGER PRIMARY KEY AUTOINCREMENT, bucketrow INTEGER NOT NULL,
                starttime INTEGER NOT NULL, endtime INTEGER NOT NULL, data TEXT NOT NULL);",
        )
        .unwrap();
        conn.execute(
            "INSERT INTO buckets (name,type,client,hostname,created) VALUES ('aw-watcher-window','windows','test','host-a','2026-01-01T00:00:00Z')",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO events (bucketrow,starttime,endtime,data) VALUES (1,1000,2000,'{\"app\":\"x\"}')",
            [],
        )
        .unwrap();
        drop(conn);

        // 对端快照：本地已存在的 event 原样返回（同指纹），另加一条新 event
        let incoming = serde_json::json!({
            "buckets": [{
                "id": 5, "name": "aw-watcher-window", "bucket_type": "windows",
                "client": "test", "hostname": "host-a", "created": "2026-01-01T00:00:00Z"
            }],
            "events": [
                { "id": 5, "bucketrow": 5, "bucket_name": "aw-watcher-window",
                  "timestamp": 1000, "duration": 1000, "data": "{\"app\":\"x\"}" },
                { "id": 6, "bucketrow": 5, "bucket_name": "aw-watcher-window",
                  "timestamp": 3000, "duration": 1000, "data": "{\"app\":\"y\"}" }
            ]
        });
        let out = import_activity(&p, &incoming.to_string()).unwrap();
        assert_eq!(out.created, 1); // 只有新 event 插入
        assert_eq!(out.ignored_dup, 1); // 重复 event 被去重
        assert_eq!(out.updated, 1); // 同名 bucket 更新元数据

        let conn = Connection::open(&p).unwrap();
        let n: i64 = conn
            .query_row("SELECT COUNT(*) FROM events", [], |r| r.get(0))
            .unwrap();
        assert_eq!(n, 2);
    }
}
