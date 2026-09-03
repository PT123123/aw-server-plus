//! 同步载荷序列化：把目标库（ActivityWatch 的 sqlite.db + Inbox 的 inbox.db）
//! 用 SQL 读出为 JSON 文本，供局域网 HTTP 传输；接收端写回目标库。
//! 采用整表主键 upsert 的幂等合并；冲突处理由 conflict.rs 接入，本期留空。

use std::path::Path;

use rusqlite::Connection;
use serde::{Deserialize, Serialize};

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
        .prepare("SELECT id,bucketrow,starttime,endtime,data FROM events")
        .map_err(|e| e.to_string())?;
    let rows = stmt
        .query_map([], |r| {
            let start: i64 = r.get(2)?;
            let end: i64 = r.get(3)?;
            Ok(EventRow {
                id: r.get(0)?,
                bucketrow: r.get(1)?,
                timestamp: start,
                duration: (end - start).max(0),
                data: r.get(4)?,
            })
        })
        .map_err(|e| e.to_string())?;
    for row in rows {
        events.push(row.map_err(|e| e.to_string())?);
    }

    serde_json::to_string(&ActivityExport { buckets, events }).map_err(|e| e.to_string())
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct InboxExport {
    pub notes: Vec<NoteRow>,
    pub relations: Vec<RelationRow>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NoteRow {
    pub id: i64,
    pub content: String,
    pub tags: String,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RelationRow {
    pub id: i64,
    pub source_note_id: i64,
    pub target_note_id: i64,
    pub relation_type: String,
    pub created_at: String,
}

/// 导出 Inbox 库(inbox.db)为 JSON 文本
pub fn export_inbox(db_path: &Path) -> Result<String, String> {
    let conn = Connection::open(db_path).map_err(|e| e.to_string())?;
    let mut notes = Vec::new();
    let mut stmt = conn
        .prepare("SELECT id,content,tags,created_at,updated_at FROM notes")
        .map_err(|e| e.to_string())?;
    let rows = stmt
        .query_map([], |r| {
            Ok(NoteRow {
                id: r.get(0)?,
                content: r.get(1)?,
                tags: r.get(2)?,
                created_at: r.get(3)?,
                updated_at: r.get(4)?,
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

/// 把 ActivityWatch JSON 写回 sqlite.db（整表主键 upsert）
pub fn import_activity(db_path: &Path, json: &str) -> Result<usize, String> {
    let data: ActivityExport = serde_json::from_str(json).map_err(|e| e.to_string())?;
    let conn = Connection::open(db_path).map_err(|e| e.to_string())?;
    // 首次接收同步的目标库可能尚不存在，先确保 schema 就位（与 aw-datastore 对齐）
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS buckets (
            id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT UNIQUE NOT NULL,
            type TEXT NOT NULL, client TEXT NOT NULL, hostname TEXT NOT NULL, created TEXT NOT NULL);
         CREATE TABLE IF NOT EXISTS events (
            id INTEGER PRIMARY KEY AUTOINCREMENT, bucketrow INTEGER NOT NULL,
            starttime INTEGER NOT NULL, endtime INTEGER NOT NULL, data TEXT NOT NULL);",
    )
    .map_err(|e| format!("ensure activity schema failed: {e}"))?;
    let tx = conn.unchecked_transaction().map_err(|e| e.to_string())?;
    for b in &data.buckets {
        tx.execute(
            "INSERT INTO buckets (id,name,type,client,hostname,created) VALUES (?1,?2,?3,?4,?5,?6)
             ON CONFLICT(id) DO UPDATE SET name=excluded.name, type=excluded.type,
               client=excluded.client, hostname=excluded.hostname, created=excluded.created",
            rusqlite::params![b.id, b.name, b.bucket_type, b.client, b.hostname, b.created],
        )
        .map_err(|e| e.to_string())?;
    }
    for e in &data.events {
        let end = e.timestamp + e.duration;
        tx.execute(
            "INSERT OR REPLACE INTO events (id,bucketrow,starttime,endtime,data) VALUES (?1,?2,?3,?4,?5)",
            rusqlite::params![e.id, e.bucketrow, e.timestamp, end, e.data],
        )
        .map_err(|e| e.to_string())?;
    }
    let count = data.events.len();
    tx.commit().map_err(|e| e.to_string())?;
    Ok(count)
}

/// 把 Inbox JSON 写入 inbox.db（主键 upsert）
pub fn import_inbox(db_path: &Path, json: &str) -> Result<usize, String> {
    let data: InboxExport = serde_json::from_str(json).map_err(|e| e.to_string())?;
    let conn = Connection::open(db_path).map_err(|e| e.to_string())?;
    // 首次接收同步的目标库可能尚不存在，先确保 schema 就位（与 aw-inbox-rust 对齐）
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS notes (
            id INTEGER PRIMARY KEY AUTOINCREMENT, content TEXT NOT NULL,
            tags TEXT DEFAULT '[]', created_at TEXT NOT NULL, updated_at TEXT NOT NULL);
         CREATE TABLE IF NOT EXISTS note_relations (
            id INTEGER PRIMARY KEY AUTOINCREMENT, source_note_id INTEGER NOT NULL,
            target_note_id INTEGER NOT NULL, relation_type TEXT NOT NULL, created_at TEXT NOT NULL);",
    )
    .map_err(|e| format!("ensure inbox schema failed: {e}"))?;
    let tx = conn.unchecked_transaction().map_err(|e| e.to_string())?;
    for n in &data.notes {
        tx.execute(
            "INSERT INTO notes (id,content,tags,created_at,updated_at) VALUES (?1,?2,?3,?4,?5)
             ON CONFLICT(id) DO UPDATE SET content=excluded.content, tags=excluded.tags,
               created_at=excluded.created_at, updated_at=excluded.updated_at",
            rusqlite::params![n.id, n.content, n.tags, n.created_at, n.updated_at],
        )
        .map_err(|e| e.to_string())?;
    }
    for r in &data.relations {
        tx.execute(
            "INSERT OR REPLACE INTO note_relations (id,source_note_id,target_note_id,relation_type,created_at)
             VALUES (?1,?2,?3,?4,?5)",
            rusqlite::params![
                r.id, r.source_note_id, r.target_note_id, r.relation_type, r.created_at
            ],
        )
        .map_err(|e| e.to_string())?;
    }
    let count = data.notes.len();
    tx.commit().map_err(|e| e.to_string())?;
    Ok(count)
}

// ---- Todo（todo.db）导出 / 导入 ----
// 与 aw-inbox-rust migrate_todo 的 todos 表结构一一对应（含 version / device_id /
// deleted / synced_at 四个同步元数据列）。合并策略：主键幂等 upsert，且仅当
// 对端 updated_at 不早于本地时才覆盖（时间戳比较），否则保留本地 —— 这是
// 与 inbox/activity 不同的「冲突感知」合并。

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct TodoExport {
    pub todos: Vec<TodoRow>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TodoRow {
    pub id: i64,
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
            "SELECT id,title,content,completed,priority,due_date,tags,created_at,updated_at,
                    completed_at,version,device_id,deleted,synced_at FROM todos",
        )
        .map_err(|e| e.to_string())?;
    let rows = stmt
        .query_map([], |r| {
            Ok(TodoRow {
                id: r.get(0)?,
                title: r.get(1)?,
                content: r.get(2)?,
                completed: r.get(3)?,
                priority: r.get(4)?,
                due_date: r.get(5)?,
                tags: r.get(6)?,
                created_at: r.get(7)?,
                updated_at: r.get(8)?,
                completed_at: r.get(9)?,
                version: r.get(10)?,
                device_id: r.get(11)?,
                deleted: r.get(12)?,
                synced_at: r.get(13)?,
            })
        })
        .map_err(|e| e.to_string())?;
    let mut todos = Vec::new();
    for row in rows {
        todos.push(row.map_err(|e| e.to_string())?);
    }
    serde_json::to_string(&TodoExport { todos }).map_err(|e| e.to_string())
}

/// 把 Todo JSON 写入 todo.db：
/// - 行不存在 → 插入；
/// - 行存在且对端 updated_at >= 本地 → 整行覆盖（含软删除标记）；
/// - 行存在但本地更新（本地 updated_at 更新）→ 保留本地（冲突时本地优先）。
/// 返回应用（插入 + 更新）的行数。
pub fn import_todo(db_path: &Path, json: &str) -> Result<usize, String> {
    let data: TodoExport = serde_json::from_str(json).map_err(|e| e.to_string())?;
    let conn = Connection::open(db_path).map_err(|e| e.to_string())?;
    // 与 aw-inbox-rust migrate_todo 保持一致的 schema（首次同步时目标库可能不存在）
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
            synced_at TEXT);",
    )
    .map_err(|e| format!("ensure todo schema failed: {e}"))?;
    let tx = conn.unchecked_transaction().map_err(|e| e.to_string())?;
    let mut applied = 0usize;
    for t in &data.todos {
        let changed = tx
            .execute(
                "INSERT INTO todos (id,title,content,completed,priority,due_date,tags,
                                    created_at,updated_at,completed_at,version,device_id,deleted,synced_at)
                 VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14)
                 ON CONFLICT(id) DO UPDATE SET
                    title=excluded.title, content=excluded.content,
                    completed=excluded.completed, priority=excluded.priority,
                    due_date=excluded.due_date, tags=excluded.tags,
                    created_at=excluded.created_at, updated_at=excluded.updated_at,
                    completed_at=excluded.completed_at, version=excluded.version,
                    device_id=excluded.device_id, deleted=excluded.deleted,
                    synced_at=excluded.synced_at
                 WHERE excluded.updated_at >= todos.updated_at",
                rusqlite::params![
                    t.id, t.title, t.content, t.completed, t.priority, t.due_date, t.tags,
                    t.created_at, t.updated_at, t.completed_at, t.version, t.device_id,
                    t.deleted, t.synced_at
                ],
            )
            .map_err(|e| e.to_string())?;
        applied += changed;
    }
    tx.commit().map_err(|e| e.to_string())?;
    Ok(applied)
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
                tags TEXT DEFAULT '[]', created_at TEXT NOT NULL, updated_at TEXT NOT NULL);
             CREATE TABLE IF NOT EXISTS note_relations (
                id INTEGER PRIMARY KEY AUTOINCREMENT, source_note_id INTEGER NOT NULL,
                target_note_id INTEGER NOT NULL, relation_type TEXT NOT NULL, created_at TEXT NOT NULL);",
        )
        .unwrap();
        conn.execute(
            "INSERT INTO notes (content,tags,created_at,updated_at) VALUES (?1,?2,?3,?3)",
            rusqlite::params!["你好", "[\"work\"]", "2026-08-25T00:00:00Z"],
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
        let n = import_inbox(&p, &json).unwrap();
        assert_eq!(n, 1);
        let conn = Connection::open(&p).unwrap();
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM notes", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 1);
    }
}