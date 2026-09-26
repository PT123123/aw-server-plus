// src/db.rs
use crate::models::{
    BatchAction, BatchOpResult, BatchResponse, CreateCommentPayload, CreateNotePayload,
    CreateNoteRelationPayload, CreateTodoListPayload, CreateTodoPayload, DetailedTag, Note,
    NoteBatchPayload, NoteRelation, NoteRelationType, TagNode, Todo, TodoBatchPayload,
    TodoListRecord, UpdateNotePayload, UpdateTodoListPayload, UpdateTodoPayload,
}; // Updated imports
use chrono::{DateTime, Utc};
use log::{info, warn};
use rusqlite::OptionalExtension; // 添加OptionalExtension trait
use rusqlite::{params, Connection, Error, Row, ToSql}; // Ensure rusqlite is in Cargo.toml!
use serde_json;
use std::env;
use std::path::Path;

// --- 错误处理助手 ---
fn map_serde_error(e: serde_json::Error) -> Error {
    Error::InvalidParameterName(format!("JSON serialization/deserialization error: {}", e))
}

// --- 标签过滤助手 ---

/// 转义 LIKE 通配符，配合 `ESCAPE '\'` 使用。
fn escape_like(s: &str) -> String {
    s.replace('\\', "\\\\").replace('%', "\\%").replace('_', "\\_")
}

/// 追加层级 tag 前缀匹配条件（段边界）：`项目` 命中 tag `项目` 及 `项目/...` 全部子孙，
/// 但不命中 `项目2`。tags 列存 JSON 数组文本（元素形如 "tag"），故匹配 `"t"`（整元素）
/// 或 `"t/`（元素前缀）。`exclude = true` 时取反（反向筛选：排除该标签及其子孙）。
fn push_tag_clause(
    sql: &mut String,
    params: &mut Vec<Box<dyn ToSql>>,
    tag: &str,
    exclude: bool,
) {
    let escaped = escape_like(tag);
    if exclude {
        sql.push_str(" AND NOT (tags LIKE ? ESCAPE '\\' OR tags LIKE ? ESCAPE '\\')");
    } else {
        sql.push_str(" AND (tags LIKE ? ESCAPE '\\' OR tags LIKE ? ESCAPE '\\')");
    }
    params.push(Box::new(format!("%\"{}\"%", escaped)));
    params.push(Box::new(format!("%\"{}/%", escaped)));
}

/// 把 `?` 占位符重编号为 `?1, ?2, ...`（与 get_notes_db 的构建方式一致）。
fn renumber_placeholders(query: &str) -> String {
    let mut out = String::with_capacity(query.len() + 8);
    let mut index = 1;
    for c in query.chars() {
        if c == '?' {
            out.push_str(&format!("?{}", index));
            index += 1;
        } else {
            out.push(c);
        }
    }
    out
}

// --- 数据库连接类型 ---
pub type DbConnection = Connection;

// --- 常量 ---
const DATABASE_URL_ENV_VAR: &str = "DATABASE_URL";
const DEFAULT_DATABASE_URL: &str = "inbox.db";
const TODO_DATABASE_URL_ENV_VAR: &str = "TODO_DATABASE_URL";
const DEFAULT_TODO_DATABASE_URL: &str = "todo.db";

// --- 初始化 ---

/// 打开 SQLite 连接（指定路径），启用 WAL 模式（崩溃/强杀后数据完整性）并确保父目录存在。
fn open_conn(db_path: &str) -> Result<DbConnection, Error> {
    let db_path = Path::new(db_path);
    if let Some(parent) = db_path.parent() {
        if !parent.as_os_str().is_empty() && !parent.exists() {
            std::fs::create_dir_all(parent).map_err(|e| {
                Error::SqliteFailure(
                    rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_CANTOPEN),
                    Some(format!("Failed to create parent directory: {}", e)),
                )
            })?;
        }
    }

    let conn = Connection::open(db_path)?;
    conn.execute("PRAGMA foreign_keys = ON;", [])?;
    // WAL + NORMAL 同步：崩溃/断电后数据库不损坏，读性能更好（journal_mode 为数据库级持久属性）
    conn.pragma_update(None, "journal_mode", "WAL")?;
    conn.pragma_update(None, "synchronous", "NORMAL")?;
    info!("🗄️ 连接到数据库 (WAL): {}", db_path.display());
    Ok(conn)
}

/// 用指定路径初始化连接（桌面端：由 main.rs 根据 --data-dir 传入绝对路径）。
pub async fn init_pool_at(db_path: &str) -> Result<DbConnection, Error> {
    open_conn(db_path)
}

pub async fn init_pool() -> Result<DbConnection, Error> {
    let database_url = if cfg!(target_os = "android") {
        // Android环境下使用应用私有数据目录
        let data_dir = std::env::var("DATA_DIR").unwrap_or_else(|_| ".".to_string());
        let db_path = Path::new(&data_dir).join(DEFAULT_DATABASE_URL);
        db_path.to_string_lossy().into_owned()
    } else {
        // 非Android环境保持原有逻辑
        env::var(DATABASE_URL_ENV_VAR).unwrap_or_else(|_| DEFAULT_DATABASE_URL.to_string())
    };

    open_conn(&database_url)
}

/// 打开 todo 数据库连接（Todo 使用独立 DB 文件，与笔记 inbox.db 分开）。
pub async fn init_todo_pool() -> Result<DbConnection, Error> {
    let database_url = if cfg!(target_os = "android") {
        // Android环境下使用应用私有数据目录
        let data_dir = std::env::var("DATA_DIR").unwrap_or_else(|_| ".".to_string());
        let db_path = Path::new(&data_dir).join(DEFAULT_TODO_DATABASE_URL);
        db_path.to_string_lossy().into_owned()
    } else {
        env::var(TODO_DATABASE_URL_ENV_VAR)
            .unwrap_or_else(|_| DEFAULT_TODO_DATABASE_URL.to_string())
    };

    open_conn(&database_url)
}

/// 用指定路径初始化 todo 数据库连接。
pub async fn init_todo_pool_at(db_path: &str) -> Result<DbConnection, Error> {
    open_conn(db_path)
}

// --- 迁移 ---
fn ensure_column(conn: &DbConnection, table: &str, column: &str, definition: &str) -> Result<(), Error> {
    let mut stmt = conn.prepare(&format!("PRAGMA table_info({})", table))?;
    let columns: Vec<String> = stmt
        .query_map([], |row| row.get::<_, String>(1))?
        .filter_map(|r| r.ok())
        .collect();
    drop(stmt);
    if !columns.contains(&column.to_string()) {
        conn.execute_batch(&format!("ALTER TABLE {} ADD COLUMN {} {}", table, column, definition))?;
        info!("📦 迁移: 已添加列 {}.{}", table, column);
    }
    Ok(())
}

pub fn migrate(conn: &DbConnection) -> Result<(), Error> {
    conn.execute_batch(
        r#"
        CREATE TABLE IF NOT EXISTS notes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            content TEXT NOT NULL,
            tags TEXT DEFAULT '[]',
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            version INTEGER NOT NULL DEFAULT 1,
            device_id TEXT,
            deleted INTEGER NOT NULL DEFAULT 0,
            synced_at TEXT,
            uuid TEXT
        );

        DROP TABLE IF EXISTS comments;

        CREATE TABLE IF NOT EXISTS note_relations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source_note_id INTEGER NOT NULL,
            target_note_id INTEGER NOT NULL,
            relation_type TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
            FOREIGN KEY (source_note_id) REFERENCES notes(id) ON DELETE CASCADE,
            FOREIGN KEY (target_note_id) REFERENCES notes(id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS sync_versions (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            global_version INTEGER NOT NULL DEFAULT 0
        );

        "#,
    )?;

    // 兼容旧数据库：给已存在的表补上缺失的列（必须在 CREATE INDEX 之前）
    ensure_column(conn, "notes", "version", "INTEGER NOT NULL DEFAULT 1")?;
    ensure_column(conn, "notes", "device_id", "TEXT")?;
    ensure_column(conn, "notes", "deleted", "INTEGER NOT NULL DEFAULT 0")?;
    ensure_column(conn, "notes", "synced_at", "TEXT")?;
    ensure_column(conn, "notes", "uuid", "TEXT")?;
    // 为历史行补齐 uuid（P0 同步逻辑键；SQLite 对每行重新求值 randomblob）
    conn.execute(
        "UPDATE notes SET uuid = lower(hex(randomblob(16))) WHERE uuid IS NULL OR uuid = ''",
        [],
    )?;
    // 版本计数种子行（b87fcd8 重构时误删了建表语句，老库缺表会导致笔记写入报 no such table → 400）
    conn.execute(
        "INSERT OR IGNORE INTO sync_versions (id, global_version) VALUES (1, 0)",
        [],
    )?;

    // 索引（放在 ensure_column 之后，避免引用不存在的列）
    conn.execute_batch(
        r#"
        CREATE INDEX IF NOT EXISTS idx_note_relations_source ON note_relations(source_note_id);
        CREATE INDEX IF NOT EXISTS idx_note_relations_target ON note_relations(target_note_id);
        CREATE INDEX IF NOT EXISTS idx_note_relations_type ON note_relations(relation_type);
        CREATE INDEX IF NOT EXISTS idx_notes_version ON notes(version);
        CREATE INDEX IF NOT EXISTS idx_notes_device_id ON notes(device_id);
        CREATE INDEX IF NOT EXISTS idx_notes_synced_at ON notes(synced_at);
        "#,
    )?;

    // 笔记历史快照表（纯本地，不参与跨网同步）
    conn.execute_batch(
        r#"
        CREATE TABLE IF NOT EXISTS note_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            note_id INTEGER NOT NULL,
            content TEXT NOT NULL,
            tags TEXT DEFAULT '[]',
            version INTEGER NOT NULL,
            device_id TEXT,
            updated_at TEXT NOT NULL,
            snapshot_at TEXT NOT NULL,
            FOREIGN KEY (note_id) REFERENCES notes(id) ON DELETE CASCADE
        );
        CREATE INDEX IF NOT EXISTS idx_note_history_note_id ON note_history(note_id);
        CREATE INDEX IF NOT EXISTS idx_note_history_snapshot_at ON note_history(snapshot_at);
        "#,
    )?;

    info!("✅ 数据库迁移完成");
    Ok(())
}

/// 迁移 todo 数据库（独立 todo.db）：创建 todos 表与独立的 sync_versions 版本计数。
pub fn migrate_todo(conn: &DbConnection) -> Result<(), Error> {
    conn.execute_batch(
        r#"
        CREATE TABLE IF NOT EXISTS todos (
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
            uuid TEXT
        );

        CREATE TABLE IF NOT EXISTS sync_versions (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            global_version INTEGER NOT NULL DEFAULT 0
        );

        "#,
    )?;

    // 兼容旧数据库：给已存在的表补上缺失的列（必须在 CREATE INDEX 之前）
    ensure_column(conn, "todos", "version", "INTEGER NOT NULL DEFAULT 1")?;
    ensure_column(conn, "todos", "device_id", "TEXT")?;
    ensure_column(conn, "todos", "deleted", "INTEGER NOT NULL DEFAULT 0")?;
    ensure_column(conn, "todos", "synced_at", "TEXT")?;
    ensure_column(conn, "todos", "uuid", "TEXT")?;
    ensure_column(conn, "todos", "subtasks", "TEXT NOT NULL DEFAULT '[]'")?;
    ensure_column(conn, "todos", "list_id", "INTEGER NOT NULL DEFAULT 0")?;
    // 为历史行补齐 uuid（P0 同步逻辑键）
    conn.execute(
        "UPDATE todos SET uuid = lower(hex(randomblob(16))) WHERE uuid IS NULL OR uuid = ''",
        [],
    )?;

    // 清单表（清单与 tag 是两个独立概念；todo_lists 随 todo.db 一起参与同步）
    conn.execute_batch(
        r#"
        CREATE TABLE IF NOT EXISTS todo_lists (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            color TEXT DEFAULT '',
            sort_order INTEGER NOT NULL DEFAULT 0,
            uuid TEXT
        );
        "#,
    )?;
    ensure_column(conn, "todo_lists", "uuid", "TEXT")?;
    conn.execute(
        "UPDATE todo_lists SET uuid = lower(hex(randomblob(16))) WHERE uuid IS NULL OR uuid = ''",
        [],
    )?;

    conn.execute_batch(
        r#"
        CREATE INDEX IF NOT EXISTS idx_todos_version ON todos(version);
        CREATE INDEX IF NOT EXISTS idx_todos_device_id ON todos(device_id);
        CREATE INDEX IF NOT EXISTS idx_todos_synced_at ON todos(synced_at);
        "#,
    )?;

    conn.execute(
        "INSERT OR IGNORE INTO sync_versions (id, global_version) VALUES (1, 0)",
        [],
    )?;

    info!("✅ todo 数据库迁移完成");
    Ok(())
}

// --- 笔记的 CRUD 操作 ---

fn map_row_to_note(row: &Row) -> Result<Note, Error> {
    let tags_json: String = row.get("tags")?;
    let tags: Vec<String> = serde_json::from_str(&tags_json).map_err(map_serde_error)?;
    let created_at: DateTime<Utc> = row.get("created_at")?;
    let updated_at: DateTime<Utc> = row.get("updated_at")?;
    let version: i64 = row.get("version")?;
    let device_id: Option<String> = row.get("device_id")?;
    let deleted: i64 = row.get("deleted")?;
    let synced_at: Option<DateTime<Utc>> = row.get("synced_at")?;

    Ok(Note {
        id: row.get("id")?,
        uuid: row.get::<_, Option<String>>("uuid")?.unwrap_or_default(),
        content: row.get("content")?,
        tags,
        created_at,
        updated_at,
        version,
        device_id,
        deleted: deleted != 0,
        synced_at,
    })
}

pub fn create_note_db(
    conn: &mut DbConnection,
    payload: CreateNotePayload,
    device_id: Option<String>,
) -> Result<Note, Error> {
    let created_at = payload.created_at.unwrap_or_else(Utc::now);
    let updated_at = created_at;
    let tags_json =
        serde_json::to_string(&payload.tags.unwrap_or_default()).map_err(map_serde_error)?;

    let tx = conn.transaction()?;
    // 获取并递增全局版本
    let global_version: i64 = tx.query_row(
        "UPDATE sync_versions SET global_version = global_version + 1 RETURNING global_version",
        [],
        |row| row.get(0),
    )?;

    let uuid: String = tx.query_row(
        r#"
        INSERT INTO notes (uuid, content, tags, created_at, updated_at, version, device_id, deleted, synced_at)
        VALUES (lower(hex(randomblob(16))), ?1, ?2, ?3, ?4, ?5, ?6, 0, ?7)
        RETURNING uuid
        "#,
        params![
            payload.content,
            tags_json,
            created_at,
            updated_at,
            global_version,
            device_id,
            created_at
        ],
        |row| row.get(0),
    )?;

    let id = tx.last_insert_rowid();
    tx.commit()?;

    let parsed_tags: Vec<String> = serde_json::from_str(&tags_json).map_err(map_serde_error)?;

    Ok(Note {
        id,
        uuid,
        content: payload.content,
        tags: parsed_tags,
        created_at,
        updated_at,
        version: global_version,
        device_id,
        deleted: false,
        synced_at: Some(created_at),
    })
}

pub fn get_note_db(conn: &DbConnection, note_id: i64) -> Result<Option<Note>, Error> {
    let mut stmt =
        conn.prepare("SELECT id, uuid, content, tags, created_at, updated_at, version, device_id, deleted, synced_at FROM notes WHERE id = ?1")?;
    let result = stmt.query_row(params![note_id], map_row_to_note);

    match result {
        Ok(note) => Ok(Some(note)),
        Err(Error::QueryReturnedNoRows) => Ok(None),
        Err(e) => Err(e),
    }
}

pub fn get_notes_db(
    conn: &DbConnection,
    limit: Option<i64>,
    offset: Option<i64>,
    tag: Option<String>,
    exclude_tags: Vec<String>,
    created_after: Option<DateTime<Utc>>,
    created_before: Option<DateTime<Utc>>,
    search: Option<String>,
    sort_by: Option<String>,
) -> Result<Vec<Note>, Error> {
    let mut query_str =
        "SELECT id, uuid, content, tags, created_at, updated_at, version, device_id, deleted, synced_at FROM notes WHERE deleted = 0".to_string();
    let mut params_vec: Vec<Box<dyn ToSql>> = Vec::new();

    if let Some(t) = tag {
        push_tag_clause(&mut query_str, &mut params_vec, &t, false);
    }
    for t in exclude_tags {
        push_tag_clause(&mut query_str, &mut params_vec, &t, true);
    }
    if let Some(after) = created_after {
        query_str.push_str(" AND created_at >= ?");
        params_vec.push(Box::new(after));
    }
    if let Some(before) = created_before {
        query_str.push_str(" AND created_at < ?");
        params_vec.push(Box::new(before));
    }
    if let Some(s) = search {
        // 使用 LIKE 在内容中搜索（将搜索词包裹在通配符 % 中）
        query_str.push_str(" AND content LIKE ?");
        params_vec.push(Box::new(format!("%{}%", s)));
    }

    // 排序：白名单字段（created_at / updated_at），默认 created_at DESC
    let (sort_field, sort_dir) = match sort_by.as_deref() {
        Some("updated_at") | Some("updated_at:desc") => ("updated_at", "DESC"),
        Some("updated_at:asc") => ("updated_at", "ASC"),
        Some("created_at:asc") => ("created_at", "ASC"),
        _ => ("created_at", "DESC"),
    };
    query_str.push_str(&format!(" ORDER BY {} {}", sort_field, sort_dir));

    if let Some(l) = limit {
        query_str.push_str(&format!(" LIMIT {}", l));
    }
    if let Some(o) = offset {
        query_str.push_str(&format!(" OFFSET {}", o));
    }

    let final_query_str = renumber_placeholders(&query_str);

    let mut stmt = conn.prepare(&final_query_str)?;
    let params_ref: Vec<&dyn ToSql> = params_vec.iter().map(|b| b.as_ref()).collect();

    let notes_iter = stmt.query_map(&params_ref[..], map_row_to_note)?;

    let mut notes = Vec::new();
    for note_result in notes_iter {
        notes.push(note_result?);
    }

    Ok(notes)
}

pub fn update_note_db(
    conn: &mut DbConnection,
    note_id: i64,
    payload: UpdateNotePayload,
    device_id: Option<String>,
) -> Result<Option<Note>, Error> {
    let updated_at = Utc::now();
    let tags_json =
        serde_json::to_string(&payload.tags.unwrap_or_default()).map_err(map_serde_error)?;

    let tx = conn.transaction()?;
    
    // 获取并递增全局版本
    let global_version: i64 = tx.query_row(
        "UPDATE sync_versions SET global_version = global_version + 1 RETURNING global_version",
        [],
        |row| row.get(0),
    )?;

    // 修改前：把当前版本快照写入历史（本地保留，不同步）
    tx.execute(
        r#"
        INSERT INTO note_history (note_id, content, tags, version, device_id, updated_at, snapshot_at)
        SELECT id, content, tags, version, device_id, updated_at, ?1
        FROM notes
        WHERE id = ?2
        "#,
        params![updated_at, note_id],
    )?;

    let rows_affected = tx.execute(
        r#"
        UPDATE notes
        SET content = ?1, tags = ?2, updated_at = ?3, version = ?4, device_id = ?5, synced_at = ?6
        WHERE id = ?7
        "#,
        params![payload.content, tags_json, updated_at, global_version, device_id, updated_at, note_id],
    )?;

    tx.commit()?;

    if rows_affected == 0 {
        Ok(None)
    } else {
        get_note_db(conn, note_id)
    }
}

/// 查询某条笔记的全部历史快照（按快照时间倒序，最新在前）
pub fn get_note_history_db(
    conn: &DbConnection,
    note_id: i64,
) -> Result<Vec<crate::models::NoteHistory>, Error> {
    let mut stmt = conn.prepare(
        r#"
        SELECT id, note_id, content, tags, version, device_id, updated_at, snapshot_at
        FROM note_history
        WHERE note_id = ?1
        ORDER BY snapshot_at DESC, id DESC
        "#,
    )?;

    let iter = stmt.query_map(params![note_id], |row| {
        let tags_json: String = row.get("tags")?;
        let tags: Vec<String> = serde_json::from_str(&tags_json).map_err(map_serde_error)?;
        let updated_at: DateTime<Utc> = row.get("updated_at")?;
        let snapshot_at: DateTime<Utc> = row.get("snapshot_at")?;

        Ok(crate::models::NoteHistory {
            id: row.get("id")?,
            note_id: row.get("note_id")?,
            content: row.get("content")?,
            tags,
            version: row.get("version")?,
            device_id: row.get("device_id")?,
            updated_at,
            snapshot_at,
        })
    })?;

    let mut results = Vec::new();
    for r in iter {
        results.push(r?);
    }
    Ok(results)
}

pub fn delete_note_db(conn: &mut DbConnection, note_id: i64, device_id: Option<String>) -> Result<bool, Error> {
    let updated_at = Utc::now();
    
    let tx = conn.transaction()?;
    
    // 获取并递增全局版本
    let global_version: i64 = tx.query_row(
        "UPDATE sync_versions SET global_version = global_version + 1 RETURNING global_version",
        [],
        |row| row.get(0),
    )?;

    // 删除前：把当前版本快照写入历史（本地保留，不同步）
    tx.execute(
        r#"
        INSERT INTO note_history (note_id, content, tags, version, device_id, updated_at, snapshot_at)
        SELECT id, content, tags, version, device_id, updated_at, ?1
        FROM notes
        WHERE id = ?2
        "#,
        params![updated_at, note_id],
    )?;

    let rows_affected = tx.execute(
        r#"
        UPDATE notes
        SET deleted = 1, updated_at = ?1, version = ?2, device_id = ?3, synced_at = ?4
        WHERE id = ?5 AND deleted = 0
        "#,
        params![updated_at, global_version, device_id, updated_at, note_id],
    )?;

    tx.commit()?;
    Ok(rows_affected > 0)
}

// --- 标签操作 ---

pub fn get_all_tags_db(conn: &DbConnection) -> Result<Vec<String>, Error> {
    let mut stmt = conn
        .prepare("SELECT tags FROM notes WHERE json_valid(tags) AND json_type(tags) = 'array'")?;
    let rows_iter = stmt.query_map(params![], |row| row.get::<_, String>(0))?;

    // *** Attempt to fix E0277 by collecting results first ***
    let tags_json_results: Vec<Result<String, Error>> = rows_iter.collect();

    let mut tag_set = std::collections::HashSet::new();
    for row_result in tags_json_results {
        match row_result {
            Ok(tags_json) => {
                // tags_json is String
                if let Ok(tags) = serde_json::from_str::<Vec<String>>(&tags_json) {
                    for tag in tags {
                        tag_set.insert(tag);
                    }
                } else {
                    warn!("警告：无法从数据库解析标签 JSON：{}", tags_json);
                }
            }
            Err(e) => {
                // Propagate error from collection step
                return Err(e);
            }
        }
    }
    Ok(tag_set.into_iter().collect())
}

pub fn get_detailed_tags_db(conn: &DbConnection) -> Result<Vec<DetailedTag>, Error> {
    let mut stmt = conn.prepare(
        r#"
        SELECT
            jt.value as tag_name,
            COUNT(*) as count,
            MAX(n.updated_at) as last_modified
        FROM
            notes n, json_each(n.tags) jt
        WHERE json_valid(n.tags) AND json_type(n.tags) = 'array'
        GROUP BY
            jt.value
        ORDER BY
            count DESC;
        "#,
    )?;

    let tag_iter = stmt.query_map(params![], |row| {
        let last_modified: Option<DateTime<Utc>> = row.get("last_modified")?;
        Ok(DetailedTag {
            name: row.get("tag_name")?,
            count: row.get("count")?,
            last_modified,
        })
    })?;

    let mut result = Vec::new();
    for tag_result in tag_iter {
        result.push(tag_result?);
    }
    Ok(result)
}

/// 层级标签树：tag 字符串按 `/` 分段构成路径树（如 `项目/工作`）。
/// count 为前缀匹配计数（本路径 + 全部子孙的笔记数），与 GET /inbox/notes?tag= 的语义一致。
/// 只统计未删除笔记；空段（`a//b`、首尾 `/`）被跳过。
pub fn get_tag_tree_db(conn: &DbConnection) -> Result<Vec<TagNode>, Error> {
    let mut stmt = conn.prepare(
        "SELECT tags FROM notes WHERE deleted = 0 AND json_valid(tags) AND json_type(tags) = 'array'",
    )?;
    let rows: Vec<String> = stmt
        .query_map([], |row| row.get(0))?
        .filter_map(|r| r.ok())
        .collect();

    // 精确 tag → 笔记数（同一笔记内的重复 tag 只计一次）
    let mut exact: std::collections::HashMap<String, i64> = std::collections::HashMap::new();
    for tags_json in rows {
        if let Ok(tags) = serde_json::from_str::<Vec<String>>(&tags_json) {
            let mut seen = std::collections::HashSet::new();
            for tag in tags {
                let tag = tag.trim().to_string();
                if tag.is_empty() || !seen.insert(tag.clone()) {
                    continue;
                }
                *exact.entry(tag).or_insert(0) += 1;
            }
        } else {
            warn!("警告：无法从数据库解析标签 JSON：{}", tags_json);
        }
    }

    // 逐 tag 沿路径累加前缀计数，并记录父子关系
    let mut incl: std::collections::HashMap<String, i64> = std::collections::HashMap::new();
    let mut children_map: std::collections::HashMap<String, Vec<String>> =
        std::collections::HashMap::new(); // 父路径 → 子路径（仅直接子级）
    for (tag, count) in &exact {
        let mut path = String::new();
        for seg in tag.split('/') {
            let seg = seg.trim();
            if seg.is_empty() {
                continue;
            }
            let parent = if path.is_empty() { None } else { Some(path.clone()) };
            if path.is_empty() {
                path.push_str(seg);
            } else {
                path.push('/');
                path.push_str(seg);
            }
            *incl.entry(path.clone()).or_insert(0) += count;
            if let Some(p) = parent {
                let entry = children_map.entry(p).or_default();
                if !entry.contains(&path) {
                    entry.push(path.clone());
                }
            }
        }
    }

    // 递归构建树，子节点按路径排序保证输出稳定
    fn build(path: String, incl: &std::collections::HashMap<String, i64>, children_map: &std::collections::HashMap<String, Vec<String>>) -> TagNode {
        let children = children_map
            .get(&path)
            .map(|childs| {
                let mut sorted = childs.clone();
                sorted.sort();
                sorted
                    .into_iter()
                    .map(|c| build(c, incl, children_map))
                    .collect()
            })
            .unwrap_or_default();
        TagNode {
            count: incl.get(&path).copied().unwrap_or(0),
            path,
            children,
        }
    }

    // 根节点 = 无 `/` 的前缀路径。注意不能只看被精确打过的 tag：
    // `项目` 可能只是 `项目/工作` 的中间节点，没人直接打 `项目`，但它必须在树里（chips 要能逐级下钻）。
    let mut roots: Vec<String> = incl
        .keys()
        .filter(|t| !t.contains('/'))
        .cloned()
        .collect();
    roots.sort();
    Ok(roots.into_iter().map(|r| build(r, &incl, &children_map)).collect())
}

// --- 笔记关系操作 ---

fn map_row_to_relation(row: &Row) -> Result<NoteRelation, Error> {
    let relation_type_str: String = row.get("relation_type")?;
    let relation_type = match relation_type_str.as_str() {
        "Comment" => NoteRelationType::Comment,
        "Reference" => NoteRelationType::Reference,
        "Link" => NoteRelationType::Link,
        _ => NoteRelationType::Reference, // 默认值
    };

    Ok(NoteRelation {
        id: row.get("id")?,
        source_note_id: row.get("source_note_id")?,
        target_note_id: row.get("target_note_id")?,
        relation_type,
        created_at: row.get("created_at")?,
    })
}

// 获取特定笔记的所有关系（无论作为 source 还是 target）
pub fn get_relations_for_note_db(
    conn: &DbConnection,
    note_id: i64,
    relation_type: Option<NoteRelationType>,
) -> Result<Vec<NoteRelation>, Error> {
    let mut query = String::from(
        "SELECT id, source_note_id, target_note_id, relation_type, created_at 
         FROM note_relations 
         WHERE source_note_id = ? OR target_note_id = ?",
    );

    let mut params_vec: Vec<Box<dyn ToSql>> = Vec::new();
    params_vec.push(Box::new(note_id));
    params_vec.push(Box::new(note_id));

    let relation_type_str = match &relation_type {
        Some(rt) => match rt {
            NoteRelationType::Comment => Some("Comment"),
            NoteRelationType::Reference => Some("Reference"),
            NoteRelationType::Link => Some("Link"),
        },
        None => None,
    };

    if relation_type_str.is_some() {
        query.push_str(" AND relation_type = ?");
        params_vec.push(Box::new(relation_type_str.unwrap()));
    }

    query.push_str(" ORDER BY created_at");

    let mut stmt = conn.prepare(&query)?;
    let params_ref: Vec<&dyn ToSql> = params_vec.iter().map(|b| b.as_ref()).collect();

    let relations_iter = stmt.query_map(&params_ref[..], map_row_to_relation)?;

    let mut relations = Vec::new();
    for relation_result in relations_iter {
        relations.push(relation_result?);
    }

    Ok(relations)
}

// 获取特定笔记的所有评论（作为关系的源笔记）
pub fn get_comments_for_note_db(
    conn: &DbConnection,
    note_id: i64,
) -> Result<Vec<(Note, NoteRelation)>, Error> {
    let mut stmt = conn.prepare(
        "SELECT n.id, n.uuid, n.content, n.tags, n.created_at, n.updated_at,
                n.version, n.device_id, n.deleted, n.synced_at,
                r.id as relation_id, r.source_note_id, r.target_note_id, r.relation_type, r.created_at as relation_created_at
         FROM notes n
         JOIN note_relations r ON n.id = r.source_note_id
         WHERE r.target_note_id = ? AND r.relation_type = 'Comment'
         ORDER BY r.created_at"
    )?;

    let results_iter = stmt.query_map(params![note_id], |row| {
        let tags_json: String = row.get("tags")?;
        let tags: Vec<String> = serde_json::from_str(&tags_json).map_err(map_serde_error)?;

        let note = Note {
            id: row.get("id")?,
            uuid: row.get::<_, Option<String>>("uuid")?.unwrap_or_default(),
            content: row.get("content")?,
            tags,
            created_at: row.get("created_at")?,
            updated_at: row.get("updated_at")?,
            version: row.get("version")?,
            device_id: row.get("device_id")?,
            deleted: row.get::<_, i64>("deleted")? != 0,
            synced_at: row.get("synced_at")?,
        };

        let relation = NoteRelation {
            id: row.get("relation_id")?,
            source_note_id: row.get("source_note_id")?,
            target_note_id: row.get("target_note_id")?,
            relation_type: NoteRelationType::Comment,
            created_at: row.get("relation_created_at")?,
        };

        Ok((note, relation))
    })?;

    let mut results = Vec::new();
    for result in results_iter {
        results.push(result?);
    }

    Ok(results)
}

// 创建笔记关系
pub fn create_note_relation_db(
    conn: &mut DbConnection,
    source_note_id: i64,
    target_note_id: i64,
    payload: CreateNoteRelationPayload,
) -> Result<NoteRelation, Error> {
    // 先检查两个笔记是否存在
    let source_exists = conn
        .query_row(
            "SELECT 1 FROM notes WHERE id = ? LIMIT 1",
            params![source_note_id],
            |_| Ok(true),
        )
        .optional()?
        .unwrap_or(false);

    let target_exists = conn
        .query_row(
            "SELECT 1 FROM notes WHERE id = ? LIMIT 1",
            params![target_note_id],
            |_| Ok(true),
        )
        .optional()?
        .unwrap_or(false);

    if !source_exists || !target_exists {
        return Err(Error::QueryReturnedNoRows);
    }

    let relation_type_str = match payload.relation_type {
        NoteRelationType::Comment => "Comment",
        NoteRelationType::Reference => "Reference",
        NoteRelationType::Link => "Link",
    };

    let created_at = Utc::now();

    conn.execute(
        "INSERT INTO note_relations (source_note_id, target_note_id, relation_type, created_at) VALUES (?, ?, ?, ?)",
        params![source_note_id, target_note_id, relation_type_str, created_at],
    )?;

    let id = conn.last_insert_rowid();

    Ok(NoteRelation {
        id,
        source_note_id,
        target_note_id,
        relation_type: payload.relation_type,
        created_at,
    })
}

// 添加评论（创建一个笔记并建立评论关系）
pub fn add_comment_db(
    conn: &mut DbConnection,
    target_note_id: i64,
    payload: CreateCommentPayload,
) -> Result<(Note, NoteRelation), Error> {
    // 检查目标笔记是否存在
    let target_exists = conn
        .query_row(
            "SELECT 1 FROM notes WHERE id = ? LIMIT 1",
            params![target_note_id],
            |_| Ok(true),
        )
        .optional()?
        .unwrap_or(false);

    if !target_exists {
        return Err(Error::QueryReturnedNoRows);
    }

    // 开始事务
    let tx = conn.transaction()?;

    // 1. 首先创建评论笔记
    let created_at = Utc::now();
    let updated_at = created_at;
    let tags = payload.tags.unwrap_or_default();
    let tags_json = serde_json::to_string(&tags).map_err(map_serde_error)?;

    // 获取并递增全局版本
    let global_version: i64 = tx.query_row(
        "UPDATE sync_versions SET global_version = global_version + 1 RETURNING global_version",
        [],
        |row| row.get(0),
    )?;

    let comment_uuid: String = tx.query_row(
        "INSERT INTO notes (uuid, content, tags, created_at, updated_at, version, device_id, deleted, synced_at) VALUES (lower(hex(randomblob(16))), ?, ?, ?, ?, ?, ?, 0, ?) RETURNING uuid",
        params![payload.content, tags_json, created_at, updated_at, global_version, Option::<String>::None, created_at],
        |row| row.get(0),
    )?;

    let comment_note_id = tx.last_insert_rowid();

    // 2. 创建评论关系
    tx.execute(
        "INSERT INTO note_relations (source_note_id, target_note_id, relation_type, created_at) VALUES (?, ?, ?, ?)",
        params![comment_note_id, target_note_id, "Comment", created_at],
    )?;

    let relation_id = tx.last_insert_rowid();

    // 提交事务
    tx.commit()?;

    // 返回新创建的笔记和关系
    Ok((
        Note {
            id: comment_note_id,
            uuid: comment_uuid,
            content: payload.content,
            tags,
            created_at,
            updated_at,
            version: global_version,
            device_id: None,
            deleted: false,
            synced_at: Some(created_at),
        },
        NoteRelation {
            id: relation_id,
            source_note_id: comment_note_id,
            target_note_id,
            relation_type: NoteRelationType::Comment,
            created_at,
        },
    ))
}

// ── Todo CRUD ──────────────────────────────────────────────────

fn map_row_to_todo(row: &rusqlite::Row) -> Result<Todo, Error> {
    let tags_json: String = row.get("tags")?;
    let tags: Vec<String> = serde_json::from_str(&tags_json).unwrap_or_default();
    let subtasks_json: String = row.get("subtasks")?;
    let subtasks: Vec<crate::models::TodoSubtaskItem> =
        serde_json::from_str(&subtasks_json).unwrap_or_default();
    let created_at: DateTime<Utc> = row.get("created_at")?;
    let updated_at: DateTime<Utc> = row.get("updated_at")?;
    let completed: i64 = row.get("completed")?;
    let deleted: i64 = row.get("deleted")?;

    Ok(Todo {
        id: row.get("id")?,
        uuid: row.get::<_, Option<String>>("uuid")?.unwrap_or_default(),
        title: row.get("title")?,
        content: row.get("content")?,
        completed: completed != 0,
        priority: row.get("priority")?,
        due_date: row.get("due_date")?,
        tags,
        subtasks,
        list_id: row.get("list_id")?,
        created_at,
        updated_at,
        completed_at: row.get("completed_at")?,
        version: row.get("version")?,
        device_id: row.get("device_id")?,
        deleted: deleted != 0,
        synced_at: row.get("synced_at")?,
    })
}

pub fn create_todo_db(
    conn: &mut DbConnection,
    payload: CreateTodoPayload,
    device_id: Option<String>,
) -> Result<Todo, Error> {
    let created_at = payload.created_at.unwrap_or_else(Utc::now);
    let updated_at = created_at;
    let tags_json =
        serde_json::to_string(&payload.tags.unwrap_or_default()).map_err(map_serde_error)?;
    let subtasks_json =
        serde_json::to_string(&payload.subtasks.unwrap_or_default()).map_err(map_serde_error)?;
    let list_id = payload.list_id.unwrap_or(0);

    let tx = conn.transaction()?;
    let global_version: i64 = tx.query_row(
        "UPDATE sync_versions SET global_version = global_version + 1 RETURNING global_version",
        [],
        |row| row.get(0),
    )?;

    let uuid: String = tx.query_row(
        r#"
        INSERT INTO todos (uuid, title, content, completed, priority, due_date, tags, subtasks, list_id,
                           created_at, updated_at, completed_at, version, device_id, deleted, synced_at)
        VALUES (lower(hex(randomblob(16))), ?1, ?2, 0, ?3, ?4, ?5, ?6, ?7, ?8, ?9, NULL, ?10, ?11, 0, ?12)
        RETURNING uuid
        "#,
        params![
            payload.title,
            payload.content,
            payload.priority,
            payload.due_date,
            tags_json,
            subtasks_json,
            list_id,
            created_at,
            updated_at,
            global_version,
            device_id,
            created_at,
        ],
        |row| row.get(0),
    )?;

    let id = tx.last_insert_rowid();
    tx.commit()?;

    let parsed_tags: Vec<String> = serde_json::from_str(&tags_json).map_err(map_serde_error)?;
    let parsed_subtasks: Vec<crate::models::TodoSubtaskItem> =
        serde_json::from_str(&subtasks_json).map_err(map_serde_error)?;

    Ok(Todo {
        id,
        uuid,
        title: payload.title,
        content: payload.content,
        completed: false,
        priority: payload.priority,
        due_date: payload.due_date,
        tags: parsed_tags,
        subtasks: parsed_subtasks,
        list_id,
        created_at,
        updated_at,
        completed_at: None,
        version: global_version,
        device_id,
        deleted: false,
        synced_at: Some(created_at),
    })
}

pub fn get_todos_db(
    conn: &DbConnection,
    completed: Option<bool>,
    tag: Option<String>,
    exclude_tags: Vec<String>,
    limit: Option<i64>,
    offset: Option<i64>,
) -> Result<Vec<Todo>, Error> {
    let mut sql = String::from(
        "SELECT id, uuid, title, content, completed, priority, due_date, tags, subtasks, list_id,
                created_at, updated_at, completed_at, version, device_id, deleted, synced_at
         FROM todos WHERE deleted = 0",
    );
    let mut params_vec: Vec<Box<dyn ToSql>> = Vec::new();
    if let Some(c) = completed {
        sql.push_str(&format!(" AND completed = {}", if c { 1 } else { 0 }));
    }
    if let Some(t) = tag {
        push_tag_clause(&mut sql, &mut params_vec, &t, false);
    }
    for t in exclude_tags {
        push_tag_clause(&mut sql, &mut params_vec, &t, true);
    }
    sql.push_str(" ORDER BY completed ASC, priority DESC NULLS LAST, created_at DESC");
    if let Some(l) = limit {
        sql.push_str(&format!(" LIMIT {}", l));
    }
    if let Some(o) = offset {
        sql.push_str(&format!(" OFFSET {}", o));
    }

    let final_sql = renumber_placeholders(&sql);
    let mut stmt = conn.prepare(&final_sql)?;
    let params_ref: Vec<&dyn ToSql> = params_vec.iter().map(|b| b.as_ref()).collect();
    let todos_iter = stmt.query_map(&params_ref[..], map_row_to_todo)?;
    let mut todos = Vec::new();
    for todo_result in todos_iter {
        todos.push(todo_result?);
    }
    Ok(todos)
}

pub fn get_todo_by_id_db(conn: &DbConnection, todo_id: i64) -> Result<Todo, Error> {
    let todo = conn.query_row(
        "SELECT id, uuid, title, content, completed, priority, due_date, tags, subtasks, list_id,
                created_at, updated_at, completed_at, version, device_id, deleted, synced_at
         FROM todos WHERE id = ?1",
        params![todo_id],
        map_row_to_todo,
    )?;
    Ok(todo)
}

pub fn update_todo_db(
    conn: &mut DbConnection,
    todo_id: i64,
    payload: UpdateTodoPayload,
) -> Result<Todo, Error> {
    let existing = get_todo_by_id_db(conn, todo_id)?;
    let updated_at = Utc::now();

    let title = payload.title.unwrap_or(existing.title);
    let content = payload.content.or(existing.content);
    let priority = payload.priority.or(existing.priority);
    let due_date = payload.due_date.or(existing.due_date);
    let tags = payload.tags.unwrap_or(existing.tags);
    let tags_json = serde_json::to_string(&tags).map_err(map_serde_error)?;
    let subtasks = payload.subtasks.unwrap_or(existing.subtasks);
    let subtasks_json = serde_json::to_string(&subtasks).map_err(map_serde_error)?;
    let list_id = payload.list_id.unwrap_or(existing.list_id);

    let (completed, completed_at) = if let Some(c) = payload.completed {
        if c && !existing.completed {
            (true, Some(updated_at))
        } else if !c && existing.completed {
            (false, None)
        } else {
            (c, existing.completed_at)
        }
    } else {
        (existing.completed, existing.completed_at)
    };

    let tx = conn.transaction()?;
    let global_version: i64 = tx.query_row(
        "UPDATE sync_versions SET global_version = global_version + 1 RETURNING global_version",
        [],
        |row| row.get(0),
    )?;

    tx.execute(
        r#"
        UPDATE todos SET
            title = ?1, content = ?2, completed = ?3, priority = ?4,
            due_date = ?5, tags = ?6, subtasks = ?7, list_id = ?8,
            updated_at = ?9, completed_at = ?10, version = ?11
        WHERE id = ?12
        "#,
        params![
            title,
            content,
            if completed { 1 } else { 0 },
            priority,
            due_date,
            tags_json,
            subtasks_json,
            list_id,
            updated_at,
            completed_at,
            global_version,
            todo_id,
        ],
    )?;
    tx.commit()?;

    Ok(Todo {
        id: todo_id,
        uuid: existing.uuid,
        title,
        content,
        completed,
        priority,
        due_date,
        tags,
        subtasks,
        list_id,
        created_at: existing.created_at,
        updated_at,
        completed_at,
        version: global_version,
        device_id: existing.device_id,
        deleted: false,
        synced_at: Some(updated_at),
    })
}

pub fn delete_todo_db(conn: &mut DbConnection, todo_id: i64) -> Result<(), Error> {
    let tx = conn.transaction()?;
    let global_version: i64 = tx.query_row(
        "UPDATE sync_versions SET global_version = global_version + 1 RETURNING global_version",
        [],
        |row| row.get(0),
    )?;
    tx.execute(
        "UPDATE todos SET deleted = 1, version = ?1 WHERE id = ?2",
        params![global_version, todo_id],
    )?;
    tx.commit()?;
    Ok(())
}

// --- 恢复（从回收站 / 软删恢复）---

/// 恢复一条软删除的笔记（deleted 1->0），bump 版本号；返回恢复后的笔记或 None（不存在/未删除）。
pub fn restore_note_db(
    conn: &mut DbConnection,
    note_id: i64,
    device_id: Option<String>,
) -> Result<Option<Note>, Error> {
    let updated_at = Utc::now();
    let tx = conn.transaction()?;
    let global_version: i64 = tx.query_row(
        "UPDATE sync_versions SET global_version = global_version + 1 RETURNING global_version",
        [],
        |row| row.get(0),
    )?;
    let rows_affected = tx.execute(
        r#"
        UPDATE notes
        SET deleted = 0, updated_at = ?1, version = ?2, device_id = ?3, synced_at = ?4
        WHERE id = ?5 AND deleted = 1
        "#,
        params![updated_at, global_version, device_id, updated_at, note_id],
    )?;
    tx.commit()?;
    if rows_affected > 0 {
        get_note_db(conn, note_id)
    } else {
        Ok(None)
    }
}

/// 恢复一条软删除的 todo（deleted 1->0），bump 版本号；返回恢复后的 todo 或 None。
pub fn restore_todo_db(conn: &mut DbConnection, todo_id: i64) -> Result<Option<Todo>, Error> {
    let updated_at = Utc::now();
    let tx = conn.transaction()?;
    let global_version: i64 = tx.query_row(
        "UPDATE sync_versions SET global_version = global_version + 1 RETURNING global_version",
        [],
        |row| row.get(0),
    )?;
    let rows_affected = tx.execute(
        r#"
        UPDATE todos
        SET deleted = 0, updated_at = ?1, version = ?2
        WHERE id = ?3 AND deleted = 1
        "#,
        params![updated_at, global_version, todo_id],
    )?;
    tx.commit()?;
    if rows_affected > 0 {
        Ok(Some(get_todo_by_id_db(conn, todo_id)?))
    } else {
        Ok(None)
    }
}

// ── Todo List CRUD（清单与 tag 独立；todo_lists 随 todo.db 参与同步） ──

fn map_row_to_todo_list(row: &rusqlite::Row) -> Result<TodoListRecord, Error> {
    Ok(TodoListRecord {
        id: row.get("id")?,
        name: row.get("name")?,
        color: row.get::<_, Option<String>>("color")?.unwrap_or_default(),
        sort_order: row.get("sort_order")?,
        uuid: row.get::<_, Option<String>>("uuid")?.unwrap_or_default(),
    })
}

pub fn get_todo_lists_db(conn: &DbConnection) -> Result<Vec<TodoListRecord>, Error> {
    let mut stmt = conn.prepare(
        "SELECT id, name, color, sort_order, uuid FROM todo_lists ORDER BY sort_order ASC, id ASC",
    )?;
    let rows = stmt.query_map([], map_row_to_todo_list)?;
    let mut lists = Vec::new();
    for r in rows {
        lists.push(r?);
    }
    Ok(lists)
}

pub fn create_todo_list_db(
    conn: &mut DbConnection,
    payload: CreateTodoListPayload,
) -> Result<TodoListRecord, Error> {
    let name = payload.name.trim().to_string();
    if name.is_empty() {
        return Err(Error::from(rusqlite::Error::InvalidQuery));
    }
    conn.execute(
        "INSERT INTO todo_lists (name, color, sort_order, uuid)
         VALUES (?1, ?2, ?3, lower(hex(randomblob(16))))",
        params![name, payload.color, payload.sort_order],
    )?;
    let id = conn.last_insert_rowid();
    Ok(TodoListRecord {
        id,
        name,
        color: payload.color,
        sort_order: payload.sort_order,
        uuid: String::new(),
    })
}

pub fn update_todo_list_db(
    conn: &mut DbConnection,
    list_id: i64,
    payload: UpdateTodoListPayload,
) -> Result<Option<TodoListRecord>, Error> {
    let existing = conn
        .query_row(
            "SELECT id, name, color, sort_order, uuid FROM todo_lists WHERE id = ?1",
            params![list_id],
            map_row_to_todo_list,
        )
        .optional()?;
    let Some(existing) = existing else {
        return Ok(None);
    };
    let name = payload.name.map(|n| n.trim().to_string()).unwrap_or(existing.name);
    let color = payload.color.unwrap_or(existing.color);
    let sort_order = payload.sort_order.unwrap_or(existing.sort_order);
    conn.execute(
        "UPDATE todo_lists SET name = ?1, color = ?2, sort_order = ?3 WHERE id = ?4",
        params![name, color, sort_order, list_id],
    )?;
    Ok(Some(TodoListRecord {
        id: list_id,
        name,
        color,
        sort_order,
        uuid: existing.uuid,
    }))
}

/// 删除清单：其下任务的 list_id 归零（回收集箱），任务本身不删除。
pub fn delete_todo_list_db(conn: &mut DbConnection, list_id: i64) -> Result<bool, Error> {
    let tx = conn.transaction()?;
    tx.execute("UPDATE todos SET list_id = 0 WHERE list_id = ?1", params![list_id])?;
    let rows = tx.execute("DELETE FROM todo_lists WHERE id = ?1", params![list_id])?;
    tx.commit()?;
    Ok(rows > 0)
}

// ── 批量操作（给 AI 返回的操作指令用）───────────────────────────
// 每条指令独立执行、互不影响（不整体回滚），逐条返回成功/失败，便于定位问题项。

/// 按 uuid（优先，跨设备唯一）或自增 id 定位一条笔记，返回 (id, uuid)。
fn resolve_note_ref(
    conn: &DbConnection,
    id: Option<i64>,
    uuid: Option<&str>,
) -> Result<Option<(i64, String)>, Error> {
    let row = |id: i64| -> Result<(i64, String), Error> {
        conn.query_row(
            "SELECT id, uuid FROM notes WHERE id = ?1",
            params![id],
            |r| Ok((r.get::<_, i64>(0)?, r.get::<_, Option<String>>(1)?.unwrap_or_default())),
        )
    };
    if let Some(u) = uuid.filter(|u| !u.trim().is_empty()) {
        return conn
            .query_row(
                "SELECT id, uuid FROM notes WHERE uuid = ?1",
                params![u],
                |r| Ok((r.get::<_, i64>(0)?, r.get::<_, Option<String>>(1)?.unwrap_or_default())),
            )
            .optional();
    }
    if let Some(id) = id {
        return row(id).optional();
    }
    Ok(None)
}

/// 按 uuid（优先）或自增 id 定位一条任务，返回 (id, uuid)。
fn resolve_todo_ref(
    conn: &DbConnection,
    id: Option<i64>,
    uuid: Option<&str>,
) -> Result<Option<(i64, String)>, Error> {
    if let Some(u) = uuid.filter(|u| !u.trim().is_empty()) {
        return conn
            .query_row(
                "SELECT id, uuid FROM todos WHERE uuid = ?1",
                params![u],
                |r| Ok((r.get::<_, i64>(0)?, r.get::<_, Option<String>>(1)?.unwrap_or_default())),
            )
            .optional();
    }
    if let Some(id) = id {
        return conn
            .query_row(
                "SELECT id, uuid FROM todos WHERE id = ?1",
                params![id],
                |r| Ok((r.get::<_, i64>(0)?, r.get::<_, Option<String>>(1)?.unwrap_or_default())),
            )
            .optional();
    }
    Ok(None)
}

/// 合并标签：把 add 里的标签追加到 base 末尾（去重、忽略空白、保持原顺序）。
fn merge_tags(base: &[String], add: &[String]) -> Vec<String> {
    let mut out: Vec<String> = base.to_vec();
    for t in add {
        let t = t.trim();
        if t.is_empty() || out.iter().any(|x| x == t) {
            continue;
        }
        out.push(t.to_string());
    }
    out
}

/// 从 base 中移除 remove 里的标签（精确匹配，不做层级展开）。
fn subtract_tags(base: &[String], remove: &[String]) -> Vec<String> {
    let rm: std::collections::HashSet<&str> = remove.iter().map(|s| s.trim()).collect();
    base.iter().filter(|t| !rm.contains(t.as_str())).cloned().collect()
}

/// 按清单名解析清单 id：空或「收集箱」→ 0；未找到返回 None。
fn todo_list_id_by_name_db(conn: &DbConnection, name: &str) -> Result<Option<i64>, Error> {
    let name = name.trim();
    if name.is_empty() || name == "收集箱" {
        return Ok(Some(0));
    }
    conn.query_row(
        "SELECT id FROM todo_lists WHERE name = ?1",
        params![name],
        |r| r.get::<_, i64>(0),
    )
    .optional()
}

/// 设置或清空任务截止日期（bump 版本号）；返回更新后的任务。
fn set_todo_due_db(
    conn: &mut DbConnection,
    todo_id: i64,
    due: Option<DateTime<Utc>>,
) -> Result<Todo, Error> {
    let updated_at = Utc::now();
    let tx = conn.transaction()?;
    let global_version: i64 = tx.query_row(
        "UPDATE sync_versions SET global_version = global_version + 1 RETURNING global_version",
        [],
        |row| row.get(0),
    )?;
    tx.execute(
        "UPDATE todos SET due_date = ?1, updated_at = ?2, version = ?3 WHERE id = ?4",
        params![due, updated_at, global_version, todo_id],
    )?;
    tx.commit()?;
    get_todo_by_id_db(conn, todo_id)
}

/// 逐条执行笔记批量指令。
pub fn batch_notes_db(
    conn: &mut DbConnection,
    payload: NoteBatchPayload,
    device_id: Option<String>,
) -> Result<BatchResponse, Error> {
    let mut results: Vec<BatchOpResult> = Vec::with_capacity(payload.operations.len());
    let mut applied = 0usize;
    let mut failed = 0usize;

    for (index, op) in payload.operations.into_iter().enumerate() {
        let action = op.action;
        let outcome: Result<(Option<i64>, Option<String>), String> = match action {
            BatchAction::Create => {
                let create_payload = CreateNotePayload {
                    content: op.content.unwrap_or_default(),
                    tags: op.tags,
                    created_at: op.created_at,
                };
                create_note_db(conn, create_payload, device_id.clone())
                    .map(|n| (Some(n.id), Some(n.uuid)))
                    .map_err(|e| e.to_string())
            }
            BatchAction::Update => match resolve_note_ref(conn, op.id, op.uuid.as_deref()) {
                Ok(Some((note_id, _))) => match get_note_db(conn, note_id) {
                    Ok(Some(existing)) => {
                        let update_payload = UpdateNotePayload {
                            content: op.content.unwrap_or(existing.content),
                            tags: op.tags.or(Some(existing.tags)),
                        };
                        match update_note_db(conn, note_id, update_payload, device_id.clone()) {
                            Ok(Some(n)) => Ok((Some(n.id), Some(n.uuid))),
                            Ok(None) => Err("笔记不存在".to_string()),
                            Err(e) => Err(e.to_string()),
                        }
                    }
                    Ok(None) => Err("笔记不存在".to_string()),
                    Err(e) => Err(e.to_string()),
                },
                Ok(None) => Err("需要提供 id 或 uuid 定位笔记".to_string()),
                Err(e) => Err(e.to_string()),
            },
            BatchAction::Delete => match resolve_note_ref(conn, op.id, op.uuid.as_deref()) {
                Ok(Some((note_id, note_uuid))) => {
                    match delete_note_db(conn, note_id, device_id.clone()) {
                        Ok(true) => Ok((Some(note_id), Some(note_uuid))),
                        Ok(false) => Err("笔记不存在或已删除".to_string()),
                        Err(e) => Err(e.to_string()),
                    }
                }
                Ok(None) => Err("需要提供 id 或 uuid 定位笔记".to_string()),
                Err(e) => Err(e.to_string()),
            },
            BatchAction::Restore => match resolve_note_ref(conn, op.id, op.uuid.as_deref()) {
                Ok(Some((note_id, _))) => {
                    match restore_note_db(conn, note_id, device_id.clone()) {
                        Ok(Some(n)) => Ok((Some(n.id), Some(n.uuid))),
                        Ok(None) => Err("笔记不存在或未删除".to_string()),
                        Err(e) => Err(e.to_string()),
                    }
                }
                Ok(None) => Err("需要提供 id 或 uuid 定位笔记".to_string()),
                Err(e) => Err(e.to_string()),
            },
            // 标签增量操作：不必先知道现状即可追加/移除/整体替换
            BatchAction::AddTags | BatchAction::RemoveTags | BatchAction::SetTags => {
                match resolve_note_ref(conn, op.id, op.uuid.as_deref()) {
                    Ok(Some((note_id, _))) => match get_note_db(conn, note_id) {
                        Ok(Some(existing)) => {
                            let incoming = op.tags.unwrap_or_default();
                            let tags = match action {
                                BatchAction::AddTags => merge_tags(&existing.tags, &incoming),
                                BatchAction::RemoveTags => subtract_tags(&existing.tags, &incoming),
                                _ => incoming, // SetTags：整体替换
                            };
                            let update_payload = UpdateNotePayload {
                                content: existing.content,
                                tags: Some(tags),
                            };
                            match update_note_db(conn, note_id, update_payload, device_id.clone()) {
                                Ok(Some(n)) => Ok((Some(n.id), Some(n.uuid))),
                                Ok(None) => Err("笔记不存在".to_string()),
                                Err(e) => Err(e.to_string()),
                            }
                        }
                        Ok(None) => Err("笔记不存在".to_string()),
                        Err(e) => Err(e.to_string()),
                    },
                    Ok(None) => Err("需要提供 id 或 uuid 定位笔记".to_string()),
                    Err(e) => Err(e.to_string()),
                }
            }
            // 给笔记追加一条评论（评论本身就是一条带 Comment 关系的笔记）
            BatchAction::Comment => match resolve_note_ref(conn, op.id, op.uuid.as_deref()) {
                Ok(Some((note_id, _))) => {
                    let comment_payload = CreateCommentPayload {
                        content: op.content.unwrap_or_default(),
                        tags: op.tags,
                    };
                    match add_comment_db(conn, note_id, comment_payload) {
                        Ok((comment, _relation)) => Ok((Some(comment.id), Some(comment.uuid))),
                        Err(Error::QueryReturnedNoRows) => Err("笔记不存在".to_string()),
                        Err(e) => Err(e.to_string()),
                    }
                }
                Ok(None) => Err("需要提供 id 或 uuid 定位笔记".to_string()),
                Err(e) => Err(e.to_string()),
            },
            other => Err(format!("笔记不支持动作 {}", other.as_str())),
        };

        match outcome {
            Ok((id, uuid)) => {
                applied += 1;
                results.push(BatchOpResult {
                    index,
                    action: action.as_str().to_string(),
                    ok: true,
                    id,
                    uuid,
                    error: None,
                });
            }
            Err(err) => {
                failed += 1;
                results.push(BatchOpResult {
                    index,
                    action: action.as_str().to_string(),
                    ok: false,
                    id: None,
                    uuid: None,
                    error: Some(err),
                });
            }
        }
    }

    Ok(BatchResponse { applied, failed, results })
}

/// 逐条执行任务批量指令（create/update/delete/restore）。
pub fn batch_todos_db(
    conn: &mut DbConnection,
    payload: TodoBatchPayload,
    device_id: Option<String>,
) -> Result<BatchResponse, Error> {
    let mut results: Vec<BatchOpResult> = Vec::with_capacity(payload.operations.len());
    let mut applied = 0usize;
    let mut failed = 0usize;

    for (index, op) in payload.operations.into_iter().enumerate() {
        let action = op.action;
        let outcome: Result<(Option<i64>, Option<String>), String> = match action {
            BatchAction::Create => match op.title.clone() {
                Some(title) => {
                    let create_payload = CreateTodoPayload {
                        title,
                        content: op.content.clone(),
                        priority: op.priority,
                        due_date: op.due_date,
                        tags: op.tags.clone(),
                        subtasks: op.subtasks.clone(),
                        list_id: op.list_id,
                        created_at: op.created_at,
                    };
                    create_todo_db(conn, create_payload, device_id.clone())
                        .map(|t| (Some(t.id), Some(t.uuid)))
                        .map_err(|e| e.to_string())
                }
                None => Err("创建任务需要 title".to_string()),
            },
            BatchAction::Update => match resolve_todo_ref(conn, op.id, op.uuid.as_deref()) {
                Ok(Some((todo_id, _))) => {
                    let update_payload = UpdateTodoPayload {
                        title: op.title,
                        content: op.content,
                        completed: op.completed,
                        priority: op.priority,
                        due_date: op.due_date,
                        tags: op.tags,
                        subtasks: op.subtasks,
                        list_id: op.list_id,
                    };
                    match update_todo_db(conn, todo_id, update_payload) {
                        Ok(t) => Ok((Some(t.id), Some(t.uuid))),
                        Err(Error::QueryReturnedNoRows) => Err("任务不存在".to_string()),
                        Err(e) => Err(e.to_string()),
                    }
                }
                Ok(None) => Err("需要提供 id 或 uuid 定位任务".to_string()),
                Err(e) => Err(e.to_string()),
            },
            BatchAction::Delete => match resolve_todo_ref(conn, op.id, op.uuid.as_deref()) {
                Ok(Some((todo_id, todo_uuid))) => match delete_todo_db(conn, todo_id) {
                    Ok(()) => Ok((Some(todo_id), Some(todo_uuid))),
                    Err(e) => Err(e.to_string()),
                },
                Ok(None) => Err("需要提供 id 或 uuid 定位任务".to_string()),
                Err(e) => Err(e.to_string()),
            },
            BatchAction::Restore => match resolve_todo_ref(conn, op.id, op.uuid.as_deref()) {
                Ok(Some((todo_id, _))) => match restore_todo_db(conn, todo_id) {
                    Ok(Some(t)) => Ok((Some(t.id), Some(t.uuid))),
                    Ok(None) => Err("任务不存在或未删除".to_string()),
                    Err(e) => Err(e.to_string()),
                },
                Ok(None) => Err("需要提供 id 或 uuid 定位任务".to_string()),
                Err(e) => Err(e.to_string()),
            },
            // 标签增量操作（追加 / 移除 / 整体替换）
            BatchAction::AddTags | BatchAction::RemoveTags | BatchAction::SetTags => {
                match resolve_todo_ref(conn, op.id, op.uuid.as_deref()) {
                    Ok(Some((todo_id, _))) => match get_todo_by_id_db(conn, todo_id) {
                        Ok(existing) => {
                            let incoming = op.tags.unwrap_or_default();
                            let tags = match action {
                                BatchAction::AddTags => merge_tags(&existing.tags, &incoming),
                                BatchAction::RemoveTags => subtract_tags(&existing.tags, &incoming),
                                _ => incoming,
                            };
                            let update_payload =
                                UpdateTodoPayload { tags: Some(tags), ..Default::default() };
                            match update_todo_db(conn, todo_id, update_payload) {
                                Ok(t) => Ok((Some(t.id), Some(t.uuid))),
                                Err(Error::QueryReturnedNoRows) => Err("任务不存在".to_string()),
                                Err(e) => Err(e.to_string()),
                            }
                        }
                        Err(e) => Err(e.to_string()),
                    },
                    Ok(None) => Err("需要提供 id 或 uuid 定位任务".to_string()),
                    Err(e) => Err(e.to_string()),
                }
            }
            BatchAction::SetCompleted => match op.completed {
                Some(completed) => match resolve_todo_ref(conn, op.id, op.uuid.as_deref()) {
                    Ok(Some((todo_id, _))) => {
                        let update_payload =
                            UpdateTodoPayload { completed: Some(completed), ..Default::default() };
                        match update_todo_db(conn, todo_id, update_payload) {
                            Ok(t) => Ok((Some(t.id), Some(t.uuid))),
                            Err(Error::QueryReturnedNoRows) => Err("任务不存在".to_string()),
                            Err(e) => Err(e.to_string()),
                        }
                    }
                    Ok(None) => Err("需要提供 id 或 uuid 定位任务".to_string()),
                    Err(e) => Err(e.to_string()),
                },
                None => Err("set_completed 需要 completed 字段".to_string()),
            },
            // 转移清单：list_id 优先，否则按 list_name 精确匹配（收集箱 = 0）
            BatchAction::Move => match resolve_todo_ref(conn, op.id, op.uuid.as_deref()) {
                Ok(Some((todo_id, _))) => {
                    let list_result: Result<Option<i64>, String> = if let Some(id) = op.list_id {
                        Ok(Some(id))
                    } else if let Some(name) = op.list_name.as_deref() {
                        match todo_list_id_by_name_db(conn, name) {
                            Ok(Some(id)) => Ok(Some(id)),
                            Ok(None) => Err(format!("清单不存在: {}", name)),
                            Err(e) => Err(e.to_string()),
                        }
                    } else {
                        Ok(None)
                    };
                    match list_result {
                        Ok(Some(list_id)) => {
                            let update_payload =
                                UpdateTodoPayload { list_id: Some(list_id), ..Default::default() };
                            match update_todo_db(conn, todo_id, update_payload) {
                                Ok(t) => Ok((Some(t.id), Some(t.uuid))),
                                Err(Error::QueryReturnedNoRows) => Err("任务不存在".to_string()),
                                Err(e) => Err(e.to_string()),
                            }
                        }
                        Ok(None) => Err("move 需要 list_id 或 list_name".to_string()),
                        Err(e) => Err(e),
                    }
                }
                Ok(None) => Err("需要提供 id 或 uuid 定位任务".to_string()),
                Err(e) => Err(e.to_string()),
            },
            BatchAction::SetPriority => match op.priority {
                Some(priority) => match resolve_todo_ref(conn, op.id, op.uuid.as_deref()) {
                    Ok(Some((todo_id, _))) => {
                        let update_payload =
                            UpdateTodoPayload { priority: Some(priority), ..Default::default() };
                        match update_todo_db(conn, todo_id, update_payload) {
                            Ok(t) => Ok((Some(t.id), Some(t.uuid))),
                            Err(Error::QueryReturnedNoRows) => Err("任务不存在".to_string()),
                            Err(e) => Err(e.to_string()),
                        }
                    }
                    Ok(None) => Err("需要提供 id 或 uuid 定位任务".to_string()),
                    Err(e) => Err(e.to_string()),
                },
                None => Err("set_priority 需要 priority 字段（0 无 / 1 低 / 2 中 / 3 高）".to_string()),
            },
            // 截止日期：due_date 设置，clear_due=true 清空
            BatchAction::SetDue => match resolve_todo_ref(conn, op.id, op.uuid.as_deref()) {
                Ok(Some((todo_id, _))) => {
                    if op.clear_due == Some(true) {
                        match set_todo_due_db(conn, todo_id, None) {
                            Ok(t) => Ok((Some(t.id), Some(t.uuid))),
                            Err(e) => Err(e.to_string()),
                        }
                    } else if let Some(due) = op.due_date {
                        match set_todo_due_db(conn, todo_id, Some(due)) {
                            Ok(t) => Ok((Some(t.id), Some(t.uuid))),
                            Err(e) => Err(e.to_string()),
                        }
                    } else {
                        Err("set_due 需要 due_date 或 clear_due".to_string())
                    }
                }
                Ok(None) => Err("需要提供 id 或 uuid 定位任务".to_string()),
                Err(e) => Err(e.to_string()),
            },
            // 追加子任务（id 自动取现有最大值 +1）
            BatchAction::AddSubtask => match resolve_todo_ref(conn, op.id, op.uuid.as_deref()) {
                Ok(Some((todo_id, _))) => match get_todo_by_id_db(conn, todo_id) {
                    Ok(existing) => {
                        let title = op.title.clone().unwrap_or_default();
                        if title.trim().is_empty() {
                            Err("add_subtask 需要 title".to_string())
                        } else {
                            let mut subs = existing.subtasks.clone();
                            let next_id = subs.iter().map(|s| s.id).max().unwrap_or(0) + 1;
                            subs.push(crate::models::TodoSubtaskItem {
                                id: next_id,
                                title,
                                completed: false,
                            });
                            let update_payload =
                                UpdateTodoPayload { subtasks: Some(subs), ..Default::default() };
                            match update_todo_db(conn, todo_id, update_payload) {
                                Ok(t) => Ok((Some(t.id), Some(t.uuid))),
                                Err(e) => Err(e.to_string()),
                            }
                        }
                    }
                    Err(e) => Err(e.to_string()),
                },
                Ok(None) => Err("需要提供 id 或 uuid 定位任务".to_string()),
                Err(e) => Err(e.to_string()),
            },
            BatchAction::RemoveSubtask => match resolve_todo_ref(conn, op.id, op.uuid.as_deref()) {
                Ok(Some((todo_id, _))) => match (get_todo_by_id_db(conn, todo_id), op.subtask_id) {
                    (Ok(existing), Some(sub_id)) => {
                        let subs: Vec<_> =
                            existing.subtasks.into_iter().filter(|s| s.id != sub_id).collect();
                        let update_payload =
                            UpdateTodoPayload { subtasks: Some(subs), ..Default::default() };
                        match update_todo_db(conn, todo_id, update_payload) {
                            Ok(t) => Ok((Some(t.id), Some(t.uuid))),
                            Err(e) => Err(e.to_string()),
                        }
                    }
                    (Ok(_), None) => Err("remove_subtask 需要 subtask_id".to_string()),
                    (Err(e), _) => Err(e.to_string()),
                },
                Ok(None) => Err("需要提供 id 或 uuid 定位任务".to_string()),
                Err(e) => Err(e.to_string()),
            },
            // 勾选/取消子任务（completed 缺省按 true）
            BatchAction::SetSubtask => match resolve_todo_ref(conn, op.id, op.uuid.as_deref()) {
                Ok(Some((todo_id, _))) => match (get_todo_by_id_db(conn, todo_id), op.subtask_id) {
                    (Ok(existing), Some(sub_id)) => {
                        let completed = op.completed.unwrap_or(true);
                        let mut subs = existing.subtasks.clone();
                        let mut found = false;
                        for s in subs.iter_mut() {
                            if s.id == sub_id {
                                s.completed = completed;
                                found = true;
                            }
                        }
                        if !found {
                            Err(format!("子任务不存在: {}", sub_id))
                        } else {
                            let update_payload =
                                UpdateTodoPayload { subtasks: Some(subs), ..Default::default() };
                            match update_todo_db(conn, todo_id, update_payload) {
                                Ok(t) => Ok((Some(t.id), Some(t.uuid))),
                                Err(e) => Err(e.to_string()),
                            }
                        }
                    }
                    (Ok(_), None) => Err("set_subtask 需要 subtask_id".to_string()),
                    (Err(e), _) => Err(e.to_string()),
                },
                Ok(None) => Err("需要提供 id 或 uuid 定位任务".to_string()),
                Err(e) => Err(e.to_string()),
            },
            // 给任务追加备注/评论（追加到备注末尾，换行分隔）
            BatchAction::Comment => match resolve_todo_ref(conn, op.id, op.uuid.as_deref()) {
                Ok(Some((todo_id, _))) => match get_todo_by_id_db(conn, todo_id) {
                    Ok(existing) => {
                        let comment = op.content.unwrap_or_default();
                        let notes = match existing.content {
                            Some(prev) if !prev.trim().is_empty() => format!("{}\n{}", prev, comment),
                            _ => comment,
                        };
                        let update_payload =
                            UpdateTodoPayload { content: Some(notes), ..Default::default() };
                        match update_todo_db(conn, todo_id, update_payload) {
                            Ok(t) => Ok((Some(t.id), Some(t.uuid))),
                            Err(e) => Err(e.to_string()),
                        }
                    }
                    Err(e) => Err(e.to_string()),
                },
                Ok(None) => Err("需要提供 id 或 uuid 定位任务".to_string()),
                Err(e) => Err(e.to_string()),
            },
        };

        match outcome {
            Ok((id, uuid)) => {
                applied += 1;
                results.push(BatchOpResult {
                    index,
                    action: action.as_str().to_string(),
                    ok: true,
                    id,
                    uuid,
                    error: None,
                });
            }
            Err(err) => {
                failed += 1;
                results.push(BatchOpResult {
                    index,
                    action: action.as_str().to_string(),
                    ok: false,
                    id: None,
                    uuid: None,
                    error: Some(err),
                });
            }
        }
    }

    Ok(BatchResponse { applied, failed, results })
}
