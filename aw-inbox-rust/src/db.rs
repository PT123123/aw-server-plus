// src/db.rs
use crate::models::{
    CreateCommentPayload, CreateNotePayload, CreateNoteRelationPayload, CreateTodoPayload,
    DetailedTag, Note, NoteRelation, NoteRelationType, Todo, UpdateNotePayload, UpdateTodoPayload,
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

        "#,
    )?;

    // 兼容旧数据库：给已存在的表补上缺失的列（必须在 CREATE INDEX 之前）
    ensure_column(conn, "todos", "version", "INTEGER NOT NULL DEFAULT 1")?;
    ensure_column(conn, "todos", "device_id", "TEXT")?;
    ensure_column(conn, "todos", "deleted", "INTEGER NOT NULL DEFAULT 0")?;
    ensure_column(conn, "todos", "synced_at", "TEXT")?;
    ensure_column(conn, "todos", "uuid", "TEXT")?;
    // 为历史行补齐 uuid（P0 同步逻辑键）
    conn.execute(
        "UPDATE todos SET uuid = lower(hex(randomblob(16))) WHERE uuid IS NULL OR uuid = ''",
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

    tx.execute(
        r#"
        INSERT INTO notes (uuid, content, tags, created_at, updated_at, version, device_id, deleted, synced_at)
        VALUES (lower(hex(randomblob(16))), ?1, ?2, ?3, ?4, ?5, ?6, 0, ?7)
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
    )?;

    let id = tx.last_insert_rowid();
    tx.commit()?;

    let parsed_tags: Vec<String> = serde_json::from_str(&tags_json).map_err(map_serde_error)?;

    Ok(Note {
        id,
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
        conn.prepare("SELECT id, content, tags, created_at, updated_at, version, device_id, deleted, synced_at FROM notes WHERE id = ?1")?;
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
    created_after: Option<DateTime<Utc>>,
    created_before: Option<DateTime<Utc>>,
    search: Option<String>,
    sort_by: Option<String>,
) -> Result<Vec<Note>, Error> {
    let mut query_str =
        "SELECT id, content, tags, created_at, updated_at, version, device_id, deleted, synced_at FROM notes WHERE deleted = 0".to_string();
    let mut params_vec: Vec<Box<dyn ToSql>> = Vec::new();

    if let Some(t) = tag {
        query_str.push_str(" AND tags LIKE ?");
        params_vec.push(Box::new(format!("%\"{}\"%", t)));
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

    let mut final_query_str = String::new();
    let mut param_index = 1;
    for c in query_str.chars() {
        if c == '?' {
            final_query_str.push_str(&format!("?{}", param_index));
            param_index += 1;
        } else {
            final_query_str.push(c);
        }
    }

    let mut stmt = conn.prepare(&final_query_str)?;
    let params_ref: Vec<&dyn ToSql> = params_vec.iter().map(|b| b.as_ref()).collect();

    // *** MUST FIX THIS LINE LOCALLY: Remove '¶', use 'params_ref' ***
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

pub fn delete_note_db(conn: &mut DbConnection, note_id: i64, device_id: Option<String>) -> Result<bool, Error> {
    let updated_at = Utc::now();
    
    let tx = conn.transaction()?;
    
    // 获取并递增全局版本
    let global_version: i64 = tx.query_row(
        "UPDATE sync_versions SET global_version = global_version + 1 RETURNING global_version",
        [],
        |row| row.get(0),
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
        "SELECT n.id, n.content, n.tags, n.created_at, n.updated_at,
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

    tx.execute(
        "INSERT INTO notes (uuid, content, tags, created_at, updated_at, version, device_id, deleted, synced_at) VALUES (lower(hex(randomblob(16))), ?, ?, ?, ?, ?, ?, 0, ?)",
        params![payload.content, tags_json, created_at, updated_at, global_version, Option::<String>::None, created_at],
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
    let created_at: DateTime<Utc> = row.get("created_at")?;
    let updated_at: DateTime<Utc> = row.get("updated_at")?;
    let completed: i64 = row.get("completed")?;
    let deleted: i64 = row.get("deleted")?;

    Ok(Todo {
        id: row.get("id")?,
        title: row.get("title")?,
        content: row.get("content")?,
        completed: completed != 0,
        priority: row.get("priority")?,
        due_date: row.get("due_date")?,
        tags,
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

    let tx = conn.transaction()?;
    let global_version: i64 = tx.query_row(
        "UPDATE sync_versions SET global_version = global_version + 1 RETURNING global_version",
        [],
        |row| row.get(0),
    )?;

    tx.execute(
        r#"
        INSERT INTO todos (uuid, title, content, completed, priority, due_date, tags,
                           created_at, updated_at, completed_at, version, device_id, deleted, synced_at)
        VALUES (lower(hex(randomblob(16))), ?1, ?2, 0, ?3, ?4, ?5, ?6, ?7, NULL, ?8, ?9, 0, ?10)
        "#,
        params![
            payload.title,
            payload.content,
            payload.priority,
            payload.due_date,
            tags_json,
            created_at,
            updated_at,
            global_version,
            device_id,
            created_at,
        ],
    )?;

    let id = tx.last_insert_rowid();
    tx.commit()?;

    let parsed_tags: Vec<String> = serde_json::from_str(&tags_json).map_err(map_serde_error)?;

    Ok(Todo {
        id,
        title: payload.title,
        content: payload.content,
        completed: false,
        priority: payload.priority,
        due_date: payload.due_date,
        tags: parsed_tags,
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
    limit: Option<i64>,
    offset: Option<i64>,
) -> Result<Vec<Todo>, Error> {
    let mut sql = String::from(
        "SELECT id, title, content, completed, priority, due_date, tags,
                created_at, updated_at, completed_at, version, device_id, deleted, synced_at
         FROM todos WHERE deleted = 0",
    );
    if let Some(c) = completed {
        sql.push_str(&format!(" AND completed = {}", if c { 1 } else { 0 }));
    }
    sql.push_str(" ORDER BY completed ASC, priority DESC NULLS LAST, created_at DESC");
    if let Some(l) = limit {
        sql.push_str(&format!(" LIMIT {}", l));
    }
    if let Some(o) = offset {
        sql.push_str(&format!(" OFFSET {}", o));
    }

    let mut stmt = conn.prepare(&sql)?;
    let todos_iter = stmt.query_map([], map_row_to_todo)?;
    let mut todos = Vec::new();
    for todo_result in todos_iter {
        todos.push(todo_result?);
    }
    Ok(todos)
}

pub fn get_todo_by_id_db(conn: &DbConnection, todo_id: i64) -> Result<Todo, Error> {
    let todo = conn.query_row(
        "SELECT id, title, content, completed, priority, due_date, tags,
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
            due_date = ?5, tags = ?6, updated_at = ?7, completed_at = ?8, version = ?9
        WHERE id = ?10
        "#,
        params![
            title,
            content,
            if completed { 1 } else { 0 },
            priority,
            due_date,
            tags_json,
            updated_at,
            completed_at,
            global_version,
            todo_id,
        ],
    )?;
    tx.commit()?;

    Ok(Todo {
        id: todo_id,
        title,
        content,
        completed,
        priority,
        due_date,
        tags,
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
