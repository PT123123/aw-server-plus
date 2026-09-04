//! Cloudflare D1 云同步模块
//!
//! 通过 D1 REST API 实现 inbox/TODO 数据的云端同步。
//! 使用 D1 作为中心节点，多设备通过 D1 间接同步。
//!
//! D1 REST API:
//!   POST /accounts/{account_id}/d1/database/{database_id}/query
//!   POST /accounts/{account_id}/d1/database/{database_id}/raw
//!
//! 请求体: { "sql": "...", "params": [...] }
//! 响应: { "success": true, "results": [...], "meta": {...} }

use std::path::Path;

use chrono::Utc;
use log::info;
use reqwest::blocking::Client;
use serde::Deserialize;
use serde_json::Value;

use crate::serialize::{
    ImportOutcome, InboxExport, NoteRow, RelationRow, TodoExport, TodoRow,
    export_inbox, export_todo, import_inbox, import_todo,
};

/// D1 云同步客户端
#[derive(Clone)]
pub struct D1Client {
    http: Client,
    account_id: String,
    database_id: String,
    api_token: String,
    device_id: String,
}

/// D1 API 查询响应（/query 端点）
#[derive(Debug, Deserialize)]
pub struct D1Response {
    pub success: bool,
    #[serde(default)]
    pub results: Vec<Value>,
    #[serde(default)]
    pub meta: Option<D1Meta>,
    #[serde(default)]
    pub errors: Vec<D1Error>,
}

/// D1 API 原始查询响应（/raw 端点）
#[derive(Debug, Deserialize)]
pub struct D1RawResponse {
    pub success: bool,
    #[serde(default)]
    pub results: D1RawResults,
    #[serde(default)]
    pub meta: Option<D1Meta>,
    #[serde(default)]
    pub errors: Vec<D1Error>,
}

#[derive(Debug, Default, Deserialize)]
pub struct D1RawResults {
    #[serde(default)]
    pub columns: Vec<String>,
    #[serde(default)]
    pub rows: Vec<Vec<Value>>,
}

#[derive(Debug, Deserialize)]
pub struct D1Meta {
    #[serde(default)]
    pub changes: i64,
    #[serde(default)]
    pub last_row_id: i64,
    #[serde(default)]
    pub rows_read: i64,
    #[serde(default)]
    pub rows_written: i64,
    #[serde(default)]
    pub duration: f64,
}

#[derive(Debug, Deserialize)]
pub struct D1Error {
    pub code: Option<i64>,
    pub message: Option<String>,
}

/// D1 同步结果
#[derive(Debug, Default)]
pub struct D1SyncResult {
    pub pushed_notes: usize,
    pub pushed_todos: usize,
    pub pulled_notes: usize,
    pub pulled_todos: usize,
    pub conflicts: usize,
    pub errors: Vec<String>,
}

impl D1Client {
    /// 创建 D1 客户端
    pub fn new(account_id: String, database_id: String, api_token: String, device_id: String) -> Result<Self, String> {
        let http = Client::builder()
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .map_err(|e| format!("构建 HTTP 客户端失败: {e}"))?;
        Ok(Self {
            http,
            account_id,
            database_id,
            api_token,
            device_id,
        })
    }

    /// 基础 URL
    fn base_url(&self) -> String {
        format!(
            "https://api.cloudflare.com/client/v4/accounts/{}/d1/database/{}",
            self.account_id, self.database_id
        )
    }

    /// 执行 SQL 查询（返回 JSON 数组）
    pub fn query(&self, sql: &str) -> Result<D1Response, String> {
        let url = format!("{}/query", self.base_url());
        let body = serde_json::json!({ "sql": sql, "params": [] });
        self.do_request(&url, body)
    }

    /// 执行 SQL 查询（返回原始行列格式）
    pub fn raw(&self, sql: &str) -> Result<D1RawResponse, String> {
        let url = format!("{}/raw", self.base_url());
        let body = serde_json::json!({ "sql": sql, "params": [] });
        self.do_raw_request(&url, body)
    }

    /// 执行批量 SQL（多条语句，无参数）
    pub fn batch(&self, sql: &str) -> Result<D1Response, String> {
        let url = format!("{}/batch", self.base_url());
        let body = serde_json::json!({ "sql": sql, "params": [] });
        self.do_request(&url, body)
    }

    /// 执行无参数 SQL（简化版）
    pub fn execute(&self, sql: &str) -> Result<D1Response, String> {
        self.query(sql)
    }

    fn do_request(&self, url: &str, body: Value) -> Result<D1Response, String> {
        let resp = self.http
            .post(url)
            .header("Authorization", format!("Bearer {}", self.api_token))
            .header("Content-Type", "application/json")
            .json(&body)
            .send()
            .map_err(|e| format!("D1 请求失败: {e}"))?;

        if !resp.status().is_success() {
            let status = resp.status();
            let text = resp.text().unwrap_or_default();
            return Err(format!("D1 API 错误: HTTP {status} - {text}"));
        }

        let json: D1Response = resp.json().map_err(|e| format!("解析 D1 响应失败: {e}"))?;
        if !json.success {
            let errs: Vec<String> = json.errors.iter()
                .map(|e| e.message.clone().unwrap_or_default())
                .collect();
            return Err(format!("D1 查询失败: {}", errs.join("; ")));
        }
        Ok(json)
    }

    fn do_raw_request(&self, url: &str, body: Value) -> Result<D1RawResponse, String> {
        let resp = self.http
            .post(url)
            .header("Authorization", format!("Bearer {}", self.api_token))
            .header("Content-Type", "application/json")
            .json(&body)
            .send()
            .map_err(|e| format!("D1 请求失败: {e}"))?;

        if !resp.status().is_success() {
            let status = resp.status();
            let text = resp.text().unwrap_or_default();
            return Err(format!("D1 API 错误: HTTP {status} - {text}"));
        }

        let json: D1RawResponse = resp.json().map_err(|e| format!("解析 D1 响应失败: {e}"))?;
        if !json.success {
            let errs: Vec<String> = json.errors.iter()
                .map(|e| e.message.clone().unwrap_or_default())
                .collect();
            return Err(format!("D1 查询失败: {}", errs.join("; ")));
        }
        Ok(json)
    }

    /// 初始化 D1 数据库表结构（幂等）
    /// 注意：Cloudflare D1 没有 /batch 端点，需逐条执行 SQL。
    pub fn init_schema(&self) -> Result<(), String> {
        let statements = [
            r#"CREATE TABLE IF NOT EXISTS notes (
                uuid TEXT PRIMARY KEY,
                content TEXT NOT NULL,
                tags TEXT DEFAULT '[]',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                version INTEGER NOT NULL DEFAULT 1,
                device_id TEXT,
                deleted INTEGER NOT NULL DEFAULT 0,
                synced_at TEXT
            )"#,
            r#"CREATE TABLE IF NOT EXISTS note_relations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source_note_id INTEGER NOT NULL,
                target_note_id INTEGER NOT NULL,
                relation_type TEXT NOT NULL,
                created_at TEXT NOT NULL
            )"#,
            r#"CREATE TABLE IF NOT EXISTS todos (
                uuid TEXT PRIMARY KEY,
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
                synced_at TEXT
            )"#,
            r#"CREATE TABLE IF NOT EXISTS sync_state (
                device_id TEXT PRIMARY KEY,
                last_sync_at TEXT NOT NULL,
                device_name TEXT
            )"#,
            r#"CREATE INDEX IF NOT EXISTS idx_notes_updated ON notes(updated_at)"#,
            r#"CREATE INDEX IF NOT EXISTS idx_todos_updated ON todos(updated_at)"#,
        ];
        for sql in statements {
            self.query(sql)?;
        }
        info!("✅ D1 数据库表结构初始化完成");
        Ok(())
    }

    /// 推送本地笔记到 D1（增量：只推 updated_at > last_sync 的）
    pub fn push_notes(&self, db_path: &Path, last_sync: Option<&str>) -> Result<usize, String> {
        let json = export_inbox(db_path)?;
        let data: InboxExport = serde_json::from_str(&json)
            .map_err(|e| format!("解析 inbox 导出失败: {e}"))?;

        let mut pushed = 0;
        for n in &data.notes {
            // 增量判断：如果 last_sync 存在且记录未更新则跳过
            if let Some(ls) = last_sync {
                if n.updated_at <= ls.to_string() {
                    continue;
                }
            }
            // UPSERT 到 D1
            let sql = format!(
                "INSERT INTO notes (uuid,content,tags,created_at,updated_at,version,device_id,deleted,synced_at) \
                 VALUES ('{}','{}','{}','{}','{}',{},'{}',{},'{}') \
                 ON CONFLICT(uuid) DO UPDATE SET \
                   content=excluded.content, tags=excluded.tags, created_at=excluded.created_at, \
                   updated_at=excluded.updated_at, version=excluded.version, device_id=excluded.device_id, \
                   deleted=excluded.deleted, synced_at=excluded.synced_at \
                 WHERE excluded.updated_at > notes.updated_at OR (excluded.updated_at = notes.updated_at AND excluded.device_id > notes.device_id)",
                escape_sql(&n.uuid),
                escape_sql(&n.content),
                escape_sql(&n.tags),
                escape_sql(&n.created_at),
                escape_sql(&n.updated_at),
                n.version,
                escape_sql(n.device_id.as_deref().unwrap_or("")),
                n.deleted,
                escape_sql(n.synced_at.as_deref().unwrap_or(""))
            );
            self.execute(&sql)?;
            pushed += 1;
        }

        // 推送 relations
        for r in &data.relations {
            let sql = format!(
                "INSERT OR IGNORE INTO note_relations (source_note_id,target_note_id,relation_type,created_at) \
                 VALUES ({},{},'{}','{}')",
                r.source_note_id, r.target_note_id, escape_sql(&r.relation_type), escape_sql(&r.created_at)
            );
            self.execute(&sql)?;
        }

        info!("📤 D1 推送笔记: {pushed} 条");
        Ok(pushed)
    }

    /// 推送本地 TODO 到 D1（增量）
    pub fn push_todos(&self, db_path: &Path, last_sync: Option<&str>) -> Result<usize, String> {
        let json = export_todo(db_path)?;
        let data: TodoExport = serde_json::from_str(&json)
            .map_err(|e| format!("解析 todo 导出失败: {e}"))?;

        let mut pushed = 0;
        for t in &data.todos {
            if let Some(ls) = last_sync {
                if t.updated_at <= ls.to_string() {
                    continue;
                }
            }
            let sql = format!(
                "INSERT INTO todos (uuid,title,content,completed,priority,due_date,tags,created_at,updated_at,completed_at,version,device_id,deleted,synced_at) \
                 VALUES ('{}','{}','{}',{},{},'{}','{}','{}','{}','{}',{},'{}',{},'{}') \
                 ON CONFLICT(uuid) DO UPDATE SET \
                   title=excluded.title, content=excluded.content, completed=excluded.completed, \
                   priority=excluded.priority, due_date=excluded.due_date, tags=excluded.tags, \
                   created_at=excluded.created_at, updated_at=excluded.updated_at, \
                   completed_at=excluded.completed_at, version=excluded.version, device_id=excluded.device_id, \
                   deleted=excluded.deleted, synced_at=excluded.synced_at \
                 WHERE excluded.updated_at > todos.updated_at OR (excluded.updated_at = todos.updated_at AND excluded.device_id > todos.device_id)",
                escape_sql(&t.uuid),
                escape_sql(&t.title),
                escape_sql(t.content.as_deref().unwrap_or("")),
                t.completed,
                t.priority.unwrap_or(0),
                escape_sql(t.due_date.as_deref().unwrap_or("")),
                escape_sql(&t.tags),
                escape_sql(&t.created_at),
                escape_sql(&t.updated_at),
                escape_sql(t.completed_at.as_deref().unwrap_or("")),
                t.version,
                escape_sql(t.device_id.as_deref().unwrap_or("")),
                t.deleted,
                escape_sql(t.synced_at.as_deref().unwrap_or(""))
            );
            self.execute(&sql)?;
            pushed += 1;
        }

        info!("📤 D1 推送 TODO: {pushed} 条");
        Ok(pushed)
    }

    /// 从 D1 拉取笔记（增量：只拉 updated_at > last_sync 的）
    pub fn pull_notes(&self, db_path: &Path, last_sync: Option<&str>) -> Result<ImportOutcome, String> {
        let sql = match last_sync {
            Some(ls) => format!("SELECT uuid,content,tags,created_at,updated_at,version,device_id,deleted,synced_at FROM notes WHERE updated_at > '{}' ORDER BY updated_at", escape_sql(ls)),
            None => "SELECT uuid,content,tags,created_at,updated_at,version,device_id,deleted,synced_at FROM notes ORDER BY updated_at".to_string(),
        };

        let resp = self.raw(&sql)?;
        let mut notes = Vec::new();
        for row in &resp.results.rows {
            notes.push(NoteRow {
                id: 0,
                uuid: row.get(0).and_then(|v| v.as_str()).unwrap_or("").to_string(),
                content: row.get(1).and_then(|v| v.as_str()).unwrap_or("").to_string(),
                tags: row.get(2).and_then(|v| v.as_str()).unwrap_or("[]").to_string(),
                created_at: row.get(3).and_then(|v| v.as_str()).unwrap_or("").to_string(),
                updated_at: row.get(4).and_then(|v| v.as_str()).unwrap_or("").to_string(),
                version: row.get(5).and_then(|v| v.as_i64()).unwrap_or(1),
                device_id: row.get(6).and_then(|v| v.as_str()).map(|s| s.to_string()),
                deleted: row.get(7).and_then(|v| v.as_i64()).unwrap_or(0),
                synced_at: row.get(8).and_then(|v| v.as_str()).map(|s| s.to_string()),
            });
        }

        // 拉取 relations
        let rel_sql = match last_sync {
            Some(ls) => format!("SELECT source_note_id,target_note_id,relation_type,created_at FROM note_relations WHERE created_at > '{}'", escape_sql(ls)),
            None => "SELECT source_note_id,target_note_id,relation_type,created_at FROM note_relations".to_string(),
        };
        let rel_resp = self.raw(&rel_sql)?;
        let mut relations = Vec::new();
        for row in &rel_resp.results.rows {
            relations.push(RelationRow {
                id: 0,
                source_note_id: row.get(0).and_then(|v| v.as_i64()).unwrap_or(0),
                target_note_id: row.get(1).and_then(|v| v.as_i64()).unwrap_or(0),
                relation_type: row.get(2).and_then(|v| v.as_str()).unwrap_or("").to_string(),
                created_at: row.get(3).and_then(|v| v.as_str()).unwrap_or("").to_string(),
            });
        }

        // 构造 InboxExport 并调用 import_inbox 合并
        let inbox_export = InboxExport { notes, relations };
        let json = serde_json::to_string(&inbox_export)
            .map_err(|e| format!("序列化 inbox 失败: {e}"))?;
        let outcome = import_inbox(db_path, &json)?;

        info!("📥 D1 拉取笔记: created={} updated={} ignored_dup={} ignored_stale={} archived={}",
            outcome.created, outcome.updated, outcome.ignored_dup, outcome.ignored_stale, outcome.archived.len());
        Ok(outcome)
    }

    /// 从 D1 拉取 TODO（增量）
    pub fn pull_todos(&self, db_path: &Path, last_sync: Option<&str>) -> Result<ImportOutcome, String> {
        let sql = match last_sync {
            Some(ls) => format!("SELECT uuid,title,content,completed,priority,due_date,tags,created_at,updated_at,completed_at,version,device_id,deleted,synced_at FROM todos WHERE updated_at > '{}' ORDER BY updated_at", escape_sql(ls)),
            None => "SELECT uuid,title,content,completed,priority,due_date,tags,created_at,updated_at,completed_at,version,device_id,deleted,synced_at FROM todos ORDER BY updated_at".to_string(),
        };

        let resp = self.raw(&sql)?;
        let mut todos = Vec::new();
        for row in &resp.results.rows {
            todos.push(TodoRow {
                id: 0,
                uuid: row.get(0).and_then(|v| v.as_str()).unwrap_or("").to_string(),
                title: row.get(1).and_then(|v| v.as_str()).unwrap_or("").to_string(),
                content: row.get(2).and_then(|v| v.as_str()).map(|s| s.to_string()),
                completed: row.get(3).and_then(|v| v.as_i64()).unwrap_or(0),
                priority: row.get(4).and_then(|v| v.as_i64()),
                due_date: row.get(5).and_then(|v| v.as_str()).map(|s| s.to_string()),
                tags: row.get(6).and_then(|v| v.as_str()).unwrap_or("[]").to_string(),
                created_at: row.get(7).and_then(|v| v.as_str()).unwrap_or("").to_string(),
                updated_at: row.get(8).and_then(|v| v.as_str()).unwrap_or("").to_string(),
                completed_at: row.get(9).and_then(|v| v.as_str()).map(|s| s.to_string()),
                version: row.get(10).and_then(|v| v.as_i64()).unwrap_or(1),
                device_id: row.get(11).and_then(|v| v.as_str()).map(|s| s.to_string()),
                deleted: row.get(12).and_then(|v| v.as_i64()).unwrap_or(0),
                synced_at: row.get(13).and_then(|v| v.as_str()).map(|s| s.to_string()),
            });
        }

        let todo_export = TodoExport { todos };
        let json = serde_json::to_string(&todo_export)
            .map_err(|e| format!("序列化 todo 失败: {e}"))?;
        let outcome = import_todo(db_path, &json)?;

        info!("📥 D1 拉取 TODO: created={} updated={} ignored_dup={} ignored_stale={} archived={}",
            outcome.created, outcome.updated, outcome.ignored_dup, outcome.ignored_stale, outcome.archived.len());
        Ok(outcome)
    }

    /// 更新本机同步状态到 D1
    pub fn update_sync_state(&self) -> Result<(), String> {
        let now = Utc::now().to_rfc3339();
        let sql = format!(
            "INSERT INTO sync_state (device_id,last_sync_at,device_name) \
             VALUES ('{}','{}','{}') \
             ON CONFLICT(device_id) DO UPDATE SET last_sync_at=excluded.last_sync_at, device_name=excluded.device_name",
            escape_sql(&self.device_id),
            escape_sql(&now),
            escape_sql(&self.device_id)
        );
        self.execute(&sql)?;
        Ok(())
    }

    /// 获取本机上次同步时间
    pub fn get_last_sync(&self) -> Result<Option<String>, String> {
        let sql = format!(
            "SELECT last_sync_at FROM sync_state WHERE device_id = '{}'",
            escape_sql(&self.device_id)
        );
        let resp = self.raw(&sql)?;
        if let Some(row) = resp.results.rows.first() {
            Ok(row.get(0).and_then(|v| v.as_str()).map(|s| s.to_string()))
        } else {
            Ok(None)
        }
    }
}

/// SQL 字符串转义（单引号转义为两个单引号）
fn escape_sql(s: &str) -> String {
    s.replace('\'', "''")
}

/// 执行完整的 D1 同步周期（推 + 拉 + 更新状态）
pub fn sync_d1(
    data_dir: &Path,
    account_id: &str,
    database_id: &str,
    api_token: &str,
    device_id: &str,
) -> Result<D1SyncResult, String> {
    let client = D1Client::new(
        account_id.to_string(),
        database_id.to_string(),
        api_token.to_string(),
        device_id.to_string(),
    )?;

    // 1. 初始化表结构
    client.init_schema()?;

    // 2. 获取上次同步时间
    let last_sync = client.get_last_sync()?;

    // 3. 推送本地变更
    let inbox_path = data_dir.join("inbox.db");
    let todo_path = data_dir.join("todo.db");
    let pushed_notes = client.push_notes(&inbox_path, last_sync.as_deref())?;
    let pushed_todos = client.push_todos(&todo_path, last_sync.as_deref())?;

    // 4. 拉取远端变更
    let inbox_outcome = client.pull_notes(&inbox_path, last_sync.as_deref())?;
    let todo_outcome = client.pull_todos(&todo_path, last_sync.as_deref())?;

    // 5. 更新同步状态
    client.update_sync_state()?;

    let conflicts = inbox_outcome.archived.len() + todo_outcome.archived.len();

    Ok(D1SyncResult {
        pushed_notes,
        pushed_todos,
        pulled_notes: inbox_outcome.created + inbox_outcome.updated + inbox_outcome.deleted,
        pulled_todos: todo_outcome.created + todo_outcome.updated + todo_outcome.deleted,
        conflicts,
        errors: Vec::new(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn d1_client_new() {
        let client = D1Client::new(
            "test_account".to_string(),
            "test_db".to_string(),
            "test_token".to_string(),
            "test_device".to_string(),
        );
        assert!(client.is_ok());
    }

    #[test]
    fn base_url_format() {
        let client = D1Client::new(
            "abc123".to_string(),
            "db456".to_string(),
            "token".to_string(),
            "dev".to_string(),
        ).unwrap();
        assert_eq!(
            client.base_url(),
            "https://api.cloudflare.com/client/v4/accounts/abc123/d1/database/db456"
        );
    }

    #[test]
    fn escape_sql_escapes_quotes() {
        assert_eq!(escape_sql("it's"), "it''s");
        assert_eq!(escape_sql("no quotes"), "no quotes");
        assert_eq!(escape_sql(""), "");
    }
}
