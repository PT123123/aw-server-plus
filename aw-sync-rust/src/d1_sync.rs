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

use std::net::SocketAddr;
use std::path::Path;
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

use chrono::Utc;
use log::info;
use reqwest::blocking::Client;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::models::SyncConfig;
use crate::serialize::{
    ImportOutcome, InboxExport, NoteRow, RelationRow, TodoExport, TodoRow,
    export_inbox, export_todo, import_inbox, import_todo,
};

// ---------------------------------------------------------------------------
// DNS 韧性层（自 128c659 合并回，按平台限定作用域）
//
// 全平台：getaddrinfo 重试×3 + 60s 进程级缓存 —— DNS 抖动/污染两端都受益。
// 仅 Android（cfg）：强制 IPv4 + Cloudflare anycast 兜底池 + TCP 探测。
//   部分安卓设备的 IPv6 路径到 Cloudflare 不稳定（TCP 握手后 "error sending
//   request"），且设备 DNS 间歇性返回 EAI_NODATA；桌面网络环境没有这些问题，
//   且直连 IP 探测可能被防火墙/代理策略拦截（661c478 之前桌面翻车的根因），
//   因此只在 Android 上启用。
// ---------------------------------------------------------------------------

/// 上次探测成功的 IPv4（进程级缓存），60 秒内复用，避开抖动的 DNS。
/// TTL 短：网络抖动时缓存的地址可能很快失效，宁可重探测。
fn last_good_v4() -> &'static Mutex<Option<(Instant, SocketAddr)>> {
    static CACHE: OnceLock<Mutex<Option<(Instant, SocketAddr)>>> = OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(None))
}

/// Cloudflare anycast IPv4 兜底池：DNS 失败/结果不可用时参与候选
/// （anycast 边缘节点长期稳定；1.1.1.1 不在此列——部分网络对其单独阻断 TCP）。
#[cfg(target_os = "android")]
const CF_FALLBACK_V4: &[&str] = &[
    "104.19.193.29",
    "104.19.192.175",
    "104.19.192.29",
    "198.41.200.13",
    "172.64.32.115",
];

/// 把域名解析到 IPv4 地址。如果彻底失败返回 None（调用方回退到系统默认解析）。
fn resolve_ipv4(host: &str) -> Option<SocketAddr> {
    // 0) 命中 60 秒内的缓存直接用
    if let Some((at, addr)) = *last_good_v4().lock().unwrap() {
        if at.elapsed() < Duration::from_secs(60) {
            crate::dbglog::info(format!("[d1] resolve_ipv4({host}) cache hit = {addr}"));
            return Some(addr);
        }
    }

    // 1) getaddrinfo 重试 3 次，过滤 A 记录（设备 DNS 间歇性 EAI_NODATA）
    let mut v4: Vec<SocketAddr> = Vec::new();
    for attempt in 0..3u32 {
        match std::net::ToSocketAddrs::to_socket_addrs(&(host, 443)) {
            Ok(iter) => {
                v4 = iter.filter(|a| a.is_ipv4()).collect();
                if !v4.is_empty() {
                    crate::dbglog::info(format!(
                        "[d1] resolve_ipv4({host}) dns attempt {attempt} ok, v4={v4:?}"
                    ));
                    break;
                }
                crate::dbglog::info(format!(
                    "[d1] resolve_ipv4({host}) dns attempt {attempt}: no A record"
                ));
            }
            Err(e) => {
                crate::dbglog::info(format!(
                    "[d1] resolve_ipv4({host}) dns attempt {attempt} failed: {e}"
                ));
            }
        }
        if attempt < 2 {
            std::thread::sleep(Duration::from_millis(300));
        }
    }
    if v4.is_empty() {
        return None;
    }

    // 2) 仅 Android：合并 anycast 兜底池（去重）+ TCP 探测选路。
    //    桌面直接返回首个 DNS 结果，不做直连 IP 探测。
    #[cfg(target_os = "android")]
    {
        if host == "api.cloudflare.com" {
            for s in CF_FALLBACK_V4 {
                // 注意：只能按 Ipv4Addr 解析再拼端口 443；直接 parse::<SocketAddr>()
                // 要求 "ip:port" 格式，裸 IP 会静默失败导致兜底失效
                if let Ok(ip) = s.parse::<std::net::Ipv4Addr>() {
                    let addr = SocketAddr::new(std::net::IpAddr::V4(ip), 443);
                    if !v4.contains(&addr) {
                        v4.push(addr);
                    }
                }
            }
        }

        // 带超时的 TCP 探测（2s），选一个真正能通的 IPv4。
        // 注意不能用无超时的 TcpStream::connect((host, 443))：getaddrinfo 可能先
        // 返回 IPv6 地址，在 IPv6 黑洞的设备上探测本身会挂起 ~2 分钟。
        for a in &v4 {
            match std::net::TcpStream::connect_timeout(a, Duration::from_secs(2)) {
                Ok(_) => {
                    *last_good_v4().lock().unwrap() = Some((Instant::now(), *a));
                    return Some(*a);
                }
                Err(e) => crate::dbglog::info(format!("[d1] probe {a} failed: {e}")),
            }
        }
        // 探测全失败也可能只是瞬时丢包：返回第一个候选让 reqwest 自己再试
        // （有 15s 连接超时 + 30s 总超时兜底），同时清掉可能过期的缓存。
        *last_good_v4().lock().unwrap() = None;
        crate::dbglog::info(format!("[d1] resolve_ipv4({host}) 全部探测失败，返回首个候选",));
        return v4.first().copied();
    }

    // 非 Android：DNS 重试已够，缓存首个 A 记录后返回
    #[cfg(not(target_os = "android"))]
    {
        *last_good_v4().lock().unwrap() = Some((Instant::now(), v4[0]));
        Some(v4[0])
    }
}

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

/// D1 同步结果（供 endpoints.rs 序列化返回给前端）
#[derive(Debug, Default, Serialize)]
pub struct D1SyncResult {
    pub ok: bool,
    pub pushed_notes: usize,
    pub pushed_todos: usize,
    pub pulled_notes: usize,
    pub pulled_todos: usize,
    pub conflicts: usize,
    pub errors: Vec<String>,
}

/// D1 连接测试结果（前端「测试连接」按钮）
#[derive(Debug, Serialize)]
pub struct D1TestResult {
    pub ok: bool,
    pub message: String,
}

/// D1 同步状态（前端状态页展示）
#[derive(Debug, Serialize)]
pub struct D1Status {
    pub enabled: bool,
    pub configured: bool,
    pub last_sync: Option<String>,
}

impl D1Client {
    /// 创建 D1 客户端
    pub fn new(account_id: String, database_id: String, api_token: String, device_id: String) -> Result<Self, String> {
        if account_id.trim().is_empty() {
            return Err("D1 Account ID 为空".to_string());
        }
        if database_id.trim().is_empty() {
            return Err("D1 Database ID 为空".to_string());
        }
        if api_token.trim().is_empty() {
            return Err("D1 API Token 为空".to_string());
        }
        let base = Client::builder()
            .timeout(Duration::from_secs(30))
            .connect_timeout(Duration::from_secs(15));

        // 全平台：用 resolve_ipv4() 预解析（带 DNS 重试 + 60s 缓存）并绑定结果。
        // 仅 Android：叠加 anycast 兜底/TCP 探测（在 resolve_ipv4 内部）+ 强制本地
        // IPv4 绑定——即使对端固定到 IPv4，socket 仍可能绑定到 IPv6 wildcard
        // [::]:0（双栈设备默认），导致内核走 IPv6 路由；显式绑定 0.0.0.0:0 确保
        // socket 族为 AF_INET。桌面不做这些（直连 IP 探测在部分桌面网络会被拦截）。
        #[cfg(target_os = "android")]
        let base = match resolve_ipv4("api.cloudflare.com") {
            Some(addr) => {
                crate::dbglog::info(format!("[d1] 强制 IPv4: api.cloudflare.com -> {addr}"));
                base.resolve("api.cloudflare.com", addr).local_address(Some(
                    std::net::IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED),
                ))
            }
            None => base,
        };

        #[cfg(not(target_os = "android"))]
        let base = match resolve_ipv4("api.cloudflare.com") {
            Some(addr) => {
                crate::dbglog::info(format!("[d1] 预解析: api.cloudflare.com -> {addr}"));
                base.resolve("api.cloudflare.com", addr)
            }
            None => base,
        };

        let http = base
            .build()
            .map_err(|e| format!("构建 HTTP 客户端失败: {e}"))?;
        Ok(Self {
            http,
            account_id: account_id.trim().to_string(),
            database_id: database_id.trim().to_string(),
            api_token: api_token.trim().to_string(),
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

    /// 把 reqwest 错误的整条 source 链拼成可读字符串（连接超时/TLS/DNS 各是一层）。
    fn error_chain(e: &reqwest::Error) -> String {
        let mut msg = format!("{e}");
        let mut src: Option<&dyn std::error::Error> = std::error::Error::source(e);
        while let Some(s) = src {
            msg.push_str(&format!(" <- {s}"));
            src = s.source();
        }
        msg
    }

    fn do_request(&self, url: &str, body: Value) -> Result<D1Response, String> {
        let started = Instant::now();
        let resp = self.http
            .post(url)
            .header("Authorization", format!("Bearer {}", self.api_token))
            .header("Content-Type", "application/json")
            .json(&body)
            .send()
            .map_err(|e| {
                crate::dbglog::info(format!(
                    "[d1] send() 失败 (耗时 {:.1}s): {}",
                    started.elapsed().as_secs_f32(),
                    Self::error_chain(&e)
                ));
                // 请求失败时清掉进程级 DNS 缓存，下次同步重新探测选路
                *last_good_v4().lock().unwrap() = None;
                format!("D1 请求失败: {}", Self::error_chain(&e))
            })?;

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
        let started = Instant::now();
        let resp = self.http
            .post(url)
            .header("Authorization", format!("Bearer {}", self.api_token))
            .header("Content-Type", "application/json")
            .json(&body)
            .send()
            .map_err(|e| {
                crate::dbglog::info(format!(
                    "[d1] raw send() 失败 (耗时 {:.1}s): {}",
                    started.elapsed().as_secs_f32(),
                    Self::error_chain(&e)
                ));
                // 请求失败时清掉进程级 DNS 缓存，下次同步重新探测选路
                *last_good_v4().lock().unwrap() = None;
                format!("D1 请求失败: {}", Self::error_chain(&e))
            })?;

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
        ok: true,
        pushed_notes,
        pushed_todos,
        pulled_notes: inbox_outcome.created + inbox_outcome.updated + inbox_outcome.deleted,
        pulled_todos: todo_outcome.created + todo_outcome.updated + todo_outcome.deleted,
        conflicts,
        errors: Vec::new(),
    })
}

// ---------------------------------------------------------------------------
// 公共 API（manager.rs / endpoints.rs 消费）
// ---------------------------------------------------------------------------

/// 测试 D1 连接（前端「测试连接」按钮）。每次尝试都新建客户端（重新解析/选路）。
pub fn d1_test(cfg: &SyncConfig) -> Result<D1TestResult, String> {
    crate::dbglog::info(format!(
        "[d1] d1_test() 被调用, account={}, db={}, token_len={}",
        cfg.d1_account_id, cfg.d1_database_id, cfg.d1_api_token.len()
    ));
    if cfg.d1_account_id.trim().is_empty()
        || cfg.d1_database_id.trim().is_empty()
        || cfg.d1_api_token.trim().is_empty()
    {
        return Ok(D1TestResult {
            ok: false,
            message: "请填写 Account ID / Database ID / API Token".to_string(),
        });
    }
    // 网络抖动时给第二次机会
    let attempt = || {
        D1Client::new(
            cfg.d1_account_id.trim().to_string(),
            cfg.d1_database_id.trim().to_string(),
            cfg.d1_api_token.trim().to_string(),
            "connectivity-test".to_string(),
        )
        .and_then(|client| {
            client
                .query("SELECT 1")
                .map(|_| ())
                .map_err(|e| format!("D1 查询失败: {e}"))
        })
    };
    match attempt().or_else(|_| attempt()) {
        Ok(()) => {
            crate::dbglog::info("[d1] d1_test() 连接成功".to_string());
            Ok(D1TestResult {
                ok: true,
                message: "D1 连接成功".to_string(),
            })
        }
        Err(e) => Ok(D1TestResult {
            ok: false,
            message: format!("连接失败: {e}"),
        }),
    }
}

/// 获取 D1 同步状态（前端状态页刷新）。
pub fn d1_status(cfg: &SyncConfig, last_sync: Option<String>) -> D1Status {
    D1Status {
        enabled: cfg.d1_enabled,
        configured: !cfg.d1_account_id.trim().is_empty()
            && !cfg.d1_database_id.trim().is_empty()
            && !cfg.d1_api_token.trim().is_empty(),
        last_sync,
    }
}

/// 触发一次 D1 双向同步（manager 自动同步 + 前端「立即同步」共用入口）。
pub fn d1_sync_now(
    data_dir: &Path,
    self_id: &str,
    cfg: &SyncConfig,
) -> Result<D1SyncResult, String> {
    let result = sync_d1(
        data_dir,
        &cfg.d1_account_id,
        &cfg.d1_database_id,
        &cfg.d1_api_token,
        self_id,
    )?;
    crate::dbglog::info(format!(
        "[d1] 同步完成: ok={}, pushed={}n/{}t, pulled={}n/{}t, conflicts={}",
        result.ok,
        result.pushed_notes,
        result.pushed_todos,
        result.pulled_notes,
        result.pulled_todos,
        result.conflicts
    ));
    Ok(result)
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
