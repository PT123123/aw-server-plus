//! Cloudflare D1 云同步。
//!
//! 通过 Cloudflare D1 REST API（https://developers.cloudflare.com/d1/）实现笔记/TODO
//! 的双向同步。Rust 后端直接调用 D1 HTTP 端点，用 API Token 鉴权。
//!
//! 同步策略：
//! - Push（本地 → D1）：本地有更新（或 D1 不存在）时，INSERT OR REPLACE 推送到 D1。
//! - Pull（D1 → 本地）：D1 有更新（或本地不存在）时，收集后走 import_inbox/import_todo
//!   做 LWW 合并 + 回收站归档（复用局域网同步的冲突仲裁逻辑）。
//!
//! 冲突仲裁：rev = (updated_at 毫秒, device_id) 字典序，复用 conflict::incoming_newer。

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use chrono::Utc;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::conflict::incoming_newer;
use crate::models::SyncConfig;
use crate::serialize::{export_inbox, export_todo, import_inbox, import_todo, InboxExport, TodoExport};

// ---------------------------------------------------------------------------
// 强制 IPv4 的 DNS 解析器：部分 Android 设备的 IPv6 路径到 Cloudflare 不稳定，
// TCP 握手后发送请求会失败（"error sending request"）。只返回 A 记录可避免此问题。
// 实测设备 DNS 还有另一毛病：对 api.cloudflare.com 间歇性返回 EAI_NODATA
// （DNS 污染/抖动），单次 getaddrinfo 靠不住 → 重试 + 进程级缓存 + anycast 兜底。
// ---------------------------------------------------------------------------

/// 上次探测成功的 IPv4（进程级缓存），60 秒内复用，避开抖动的 DNS。
/// TTL 短：网络抖动时缓存的地址可能很快失效，宁可重探测。
fn last_good_v4() -> &'static Mutex<Option<(std::time::Instant, SocketAddr)>> {
    static CACHE: std::sync::OnceLock<Mutex<Option<(std::time::Instant, SocketAddr)>>> =
        std::sync::OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(None))
}

/// Cloudflare anycast IPv4 兜底池：DNS 失败/结果不可用时参与候选
/// （anycast 边缘节点长期稳定；1.1.1.1 不在此列——部分网络对其单独阻断 TCP）。
const CF_FALLBACK_V4: &[&str] = &[
    "104.19.193.29",
    "104.19.192.175",
    "104.19.192.29",
    "198.41.200.13",
    "172.64.32.115",
];

/// 把域名解析到 IPv4 地址。如果彻底失败返回 None（调用方应回退到默认行为）。
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

    // 2) 候选合并：DNS 结果 + anycast 兜底池（去重）。
    //    DNS 失败时兜底仍可用；DNS 成功时兜底作为额外候选参与探测。
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
    if v4.is_empty() {
        return None;
    }

    // 3) 带超时的 TCP 探测（2s），选一个真正能通的 IPv4。
    //    注意不能用无超时的 TcpStream::connect((host, 443))：getaddrinfo 可能先返回
    //    IPv6 地址，在 IPv6 黑洞的设备上探测本身会挂起 ~2 分钟。
    for a in &v4 {
        match std::net::TcpStream::connect_timeout(a, Duration::from_secs(2)) {
            Ok(_) => {
                *last_good_v4().lock().unwrap() = Some((std::time::Instant::now(), *a));
                return Some(*a);
            }
            Err(e) => crate::dbglog::info(format!("[d1] probe {a} failed: {e}")),
        }
    }
    // 4) 探测全失败也可能只是瞬时丢包：返回第一个候选让 reqwest 自己再试
    //    （有 15s 连接超时 + 30s 总超时兜底），同时清掉可能过期的缓存。
    *last_good_v4().lock().unwrap() = None;
    crate::dbglog::info(format!("[d1] resolve_ipv4({host}) 全部探测失败，返回首个候选",));
    v4.first().copied()
}

// ---------------------------------------------------------------------------
// D1 REST API 响应结构
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
struct D1Response {
    success: bool,
    #[serde(default)]
    errors: Vec<D1Error>,
    #[serde(default)]
    result: Vec<D1QueryResult>,
}

#[derive(Debug, Deserialize)]
struct D1QueryResult {
    #[serde(default)]
    success: bool,
    #[serde(default)]
    results: Vec<Value>,
    #[serde(default)]
    #[allow(dead_code)]
    meta: Option<D1Meta>,
}

#[derive(Debug, Deserialize)]
struct D1Meta {
    #[serde(default)]
    #[allow(dead_code)]
    changes: i64,
    #[serde(default)]
    #[allow(dead_code)]
    last_row_id: i64,
}

#[derive(Debug, Deserialize)]
struct D1Error {
    #[allow(dead_code)]
    code: Option<i64>,
    message: Option<String>,
}

// ---------------------------------------------------------------------------
// 响应类型（供 endpoints.rs 序列化返回给前端）
// ---------------------------------------------------------------------------

#[derive(Debug, Serialize)]
pub struct D1TestResult {
    pub ok: bool,
    pub message: String,
}

#[derive(Debug, Serialize)]
pub struct D1Status {
    pub enabled: bool,
    pub configured: bool,
    pub last_sync: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct D1SyncResult {
    pub ok: bool,
    #[serde(rename = "pushed_notes")]
    pub pushed_notes: usize,
    #[serde(rename = "pushed_todos")]
    pub pushed_todos: usize,
    #[serde(rename = "pulled_notes")]
    pub pulled_notes: usize,
    #[serde(rename = "pulled_todos")]
    pub pulled_todos: usize,
    pub conflicts: usize,
    #[serde(default)]
    pub errors: Vec<String>,
}

impl Default for D1SyncResult {
    fn default() -> Self {
        D1SyncResult {
            ok: true,
            pushed_notes: 0,
            pushed_todos: 0,
            pulled_notes: 0,
            pulled_todos: 0,
            conflicts: 0,
            errors: Vec::new(),
        }
    }
}

// ---------------------------------------------------------------------------
// D1 客户端
// ---------------------------------------------------------------------------

pub struct D1Client {
    account_id: String,
    database_id: String,
    api_token: String,
    client: reqwest::blocking::Client,
}

impl D1Client {
    pub fn new(account_id: &str, database_id: &str, api_token: &str) -> Result<Self, String> {
        crate::dbglog::info(format!("[d1] D1Client::new() 被调用, account={account_id}"));
        if account_id.trim().is_empty() {
            return Err("D1 Account ID 为空".to_string());
        }
        if database_id.trim().is_empty() {
            return Err("D1 Database ID 为空".to_string());
        }
        if api_token.trim().is_empty() {
            return Err("D1 API Token 为空".to_string());
        }
        // 强制 IPv4：部分 Android 设备/运营商的 IPv6 路径到 Cloudflare 不稳定，
        // TCP 握手后发送请求会失败（"error sending request"）。
        // 用 resolve() 把 Cloudflare 域名绑定到 IPv4 地址，跳过 IPv6。
        let mut builder = reqwest::blocking::Client::builder()
            .timeout(Duration::from_secs(30))
            .connect_timeout(Duration::from_secs(15))
            // 强制本地绑定 IPv4：即使 resolve() 把对端固定到 IPv4，socket 仍可能
            // 绑定到 IPv6 wildcard [::]:0（双栈设备默认），导致内核走 IPv6 路由。
            // 显式绑定 0.0.0.0:0 确保 socket 族为 AF_INET。
            .local_address(Some(std::net::IpAddr::V4(
                std::net::Ipv4Addr::UNSPECIFIED
            )));
        if let Some(addr) = resolve_ipv4("api.cloudflare.com") {
            builder = builder.resolve("api.cloudflare.com", addr);
            crate::dbglog::info(format!("[d1] 强制 IPv4: api.cloudflare.com -> {addr}"));
        }
        let client = builder
            .build()
            .map_err(|e| format!("构建 HTTP 客户端失败: {e}"))?;
        Ok(Self {
            account_id: account_id.trim().to_string(),
            database_id: database_id.trim().to_string(),
            api_token: api_token.trim().to_string(),
            client,
        })
    }

    /// 构造 D1 REST API 端点 URL。
    fn url(&self, action: &str) -> String {
        format!(
            "https://api.cloudflare.com/client/v4/accounts/{}/d1/database/{}/{action}",
            self.account_id, self.database_id
        )
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

    /// 发送请求并解析 D1 响应，返回 result[0].results（行对象数组）。
    fn request(&self, sql: &str, params: &[Value]) -> Result<Vec<Value>, String> {
        let body = if params.is_empty() {
            json!({ "sql": sql })
        } else {
            json!({ "sql": sql, "params": params })
        };
        let started = std::time::Instant::now();
        let resp = self
            .client
            .post(&self.url("query"))
            .header("Authorization", format!("Bearer {}", self.api_token))
            .header("Content-Type", "application/json")
            .json(&body)
            .send()
            .map_err(|e| {
                let chain = Self::error_chain(&e);
                crate::dbglog::info(format!(
                    "[d1] send() 失败 (耗时 {:.1}s): {chain}",
                    started.elapsed().as_secs_f32()
                ));
                format!("D1 请求失败: {chain}")
            })?;

        let status = resp.status();
        if !status.is_success() {
            let text = resp.text().unwrap_or_default();
            return Err(format!("D1 返回 HTTP {status}: {text}"));
        }

        let d1_resp: D1Response =
            resp.json().map_err(|e| format!("解析 D1 响应失败: {e}"))?;

        if !d1_resp.success {
            let msg = d1_resp
                .errors
                .iter()
                .filter_map(|e| e.message.clone())
                .collect::<Vec<_>>()
                .join("; ");
            return Err(if msg.is_empty() {
                "D1 返回 success=false".to_string()
            } else {
                msg
            });
        }

        Ok(d1_resp
            .result
            .into_iter()
            .next()
            .map(|r| r.results)
            .unwrap_or_default())
    }

    /// 执行查询，返回结果行（JSON 对象数组）。
    pub fn query(&self, sql: &str, params: &[Value]) -> Result<Vec<Value>, String> {
        self.request(sql, params)
    }

    /// 执行写 SQL（INSERT / UPDATE / CREATE TABLE 等）。
    pub fn execute(&self, sql: &str, params: &[Value]) -> Result<(), String> {
        self.request(sql, params)?;
        Ok(())
    }

    /// 幂等创建 notes / todos 表（schema 对齐本地 inbox.db / todo.db）。
    pub fn ensure_schema(&self) -> Result<(), String> {
        self.execute(
            "CREATE TABLE IF NOT EXISTS notes (
                uuid TEXT PRIMARY KEY,
                content TEXT NOT NULL DEFAULT '',
                tags TEXT NOT NULL DEFAULT '[]',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                version INTEGER NOT NULL DEFAULT 1,
                device_id TEXT,
                deleted INTEGER NOT NULL DEFAULT 0,
                synced_at TEXT
            )",
            &[],
        )?;
        self.execute(
            "CREATE TABLE IF NOT EXISTS todos (
                uuid TEXT PRIMARY KEY,
                title TEXT NOT NULL,
                content TEXT,
                completed INTEGER NOT NULL DEFAULT 0,
                priority INTEGER,
                due_date TEXT,
                tags TEXT NOT NULL DEFAULT '[]',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                completed_at TEXT,
                version INTEGER NOT NULL DEFAULT 1,
                device_id TEXT,
                deleted INTEGER NOT NULL DEFAULT 0,
                synced_at TEXT
            )",
            &[],
        )?;
        crate::dbglog::info("[d1] D1 表结构已就绪 (notes, todos)".to_string());
        Ok(())
    }

    /// 测试 D1 连通性：执行 SELECT 1。
    pub fn test_connection(&self) -> Result<(), String> {
        self.execute("SELECT 1", &[])
    }
}

// ---------------------------------------------------------------------------
// 从 D1 行 Value 解析出 (updated_at, device_id) 用于 LWW 比对
// ---------------------------------------------------------------------------

fn rev_from_row(row: &Value) -> (String, Option<String>) {
    let ts = row
        .get("updated_at")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let dev = row
        .get("device_id")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());
    (ts, dev)
}

// ---------------------------------------------------------------------------
// 公开接口
// ---------------------------------------------------------------------------

/// 测试 D1 连接（前端「测试连接」按钮）。
pub fn d1_test(cfg: &SyncConfig) -> Result<D1TestResult, String> {
    crate::dbglog::info(format!("[d1] d1_test() 被调用, account={}, db={}, token_len={}",
        cfg.d1_account_id, cfg.d1_database_id, cfg.d1_api_token.len()));
    if cfg.d1_account_id.trim().is_empty()
        || cfg.d1_database_id.trim().is_empty()
        || cfg.d1_api_token.trim().is_empty()
    {
        crate::dbglog::info("[d1] d1_test() 返回: 参数为空".to_string());
        return Ok(D1TestResult {
            ok: false,
            message: "请填写 Account ID / Database ID / API Token".to_string(),
        });
    }
    // 每次尝试都新建客户端（重新解析/选路）；网络抖动时给第二次机会
    let attempt = || {
        D1Client::new(&cfg.d1_account_id, &cfg.d1_database_id, &cfg.d1_api_token)
            .and_then(|client| client.test_connection())
    };
    match attempt().or_else(|_| attempt()) {
        Ok(()) => Ok(D1TestResult {
            ok: true,
            message: "D1 连接成功".to_string(),
        }),
        Err(e) => Ok(D1TestResult {
            ok: false,
            message: format!("连接失败: {e}"),
        }),
    }
}

/// 获取 D1 同步状态（前端 onResume 刷新）。
pub fn d1_status(cfg: &SyncConfig, last_sync: Option<String>) -> D1Status {
    D1Status {
        enabled: cfg.d1_enabled,
        configured: !cfg.d1_account_id.trim().is_empty()
            && !cfg.d1_database_id.trim().is_empty()
            && !cfg.d1_api_token.trim().is_empty(),
        last_sync,
    }
}

/// 触发一次 D1 双向同步（前端「立即同步」按钮）。
pub fn d1_sync_now(
    data_dir: &Path,
    self_id: &str,
    cfg: &SyncConfig,
) -> Result<D1SyncResult, String> {
    let client = D1Client::new(&cfg.d1_account_id, &cfg.d1_database_id, &cfg.d1_api_token)?;
    client.ensure_schema()?;

    let mut result = D1SyncResult::default();

    // Push：本地 → D1
    if let Err(e) = push_inbox(&client, data_dir, self_id, &mut result) {
        result.errors.push(format!("推送笔记失败: {e}"));
    }
    if let Err(e) = push_todo(&client, data_dir, self_id, &mut result) {
        result.errors.push(format!("推送 TODO 失败: {e}"));
    }

    // Pull：D1 → 本地
    if let Err(e) = pull_inbox(&client, data_dir, self_id, &mut result) {
        result.errors.push(format!("拉取笔记失败: {e}"));
    }
    if let Err(e) = pull_todo(&client, data_dir, self_id, &mut result) {
        result.errors.push(format!("拉取 TODO 失败: {e}"));
    }

    result.ok = result.errors.is_empty();
    crate::dbglog::info(format!(
        "[d1] 同步完成: ok={}, pushed={}n/{}t, pulled={}n/{}t, conflicts={}, errors={}",
        result.ok,
        result.pushed_notes,
        result.pushed_todos,
        result.pulled_notes,
        result.pulled_todos,
        result.conflicts,
        result.errors.len()
    ));
    Ok(result)
}

// ---------------------------------------------------------------------------
// Push：本地 → D1
// ---------------------------------------------------------------------------

fn push_inbox(
    client: &D1Client,
    data_dir: &Path,
    self_id: &str,
    result: &mut D1SyncResult,
) -> Result<(), String> {
    // 1. 读取 D1 全部 notes 的 (uuid, updated_at, device_id) 建索引
    let d1_rows = client.query(
        "SELECT uuid, updated_at, device_id FROM notes",
        &[],
    )?;
    let d1_index: HashMap<String, (String, Option<String>)> = d1_rows
        .iter()
        .filter_map(|row| {
            let uuid = row.get("uuid")?.as_str()?.to_string();
            Some((uuid, rev_from_row(row)))
        })
        .collect();

    // 2. 导出本地 notes
    let local_json = export_inbox(&data_dir.join("inbox.db"))?;
    let local: InboxExport = serde_json::from_str(&local_json).map_err(|e| e.to_string())?;

    // 3. 逐条比对，需要推送的写入 D1
    for note in &local.notes {
        let uuid = if note.uuid.trim().is_empty() {
            format!("legacy:{}", note.id)
        } else {
            note.uuid.clone()
        };
        let local_newer = match d1_index.get(&uuid) {
            None => true, // D1 不存在 → 推送
            Some((d1_ts, d1_dev)) => incoming_newer(
                &note.updated_at,
                note.device_id.as_deref(),
                d1_ts,
                d1_dev.as_deref(),
            ),
        };
        if local_newer {
            client.execute(
                "INSERT OR REPLACE INTO notes
                    (uuid, content, tags, created_at, updated_at, version, device_id, deleted, synced_at)
                 VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9)",
                &[
                    json!(uuid),
                    json!(&note.content),
                    json!(&note.tags),
                    json!(&note.created_at),
                    json!(&note.updated_at),
                    json!(note.version),
                    json!(self_id),
                    json!(note.deleted),
                    json!(Utc::now().to_rfc3339()),
                ],
            )?;
            result.pushed_notes += 1;
        }
    }
    Ok(())
}

fn push_todo(
    client: &D1Client,
    data_dir: &Path,
    self_id: &str,
    result: &mut D1SyncResult,
) -> Result<(), String> {
    let d1_rows = client.query(
        "SELECT uuid, updated_at, device_id FROM todos",
        &[],
    )?;
    let d1_index: HashMap<String, (String, Option<String>)> = d1_rows
        .iter()
        .filter_map(|row| {
            let uuid = row.get("uuid")?.as_str()?.to_string();
            Some((uuid, rev_from_row(row)))
        })
        .collect();

    let local_json = export_todo(&data_dir.join("todo.db"))?;
    let local: TodoExport = serde_json::from_str(&local_json).map_err(|e| e.to_string())?;

    for todo in &local.todos {
        let uuid = if todo.uuid.trim().is_empty() {
            format!("legacy:{}", todo.id)
        } else {
            todo.uuid.clone()
        };
        let local_newer = match d1_index.get(&uuid) {
            None => true,
            Some((d1_ts, d1_dev)) => incoming_newer(
                &todo.updated_at,
                todo.device_id.as_deref(),
                d1_ts,
                d1_dev.as_deref(),
            ),
        };
        if local_newer {
            client.execute(
                "INSERT OR REPLACE INTO todos
                    (uuid, title, content, completed, priority, due_date, tags,
                     created_at, updated_at, completed_at, version, device_id, deleted, synced_at)
                 VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14)",
                &[
                    json!(uuid),
                    json!(&todo.title),
                    json!(&todo.content),
                    json!(todo.completed),
                    json!(&todo.priority),
                    json!(&todo.due_date),
                    json!(&todo.tags),
                    json!(&todo.created_at),
                    json!(&todo.updated_at),
                    json!(&todo.completed_at),
                    json!(todo.version),
                    json!(self_id),
                    json!(todo.deleted),
                    json!(Utc::now().to_rfc3339()),
                ],
            )?;
            result.pushed_todos += 1;
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Pull：D1 → 本地
// ---------------------------------------------------------------------------

fn pull_inbox(
    client: &D1Client,
    data_dir: &Path,
    _self_id: &str,
    result: &mut D1SyncResult,
) -> Result<(), String> {
    // 1. 读取 D1 全部 notes
    let d1_rows = client.query(
        "SELECT uuid, content, tags, created_at, updated_at, version, device_id, deleted, synced_at FROM notes",
        &[],
    )?;
    let d1_map: HashMap<String, &Value> = d1_rows
        .iter()
        .filter_map(|row| {
            let uuid = row.get("uuid")?.as_str()?.to_string();
            Some((uuid, row))
        })
        .collect();

    // 2. 读取本地全部 notes
    let local_json = export_inbox(&data_dir.join("inbox.db"))?;
    let local: InboxExport = serde_json::from_str(&local_json).map_err(|e| e.to_string())?;
    let local_map: HashMap<String, &crate::serialize::NoteRow> = local
        .notes
        .iter()
        .map(|n| {
            let key = if n.uuid.trim().is_empty() {
                format!("legacy:{}", n.id)
            } else {
                n.uuid.clone()
            };
            (key, n)
        })
        .collect();

    // 3. 收集需要拉取的 notes（D1 更新 或 本地不存在）
    let mut pull = InboxExport::default();
    for (uuid, d1_row) in &d1_map {
        let d1_newer = match local_map.get(uuid) {
            None => true, // 本地不存在 → 拉取
            Some(local_note) => {
                let (d1_ts, d1_dev) = rev_from_row(d1_row);
                incoming_newer(
                    &d1_ts,
                    d1_dev.as_deref(),
                    &local_note.updated_at,
                    local_note.device_id.as_deref(),
                )
            }
        };
        if d1_newer {
            let row = *d1_row;
            pull.notes.push(crate::serialize::NoteRow {
                id: row.get("id").and_then(|v| v.as_i64()).unwrap_or(0),
                uuid: row
                    .get("uuid")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string(),
                content: row
                    .get("content")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string(),
                tags: row
                    .get("tags")
                    .and_then(|v| v.as_str())
                    .unwrap_or("[]")
                    .to_string(),
                created_at: row
                    .get("created_at")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string(),
                updated_at: row
                    .get("updated_at")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string(),
                version: row.get("version").and_then(|v| v.as_i64()).unwrap_or(1),
                device_id: row
                    .get("device_id")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string()),
                deleted: row.get("deleted").and_then(|v| v.as_i64()).unwrap_or(0),
                synced_at: row
                    .get("synced_at")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string()),
            });
        }
    }

    // 4. 通过 import_inbox 做 LWW 合并 + 回收站归档
    if !pull.notes.is_empty() {
        let json = serde_json::to_string(&pull).map_err(|e| e.to_string())?;
        let outcome = import_inbox(&data_dir.join("inbox.db"), &json)?;
        result.pulled_notes += outcome.created + outcome.updated + outcome.deleted;
        result.conflicts += outcome.archived.len();
        crate::dbglog::info(format!(
            "[d1] 拉取笔记: created={} updated={} deleted={} archived={}",
            outcome.created, outcome.updated, outcome.deleted, outcome.archived.len()
        ));
    }
    Ok(())
}

fn pull_todo(
    client: &D1Client,
    data_dir: &Path,
    _self_id: &str,
    result: &mut D1SyncResult,
) -> Result<(), String> {
    let d1_rows = client.query(
        "SELECT uuid, title, content, completed, priority, due_date, tags,
                created_at, updated_at, completed_at, version, device_id, deleted, synced_at
         FROM todos",
        &[],
    )?;
    let d1_map: HashMap<String, &Value> = d1_rows
        .iter()
        .filter_map(|row| {
            let uuid = row.get("uuid")?.as_str()?.to_string();
            Some((uuid, row))
        })
        .collect();

    let local_json = export_todo(&data_dir.join("todo.db"))?;
    let local: TodoExport = serde_json::from_str(&local_json).map_err(|e| e.to_string())?;
    let local_map: HashMap<String, &crate::serialize::TodoRow> = local
        .todos
        .iter()
        .map(|t| {
            let key = if t.uuid.trim().is_empty() {
                format!("legacy:{}", t.id)
            } else {
                t.uuid.clone()
            };
            (key, t)
        })
        .collect();

    let mut pull = TodoExport::default();
    for (uuid, d1_row) in &d1_map {
        let d1_newer = match local_map.get(uuid) {
            None => true,
            Some(local_todo) => {
                let (d1_ts, d1_dev) = rev_from_row(d1_row);
                incoming_newer(
                    &d1_ts,
                    d1_dev.as_deref(),
                    &local_todo.updated_at,
                    local_todo.device_id.as_deref(),
                )
            }
        };
        if d1_newer {
            let row = *d1_row;
            pull.todos.push(crate::serialize::TodoRow {
                id: row.get("id").and_then(|v| v.as_i64()).unwrap_or(0),
                uuid: row
                    .get("uuid")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string(),
                title: row
                    .get("title")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string(),
                content: row
                    .get("content")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string()),
                completed: row.get("completed").and_then(|v| v.as_i64()).unwrap_or(0),
                priority: row.get("priority").and_then(|v| v.as_i64()),
                due_date: row
                    .get("due_date")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string()),
                tags: row
                    .get("tags")
                    .and_then(|v| v.as_str())
                    .unwrap_or("[]")
                    .to_string(),
                created_at: row
                    .get("created_at")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string(),
                updated_at: row
                    .get("updated_at")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string(),
                completed_at: row
                    .get("completed_at")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string()),
                version: row.get("version").and_then(|v| v.as_i64()).unwrap_or(1),
                device_id: row
                    .get("device_id")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string()),
                deleted: row.get("deleted").and_then(|v| v.as_i64()).unwrap_or(0),
                synced_at: row
                    .get("synced_at")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string()),
            });
        }
    }

    if !pull.todos.is_empty() {
        let json = serde_json::to_string(&pull).map_err(|e| e.to_string())?;
        let outcome = import_todo(&data_dir.join("todo.db"), &json)?;
        result.pulled_todos += outcome.created + outcome.updated + outcome.deleted;
        result.conflicts += outcome.archived.len();
        crate::dbglog::info(format!(
            "[d1] 拉取 TODO: created={} updated={} deleted={} archived={}",
            outcome.created, outcome.updated, outcome.deleted, outcome.archived.len()
        ));
    }
    Ok(())
}
