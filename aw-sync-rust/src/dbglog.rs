//! 调试日志：双写通道。
//!
//! 1. 继续走 `log` 宏 → Android 上由 android_logger 进 **logcat**（tag: aw-server-rust），
//!    桌面端进终端（可用 `adb logcat -s aw-server-rust` 过滤查看）。
//! 2. 写入进程内环形缓冲，通过 `GET /api/0/sync/debuglog?after=<seq>` 增量暴露给
//!    aw-webui，前端轮询后以 console.log 打印到**浏览器 F12 控制台**。

use chrono::Utc;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, OnceLock};

/// 环形缓冲容量（超出后淘汰最旧条目）
const CAPACITY: usize = 500;

static SEQ: AtomicU64 = AtomicU64::new(1);

#[derive(Debug, Clone, serde::Serialize)]
pub struct DebugEntry {
    pub seq: u64,
    /// HH:MM:SS.mmm 本地可读时间
    pub ts: String,
    pub level: String,
    pub msg: String,
}

fn ring() -> &'static Mutex<VecDeque<DebugEntry>> {
    static RING: OnceLock<Mutex<VecDeque<DebugEntry>>> = OnceLock::new();
    RING.get_or_init(|| Mutex::new(VecDeque::with_capacity(CAPACITY)))
}

fn push(level: &str, msg: &str) {
    let entry = DebugEntry {
        seq: SEQ.fetch_add(1, Ordering::SeqCst),
        ts: Utc::now().format("%H:%M:%S%.3f").to_string(),
        level: level.to_string(),
        msg: msg.to_string(),
    };
    if let Ok(mut q) = ring().lock() {
        q.push_back(entry);
        while q.len() > CAPACITY {
            q.pop_front();
        }
    }
}

/// 记录一条 info 级日志（双写：log 链路 + 环形缓冲）
pub fn info(msg: impl AsRef<str>) {
    let m = msg.as_ref();
    log::info!("[aw-sync] {}", m);
    push("info", m);
}

/// 记录一条 warn 级日志
pub fn warn(msg: impl AsRef<str>) {
    let m = msg.as_ref();
    log::warn!("[aw-sync] {}", m);
    push("warn", m);
}

/// 记录一条 error 级日志
pub fn error(msg: impl AsRef<str>) {
    let m = msg.as_ref();
    log::error!("[aw-sync] {}", m);
    push("error", m);
}

/// 取回 seq 大于 after 的全部条目（增量拉取）
pub fn snapshot_after(after: u64) -> Vec<DebugEntry> {
    match ring().lock() {
        Ok(q) => q.iter().filter(|e| e.seq > after).cloned().collect(),
        Err(_) => Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshot_returns_only_newer_entries() {
        let before = snapshot_after(0).last().map(|e| e.seq).unwrap_or(0);
        info("unit-test-entry-a");
        info("unit-test-entry-b");
        let all = snapshot_after(before);
        assert!(all.iter().any(|e| e.msg.contains("unit-test-entry-a")));
        assert!(all.iter().any(|e| e.msg.contains("unit-test-entry-b")));
        // after 增量语义：以最后一条的 seq 再查应为空
        let last = all.last().unwrap().seq;
        assert!(snapshot_after(last).is_empty());
    }
}
