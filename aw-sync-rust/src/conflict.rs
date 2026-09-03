//! 冲突仲裁（P0 起启用，见 docs/lan-sync-conflict-redesign.md §3）。
//!
//! rev 键设计：rev = (updated_at 的 epoch 毫秒, device_id) 字典序。
//! - epoch 毫秒大者胜；相等时 device_id 字符串大者胜（保证仲裁在任意两端确定性一致）。
//! - 语义：最新一次编辑的一方赢得仲裁；另一方的旧版本进入回收站归档，不删除数据。

use chrono::DateTime;

/// 冲突决策结果
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConflictAction {
    /// 用远端数据更新本地
    Update,
    /// 删除本地
    Delete,
    /// 保留本地（远端退回）
    KeepLocal,
    /// 无法自动判定，标记待人工处理
    Manual,
}

/// 把 RFC3339 时间串解析为 epoch 毫秒；解析失败返回 None。
pub fn epoch_ms(rfc3339: &str) -> Option<i64> {
    DateTime::parse_from_rfc3339(rfc3339)
        .ok()
        .map(|d| d.timestamp_millis())
}

/// 判断「对端版本是否更新」：
/// 先比 updated_at 的 epoch 毫秒，时间可区分则取较大者；
/// 时间相同或解析失败时用 device_id 字符串兜底决胜，保证确定性。
pub fn incoming_newer(
    incoming_ts: &str,
    incoming_dev: Option<&str>,
    local_ts: &str,
    local_dev: Option<&str>,
) -> bool {
    match (epoch_ms(incoming_ts), epoch_ms(local_ts)) {
        (Some(i), Some(l)) if i != l => i > l,
        _ => {
            let id = incoming_dev.unwrap_or("");
            let ld = local_dev.unwrap_or("");
            id > ld
        }
    }
}

/// 由 resolve 逻辑派生的行为：给定双方 rev 的仲裁结果，返回应当采取的动作。
/// incoming 是对端（正在导入）的记录，local 是本地已有记录。
pub fn resolve(incoming_newer: bool) -> ConflictAction {
    if incoming_newer {
        ConflictAction::Update
    } else {
        ConflictAction::KeepLocal
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn newer_wins_by_timestamp() {
        // 时间较晚的对端胜出
        assert!(incoming_newer(
            "2026-08-25T12:00:00Z",
            Some("dev-b"),
            "2026-08-25T08:00:00Z",
            Some("dev-a")
        ));
        assert!(!incoming_newer(
            "2026-08-25T08:00:00Z",
            Some("dev-b"),
            "2026-08-25T12:00:00Z",
            Some("dev-a")
        ));
    }

    #[test]
    fn tie_breaks_by_device_id() {
        // 时间相同 → device_id 字典序
        assert!(incoming_newer(
            "2026-08-25T08:00:00Z",
            Some("dev-b"),
            "2026-08-25T08:00:00Z",
            Some("dev-a")
        ));
        assert!(!incoming_newer(
            "2026-08-25T08:00:00Z",
            Some("dev-a"),
            "2026-08-25T08:00:00Z",
            Some("dev-b")
        ));
    }

    #[test]
    fn parse_failure_falls_back_to_device_id() {
        // 本地时间解析失败（旧格式/坏串）→ 用 device_id 兜底
        assert!(incoming_newer(
            "2026-08-25T12:00:00Z",
            Some("dev-b"),
            "not-a-time",
            Some("dev-a")
        ));
    }

    #[test]
    fn resolve_maps_to_action() {
        assert_eq!(resolve(true), ConflictAction::Update);
        assert_eq!(resolve(false), ConflictAction::KeepLocal);
    }
}
