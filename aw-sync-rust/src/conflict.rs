//! 冲突处理（本期留空占位）。
//!
//! 后续迭代将读取对端与本地数据库，依据数据的 updated_at / 时间戳等字段，
//! 决定对本地记录执行「更新 / 删除 / 保留」，并记录 Conflict 类型的同步日志。

use crate::models::{SyncSnapshot};

/// 冲突决策结果（后续扩展）
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

/// 对某个过来的快照/记录执行冲突决策。
/// 本期统一返回 `KeepLocal`（不做任何修改），仅保留接口与语义占位。
pub fn resolve_conflict(
    _incoming: &SyncSnapshot,
    _local_timestamp: Option<i64>,
    _incoming_timestamp: Option<i64>,
) -> ConflictAction {
    // TODO(后续迭代)：比较时间戳，决定 Update / Delete / KeepLocal / Conflict。
    ConflictAction::KeepLocal
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn placeholder_keeps_local() {
        let snap = SyncSnapshot::default();
        assert_eq!(
            resolve_conflict(&snap, Some(1), Some(2)),
            ConflictAction::KeepLocal
        );
    }
}