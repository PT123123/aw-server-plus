// src/models.rs
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
// Removed: use sqlx::FromRow;

// 用于数据库交互的 Note 结构体
// Removed FromRow, Updated tags type
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Note {
    pub id: i64,
    pub content: String,
    pub tags: Vec<String>, // <<< Changed from String to Vec<String>
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub version: i64,
    pub device_id: Option<String>,
    pub deleted: bool,
    pub synced_at: Option<DateTime<Utc>>,
}

// 用于创建新笔记的请求体结构 (Remains the same)
#[derive(Deserialize, Debug)]
pub struct CreateNotePayload {
    pub content: String,
    pub tags: Option<Vec<String>>,
    pub created_at: Option<DateTime<Utc>>,
}

// 用于更新笔记的请求体结构 (Remains the same)
#[derive(Deserialize, Debug)]
pub struct UpdateNotePayload {
    pub content: String,
    pub tags: Option<Vec<String>>,
}

// 用于 API 响应的笔记结构 (Remains the same, tags is Vec<String>)
#[derive(Serialize, Debug)]
pub struct NoteResponse {
    pub id: i64,
    pub content: String,
    pub tags: Vec<String>,  // API 层面返回 Vec<String>
    pub created_at: String, // ISO 8601 格式字符串
    pub updated_at: String, // ISO 8601 格式字符串
    pub version: i64,
    pub device_id: Option<String>,
    pub deleted: bool,
    pub synced_at: Option<String>,
    pub conflict: bool,
}

// 用于数据库交互和 API 响应的 Tag 结构体
// Removed FromRow
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Tag {
    pub id: i64,
    pub name: String,
    // pub path: String, // 根据需要添加
}

// 用于 API 响应的详细标签结构
// Updated last_modified type
#[derive(Serialize, Debug)]
pub struct DetailedTag {
    pub name: String,
    pub count: i64,
    // Changed to DateTime<Utc> to match data from db layer
    // Serde chrono feature usually handles serialization to string automatically
    pub last_modified: Option<DateTime<Utc>>, // <<< Changed from Option<String>
}

// 笔记关系类型枚举
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum NoteRelationType {
    Comment,   // 评论关系
    Reference, // 引用关系
    Link,      // 链接关系
               // 可以根据需要添加更多关系类型
}

// 用于数据库交互的笔记关系结构体
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct NoteRelation {
    pub id: i64,
    pub source_note_id: i64,             // 源笔记ID（如评论笔记）
    pub target_note_id: i64,             // 目标笔记ID（如被评论的笔记）
    pub relation_type: NoteRelationType, // 关系类型
    pub created_at: DateTime<Utc>,
}

// 用于创建笔记关系的请求体结构
#[derive(Deserialize, Debug)]
pub struct CreateNoteRelationPayload {
    pub relation_type: NoteRelationType, // 关系类型（默认为Comment）
}

// 用于创建评论的请求体结构 (与CreateNotePayload结合)
#[derive(Deserialize, Debug)]
pub struct CreateCommentPayload {
    pub content: String,           // 评论内容
    pub tags: Option<Vec<String>>, // 评论标签（可选）
}

// ── Todo ───────────────────────────────────────────────────────

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Todo {
    pub id: i64,
    pub title: String,
    pub content: Option<String>,
    pub completed: bool,
    pub priority: Option<i64>,
    pub due_date: Option<DateTime<Utc>>,
    pub tags: Vec<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
    pub version: i64,
    pub device_id: Option<String>,
    pub deleted: bool,
    pub synced_at: Option<DateTime<Utc>>,
}

#[derive(Serialize, Debug)]
pub struct TodoResponse {
    pub id: i64,
    pub title: String,
    pub content: Option<String>,
    pub completed: bool,
    pub priority: Option<i64>,
    pub due_date: Option<String>,
    pub tags: Vec<String>,
    pub created_at: String,
    pub updated_at: String,
    pub completed_at: Option<String>,
    pub version: i64,
    pub device_id: Option<String>,
    pub deleted: bool,
    pub synced_at: Option<String>,
    pub conflict: bool,
}

#[derive(Deserialize, Debug)]
pub struct CreateTodoPayload {
    pub title: String,
    pub content: Option<String>,
    pub priority: Option<i64>,
    pub due_date: Option<DateTime<Utc>>,
    pub tags: Option<Vec<String>>,
    pub created_at: Option<DateTime<Utc>>,
}

#[derive(Deserialize, Debug)]
pub struct UpdateTodoPayload {
    pub title: Option<String>,
    pub content: Option<String>,
    pub completed: Option<bool>,
    pub priority: Option<i64>,
    pub due_date: Option<DateTime<Utc>>,
    pub tags: Option<Vec<String>>,
}

// ── Note History ───────────────────────────────────────────────

/// 数据库内部的笔记历史快照
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct NoteHistory {
    pub id: i64,
    pub note_id: i64,
    pub content: String,
    pub tags: Vec<String>,
    pub version: i64,
    pub device_id: Option<String>,
    pub updated_at: DateTime<Utc>,
    pub snapshot_at: DateTime<Utc>,
}

/// API 响应结构（时间字段序列化为字符串，与 NoteResponse 风格一致）
#[derive(Serialize, Debug)]
pub struct NoteHistoryResponse {
    pub id: i64,
    pub note_id: i64,
    pub content: String,
    pub tags: Vec<String>,
    pub version: i64,
    pub device_id: Option<String>,
    pub updated_at: String,
    pub snapshot_at: String,
}

pub fn note_history_to_response(h: &NoteHistory) -> NoteHistoryResponse {
    NoteHistoryResponse {
        id: h.id,
        note_id: h.note_id,
        content: h.content.clone(),
        tags: h.tags.clone(),
        version: h.version,
        device_id: h.device_id.clone(),
        updated_at: h.updated_at.to_rfc3339(),
        snapshot_at: h.snapshot_at.to_rfc3339(),
    }
}
