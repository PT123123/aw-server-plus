// src/lib.rs 或 src/main.rs
use log::{error, info};
use rocket::request::{self, FromRequest, Request};
use rocket::serde::json::Json;
use rocket::{delete, get, post, put, routes, Build, Rocket, State};
use rocket::form::FromForm;
use rocket::http::Status;
use rocket::response::status::Created;
use std::sync::Arc;
use std::sync::Mutex;
use tokio::task;

// 自定义请求守卫：提取 X-Device-ID 头部
struct DeviceIdGuard(Option<String>);

#[rocket::async_trait]
impl<'r> FromRequest<'r> for DeviceIdGuard {
    type Error = ();

    async fn from_request(req: &'r Request<'_>) -> request::Outcome<Self, Self::Error> {
        let device_id = req.headers().get_one("X-Device-ID").map(|s| s.to_string());
        request::Outcome::Success(DeviceIdGuard(device_id))
    }
}

pub mod db;
mod models;
// Ensure models.rs has correct Note/NoteResponse definitions (tags: Vec<String>)
use crate::models::UpdateNotePayload;
use models::{CreateNotePayload, DetailedTag, Note, NoteResponse};
// 添加评论相关模型
use crate::models::{
    CreateCommentPayload, CreateNoteRelationPayload, NoteRelation,
};
use crate::models::{Todo, TodoResponse, CreateTodoPayload, UpdateTodoPayload};

// --- Use correct DbConnection type ---
pub type SharedDb = Arc<Mutex<db::DbConnection>>;
pub struct SharedTodoDb(pub Arc<Mutex<db::DbConnection>>);

// --- note_to_response expects Note with tags: Vec<String> ---
fn note_to_response(note: &Note) -> NoteResponse {
    NoteResponse {
        id: note.id,
        content: note.content.clone(),
        tags: note.tags.clone(),
        created_at: note.created_at.to_rfc3339(),
        updated_at: note.updated_at.to_rfc3339(),
        version: note.version,
        device_id: note.device_id.clone(),
        deleted: note.deleted,
        synced_at: note.synced_at.map(|dt| dt.to_rfc3339()),
        conflict: false,
    }
}

// --- 辅助函数处理 DB 错误 (uses rusqlite::Error) ---
fn handle_db_error(db_err: rusqlite::Error) -> Status {
    // Use full path
    let msg = format!("DB function failed: {:?}", db_err);
    error!("{}", msg);
    match db_err {
        e if e.to_string().contains("no such table") => Status::BadRequest,
        // Use full path for QueryReturnedNoRows
        rusqlite::Error::QueryReturnedNoRows => Status::NotFound,
        _ => Status::InternalServerError,
    }
}

// --- 辅助函数处理 spawn_blocking 错误 (returns Status) ---
fn handle_spawn_error(spawn_err: task::JoinError) -> Status {
    // Return Status directly
    error!("Spawn blocking task failed: {:?}", spawn_err);
    Status::InternalServerError
}

#[get("/tags/detailed")]
async fn get_detailed_tags(db_state: &State<SharedDb>) -> Result<Json<Vec<DetailedTag>>, Status> {
    let db_arc = db_state.inner().clone();

    let tags = task::spawn_blocking(move || {
        let conn = db_arc.lock().map_err(|_| Status::InternalServerError)?;
        match db::get_detailed_tags_db(&conn) {
            Ok(tags) => Ok(tags),
            Err(e) => Err(handle_db_error(e)),
        }
    })
    .await
    .map_err(handle_spawn_error)??;

    Ok(Json(tags))
}

#[get("/tags")]
async fn get_tags(db_state: &State<SharedDb>) -> Result<Json<Vec<String>>, Status> {
    let db_arc = db_state.inner().clone();

    task::spawn_blocking(move || {
        let conn = db_arc.lock().map_err(|_| Status::InternalServerError)?;
        db::get_all_tags_db(&conn).map_err(handle_db_error)
    })
    .await
    .map_err(handle_spawn_error)? // Single '?'
    .map(Json)
}

// 获取笔记的评论
#[get("/notes/<note_id>/comments")]
async fn get_comments(
    db_state: &State<SharedDb>,
    note_id: i64,
) -> Result<Json<Vec<NoteResponse>>, Status> {
    let db_arc = db_state.inner().clone();

    let comments_with_relations = task::spawn_blocking(move || {
        let conn = db_arc.lock().map_err(|_| Status::InternalServerError)?;
        db::get_comments_for_note_db(&conn, note_id).map_err(handle_db_error)
    })
    .await
    .map_err(handle_spawn_error)??;

    // 转换为NoteResponse，只返回笔记部分
    let response = comments_with_relations
        .iter()
        .map(|(note, _relation)| note_to_response(note))
        .collect();

    Ok(Json(response))
}

// 添加评论
#[post("/notes/<note_id>/comments", data = "<payload>", format = "json")]
async fn add_comment(
    db_state: &State<SharedDb>,
    note_id: i64,
    payload: Json<CreateCommentPayload>,
) -> Result<Created<Json<NoteResponse>>, Status> {
    let db_arc = db_state.inner().clone();
    let comment_payload = payload.into_inner();

    let (created_note, _relation) = task::spawn_blocking(move || {
        let mut conn = db_arc.lock().map_err(|_| Status::InternalServerError)?;
        db::add_comment_db(&mut conn, note_id, comment_payload).map_err(handle_db_error)
    })
    .await
    .map_err(handle_spawn_error)??;

    Ok(Created::new(format!("/inbox/notes/{}/comments", note_id))
        .body(Json(note_to_response(&created_note))))
}

// 创建笔记关系
#[post(
    "/notes/<source_id>/relations/<target_id>",
    data = "<payload>",
    format = "json"
)]
async fn create_relation(
    db_state: &State<SharedDb>,
    source_id: i64,
    target_id: i64,
    payload: Json<CreateNoteRelationPayload>,
) -> Result<Created<Json<NoteRelation>>, Status> {
    let db_arc = db_state.inner().clone();
    let relation_payload = payload.into_inner();

    let created_relation = task::spawn_blocking(move || {
        let mut conn = db_arc.lock().map_err(|_| Status::InternalServerError)?;
        db::create_note_relation_db(&mut conn, source_id, target_id, relation_payload)
            .map_err(handle_db_error)
    })
    .await
    .map_err(handle_spawn_error)??;

    Ok(Created::new(format!(
        "/inbox/notes/{}/relations/{}",
        source_id, target_id
    ))
    .body(Json(created_relation)))
}

// 获取笔记的所有关系
#[get("/notes/<note_id>/relations")]
async fn get_relations(
    db_state: &State<SharedDb>,
    note_id: i64,
) -> Result<Json<Vec<NoteRelation>>, Status> {
    let db_arc = db_state.inner().clone();

    let relations = task::spawn_blocking(move || {
        let conn = db_arc.lock().map_err(|_| Status::InternalServerError)?;
        db::get_relations_for_note_db(&conn, note_id, None).map_err(handle_db_error)
    })
    .await
    .map_err(handle_spawn_error)??;

    Ok(Json(relations))
}

// mount_rocket remains the same
pub fn mount_rocket(rocket: Rocket<Build>, db: SharedDb, todo_db: SharedTodoDb) -> Rocket<Build> {
    info!("开始注册 Inbox Server 路由...");
    info!("注册数据库连接池 (同步包装)...");
    let rocket = rocket.manage(db).manage(todo_db);

    info!("注册 API 路由:");
    // ... (routes) ...
    info!("  - GET    /inbox/");
    info!("  - POST   /inbox/notes (format=json)");
    info!("  - GET    /inbox/notes");
    info!("  - GET    /inbox/notes/<id>");
    info!("  - PUT    /inbox/notes/<id> (format=json)");
    info!("  - DELETE /inbox/notes/<id>");
    info!("  - PUT    /inbox/notes/<id>/restore");
    info!("  - GET    /inbox/tags");
    info!("  - GET    /inbox/tags/detailed");
    info!("  - GET    /inbox/notes/<note_id>/comments");
    info!("  - POST   /inbox/notes/<note_id>/comments (format=json)");
    info!("  - POST   /inbox/notes/<source_id>/relations/<target_id> (format=json)");
    info!("  - GET    /inbox/notes/<note_id>/relations");
    // 同步相关路由
    info!("  - GET    /inbox/todos");
    info!("  - GET    /inbox/todos/<id>");
    info!("  - POST   /inbox/todos (format=json)");
    info!("  - PUT    /inbox/todos/<id> (format=json)");
    info!("  - DELETE /inbox/todos/<id>");
    info!("  - PUT    /inbox/todos/<id>/restore");

    let rocket = rocket.mount(
        "/inbox",
        routes![
            root,
            create_note,
            get_notes,
            get_note,
            update_note,
            delete_note,
            restore_note,
            get_tags,
            get_detailed_tags,
            // 评论和关系相关路由
            get_comments,
            add_comment,
            create_relation,
            get_relations,
            // Todo 路由
            get_todos,
            get_todo,
            create_todo,
            update_todo,
            delete_todo,
            restore_todo,
            // 调试路由
            inbox_route_debug,
        ],
    );

    info!("Inbox Server 路由注册完成");
    info!("调试: 已注册一个兜底路由 POST /inbox/route-debug 用于排查路由匹配问题");
    rocket
}

/// 调试路由：打印请求的详细信息，帮助排查路由匹配问题
#[post("/route-debug", data = "<body>")]
async fn inbox_route_debug(
    content_type: &rocket::http::ContentType,
    body: String,
) -> String {
    error!("[INBOX_DEBUG] ===== 收到调试请求 =====");
    error!("[INBOX_DEBUG] Content-Type: {:?}", content_type);
    error!("[INBOX_DEBUG] Body: {}", body);
    error!("[INBOX_DEBUG] ===========================");
    format!("INBOX_DEBUG: received body of length {} with Content-Type {:?}", body.len(), content_type)
}

#[get("/")]
fn root() -> &'static str {
    "📥 Welcome to Inbox Inbox Server (Rust Version)"
}

#[post("/notes", data = "<payload>", format = "json")]
async fn create_note(
    db_state: &State<SharedDb>,
    payload: Json<CreateNotePayload>,
    device_id_guard: DeviceIdGuard,
) -> Result<Created<Json<NoteResponse>>, Status> {
    let db_arc = db_state.inner().clone();
    let note_payload = payload.into_inner();
    let device_id_str = device_id_guard.0;

    let created_note = task::spawn_blocking(move || {
        let mut conn_guard = db_arc.lock().map_err(|_| Status::InternalServerError)?;
        db::create_note_db(&mut conn_guard, note_payload, device_id_str).map_err(handle_db_error)
    })
    .await
    .map_err(handle_spawn_error)??;

    Ok(Created::new("/inbox/notes").body(Json(note_to_response(&created_note))))
}

#[derive(FromForm)]
struct NotesQuery {
    limit: Option<i64>,
    offset: Option<i64>,
    tag: Option<String>,
    search: Option<String>,
    sort_by: Option<String>,
}

#[get("/notes?<query..>")]
async fn get_notes(
    db_state: &State<SharedDb>,
    query: NotesQuery,
) -> Result<Json<Vec<NoteResponse>>, Status> {
    let db_arc = db_state.inner().clone();

    // 接收查询参数
    let limit = query.limit;
    let offset = query.offset;
    let tag = query.tag;
    let search = query.search;
    let sort_by = query.sort_by;

    let notes = task::spawn_blocking(move || {
        let conn = db_arc.lock().map_err(|_| Status::InternalServerError)?;
        db::get_notes_db(&conn, limit, offset, tag, None, None, search, sort_by).map_err(handle_db_error)
    })
    .await
    .map_err(handle_spawn_error)??; // Double '?'

    let response = notes.iter().map(note_to_response).collect();
    Ok(Json(response))
}

#[get("/notes/<id>")]
async fn get_note(db_state: &State<SharedDb>, id: i64) -> Result<Json<NoteResponse>, Status> {
    let db_arc = db_state.inner().clone();

    let maybe_note = task::spawn_blocking(move || {
        let conn = db_arc.lock().map_err(|_| Status::InternalServerError)?;
        db::get_note_db(&conn, id).map_err(handle_db_error)
    })
    .await
    .map_err(handle_spawn_error)??; // Double '?'

    match maybe_note {
        Some(note) => Ok(Json(note_to_response(&note))),
        None => Err(Status::NotFound),
    }
}

#[put("/notes/<id>", data = "<payload>", format = "json")]
async fn update_note(
    db_state: &State<SharedDb>,
    id: i64,
    payload: Json<UpdateNotePayload>,
    device_id_guard: DeviceIdGuard,
) -> Result<Json<NoteResponse>, Status> {
    let db_arc = db_state.inner().clone();
    let note_payload = payload.into_inner();
    let device_id_str = device_id_guard.0;

    let updated_note_option = task::spawn_blocking(move || {
        let mut conn_guard = db_arc.lock().map_err(|_| Status::InternalServerError)?;
        db::update_note_db(&mut conn_guard, id, note_payload, device_id_str).map_err(handle_db_error)
    })
    .await
    .map_err(handle_spawn_error)??;

    match updated_note_option {
        Some(note) => Ok(Json(note_to_response(&note))),
        None => Err(Status::NotFound),
    }
}

#[delete("/notes/<id>")]
async fn delete_note(
    db_state: &State<SharedDb>,
    id: i64,
    device_id_guard: DeviceIdGuard,
) -> Result<Status, Status> {
    let db_arc = db_state.inner().clone();
    let device_id_str = device_id_guard.0;

    let deleted = task::spawn_blocking(move || {
        let mut conn_guard = db_arc.lock().map_err(|_| Status::InternalServerError)?;
        db::delete_note_db(&mut conn_guard, id, device_id_str).map_err(handle_db_error)
    })
    .await
    .map_err(handle_spawn_error)??;

    if deleted {
        Ok(Status::NoContent)
    } else {
        Err(Status::NotFound)
    }
}

#[put("/notes/<id>/restore")]
async fn restore_note(
    db_state: &State<SharedDb>,
    id: i64,
    device_id_guard: DeviceIdGuard,
) -> Result<Json<NoteResponse>, Status> {
    let db_arc = db_state.inner().clone();
    let device_id_str = device_id_guard.0;
    let note = task::spawn_blocking(move || {
        let mut conn_guard = db_arc.lock().map_err(|_| Status::InternalServerError)?;
        db::restore_note_db(&mut conn_guard, id, device_id_str).map_err(handle_db_error)
    })
    .await
    .map_err(handle_spawn_error)??;
    match note {
        Some(n) => Ok(Json(note_to_response(&n))),
        None => Err(Status::NotFound),
    }
}

// 修改migrate_db函数，解决借用问题
pub async fn migrate_db(db_path: &str) -> Result<(), Status> {
    // 复制路径字符串，以便在闭包中使用
    let db_path = db_path.to_string();

    // 在独立线程上运行数据库迁移
    tokio::task::spawn_blocking(move || {
        // 在新线程中创建新连接
        let conn = rusqlite::Connection::open(&db_path).map_err(|e| {
            error!("无法打开数据库连接: {:?}", e);
            handle_db_error(e)
        })?;

        // 执行迁移
        db::migrate(&conn).map_err(|e| {
            error!("数据库迁移操作失败: {:?}", e);
            handle_db_error(e)
        })
    })
    .await
    .map_err(|_| Status::InternalServerError)?
}

// ==================== 同步端点 ====================

// ── Todo handlers ──────────────────────────────────────────────

fn todo_to_response(todo: &Todo) -> TodoResponse {
    TodoResponse {
        id: todo.id,
        title: todo.title.clone(),
        content: todo.content.clone(),
        completed: todo.completed,
        priority: todo.priority,
        due_date: todo.due_date.map(|dt| dt.to_rfc3339()),
        tags: todo.tags.clone(),
        created_at: todo.created_at.to_rfc3339(),
        updated_at: todo.updated_at.to_rfc3339(),
        completed_at: todo.completed_at.map(|dt| dt.to_rfc3339()),
        version: todo.version,
        device_id: todo.device_id.clone(),
        deleted: todo.deleted,
        synced_at: todo.synced_at.map(|dt| dt.to_rfc3339()),
        conflict: false,
    }
}

#[derive(FromForm)]
struct TodoQuery {
    completed: Option<bool>,
    limit: Option<i64>,
    offset: Option<i64>,
}

#[get("/todos?<query..>")]
async fn get_todos(
    db_state: &State<SharedTodoDb>,
    query: TodoQuery,
) -> Result<Json<Vec<TodoResponse>>, Status> {
    let db_arc = db_state.inner().0.clone();
    let todos = task::spawn_blocking(move || {
        let db = db_arc.lock().map_err(|_| Status::InternalServerError)?;
        db::get_todos_db(&db, query.completed, query.limit, query.offset).map_err(handle_db_error)
    })
    .await
    .map_err(handle_spawn_error)??;
    Ok(Json(todos.iter().map(todo_to_response).collect()))
}

#[get("/todos/<todo_id>")]
async fn get_todo(
    db_state: &State<SharedTodoDb>,
    todo_id: i64,
) -> Result<Json<TodoResponse>, Status> {
    let db_arc = db_state.inner().0.clone();
    let todo = task::spawn_blocking(move || {
        let db = db_arc.lock().map_err(|_| Status::InternalServerError)?;
        db::get_todo_by_id_db(&db, todo_id).map_err(handle_db_error)
    })
    .await
    .map_err(handle_spawn_error)??;
    if todo.deleted {
        return Err(Status::NotFound);
    }
    Ok(Json(todo_to_response(&todo)))
}

#[post("/todos", format = "json", data = "<payload>")]
async fn create_todo(
    db_state: &State<SharedTodoDb>,
    device: DeviceIdGuard,
    payload: Json<CreateTodoPayload>,
) -> Result<Created<Json<TodoResponse>>, Status> {
    let device_id = device.0;
    let db_arc = db_state.inner().0.clone();
    let todo = task::spawn_blocking(move || {
        let mut db = db_arc.lock().map_err(|_| Status::InternalServerError)?;
        db::create_todo_db(&mut db, payload.into_inner(), device_id).map_err(handle_db_error)
    })
    .await
    .map_err(handle_spawn_error)??;
    info!("Created todo #{}: {}", todo.id, todo.title);
    Ok(Created::new(format!("/inbox/todos/{}", todo.id)).body(Json(todo_to_response(&todo))))
}

#[put("/todos/<todo_id>", format = "json", data = "<payload>")]
async fn update_todo(
    db_state: &State<SharedTodoDb>,
    todo_id: i64,
    payload: Json<UpdateTodoPayload>,
) -> Result<Json<TodoResponse>, Status> {
    let db_arc = db_state.inner().0.clone();
    let todo = task::spawn_blocking(move || {
        let mut db = db_arc.lock().map_err(|_| Status::InternalServerError)?;
        db::update_todo_db(&mut db, todo_id, payload.into_inner()).map_err(handle_db_error)
    })
    .await
    .map_err(handle_spawn_error)??;
    info!("Updated todo #{}: {} (completed={})", todo.id, todo.title, todo.completed);
    Ok(Json(todo_to_response(&todo)))
}

#[delete("/todos/<todo_id>")]
async fn delete_todo(
    db_state: &State<SharedTodoDb>,
    todo_id: i64,
) -> Result<Status, Status> {
    let db_arc = db_state.inner().0.clone();
    task::spawn_blocking(move || {
        let mut db = db_arc.lock().map_err(|_| Status::InternalServerError)?;
        db::delete_todo_db(&mut db, todo_id).map_err(handle_db_error)
    })
    .await
    .map_err(handle_spawn_error)??;
    info!("Deleted todo #{}", todo_id);
    Ok(Status::NoContent)
}

#[put("/todos/<todo_id>/restore")]
async fn restore_todo(
    db_state: &State<SharedTodoDb>,
    todo_id: i64,
) -> Result<Json<TodoResponse>, Status> {
    let db_arc = db_state.inner().0.clone();
    let todo = task::spawn_blocking(move || {
        let mut db = db_arc.lock().map_err(|_| Status::InternalServerError)?;
        db::restore_todo_db(&mut db, todo_id).map_err(handle_db_error)
    })
    .await
    .map_err(handle_spawn_error)??;
    match todo {
        Some(t) => Ok(Json(todo_to_response(&t))),
        None => Err(Status::NotFound),
    }
}
