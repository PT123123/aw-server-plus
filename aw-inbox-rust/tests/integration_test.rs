// 导入必要的模块
use axum::{
    body::Body,
    http::{Request, StatusCode},
    Router,
};
use serde_json::{json, Value};
// 导入 ServiceExt trait。这个导入依赖于 tower crate 已经被正确添加到 Cargo.toml。
use mime;
use tower::util::ServiceExt; // 导入 mime crate

// *** 根据你的实际项目结构调整这些导入路径。***
// 错误 E0432 'unresolved import aw_inbox_rust::app' 表明 'app' 不在 aw_inbox_rust crate 的根部。
// 你需要找到你的 Axum Router 创建函数（比如叫 app）在你的代码中被 pub 导出的位置，并修正这里的路径。
// 例如，如果 app 函数在 src/api/mod.rs 并通过 lib.rs 的 pub mod api; 导出，路径可能是 use aw_inbox_rust::api::app;
// db 的路径也需要根据其在项目中的实际位置进行调整。
// *** 你必须根据你的实际代码结构修改下面这行（或几行）！ ***
use aw_inbox_rust::app;
use aw_inbox_rust::db;

// 这个导入可能没有被直接使用，但通常不会引起错误。如果不需要可以删除。
// use aw_inbox_rust::models::Note;

// setup_app 是一个辅助函数，用于创建测试环境，不应带有 #[tokio::test] 属性
// #[tokio::test] <-- 请确保你已经移除此行
async fn setup_app() -> Router {
    // 初始化内存数据库连接池
    let db_pool = db::init_db("sqlite::memory:")
        .await
        .expect("Failed to connect to test database");

    // 运行数据库迁移
    sqlx::migrate!("./migrations")
        .run(&db_pool)
        .await
        .expect("Failed to run migrations");

    // 调用你的应用程序入口函数，传入数据库连接池
    app(db_pool).await
}

// 辅助函数：发送请求并获取状态码和 JSON 响应体
async fn request(
    app: &Router,               // 接收 Router 的引用
    method: axum::http::Method, // HTTP 方法 (GET, POST, etc.)
    uri: &str,                  // 请求 URI
    body: Value,                // 请求体 (使用 serde_json::Value)
) -> (StatusCode, Value) {
    // 构建 HTTP 请求
    let request = Request::builder()
        .method(method)
        .uri(uri)
        // 设置 Content-Type 为 application/json
        .header(
            axum::http::header::CONTENT_TYPE,
            mime::APPLICATION_JSON.as_ref(),
        )
        // 将 JSON Value 序列化为 Vec<u8> 作为请求体
        .body(Body::from(serde_json::to_vec(&body).unwrap()))
        .unwrap();

    // 使用 ServiceExt::oneshot 发送请求并获取响应。
    // 需要对 router 进行 clone，因为 oneshot 会消费 service。
    // ServiceExt trait 必须在作用域中（通过 use tower::util::ServiceExt; 导入），这依赖于 tower crate 被找到。
    let response = app.clone().oneshot(request).await.unwrap();
    let status = response.status();

    // 读取响应体
    let body_bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    // 尝试将响应体反序列化为 JSON。如果响应体为空或非 JSON，则返回一个空的 JSON 对象。
    let body_json: Value = serde_json::from_slice(&body_bytes).unwrap_or(json!({}));

    (status, body_json)
}

// 实际的集成测试函数，带有 #[tokio::test] 属性
#[tokio::test]
async fn test_add_note() {
    // 调用 setup_app() 异步函数来设置应用程序并等待其完成
    let app = setup_app().await;

    // 定义用于添加的笔记数据
    let note_data = json!({
        "content": "This is a test note from integration test",
        "tags": ["test", "integration"]
    });

    // 发送 POST 请求添加笔记
    let (status, body) = request(&app, axum::http::Method::POST, "/inbox/notes", note_data).await;

    println!("Add Note Status: {}", status); // 调试输出状态码
    println!("Add Note Body: {}", body); // 调试输出响应体

    // 断言状态码是 201 Created
    assert_eq!(status, StatusCode::CREATED);
    // 断言响应体包含一个数字类型的 id
    assert!(
        body["id"].as_i64().is_some(),
        "Expected 'id' to be a number, got: {}",
        body["id"]
    );
    // 断言响应体中的 content 与发送的数据一致
    assert_eq!(
        body["content"], "This is a test note from integration test",
        "Expected content to match"
    );

    // 新增空内容校验逻辑测试
    let empty_content = json!({ "content": "", "tags": ["empty"] });
    let (status, body) = request(
        &app,
        axum::http::Method::POST,
        "/inbox/notes",
        empty_content,
    )
    .await;
    println!("Add Empty Content Status: {}", status);
    println!("Add Empty Content Body: {}", body);
    // 断言状态码是 400 Bad Request
    assert_eq!(status, StatusCode::BAD_REQUEST);
    // 断言响应体包含错误信息，并且错误信息中包含 "content cannot be empty"
    // 使用 get().and_then().map_or() 链式调用安全访问 JSON 字段
    assert!(
        body.get("error")
            .and_then(|e| e.as_str())
            .map_or(false, |e_str| e_str.contains("content cannot be empty")),
        "Expected error message containing 'content cannot be empty', got: {}",
        body
    );

    // 新增空标签场景测试
    let note3 = json!({ "content": "Note without tags" });
    let (status_note3, body_note3) =
        request(&app, axum::http::Method::POST, "/inbox/notes", note3).await;
    println!("Add Note without Tags Status: {}", status_note3);
    println!("Add Note without Tags Body: {}", body_note3);
    // 断言状态码是 201 Created
    assert_eq!(
        status_note3,
        StatusCode::CREATED,
        "Should successfully create note without tags"
    );
    // 断言返回了有效的笔记 ID
    assert!(
        body_note3["id"].as_i64().is_some(),
        "Should return a valid note ID for note without tags"
    );
    // 可选：断言响应中的 tags 字段是 null 或空数组
    // assert!(body_note3.get("tags").map_or(true, |t| t.is_null() || (t.is_array() && t.as_array().unwrap().is_empty())));
}

#[tokio::test]
async fn test_delete_note() {
    let app = setup_app().await;

    // 1. 先添加一个笔记以便删除
    let note_data = json!({
        "content": "Note to be deleted",
        "tags": ["delete_test"]
    });
    let (create_status, create_body) =
        request(&app, axum::http::Method::POST, "/inbox/notes", note_data).await;
    println!("Delete Test: Create Status: {}", create_status);
    println!("Delete Test: Create Body: {}", create_body);
    assert_eq!(create_status, StatusCode::CREATED);
    // 从响应体中获取笔记 ID
    let note_id = create_body["id"]
        .as_i64()
        .expect("Note ID should be a number");

    // 2. 删除笔记
    let delete_uri = format!("/inbox/notes/{}", note_id);
    // DELETE 请求通常没有请求体，传一个空的 JSON 对象
    let (delete_status, delete_body) =
        request(&app, axum::http::Method::DELETE, &delete_uri, json!({})).await;
    println!("Delete Test: Delete Status: {}", delete_status);
    println!("Delete Test: Delete Body: {}", delete_body);

    // 断言状态码是 204 No Content
    assert_eq!(delete_status, StatusCode::NO_CONTENT);
    // 断言响应体是 null 或空的 JSON 对象/数组
    // json!({}) 会反序列化成一个空的 Object Value::Object({})
    assert!(
        delete_body.is_null()
            || (delete_body.is_object() && delete_body.as_object().unwrap().is_empty())
            || (delete_body.is_array() && delete_body.as_array().unwrap().is_empty()),
        "Expected empty or null body on successful delete"
    );

    // 3. 验证删除是否成功 (尝试获取该笔记)
    // GET 请求通常没有请求体，传一个空的 JSON 对象
    let (get_status, _) = request(&app, axum::http::Method::GET, &delete_uri, json!({})).await;
    println!("Delete Test: Get After Delete Status: {}", get_status);
    // 断言状态码是 404 Not Found
    assert_eq!(get_status, StatusCode::NOT_FOUND);

    // 重复删除验证
    // 再次尝试删除同一个 ID，应该仍然返回 Not Found
    let (repeat_status, _) =
        request(&app, axum::http::Method::DELETE, &delete_uri, json!({})).await;
    println!("Delete Test: Repeat Delete Status: {}", repeat_status);
    assert_eq!(repeat_status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_get_tags_detailed() {
    let app = setup_app().await;

    // 1. 添加一些带有标签的笔记
    let note1 = json!({ "content": "Note 1", "tags": ["tag1", "shared"] });
    let note2 = json!({ "content": "Note 2", "tags": ["tag2", "shared"] });
    let (status1, _) = request(&app, axum::http::Method::POST, "/inbox/notes", note1).await;
    let (status2, _) = request(&app, axum::http::Method::POST, "/inbox/notes", note2).await;
    assert_eq!(
        status1,
        StatusCode::CREATED,
        "Failed to create note 1 for tag test"
    );
    assert_eq!(
        status2,
        StatusCode::CREATED,
        "Failed to create note 2 for tag test"
    );

    // 2. 获取详细标签信息
    // GET 请求通常没有请求体
    let (status, body) = request(&app, axum::http::Method::GET, "/inbox/tags", json!({})).await;
    println!("Get Tags Status: {}", status);
    println!("Get Tags Body: {}", body);

    // 断言状态码是 200 OK
    assert_eq!(status, StatusCode::OK);
    // 断言响应体是一个 JSON 数组
    assert!(
        body.is_array(),
        "Response body should be an array, got: {}",
        body
    );

    // 将响应体转换为数组以便查找和断言
    let tags_array = body.as_array().expect("Body should be an array");

    // 查找 'shared' 标签并检查其计数
    let shared_tag = tags_array.iter().find(|tag| tag["name"] == "shared");
    assert!(shared_tag.is_some(), "'shared' tag should exist");
    // 安全地获取计数并断言其值
    assert_eq!(
        shared_tag.unwrap()["count"]
            .as_i64()
            .expect("Count should be a number"),
        2,
        "'shared' tag count should be 2"
    );

    // 查找 'tag1' 标签并检查其计数
    let tag1 = tags_array.iter().find(|tag| tag["name"] == "tag1");
    assert!(tag1.is_some(), "'tag1' tag should exist");
    assert_eq!(
        tag1.unwrap()["count"]
            .as_i64()
            .expect("Count should be a number"),
        1,
        "'tag1' tag count should be 1"
    );

    // 查找 'tag2' 标签并检查其计数
    let tag2 = tags_array.iter().find(|tag| tag["name"] == "tag2");
    assert!(tag2.is_some(), "'tag2' tag should exist");
    assert_eq!(
        tag2.unwrap()["count"]
            .as_i64()
            .expect("Count should be a number"),
        1,
        "'tag2' tag count should be 1"
    );

    // 测试添加空标签的笔记，验证它不影响标签列表或计数
    let note3 = json!({ "content": "Note without tags" });
    let (status3, _) = request(&app, axum::http::Method::POST, "/inbox/notes", note3).await;
    assert_eq!(
        status3,
        StatusCode::CREATED,
        "Failed to create note 3 for tag test"
    );

    // 再次获取标签列表
    let (_, body) = request(&app, axum::http::Method::GET, "/inbox/tags", json!({})).await;
    println!("Get Tags After Note without Tags Status: {}", status); // 状态码应该还是 OK
    println!("Get Tags After Note without Tags Body: {}", body);

    // 验证空标签不会出现在结果中
    let tags_array_after = body.as_array().expect("Body should still be an array");
    let empty_tag = tags_array_after
        .iter()
        .find(|tag| tag["name"].as_str().unwrap_or_default().is_empty() || tag["name"].is_null());
    assert!(empty_tag.is_none(), "Should not return empty or null tags");

    // 再次验证计数没有因为添加空标签笔记而改变
    let shared_tag_after = tags_array_after.iter().find(|tag| tag["name"] == "shared");
    assert!(
        shared_tag_after.is_some(),
        "'shared' tag should still exist after adding empty tag note"
    );
    assert_eq!(
        shared_tag_after.unwrap()["count"]
            .as_i64()
            .expect("Count should be a number"),
        2,
        "'shared' tag count should still be 2"
    );
    let tag1_after = tags_array_after.iter().find(|tag| tag["name"] == "tag1");
    assert!(
        tag1_after.is_some(),
        "'tag1' tag should still exist after adding empty tag note"
    );
    assert_eq!(
        tag1_after.unwrap()["count"]
            .as_i64()
            .expect("Count should be a number"),
        1,
        "'tag1' tag count should be 1"
    );
    let tag2_after = tags_array_after.iter().find(|tag| tag["name"] == "tag2");
    assert!(
        tag2_after.is_some(),
        "'tag2' tag should still exist after adding empty tag note"
    );
    assert_eq!(
        tag2_after.unwrap()["count"]
            .as_i64()
            .expect("Count should be a number"),
        1,
        "'tag2' tag count should be 1"
    );
}
