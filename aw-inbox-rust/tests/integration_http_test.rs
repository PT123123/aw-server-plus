// 集成 HTTP 测试：启动主程序后用 reqwest 访问
// 运行此测试前请确保主程序未占用 8080 端口或修改端口

use reqwest::blocking::Client;
use reqwest::StatusCode;
use std::{
    process::{Child, Command, Stdio},
    thread,
    time::Duration,
};

fn spawn_server() -> Child {
    Command::new("cargo")
        .arg("run")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("Failed to start server")
}

#[test]
fn test_server_add_note() {
    // 启动主程序
    let mut server = spawn_server();
    // 等待服务器启动（可根据实际情况调整时间）
    thread::sleep(Duration::from_secs(5));

    let client = Client::new();
    let url = "http://127.0.0.1:5600/inbox/notes";
    let payload = serde_json::json!({
        "content": "integration http test note",
        "tags": ["http", "integration"]
    });
    let resp = client
        .post(url)
        .json(&payload)
        .send()
        .expect("Failed to send request");
    assert_eq!(
        resp.status(),
        StatusCode::CREATED,
        "Add note failed: {:?}",
        resp
    );

    // 关闭服务器
    let _ = server.kill();
}
