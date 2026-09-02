use serde_json::json;
use std::net::TcpListener;
use std::process::Command;
use std::str;
use tokio::time::{sleep, Duration};

fn is_port_occupied(port: u16) -> bool {
    TcpListener::bind(("127.0.0.1", port)).is_err()
}

fn handle_curl_output(output: &std::process::Output) {
    match std::str::from_utf8(&output.stdout) {
        Ok(output_str) => {
            let status_line = output_str.lines().find(|l| l.starts_with("HTTP/1.1"));
            let mut status_code = 0;
            if let Some(status_line) = status_line {
                let status_code_str = status_line.split_whitespace().nth(1);
                if let Some(status_code_str) = status_code_str {
                    if let Ok(code) = status_code_str.parse() {
                        status_code = code;
                    }
                }
            }
            let body_str = output_str.rsplitn(2, "\r\n\r\n").next().unwrap_or("");
            match status_code {
                404 => println!("404 Not Found, body: {}", body_str.trim()),
                204 => println!("204 No Content"),
                _ => println!("HTTP {} body: {}", status_code, body_str.trim()),
            }
        }
        Err(_) => {
            println!("Output is not valid UTF-8");
        }
    }
}

#[tokio::test]
async fn test_note_crud_operations() {
    unsafe {
        std::env::set_var("RUST_LOG", "info");
    }
    let mut note_id = 0;
    // 杀掉占用 5600 端口的进程，并确认端口已释放
    let mut port_cleared = false;
    for i in 0..10 {
        let _ = std::process::Command::new("sh")
            .arg("-c")
            .arg("fuser -k 5600/tcp || true")
            .status();
        // 检查端口是否还被占用
        if !is_port_occupied(5600) {
            println!("Port 5600 is now free after {} attempt(s)", i + 1);
            port_cleared = true;
            break;
        }
        println!(
            "Port 5600 still occupied after kill attempt {}, sleeping...",
            i + 1
        );
        sleep(Duration::from_millis(300)).await;
    }
    assert!(
        port_cleared,
        "Port 5600 could not be cleared after multiple attempts"
    );

    // 启动后台服务器进程
    let shell_script = r#"env ROCKET_CONFIG=aw-inbox-rust/Rocket.toml ./target/debug/aw-inbox-rust & echo $! > /tmp/aw_inbox_test_server.pid"#;
    let _ = std::process::Command::new("sh")
        .arg("-c")
        .arg(shell_script)
        .status()
        .expect("Failed to start server with shell");

    // 读取后台进程PID
    let pid_str = std::fs::read_to_string("/tmp/aw_inbox_test_server.pid").expect("read pid");
    let server_pid: i32 = pid_str.trim().parse().expect("parse pid");
    println!("[TEST] 启动服务进程 PID: {}", server_pid);

    // 等待服务端口真正 ready
    let mut ready = false;
    for _ in 0..20 {
        if std::process::Command::new("sh")
            .arg("-c")
            .arg("nc -z 127.0.0.1:5600")
            .status()
            .map(|s| s.success())
            .unwrap_or(false)
        {
            ready = true;
            break;
        }
        sleep(Duration::from_millis(300)).await;
    }
    assert!(ready, "Server did not become ready in time");

    // 0. 创建前先查一次，应该404
    let pre_create_uri = "/inbox/notes/99999"; // 用不存在的id
    println!(
        "[PRE-CREATE GET] 请求: GET http://localhost:5600{}",
        pre_create_uri
    );
    let output = Command::new("curl")
        .args([
            "-i",
            "-X",
            "GET",
            &format!("http://localhost:5600{}", pre_create_uri),
        ])
        .output();
    match output {
        Ok(output) => {
            handle_curl_output(&output);
            let output_str = str::from_utf8(&output.stdout).unwrap();
            let status_line = output_str
                .lines()
                .find(|l| l.starts_with("HTTP/1.1"))
                .expect("No HTTP status line");
            let status_code: u16 = status_line
                .split_whitespace()
                .nth(1)
                .expect("No status code")
                .parse()
                .expect("Status code parse error");
            assert_eq!(status_code, 404, "Pre-create GET should be 404");
        }
        Err(e) => {
            println!("Failed to execute curl command: {}", e);
        }
    }

    // 1. 创建笔记
    let note_data = json!({
        "content": "测试笔记内容",
        "tags": ["test", "rust"]
    });

    println!(
        "[CREATE] 请求: POST http://localhost:5600/inbox/notes\n请求体: {}",
        note_data
    );
    let output = Command::new("curl")
        .args([
            "-i",
            "-X",
            "POST",
            "http://localhost:5600/inbox/notes",
            "-H",
            "Content-Type: application/json",
            "-d",
            &note_data.to_string(),
        ])
        .output();

    match output {
        Ok(output) => {
            handle_curl_output(&output);
            let output_str = str::from_utf8(&output.stdout).unwrap();
            let status_line = output_str
                .lines()
                .find(|l| l.starts_with("HTTP/1.1"))
                .expect("No HTTP status line");
            let status_code: u16 = status_line
                .split_whitespace()
                .nth(1)
                .expect("No status code")
                .parse()
                .expect("Status code parse error");
            let body_str = output_str.rsplitn(2, "\r\n\r\n").next().unwrap_or("");
            assert_eq!(
                status_code,
                201,
                "Create note unexpected status: {status_code}, stderr: {}",
                String::from_utf8_lossy(&output.stderr)
            );
            let create_body: serde_json::Value = serde_json::from_str(body_str.trim()).unwrap();
            note_id = create_body["id"].as_i64().expect("笔记ID应为数字");
            assert!(note_id > 0, "笔记ID应为正数");
            assert_eq!(create_body["content"], note_data["content"]);
            assert_eq!(create_body["tags"], note_data["tags"]);
        }
        Err(e) => {
            println!("Failed to execute curl command: {}", e);
        }
    }

    // 2. 创建后再次查询
    let get_uri = format!("/inbox/notes/{}", note_id);
    println!(
        "[POST-CREATE GET] 请求: GET http://localhost:5600{}",
        get_uri
    );
    let output = Command::new("curl")
        .args([
            "-i",
            "-X",
            "GET",
            &format!("http://localhost:5600{}", get_uri),
        ])
        .output();

    match output {
        Ok(output) => {
            handle_curl_output(&output);
            let output_str = str::from_utf8(&output.stdout).unwrap();
            let status_line = output_str
                .lines()
                .find(|l| l.starts_with("HTTP/1.1"))
                .expect("No HTTP status line");
            let status_code: u16 = status_line
                .split_whitespace()
                .nth(1)
                .expect("No status code")
                .parse()
                .expect("Status code parse error");
            let body_str = output_str.rsplitn(2, "\r\n\r\n").next().unwrap_or("");
            assert_eq!(
                status_code,
                200,
                "Get note unexpected status: {status_code}, stderr: {}",
                String::from_utf8_lossy(&output.stderr)
            );
            let get_body: serde_json::Value = serde_json::from_str(body_str.trim()).unwrap();
            assert_eq!(get_body["id"], note_id);
            assert_eq!(get_body["content"], note_data["content"]);
            assert_eq!(get_body["tags"], note_data["tags"]);
        }
        Err(e) => {
            println!("Failed to execute curl command: {}", e);
        }
    }

    // 3. 删除笔记
    println!("[DELETE] 请求: DELETE http://localhost:5600{}", get_uri);
    let output = Command::new("curl")
        .args([
            "-i",
            "-X",
            "DELETE",
            &format!("http://localhost:5600{}", get_uri),
        ])
        .output();

    match output {
        Ok(output) => {
            handle_curl_output(&output);
            let output_str = str::from_utf8(&output.stdout).unwrap();
            let status_line = output_str
                .lines()
                .find(|l| l.starts_with("HTTP/1.1"))
                .expect("No HTTP status line");
            let status_code: u16 = status_line
                .split_whitespace()
                .nth(1)
                .expect("No status code")
                .parse()
                .expect("Status code parse error");
            assert_eq!(
                status_code,
                204,
                "Delete note unexpected status: {status_code}, stderr: {}",
                String::from_utf8_lossy(&output.stderr)
            );
        }
        Err(e) => {
            println!("Failed to execute curl command: {}", e);
        }
    }

    // 4. 验证删除
    println!("[VERIFY DELETE] 请求: GET http://localhost:5600{}", get_uri);
    let output = Command::new("curl")
        .args([
            "-i",
            "-X",
            "GET",
            &format!("http://localhost:5600{}", get_uri),
        ])
        .output();

    match output {
        Ok(output) => {
            handle_curl_output(&output);
            let output_str = str::from_utf8(&output.stdout).unwrap();
            let status_line = output_str
                .lines()
                .find(|l| l.starts_with("HTTP/1.1"))
                .expect("No HTTP status line");
            let status_code: u16 = status_line
                .split_whitespace()
                .nth(1)
                .expect("No status code")
                .parse()
                .expect("Status code parse error");
            assert_eq!(
                status_code,
                404,
                "Verify delete unexpected status: {status_code}, stderr: {}",
                String::from_utf8_lossy(&output.stderr)
            );
        }
        Err(e) => {
            println!("Failed to execute curl command: {}", e);
        }
    }

    // 5. 批量获取笔记（GET /inbox/notes）
    println!("[LIST] 请求: GET http://localhost:5600/inbox/notes");
    let output = Command::new("curl")
        .args(["-i", "-X", "GET", "http://localhost:5600/inbox/notes"])
        .output();
    match output {
        Ok(output) => {
            handle_curl_output(&output);
            let output_str = str::from_utf8(&output.stdout).unwrap();
            let status_line = output_str
                .lines()
                .find(|l| l.starts_with("HTTP/1.1"))
                .expect("No HTTP status line");
            let status_code: u16 = status_line
                .split_whitespace()
                .nth(1)
                .expect("No status code")
                .parse()
                .expect("Status code parse error");
            assert_eq!(
                status_code,
                200,
                "List notes unexpected status: {status_code}, stderr: {}",
                String::from_utf8_lossy(&output.stderr)
            );
            // 可选: 检查 body 为数组
        }
        Err(e) => {
            println!("Failed to execute curl command: {}", e);
        }
    }

    // 6. 获取所有标签（GET /inbox/tags）
    println!("[TAGS] 请求: GET http://localhost:5600/inbox/tags");
    let output = Command::new("curl")
        .args(["-i", "-X", "GET", "http://localhost:5600/inbox/tags"])
        .output();
    match output {
        Ok(output) => {
            handle_curl_output(&output);
            let output_str = str::from_utf8(&output.stdout).unwrap();
            let status_line = output_str
                .lines()
                .find(|l| l.starts_with("HTTP/1.1"))
                .expect("No HTTP status line");
            let status_code: u16 = status_line
                .split_whitespace()
                .nth(1)
                .expect("No status code")
                .parse()
                .expect("Status code parse error");
            assert_eq!(
                status_code,
                200,
                "Get tags unexpected status: {status_code}, stderr: {}",
                String::from_utf8_lossy(&output.stderr)
            );
            // 可选: 检查 body 为数组
        }
        Err(e) => {
            println!("Failed to execute curl command: {}", e);
        }
    }

    // 7. 更新笔记（PUT /inbox/notes/:id），用已删除的id应404
    println!("[UPDATE] 请求: PUT http://localhost:5600{}", get_uri);
    let update_body = json!({"content": "new content", "tags": ["updated"]});
    let output = Command::new("curl")
        .args([
            "-i",
            "-X",
            "PUT",
            "-H",
            "Content-Type: application/json",
            "-d",
            &update_body.to_string(),
            &format!("http://localhost:5600{}", get_uri),
        ])
        .output();
    match output {
        Ok(output) => {
            handle_curl_output(&output);
            let output_str = str::from_utf8(&output.stdout).unwrap();
            let status_line = output_str
                .lines()
                .find(|l| l.starts_with("HTTP/1.1"))
                .expect("No HTTP status line");
            let status_code: u16 = status_line
                .split_whitespace()
                .nth(1)
                .expect("No status code")
                .parse()
                .expect("Status code parse error");
            assert_eq!(status_code, 404, "Update deleted note should be 404");
        }
        Err(e) => {
            println!("Failed to execute curl command: {}", e);
        }
    }

    // 测试结束后关闭后台服务器进程
    let _ = std::process::Command::new("sh")
        .arg("-c")
        .arg(format!("kill {} || true", server_pid))
        .status();
}
