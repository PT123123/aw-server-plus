//! HTTP 同步传输：把本机导出的目标库快照通过 HTTP POST 推送到对端。
//!
//! 使用阻塞 reqwest（关闭 default TLS，局域网 HTTP 传输，避免 Android/桌面 TLS 依赖）。

use crate::models::{Device, SyncSnapshot};

/// 构造一个带超时的 blocking HTTP 客户端（局域网内使用，短连接）。
fn client() -> Result<reqwest::blocking::Client, String> {
    reqwest::blocking::Client::builder()
        .timeout(std::time::Duration::from_secs(30))
        .build()
        .map_err(|e| e.to_string())
}

/// 向对端发起配对请求：`POST http://<peer>/api/0/sync/pair/request`，body 为本机 device。
/// 对端若接受，会向本机回调 `pair/accept`。返回对端返回的 JSON。
pub fn send_pair_request(target: &Device, self_device: &Device) -> Result<serde_json::Value, String> {
    let url = format!("{}/pair/request", target.endpoint());
    crate::dbglog::info(format!("[pair] 向 {} 发起配对请求 ({url})", target.name));
    let resp = client()?
        .post(&url)
        .json(self_device)
        .send()
        .map_err(|e| format!("配对请求发送失败: {e}"))?;
    if !resp.status().is_success() {
        return Err(format!("配对请求被拒: HTTP {}", resp.status()));
    }
    resp.json().map_err(|e| format!("解析配对请求响应失败: {e}"))
}

/// 接受对端配对：`POST http://<peer>/api/0/sync/pair/accept`，body 为本机 device。
/// 对端收到后会把本机标记为已配对，并把自身信息返回给本机登记。
pub fn accept_pair(target: &Device, self_device: &Device) -> Result<serde_json::Value, String> {
    let url = format!("{}/pair/accept", target.endpoint());
    crate::dbglog::info(format!("[pair] 接受来自 {} 的配对 ({url})", target.name));
    let resp = client()?
        .post(&url)
        .json(self_device)
        .send()
        .map_err(|e| format!("接受配对失败: {e}"))?;
    if !resp.status().is_success() {
        return Err(format!("接受配对被拒: HTTP {}", resp.status()));
    }
    resp.json().map_err(|e| format!("解析接受配对响应失败: {e}"))
}

/// 向对端确认配对完成：`POST http://<peer>/api/0/sync/pair/confirm`，body 为本机 device。
pub fn confirm_pair(target: &Device, self_device: &Device) -> Result<serde_json::Value, String> {
    let url = format!("{}/pair/confirm", target.endpoint());
    crate::dbglog::info(format!("[pair] 向 {} 确认配对 ({url})", target.name));
    let resp = client()?
        .post(&url)
        .json(self_device)
        .send()
        .map_err(|e| format!("确认配对失败: {e}"))?;
    if !resp.status().is_success() {
        return Err(format!("确认配对被拒: HTTP {}", resp.status()));
    }
    resp.json().map_err(|e| format!("解析确认配对响应失败: {e}"))
}

/// 在线探测：`GET http://<peer>/api/0/sync/info`。成功即对端在线，返回本机信息。
/// 使用更短的超时（连接 2s + 读取 3s），并校验响应包含预期字段（device_id），防止误判。
pub fn probe_online(target: &Device) -> Result<serde_json::Value, String> {
    let url = format!("{}/info", target.endpoint());
    let probe_client = reqwest::blocking::Client::builder()
        .timeout(std::time::Duration::from_secs(3))
        .connect_timeout(std::time::Duration::from_secs(2))
        .build()
        .map_err(|e| format!("构建探测客户端失败: {e}"))?;

    let resp = probe_client
        .get(&url)
        .send()
        .map_err(|e| format!("探测连接失败: {e}"))?;

    if !resp.status().is_success() {
        return Err(format!("探测被拒: HTTP {}", resp.status()));
    }

    let json: serde_json::Value = resp.json().map_err(|e| format!("解析探测响应失败: {e}"))?;

    // 校验响应包含 self_device 字段（确认是 aw-sync 服务而非其他 HTTP 服务）
    if json.get("self_device").is_none() {
        return Err("探测响应缺少 self_device 字段，非 aw-sync 服务".to_string());
    }

    Ok(json)
}

/// 把一个同步快照推送到对端（`POST http://<peer>:<port>/api/0/sync/push`）。
/// `target` 为推送目标设备；`snapshot.source_device` 一般为本机（发送方）信息。
pub fn push_snapshot(target: &crate::models::Device, snapshot: &SyncSnapshot) -> Result<usize, String> {
    let url = format!("{}/push", target.endpoint());
    crate::dbglog::info(format!("[push] 开始推送到 {} ...", url));
    let client = reqwest::blocking::Client::builder()
        .timeout(std::time::Duration::from_secs(30))
        .build()
        .map_err(|e| e.to_string())?;
    let resp = client
        .post(&url)
        .json(snapshot)
        .send()
        .map_err(|e| format!("发送同步请求到 {url} 失败: {e}"))?;
    if !resp.status().is_success() {
        crate::dbglog::warn(format!("[push] 推送失败: HTTP {} ({url})", resp.status()));
        return Err(format!("对端返回非成功状态 {} (url: {url})", resp.status()));
    }
    let body: serde_json::Value = resp.json().map_err(|e| e.to_string())?;
    let applied = body.get("applied").and_then(|v| v.as_u64()).unwrap_or(0);
    crate::dbglog::info(format!("[push] 推送完成: 对端应用记录数 {}", applied));
    Ok(applied as usize)
}

#[cfg(test)]
mod tests {
    #[test]
    fn endpoint_format() {
        let url = crate::models::Device {
            id: "d".into(),
            name: "n".into(),
            device_kind: crate::models::DeviceKind::Linux,
            ip: "192.168.1.5".into(),
            port: 56001,
            paired_at: chrono::Utc::now(),
            last_sync_at: None,
        last_seen_at: None,
                        is_online: true,
            is_self: false,
            paired: false,
            alias: None,
        }
        .endpoint();
        assert_eq!(url, "http://192.168.1.5:56001/api/0/sync");
    }
}