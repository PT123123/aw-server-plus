use rocket::http::Method;
use rocket_cors::{AllowedHeaders, AllowedOrigins};

use crate::config::AWConfig;

pub fn cors(config: &AWConfig) -> rocket_cors::Cors {
    let root_url = format!("http://127.0.0.1:{}", config.port);
    let root_url_localhost = format!("http://localhost:{}", config.port);
    let mut allowed_exact_origins = vec![root_url, root_url_localhost];
    allowed_exact_origins.extend(config.cors.clone());

    // 添加对 5600 和 5601 端口的允许 (HTTP 和 WebSocket)
    allowed_exact_origins.push("http://localhost:5600".to_string());
    allowed_exact_origins.push("http://localhost:5601".to_string());
    allowed_exact_origins.push("http://127.0.0.1:5600".to_string());
    allowed_exact_origins.push("http://127.0.0.1:5601".to_string());

    // 如果你的前端通过 WebSocket 连接到这些端口，你可能还需要添加到 allowed_origins
    // 但 AllowedOrigins::some 主要处理 HTTP(S)
    // 对于 WebSocket，浏览器通常不会发送 Origin 头进行预检
    // 你可能需要在你的后端 WebSocket 处理逻辑中进行额外的来源验证

    if config.testing {
        allowed_exact_origins.push("http://127.0.0.1:27180".to_string());
        allowed_exact_origins.push("http://localhost:27180".to_string());
    }
    let mut allowed_regex_origins = vec![
        "chrome-extension://nglaklhklhcoonedhgnpgddginnjdadi".to_string(),
        // Every version of a mozilla extension has its own ID to avoid fingerprinting, so we
        // unfortunately have to allow all extensions to have access to aw-server.
        // The endpoints this wildcard actually reaches are narrowed to the ones
        // aw-watcher-web needs by the ExtensionCorsScope fairing — see
        // endpoints/extension_cors.rs.
        "moz-extension://.*".to_string(),
    ];
    allowed_regex_origins.extend(config.cors_regex.clone());
    if config.testing {
        allowed_regex_origins.push("chrome-extension://.*".to_string());
    }

    let allowed_origins = AllowedOrigins::some(&allowed_exact_origins, &allowed_regex_origins);
    let allowed_methods = vec![Method::Get, Method::Post, Method::Delete]
        .into_iter()
        .map(From::from)
        .collect();
    let allowed_headers = AllowedHeaders::all(); // TODO: is this unsafe?

    // You can also deserialize this
    rocket_cors::CorsOptions {
        allowed_origins,
        allowed_methods,
        allowed_headers,
        allow_credentials: false,
        ..Default::default()
    }
    .to_cors()
    .expect("Failed to set up CORS")
}