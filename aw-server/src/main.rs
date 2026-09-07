#[macro_use]
extern crate log;

use std::env;
use std::path::PathBuf;

use clap::crate_version;
use clap::Parser;

use aw_server::*;

#[cfg(target_os = "linux")]
use sd_notify::NotifyState;
#[cfg(all(target_os = "linux", target_arch = "x86"))]
extern crate jemallocator;
#[cfg(all(target_os = "linux", target_arch = "x86"))]
#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

/// Rust server for ActivityWatch
#[derive(Parser)]
#[clap(version = crate_version!(), author = "Johan Bjäreholt, Erik Bjäreholt, et al.")]
struct Opts {
    /// Run in testing mode
    #[clap(long)]
    testing: bool,

    /// Verbose output
    #[clap(long)]
    verbose: bool,

    /// Address to listen to
    #[clap(long)]
    host: Option<String>,

    /// Port to listen on
    #[clap(long)]
    port: Option<String>,

    /// Path to database override
    /// Also implies --no-legacy-import if no db found
    #[clap(long)]
    dbpath: Option<String>,

    /// Path to config file override
    #[clap(short = 'c', long = "config")]
    config: Option<PathBuf>,

    /// Mapping of custom static paths to serve, in the format: watcher1=/path,watcher2=/path2
    #[clap(long)]
    custom_static: Option<String>,

    /// Device ID override
    #[clap(long)]
    device_id: Option<String>,

    /// Don't import from aw-server-python if no aw-server-rust db found
    #[clap(long)]
    no_legacy_import: bool,

    /// Encryption key for the database (requires 'encryption' feature).
    /// Can also be set via the AW_DB_PASSWORD environment variable.
    /// WARNING: passing a password on the command line may expose it in process listings.
    #[clap(long, env = "AW_DB_PASSWORD")]
    #[cfg(any(feature = "encryption", feature = "encryption-vendored"))]
    db_password: Option<String>,
}

#[rocket::main]
#[allow(clippy::result_large_err)]
async fn main() -> Result<(), rocket::Error> {
    let opts: Opts = Opts::parse();

    // Clear sensitive env vars immediately after parse, before setup_logger can start
    // background threads (std::env::remove_var is not thread-safe on all platforms).
    #[cfg(any(feature = "encryption", feature = "encryption-vendored"))]
    std::env::remove_var("AW_DB_PASSWORD");

    let mut testing = opts.testing;

    // Always override environment if --testing is specified
    if !testing && cfg!(debug_assertions) {
        testing = true;
    }

    logging::setup_logger("aw-server-rust", testing, opts.verbose)
        .expect("Failed to setup logging");

    if testing {
        info!("Running server in Testing mode");
    }

    let mut config = config::create_config(testing, opts.config.as_deref());

    // set host if overridden
    if let Some(host) = opts.host {
        config.address = host;
    }

    // set port if overridden
    if let Some(port) = opts.port {
        config.port = port.parse().unwrap();
    }

    // set custom_static if overridden, transform into map
    if let Some(custom_static_str) = opts.custom_static {
        let custom_static_map: std::collections::HashMap<String, String> = custom_static_str
            .split(',')
            .map(|s| {
                let mut split = s.split('=');
                let key = split.next().unwrap().to_string();
                let value = split.next().unwrap().to_string();
                (key, value)
            })
            .collect();
        config.custom_static.extend(custom_static_map);

        // validate paths, log error if invalid
        // remove invalid paths
        for (name, path) in config.custom_static.clone().iter() {
            if !std::path::Path::new(path).exists() {
                error!("custom_static path for {} does not exist ({})", name, path);
                config.custom_static.remove(name);
            }
        }
    }

    // Set db path if overridden
    let db_path: String = if let Some(dbpath) = opts.dbpath.clone() {
        dbpath
    } else {
        dirs::db_path(testing)
            .expect("Failed to get db path")
            .to_str()
            .unwrap()
            .to_string()
    };
    info!("Using DB at path {:?}", db_path);

    // 同步子系统（LAN 快照 + D1 云同步）的内容目录：与 UI 实际使用的内容库同目录，
    // 即 --dbpath 的父目录；未指定 --dbpath 时回退 ActivityWatch 默认数据目录。
    let sync_content_dir = std::path::Path::new(&db_path)
        .parent()
        .map(|p| p.to_path_buf())
        .or_else(|| dirs::get_data_dir().ok());

    // Only use legacy import if opts.dbpath is not set
    let legacy_import = !opts.no_legacy_import && opts.dbpath.is_none();
    if opts.dbpath.is_some() {
        info!("Since custom dbpath is set, --no-legacy-import is implied");
    }

    let device_id: String = if let Some(id) = opts.device_id {
        id
    } else {
        device_id::get_device_id()
    };

    #[cfg(any(feature = "encryption", feature = "encryption-vendored"))]
    let datastore = match opts.db_password {
        Some(key) if key.is_empty() => {
            // SQLCipher silently treats PRAGMA key '' as no encryption — reject early so
            // callers don't end up with a plaintext database while believing it is encrypted.
            panic!("--db-password / AW_DB_PASSWORD must not be empty; aborting to prevent silent plaintext storage");
        }
        Some(key) => {
            info!("Using encrypted database (SQLCipher)");
            aw_datastore::Datastore::new_encrypted(db_path, key, legacy_import)
        }
        None => aw_datastore::Datastore::new(db_path, legacy_import),
    };
    #[cfg(not(any(feature = "encryption", feature = "encryption-vendored")))]
    {
        if std::env::var("AW_DB_PASSWORD").is_ok() {
            panic!(
                "AW_DB_PASSWORD is set but this binary was not compiled with encryption support. \
                 Refusing to start with an unencrypted database when the user requested encryption. \
                 Rebuild with the 'encryption' or 'encryption-vendored' feature, or unset \
                 AW_DB_PASSWORD to use an unencrypted database."
            );
        }
    }
    #[cfg(not(any(feature = "encryption", feature = "encryption-vendored")))]
    let datastore = aw_datastore::Datastore::new(db_path, legacy_import);

    let server_state = endpoints::ServerState {
        // Even if legacy_import is set to true it is disabled on Android so
        // it will not happen there
        datastore,
        device_id,
    };
    let sync_device_id = server_state.device_id.clone();

    let rocket = endpoints::build_rocket(server_state, config);

    // 初始化 aw-inbox-rust 的数据库连接池并注入
    use aw_inbox_rust::db;
    use aw_inbox_rust::{SharedDb, SharedTodoDb};
    use std::sync::Arc;
    use std::sync::Mutex as StdMutex;
    let pool = db::init_pool().await.expect("Failed to init inbox db pool");
    db::migrate(&pool).expect("数据库迁移失败");
    let shared_db: SharedDb = Arc::new(StdMutex::new(pool));
    let todo_pool = db::init_todo_pool().await.expect("Failed to init todo db pool");
    db::migrate_todo(&todo_pool).expect("todo 数据库迁移失败");
    let shared_todo_db: SharedTodoDb = SharedTodoDb(Arc::new(StdMutex::new(todo_pool)));
    let rocket = plugins::register_all_plugins(rocket, shared_db, shared_todo_db);
    let mut rocket = rocket;

    // ===== 挂载局域网同步 (aw-sync-rust) =====
    if let Some(data_dir_sync) = sync_content_dir {
        // 一次性迁移：旧默认目录中的 sync.db（D1 配置 + LAN 配对记录）搬到内容目录
        let sync_db = data_dir_sync.join("sync.db");
        if !sync_db.exists() {
            if let Ok(legacy_dir) = dirs::get_data_dir() {
                let legacy_sync_db = legacy_dir.join("sync.db");
                if legacy_sync_db.exists() {
                    match std::fs::copy(&legacy_sync_db, &sync_db) {
                        Ok(_) => info!("已迁移 sync.db 到内容数据目录: {:?}", sync_db),
                        Err(e) => info!("sync.db 迁移失败(以空配置继续启动): {e}"),
                    }
                }
            }
        }
        match aw_sync_rust::SyncManager::new(data_dir_sync.as_path(), sync_device_id) {
            Ok(mgr) => {
                if let Ok(g) = mgr.lock() {
                    let _ = g.spawn_discovery();
                    // 在线探测循环（循环内按 enabled 门控；is_online 供自动同步过滤与前端展示）
                    let _ = g.spawn_probe();
                    // D1 云同步后台线程（按 d1_sync_interval 周期触发 D1 双向同步）
                    let _ = g.spawn_d1_sync();
                }
                // 局域网自动同步循环（enabled 时按 sync_interval 周期对所有已配对设备双向同步）
                let _ = aw_sync_rust::SyncManager::spawn_auto_sync(&mgr);
                rocket = aw_sync_rust::endpoints::mount_rocket(rocket, mgr);
                info!("局域网同步路由已挂载 (aw-sync-rust)，后台线程已启动（发现/探测/自动同步/D1）");
            }
            Err(e) => info!("局域网同步挂载失败(继续启动): {e}"),
        }
    }

    let _rocket = rocket.ignite().await?;
    #[cfg(target_os = "linux")]
    let _ = sd_notify::notify(true, &[NotifyState::Ready]);
    _rocket.launch().await?;

    Ok(())
}
