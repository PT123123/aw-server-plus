use std::path::PathBuf;

#[cfg(not(target_os = "android"))]
use std::fs;

#[cfg(target_os = "android")]
use std::sync::Mutex;

#[cfg(target_os = "android")]
use log::{debug, error};

#[cfg(target_os = "android")]
lazy_static! {
    static ref ANDROID_DATA_DIR: Mutex<PathBuf> = Mutex::new(PathBuf::from(
        "/data/user/0/net.activitywatch.android/files"
    ));
}

#[cfg(not(target_os = "android"))]
pub fn get_config_dir() -> Result<PathBuf, ()> {
    let dir = dirs::config_dir()
        .ok_or(())?
        .join("activitywatch")
        .join("aw-server-rust");
    fs::create_dir_all(&dir).expect("Unable to create config dir");
    Ok(dir)
}

#[cfg(target_os = "android")]
pub fn get_config_dir() -> Result<PathBuf, ()> {
    Ok(ANDROID_DATA_DIR.lock().unwrap().to_path_buf())
}

#[cfg(not(target_os = "android"))]
pub fn get_data_dir() -> Result<PathBuf, ()> {
    let dir = dirs::data_dir()
        .ok_or(())?
        .join("activitywatch")
        .join("aw-server-rust");
    fs::create_dir_all(&dir).expect("Unable to create data dir");
    Ok(dir)
}

#[cfg(target_os = "android")]
pub fn get_data_dir() -> Result<PathBuf, ()> {
    debug!("[修改版本2] get_data_dir 开始执行");

    // 尝试获取锁，并处理可能的错误
    debug!("[修改版本2] 尝试获取 ANDROID_DATA_DIR 锁");
    let lock_result = ANDROID_DATA_DIR.lock();

    match lock_result {
        Ok(android_data_dir) => {
            debug!("[修改版本2] 成功获取 ANDROID_DATA_DIR 锁");
            let path = android_data_dir.to_path_buf();
            debug!("[修改版本2] 当前数据目录路径: {:?}", path);
            debug!("[修改版本2] get_data_dir 执行完成");
            Ok(path)
        }
        Err(e) => {
            // 锁可能已经被毒化(poisoned)，这通常发生在另一个线程在持有锁时发生了panic
            error!("[修改版本2] 无法获取 ANDROID_DATA_DIR 锁: {:?}", e);

            // 尝试获取被毒化的锁
            if let Ok(poisoned_lock) = ANDROID_DATA_DIR.lock() {
                error!("[修改版本2] 获取到被毒化的锁，尝试恢复");
                let path = poisoned_lock.to_path_buf();
                debug!("[修改版本2] 使用被毒化的锁获取路径: {:?}", path);
                Ok(path)
            } else {
                error!("[修改版本2] 无法恢复被毒化的锁，返回默认路径");
                // 返回一个默认路径，避免崩溃
                Ok(PathBuf::from(
                    "/data/user/0/net.activitywatch.android/files",
                ))
            }
        }
    }
}

#[cfg(not(target_os = "android"))]
pub fn get_cache_dir() -> Result<PathBuf, ()> {
    let dir = dirs::cache_dir()
        .ok_or(())?
        .join("activitywatch")
        .join("aw-server-rust");
    fs::create_dir_all(&dir).expect("Unable to create cache dir");
    Ok(dir)
}

#[cfg(target_os = "android")]
pub fn get_cache_dir() -> Result<PathBuf, ()> {
    panic!("not implemented on Android");
}

#[cfg(not(target_os = "android"))]
pub fn get_log_dir(module: &str) -> Result<PathBuf, ()> {
    let dir = get_user_log_dir()?.join(module);
    fs::create_dir_all(&dir).expect("Unable to create log dir");
    Ok(dir)
}

/// Returns the platform-appropriate log directory for ActivityWatch.
///
/// Replicates the behavior of the old `appdirs::user_log_dir("activitywatch")`:
/// - Linux:   ~/.cache/activitywatch/log/
/// - macOS:   ~/Library/Logs/activitywatch/
/// - Windows: {LOCALAPPDATA}\activitywatch\Logs\
#[cfg(target_os = "linux")]
fn get_user_log_dir() -> Result<PathBuf, ()> {
    Ok(dirs::cache_dir()
        .ok_or(())?
        .join("activitywatch")
        .join("log"))
}

#[cfg(target_os = "macos")]
fn get_user_log_dir() -> Result<PathBuf, ()> {
    Ok(dirs::home_dir()
        .ok_or(())?
        .join("Library")
        .join("Logs")
        .join("activitywatch"))
}

#[cfg(target_os = "windows")]
fn get_user_log_dir() -> Result<PathBuf, ()> {
    Ok(dirs::data_local_dir()
        .ok_or(())?
        .join("activitywatch")
        .join("Logs"))
}

#[cfg(target_os = "android")]
pub fn get_log_dir(module: &str) -> Result<PathBuf, ()> {
    panic!("not implemented on Android");
}

pub fn db_path(testing: bool) -> Result<PathBuf, ()> {
    debug!("[修改版本2] db_path 开始执行，testing: {}", testing);

    debug!("[修改版本2] 步骤1: 调用 get_data_dir 获取数据目录");
    let data_dir_result = get_data_dir();

    match data_dir_result {
        Ok(mut db_path) => {
            debug!("[修改版本2] 成功获取数据目录: {:?}", db_path);

            debug!("[修改版本2] 步骤2: 添加数据库文件名");
            if testing {
                debug!("[修改版本2] 使用测试数据库文件名");
                db_path.push("sqlite-testing.db");
            } else {
                debug!("[修改版本2] 使用正式数据库文件名");
                db_path.push("sqlite.db");
            }

            debug!("[修改版本2] 最终数据库路径: {:?}", db_path);
            debug!("[修改版本2] db_path 执行完成");
            Ok(db_path)
        }
        Err(e) => {
            error!("[修改版本2] 获取数据目录失败: {:?}", e);

            // 创建一个默认路径，避免崩溃
            let mut default_path = PathBuf::from("/data/user/0/net.activitywatch.android/files");
            if testing {
                default_path.push("sqlite-testing.db");
            } else {
                default_path.push("sqlite.db");
            }

            error!("[修改版本2] 使用默认数据库路径: {:?}", default_path);
            Ok(default_path)
        }
    }
}

#[cfg(target_os = "android")]
pub fn set_android_data_dir(path: &str) {
    debug!("[修改版本2] set_android_data_dir 开始执行，路径: {}", path);

    // 尝试获取锁，并处理可能的错误
    debug!("[修改版本2] 尝试获取 ANDROID_DATA_DIR 锁");
    let lock_result = ANDROID_DATA_DIR.lock();

    match lock_result {
        Ok(mut android_data_dir) => {
            debug!("[修改版本2] 成功获取 ANDROID_DATA_DIR 锁");
            debug!(
                "[修改版本2] 当前路径: {:?}, 准备更新为: {}",
                *android_data_dir, path
            );

            // 更新路径
            *android_data_dir = PathBuf::from(path);

            debug!("[修改版本2] 路径已更新为: {:?}", *android_data_dir);
        }
        Err(e) => {
            // 锁可能已经被毒化(poisoned)，这通常发生在另一个线程在持有锁时发生了panic
            error!("[修改版本2] 无法获取 ANDROID_DATA_DIR 锁: {:?}", e);

            // 尝试获取被毒化的锁
            if let Ok(mut poisoned_lock) = ANDROID_DATA_DIR.lock() {
                error!("[修改版本2] 获取到被毒化的锁，尝试恢复");
                *poisoned_lock = PathBuf::from(path);
                debug!("[修改版本2] 使用被毒化的锁更新路径为: {:?}", *poisoned_lock);
            } else {
                error!("[修改版本2] 无法恢复被毒化的锁，放弃设置数据目录");
            }
        }
    }

    debug!("[修改版本2] set_android_data_dir 执行完成");
}

#[test]
fn test_get_dirs() {
    #[cfg(target_os = "android")]
    set_android_data_dir("/test");

    get_cache_dir().unwrap();
    get_log_dir("aw-server-rust").unwrap();
    db_path(true).unwrap();
    db_path(false).unwrap();
}
