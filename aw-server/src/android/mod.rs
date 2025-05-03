// Based On the following guide from Mozilla:
//   https://mozilla.github.io/firefox-browser-architecture/experiments/2017-09-21-rust-on-android.html

extern crate android_logger;

use std::ffi::{CStr, CString};
use std::os::raw::c_char;

use crate::device_id;
use crate::dirs;

use android_logger::Config;
use rocket::serde::json::json;

#[no_mangle]
pub extern "C" fn rust_greeting(to: *const c_char) -> *mut c_char {
    // 检查输入指针是否为空
    if to.is_null() {
        error!("rust_greeting 收到空指针");
        return std::ptr::null_mut();
    }
    
    // 使用std::panic::catch_unwind捕获可能的panic
    match std::panic::catch_unwind(|| {
        let c_str = unsafe { CStr::from_ptr(to) };
        let recipient = match c_str.to_str() {
            Err(e) => {
                error!("无法将C字符串转换为Rust字符串: {:?}", e);
                "there"
            },
            Ok(string) => string,
        };

        match CString::new("Hello ".to_owned() + recipient + " (from Rust!)") {
            Ok(result) => result.into_raw(),
            Err(e) => {
                error!("无法创建CString: {:?}", e);
                std::ptr::null_mut()
            }
        }
    }) {
        Ok(result) => result,
        Err(e) => {
            error!("rust_greeting处理时发生panic: {:?}", e);
            std::ptr::null_mut()
        }
    }
}

#[cfg(target_os = "android")]
#[allow(non_snake_case)]
pub mod android {
    extern crate jni;

    use self::jni::objects::{JClass, JString, JObject};
    use self::jni::sys::{jdouble, jint, jstring};
    use self::jni::JNIEnv;
    use super::*;

    use std::path::PathBuf;

    use crate::endpoints;
    use crate::endpoints::ServerState;
    use aw_client_rust::blocking::AwClient;
    use aw_client_rust::classes::default_classes;
    use aw_client_rust::classes::{CategoryId, CategorySpec};
    use aw_client_rust::queries::{
        build_android_canonical_events, AndroidQueryParams, QueryParamsBase,
    };
    use aw_datastore::Datastore;
    use aw_models::{Bucket, Event, TimeInterval};

    static mut DATASTORE: Option<Datastore> = None;

    unsafe fn openDatastore() -> Datastore {
        debug!("开始检查数据存储实例是否已存在...");
        match DATASTORE {
            Some(ref ds) => {
                debug!("数据存储实例已存在，返回现有实例...");
                ds.clone()
            },
            None => {
                debug!("数据存储实例不存在，开始获取数据库路径...");
                let db_dir = dirs::db_path(false)
                    .expect("Failed to get db path")
                    .to_str()
                    .unwrap()
                    .to_string();
                debug!("成功获取数据库路径: {}", db_dir);
                debug!("开始创建新的数据存储实例...");
                DATASTORE = Some(Datastore::new(db_dir, false));
                debug!("新的数据存储实例创建完成，递归调用以返回实例...");
                openDatastore()
            }
        }
    }

    #[no_mangle]
    pub unsafe extern "C" fn Java_net_activitywatch_android_RustInterface_greeting(
        env: JNIEnv,
        _: JClass,
        java_pattern: JString,
    ) -> jstring {
        debug!("[修改版本3] greeting 开始执行...");
        
        // 检查 java_pattern 是否为空
        if java_pattern.is_null() {
            error!("[修改版本3] 错误: 传入的 java_pattern 为空指针");
            return std::ptr::null_mut();
        }
        
        // 使用catch_unwind捕获可能的panic
        let is_null_object = match std::panic::catch_unwind(|| {
            env.is_same_object(java_pattern, JObject::null())
        }) {
            Ok(result) => match result {
                Ok(is_null) => is_null,
                Err(e) => {
                    error!("[修改版本3] 错误: is_same_object调用失败: {:?}", e);
                    false // 假设不是null对象，继续处理
                }
            },
            Err(e) => {
                error!("[修改版本3] 错误: is_same_object调用时发生panic: {:?}", e);
                return std::ptr::null_mut(); // 发生panic，返回null
            }
        };
        
        if is_null_object {
            error!("[修改版本3] 错误: 传入的 java_pattern 是null对象");
            return std::ptr::null_mut();
        }
        
        // 安全地获取字符串
        let java_str = match std::panic::catch_unwind(|| {
            env.get_string(java_pattern)
        }) {
            Ok(get_string_result) => match get_string_result {
                Ok(s) => {
                    debug!("[修改版本3] 成功获取 Java 字符串");
                    s
                },
                Err(e) => {
                    error!("[修改版本3] 错误: 无法从 JNI 获取字符串: {:?}", e);
                    return std::ptr::null_mut();
                }
            },
            Err(e) => {
                error!("[修改版本3] 错误: get_string调用时发生panic: {:?}", e);
                return std::ptr::null_mut(); // 发生panic，返回null
            }
        };
        
        // 调用 rust_greeting，使用catch_unwind捕获可能的panic
        let world = match std::panic::catch_unwind(|| {
            rust_greeting(java_str.as_ptr())
        }) {
            Ok(ptr) => {
                debug!("[修改版本3] rust_greeting 返回指针: {:p}", ptr as *const ());
                ptr
            },
            Err(e) => {
                error!("[修改版本3] 错误: rust_greeting调用时发生panic: {:?}", e);
                return std::ptr::null_mut(); // 发生panic，返回null
            }
        };
        
        // 检查返回的指针是否为空
        if world.is_null() {
            error!("[修改版本3] 错误: rust_greeting 返回了空指针");
            return std::ptr::null_mut();
        }
        
        // 重新获取指针，使用catch_unwind捕获可能的panic
        let world_str = match std::panic::catch_unwind(|| {
            let ptr = CString::from_raw(world);
            let result = match ptr.to_str() {
                Ok(s) => {
                    debug!("[修改版本3] 成功转换为 Rust 字符串: {}", s);
                    Ok(s.to_string())
                },
                Err(e) => {
                    error!("[修改版本3] 错误: 无法转换为 Rust 字符串: {:?}", e);
                    Err(e)
                }
            };
            // 这里不需要保存ptr，因为它会在作用域结束时自动释放
            result
        }) {
            Ok(Ok(str)) => str,
            Ok(Err(_)) | Err(_) => {
                error!("[修改版本3] 错误: CString处理时发生错误");
                return std::ptr::null_mut();
            }
        };
        
        // 转换为 Java 字符串，使用string_to_jstring函数
        debug!("[修改版本3] 步骤4: 将结果转换为 Java 字符串");
        let result = string_to_jstring(&env, world_str);
        
        debug!("[修改版本3] greeting 执行完成");
        result
    }

    unsafe fn jstring_to_string(env: &JNIEnv, string: JString) -> String {
        debug!("[修改版本3] jstring_to_string 开始执行...");
        
        // 检查 JString 是否为空
        if string.is_null() {
            error!("[修改版本3] 错误: 传入的 JString 为空指针");
            return String::from("");
        }
        
        // 使用catch_unwind捕获可能的panic
        let is_null_object = match std::panic::catch_unwind(|| {
            env.is_same_object(string, JObject::null())
        }) {
            Ok(result) => match result {
                Ok(is_null) => is_null,
                Err(e) => {
                    error!("[修改版本3] 错误: is_same_object调用失败: {:?}", e);
                    false // 假设不是null对象，继续处理
                }
            },
            Err(e) => {
                error!("[修改版本3] 错误: is_same_object调用时发生panic: {:?}", e);
                return String::from(""); // 发生panic，返回空字符串
            }
        };
        
        if is_null_object {
            error!("[修改版本3] 错误: 传入的 JString 是 null 对象");
            return String::from("");
        }
        
        // 安全地获取字符串，使用catch_unwind捕获可能的panic
        let java_str = match std::panic::catch_unwind(|| {
            env.get_string(string)
        }) {
            Ok(get_string_result) => match get_string_result {
                Ok(s) => {
                    debug!("[修改版本3] 成功获取 JString 内容");
                    s
                },
                Err(e) => {
                    error!("[修改版本3] 错误: 无法从 JNI 获取字符串: {:?}", e);
                    return String::from("");
                }
            },
            Err(e) => {
                error!("[修改版本3] 错误: get_string调用时发生panic: {:?}", e);
                return String::from(""); // 发生panic，返回空字符串
            }
        };
        
        // 使用catch_unwind捕获可能的panic
        let result = match std::panic::catch_unwind(|| {
            let result = java_str.to_string_lossy().into_owned();
            if result.is_empty() {
                debug!("[修改版本3] 转换后的 Rust 字符串为空");
                String::from("")
            } else {
                debug!("[修改版本3] Java 字符串成功转换为 Rust 字符串: {}", result);
                result
            }
        }) {
            Ok(result) => {
                debug!("[修改版本3] jstring_to_string 执行完成，返回结果");
                result
            },
            Err(e) => {
                error!("[修改版本3] 错误: to_string_lossy调用时发生panic: {:?}", e);
                String::from("") // 发生panic，返回空字符串
            }
        };
        result
    }

    unsafe fn string_to_jstring(env: &JNIEnv, string: String) -> jstring {
        debug!("[修改版本3] string_to_jstring 开始执行...");
        debug!("[修改版本3] 输入字符串长度: {}", string.len());
        
        debug!("[修改版本3] 步骤1: 检查输入字符串是否为空");
        if string.is_empty() {
            error!("[修改版本3] 警告: 输入的字符串为空");
            // 返回空字符串而不是null指针
            let empty_str = "";
            let new_empty_result = env.new_string(empty_str);
            match new_empty_result {
                Ok(jstring_obj) => {
                    let result = jstring_obj.into_raw();
                    debug!("[修改版本3] 返回空字符串，指针值: {:p}", result as *const ());
                    return result;
                },
                Err(e) => {
                    error!("[修改版本3] 错误: 无法创建空Java字符串: {:?}", e);
                    // 如果连空字符串都无法创建，返回null但不终止程序
                    return std::ptr::null_mut();
                }
            }
        }
        
        debug!("[修改版本3] 步骤2: 调用 env.new_string 创建 Java 字符串");
        // 使用try_catch块捕获可能的JNI异常
        let new_string_result = match std::panic::catch_unwind(|| {
            env.new_string(string)
        }) {
            Ok(result) => result,
            Err(e) => {
                error!("[修改版本3] 严重错误: new_string调用时发生panic: {:?}", e);
                return std::ptr::null_mut();
            }
        };
        
        match &new_string_result {
            Ok(_) => debug!("[修改版本3] new_string 成功"),
            Err(e) => debug!("[修改版本3] new_string 失败: {:?}", e),
        }
        
        let result = match new_string_result {
            Ok(jstring_obj) => {
                debug!("[修改版本3] 成功创建 Java 字符串对象");
                debug!("[修改版本3] 步骤3: 调用 into_raw 转换为原始指针");
                // 确保正确转换为 jstring 类型
                let raw_result = jstring_obj.into_raw();
                debug!("[修改版本3] into_raw 完成，指针值: {:p}", raw_result as *const ());
                raw_result
            },
            Err(e) => {
                error!("[修改版本3] 错误: 无法创建 Java 字符串: {:?}", e);
                // 返回 null 指针
                std::ptr::null_mut()
            }
        };
        
        debug!("[修改版本3] string_to_jstring 执行完成，返回结果");
        result
    }

    unsafe fn create_error_object(env: &JNIEnv, msg: String) -> jstring {
        debug!("[修改版本3] create_error_object 开始执行...");
        debug!("[修改版本3] 错误信息: {}", msg);
        
        // 使用catch_unwind捕获可能的panic
        match std::panic::catch_unwind(|| {
            debug!("[修改版本3] 步骤1: 创建 JSON 错误对象");
            let obj = json!({ "error": &msg });
            let json_str = obj.to_string();
            debug!("[修改版本3] 错误对象 JSON: {}", json_str);
            
            debug!("[修改版本3] 步骤2: 将 JSON 字符串转换为 Java 字符串");
            string_to_jstring(&env, json_str)
        }) {
            Ok(result) => {
                debug!("[修改版本3] create_error_object 执行完成，返回结果");
                result
            },
            Err(e) => {
                error!("[修改版本3] 错误: create_error_object 处理时发生panic: {:?}", e);
                // 返回null指针
                std::ptr::null_mut()
            }
        }
    }

    #[no_mangle]
    pub unsafe extern "C" fn Java_net_activitywatch_android_RustInterface_startServer(
        env: JNIEnv,
        _: JClass,
    ) {
        info!("开始启动服务器...");
        start_server();
        info!("服务器已退出...");
    }

    #[rocket::main]
    async fn start_server() {
        info!("开始构建服务器状态...");

<<<<<<< HEAD
        // FIXME: Why is unsafe needed here? Can we get rid of it?
        unsafe {
            let server_state: ServerState = endpoints::ServerState {
                datastore: openDatastore(),
                asset_resolver: endpoints::AssetResolver::new(None),
                device_id: device_id::get_device_id(),
            };
            info!("Using server_state:: device_id: {}", server_state.device_id);

            let mut server_config = crate::config::create_config(false, None);
            server_config.port = 5600;
=======
// FIXME: Why is unsafe needed here? Can we get rid of it?
unsafe {
    debug!("开始创建服务器状态...");
    let server_state: ServerState = endpoints::ServerState {
        datastore: Mutex::new(openDatastore()),
        asset_resolver: endpoints::AssetResolver::new(None),
        device_id: device_id::get_device_id(),
    };
    info!("使用的 server_state 设备 ID: {}", server_state.device_id);
    debug!("服务器状态创建完成，设备 ID: {}", server_state.device_id);

    debug!("开始加载默认服务器配置...");
    let mut server_config: AWConfig = AWConfig::default();
    server_config.port = 5600;
    debug!("服务器配置加载完成，端口设置为: {}", server_config.port);
>>>>>>> 19e80b1 (解决setdatadir中str相关crash的问题)

    debug!("开始构建 Rocket 服务器...");
    endpoints::build_rocket(server_state, server_config)
        .launch()
        .await;
    debug!("Rocket 服务器启动流程结束");
}
    }

    static mut INITIALIZED: bool = false;

    #[no_mangle]
    pub unsafe extern "C" fn Java_net_activitywatch_android_RustInterface_initialize(
        env: JNIEnv,
        _: JClass,
    ) {
        debug!("[修改版本3] initialize 开始执行...");
        debug!("[修改版本3] 步骤1: 检查 aw-server-rust 是否已经初始化");
        
        // 首先初始化日志记录器，确保后续日志能够正常输出
        if !INITIALIZED {
            debug!("[修改版本3] aw-server-rust 尚未初始化，开始初始化日志记录器...");
            debug!("[修改版本3] 步骤2: 调用 android_logger::init_once 初始化日志记录器");
            
            // 使用catch_unwind捕获可能的panic
            match std::panic::catch_unwind(|| {
                android_logger::init_once(
                    Config::default()
                        .with_max_level(log::LevelFilter::Trace)
                        .with_tag("aw-server-rust")
                )
            }) {
                Ok(init_result) => {
                    debug!("[修改版本3] 日志记录器初始化结果: {:?}", init_result);
                    debug!("[修改版本3] aw-server-rust 日志记录器初始化完成");
                },
                Err(e) => {
                    // 如果初始化失败，尝试使用标准输出记录错误
                    eprintln!("[修改版本3] 错误: 日志记录器初始化时发生panic: {:?}", e);
                    // 继续执行，不要因为日志初始化失败而中断整个流程
                }
            }
        } else {
            info!("[修改版本3] aw-server-rust 已经初始化");
            debug!("[修改版本3] aw-server-rust 初始化检查完成，状态：已初始化");
        }
        
        debug!("[修改版本3] 步骤3: 设置 INITIALIZED 标志为 true");
        INITIALIZED = true;
        debug!("[修改版本3] 标记 aw-server-rust 为已初始化状态");
        
        // 不再调用string_to_jstring，避免不必要的JNI调用
        // 而是直接返回，简化初始化流程
        debug!("[修改版本3] initialize 执行完成");
    }

    #[no_mangle]
    pub unsafe extern "C" fn Java_net_activitywatch_android_RustInterface_setDataDir(
        env: JNIEnv,
        _: JClass,
        java_dir: JString,
    ) {
        debug!("[修改版本3] setDataDir 开始执行...");
        
        // 确保日志记录器已初始化
        if !INITIALIZED {
            debug!("[修改版本3] aw-server-rust 尚未初始化，开始初始化日志记录器...");
            
            // 使用catch_unwind捕获可能的panic
            match std::panic::catch_unwind(|| {
                android_logger::init_once(
                    Config::default()
                        .with_max_level(log::LevelFilter::Trace)
                        .with_tag("aw-server-rust")
                )
            }) {
                Ok(init_result) => {
                    debug!("[修改版本3] 日志记录器初始化结果: {:?}", init_result);
                    debug!("[修改版本3] aw-server-rust 日志记录器初始化完成");
                },
                Err(e) => {
                    // 如果初始化失败，尝试使用标准输出记录错误
                    eprintln!("[修改版本3] 错误: 日志记录器初始化时发生panic: {:?}", e);
                    // 继续执行，不要因为日志初始化失败而中断整个流程
                }
            }
            
            INITIALIZED = true;
            debug!("[修改版本3] 标记 aw-server-rust 为已初始化状态");
        } else {
            debug!("[修改版本3] aw-server-rust 已经初始化，跳过初始化步骤");
        }

        info!("[修改版本3] 开始获取 Android 数据目录路径...");
        
        // 检查 java_dir 是否为空
        if java_dir.is_null() {
            error!("[修改版本3] 错误: 传入的 java_dir 为空指针，无法设置数据目录");
            return;
        }
        
        // 使用catch_unwind捕获可能的panic
        let is_null_object = match std::panic::catch_unwind(|| {
            env.is_same_object(java_dir, JObject::null())
        }) {
            Ok(result) => match result {
                Ok(is_null) => is_null,
                Err(e) => {
                    error!("[修改版本3] 错误: is_same_object调用失败: {:?}", e);
                    false // 假设不是null对象，继续处理
                }
            },
            Err(e) => {
                error!("[修改版本3] 错误: is_same_object调用时发生panic: {:?}", e);
                false // 假设不是null对象，继续处理
            }
        };
        
        if is_null_object {
            error!("[修改版本3] 错误: 传入的 java_dir 是null对象，无法设置数据目录");
            return;
        }
        
        // 安全地获取路径字符串
        let path = match std::panic::catch_unwind(|| {
            jstring_to_string(&env, java_dir)
        }) {
            Ok(path_str) => path_str,
            Err(e) => {
                error!("[修改版本3] 错误: jstring_to_string调用时发生panic: {:?}", e);
                return; // 无法获取路径，直接返回
            }
        };
        
        if path.is_empty() {
            error!("[修改版本3] 错误: 获取到的数据目录路径为空，无法设置数据目录");
            return;
        }
        
        info!("[修改版本3] 成功获取 Android 数据目录路径: {}", path);
        
        // 检查路径是否有效
        if path.contains("\0") {
            error!("[修改版本3] 错误: 路径包含空字符(\0)，这可能导致问题");
            // 继续执行，但记录警告
        }
        
        // 设置数据目录
        match std::panic::catch_unwind(|| {
            dirs::set_android_data_dir(&path);
        }) {
            Ok(_) => {
                debug!("[修改版本3] dirs::set_android_data_dir 调用成功");
            },
            Err(e) => {
                error!("[修改版本3] 错误: dirs::set_android_data_dir 调用时发生 panic: {:?}", e);
                // 即使发生 panic 也继续执行，不要中断流程
            }
        }
        
        info!("[修改版本3] Android 数据目录设置完成");
        debug!("[修改版本3] setDataDir 执行完成");
    }

    #[no_mangle]
    pub unsafe extern "C" fn Java_net_activitywatch_android_RustInterface_getBuckets(
        env: JNIEnv,
        _: JClass,
    ) -> jstring {
        debug!("开始从数据存储中获取存储桶列表...");
        let buckets = openDatastore().get_buckets().unwrap();
        debug!("成功从数据存储中获取存储桶列表");
        debug!("开始将存储桶列表转换为 Java 字符串...");
        let result = string_to_jstring(&env, json!(buckets).to_string());
        debug!("存储桶列表转换为 Java 字符串完成");
        result
    }

    #[no_mangle]
    pub unsafe extern "C" fn Java_net_activitywatch_android_RustInterface_createBucket(
        env: JNIEnv,
        _: JClass,
        java_bucket: JString,
    ) -> jstring {
        debug!("开始获取要创建的存储桶信息...");
        let bucket = jstring_to_string(&env, java_bucket);
        debug!("成功获取要创建的存储桶信息: {}", bucket);
        debug!("开始将存储桶信息解析为 JSON 对象...");
        let bucket_json: Bucket = match serde_json::from_str(&bucket) {
            Ok(json) => {
                debug!("存储桶信息解析为 JSON 对象成功");
                json
            },
            Err(err) => {
                debug!("存储桶信息解析为 JSON 对象失败: {}", err);
                return create_error_object(&env, err.to_string());
            }
        };
        debug!("开始在数据存储中创建存储桶...");
        match openDatastore().create_bucket(&bucket_json) {
            Ok(()) => {
                debug!("存储桶在数据存储中创建成功");
                string_to_jstring(&env, "Bucket successfully created".to_string())
            },
            Err(e) => {
                debug!("存储桶在数据存储中创建失败: {:?}", e);
                create_error_object(
                    &env,
                    format!("Something went wrong when trying to create bucket: {:?}", e),
                )
            }
        }
    }

    #[no_mangle]
    pub unsafe extern "C" fn Java_net_activitywatch_android_RustInterface_heartbeat(
        env: JNIEnv,
        _: JClass,
        java_bucket_id: JString,
        java_event: JString,
        java_pulsetime: jdouble,
    ) -> jstring {
        debug!("开始获取存储桶 ID...");
        let bucket_id = jstring_to_string(&env, java_bucket_id);
        debug!("成功获取存储桶 ID: {}", bucket_id);

        debug!("开始获取事件信息...");
        let event = jstring_to_string(&env, java_event);
        debug!("成功获取事件信息: {}", event);

        debug!("开始转换脉冲时间...");
        let pulsetime = java_pulsetime as f64;
        debug!("脉冲时间转换完成，值为: {}", pulsetime);

        debug!("开始将事件信息解析为 Event 对象...");
        let event_json: Event = match serde_json::from_str(&event) {
            Ok(json) => {
                debug!("事件信息解析为 Event 对象成功");
                json
            },
            Err(err) => {
                debug!("事件信息解析为 Event 对象失败: {}", err);
                return create_error_object(&env, err.to_string());
            }
        };

        debug!("开始发送心跳到数据存储...");
        match openDatastore().heartbeat(&bucket_id, event_json, pulsetime) {
            Ok(_) => {
                debug!("心跳成功发送到数据存储");
                string_to_jstring(&env, "Heartbeat successfully received".to_string())
            },
            Err(e) => {
                debug!("发送心跳到数据存储时出错: {:?}", e);
                create_error_object(
                    &env,
                    format!(
                        "Something went wrong when trying to send heartbeat: {:?}",
                        e
                    ),
                )
            }
        }
    }

    #[no_mangle]
    pub unsafe extern "C" fn Java_net_activitywatch_android_RustInterface_getEvents(
        env: JNIEnv,
        _: JClass,
        java_bucket_id: JString,
        java_limit: jint,
    ) -> jstring {
        debug!("开始获取存储桶 ID...");
        let bucket_id = jstring_to_string(&env, java_bucket_id);
        debug!("成功获取存储桶 ID: {}", bucket_id);

        debug!("开始转换事件数量限制值...");
        let limit = java_limit as u64;
        debug!("事件数量限制值转换完成，值为: {}", limit);

        debug!("开始从数据存储中获取事件...");
        match openDatastore().get_events(&bucket_id, None, None, Some(limit)) {
            Ok(events) => {
                debug!("成功从数据存储中获取事件");
                debug!("开始将事件列表转换为 Java 字符串...");
                let result = string_to_jstring(&env, json!(events).to_string());
                debug!("事件列表转换为 Java 字符串完成");
                result
            },
            Err(e) => {
                debug!("从数据存储中获取事件时出错: {:?}", e);
                create_error_object(
                    &env,
                    format!("Something went wrong when trying to get events: {:?}", e),
                )
            }
        }
    }
<<<<<<< HEAD

    #[no_mangle]
    pub unsafe extern "C" fn Java_net_activitywatch_android_RustInterface_migrateHostname(
        env: JNIEnv,
        _: JClass,
        hostname: JString,
    ) -> jstring {
        let hostname = jstring_to_string(&env, hostname);
        if hostname.is_empty() {
            return create_error_object(&env, "hostname must not be empty".to_string());
        }
        match openDatastore().migrate_hostname(&hostname) {
            Ok(count) => {
                string_to_jstring(&env, format!("Migrated hostname for {} bucket(s)", count))
            }
            Err(e) => create_error_object(&env, format!("Failed to migrate hostname: {:?}", e)),
        }
    }

    #[no_mangle]
    pub unsafe extern "C" fn Java_net_activitywatch_android_RustInterface_migrateAndroidBucketName(
        env: JNIEnv,
        _: JClass,
    ) -> jstring {
        match openDatastore().rename_bucket("aw-android-test", "aw-android") {
            Ok(()) => string_to_jstring(
                &env,
                "Renamed bucket 'aw-android-test' to 'aw-android'".to_string(),
            ),
            Err(e) => create_error_object(&env, format!("Failed to rename bucket: {:?}", e)),
        }
    }

    #[no_mangle]
    pub unsafe extern "C" fn Java_net_activitywatch_android_RustInterface_migrateWatcherAndroidBucketNames(
        env: JNIEnv,
        _: JClass,
    ) -> jstring {
        match openDatastore().migrate_test_bucket_names() {
            Ok(count) => string_to_jstring(
                &env,
                format!("Migrated {} 'aw-watcher-android-test' bucket(s)", count),
            ),
            Err(e) => create_error_object(
                &env,
                format!("Failed to migrate watcher bucket names: {:?}", e),
            ),
        }
    }

    #[no_mangle]
    pub unsafe extern "C" fn Java_net_activitywatch_android_RustInterface_query(
        env: JNIEnv,
        _: JClass,
        java_query: JString,
        java_timeperiods: JString,
    ) -> jstring {
        let query_code = jstring_to_string(&env, java_query);
        let timeperiods_str = jstring_to_string(&env, java_timeperiods);
        let timeperiods: Vec<TimeInterval> = match serde_json::from_str(&timeperiods_str) {
            Ok(json) => json,
            Err(err) => return create_error_object(&env, err.to_string()),
        };

        let datastore = openDatastore();
        let mut results = Vec::new();

        for interval in &timeperiods {
            let result = match aw_query::query(&query_code, interval, &datastore) {
                Ok(data) => data,
                Err(e) => {
                    return create_error_object(
                        &env,
                        format!("Something went wrong when trying to query: {:?}", e),
                    )
                }
            };
            results.push(result);
        }

        string_to_jstring(&env, json!(results).to_string())
    }

    #[no_mangle]
    pub unsafe extern "C" fn Java_net_activitywatch_android_RustInterface_androidQuery(
        env: JNIEnv,
        _: JClass,
        java_timeperiods: JString,
    ) -> jstring {
        let timeperiods_str = jstring_to_string(&env, java_timeperiods);

        let timeperiods: Vec<TimeInterval> = match serde_json::from_str(&timeperiods_str) {
            Ok(json) => json,
            Err(err) => return create_error_object(&env, err.to_string()),
        };

        // Hardcoded bucket ID
        let bid_android = "aw-watcher-android".to_string();

        // Get classes from server settings via HTTP API
        let classes = match AwClient::new("127.0.0.1", 5600, "aw-android-query") {
            Ok(client) => {
                match client.get_setting("classes") {
                    Ok(classes_value) => {
                        // Parse the server-side classes from JSON value
                        match serde_json::from_value::<Vec<aw_models::Class>>(classes_value) {
                            Ok(server_classes) => {
                                if server_classes.is_empty() {
                                    info!("Server classes list is empty, using default classes");
                                    default_classes()
                                } else {
                                    // Convert from aw_models::Class to CategorySpec format
                                    server_classes
                                        .iter()
                                        .map(|c| {
                                            let category_id: CategoryId = c.name.clone();
                                            let category_spec = CategorySpec {
                                                spec_type: c.rule.rule_type.clone(),
                                                regex: c.rule.regex.clone().unwrap_or_default(),
                                                ignore_case: c.rule.ignore_case.unwrap_or(false),
                                            };
                                            (category_id, category_spec)
                                        })
                                        .collect()
                                }
                            }
                            Err(e) => {
                                warn!("Failed to parse server classes, using defaults: {:?}", e);
                                default_classes()
                            }
                        }
                    }
                    Err(e) => {
                        info!("Failed to get server classes, using defaults: {:?}", e);
                        default_classes()
                    }
                }
            }
            Err(e) => {
                warn!(
                    "Failed to create client for fetching classes, using defaults: {:?}",
                    e
                );
                default_classes()
            }
        };

        // Build canonical Android query
        let params = AndroidQueryParams {
            base: QueryParamsBase {
                bid_browsers: Vec::new(),
                classes,
                filter_classes: Vec::new(),
                filter_afk: true,
                include_audible: true,
            },
            bid_android,
        };
        let query_code = format!(
            r#"{}
duration = sum_durations(events);
cat_events = sort_by_duration(merge_events_by_keys(events, ["$category"]));
RETURN = {{"events": events, "duration": duration, "cat_events": cat_events}};"#,
            build_android_canonical_events(&params)
        );

        let datastore = openDatastore();
        let mut results = Vec::new();

        for interval in &timeperiods {
            let result = match aw_query::query(&query_code, interval, &datastore) {
                Ok(data) => data,
                Err(e) => {
                    return create_error_object(
                        &env,
                        format!("Something went wrong when trying to query: {:?}", e),
                    )
                }
            };
            results.push(result);
        }

        string_to_jstring(&env, json!(results).to_string())
    }
}
=======
}
>>>>>>> 19e80b1 (解决setdatadir中str相关crash的问题)
