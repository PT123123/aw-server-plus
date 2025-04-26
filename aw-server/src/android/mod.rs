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
    let c_str = unsafe { CStr::from_ptr(to) };
    let recipient = match c_str.to_str() {
        Err(_) => "there",
        Ok(string) => string,
    };

    CString::new("Hello ".to_owned() + recipient + " (from Rust!)")
        .unwrap()
        .into_raw()
}

#[cfg(target_os = "android")]
#[allow(non_snake_case)]
pub mod android {
    extern crate jni;

    use self::jni::objects::{JClass, JString};
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
        debug!("开始从 Java 环境获取传入的字符串...");
        // Our Java companion code might pass-in "world" as a string, hence the name.
        let world = rust_greeting(
            env.get_string(java_pattern)
                .expect("invalid pattern string")
                .as_ptr(),
        );
        debug!("成功调用 rust_greeting 函数，开始重新获取指针...");
        // Retake pointer so that we can use it below and allow memory to be freed when it goes out of scope.
        let world_ptr = CString::from_raw(world);
        debug!("开始将结果转换为 Java 字符串...");
        let output = env
            .new_string(world_ptr.to_str().unwrap())
            .expect("Couldn't create java string!");
        debug!("成功转换为 Java 字符串，返回结果...");
        output.into_raw()
    }

    unsafe fn jstring_to_string(env: &JNIEnv, string: JString) -> String {
        debug!("开始将 Java 字符串转换为 Rust 字符串...");

        // 1. 获取 JNIString 并将其绑定到变量，以延长其生命周期
        let java_string = env.get_string(string).expect("无法从 JNI 获取字符串");

        // 2. 从有效的 JNIString 获取 C 指针
        let raw_c_str = java_string.as_ptr();

        // 3. 从有效的指针创建 CStr
        let c_str = CStr::from_ptr(raw_c_str);

        debug!("成功获取 C 字符串: {:?}，开始转换为 Rust 字符串...", c_str);

        // 4. 尝试转换为 Rust &str，并处理可能的错误，避免使用 unwrap()
        let result_str_slice = match c_str.to_str() {
            Ok(s) => s,
            Err(e) => {
                // 记录详细错误，而不是直接 panic
                error!("无法将 C 字符串 {:?} 转换为有效的 UTF-8 字符串: {}", c_str, e);
                // 这里可以选择 panic 或返回一个错误标记字符串
                // 为了调试，我们暂时 panic，但在生产代码中可能需要更优雅的处理
                panic!("JNI 字符串包含无效的 UTF-8 序列");
                // 或者返回错误标记: return String::from("Error: Invalid UTF-8");
            }
        };

        // 5. 从有效的 &str 创建拥有的 String
        let result = String::from(result_str_slice);

        // java_string 会在此处离开作用域并被安全地清理，其管理的指针 raw_c_str 也随之失效

        debug!("Java 字符串成功转换为 Rust 字符串: {}", result); // 打印最终结果
        result
    }

    unsafe fn string_to_jstring(env: &JNIEnv, string: String) -> jstring {
        debug!("开始将 Rust 字符串转换为 Java 字符串...");
        let result = env.new_string(string)
            .expect("Couldn't create java string")
            .into_raw();
        debug!("Rust 字符串成功转换为 Java 字符串...");
        result
    }

    unsafe fn create_error_object(env: &JNIEnv, msg: String) -> jstring {
        debug!("开始创建错误对象...");
        let obj = json!({ "error": &msg });
        debug!("错误对象 JSON 构建完成，开始转换为 Java 字符串...");
        string_to_jstring(&env, obj.to_string())
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
        debug!("开始检查 aw-server-rust 是否已经初始化...");
        if !INITIALIZED {
            debug!("aw-server-rust 尚未初始化，开始初始化日志记录器...");
            android_logger::init_once(
                Config::default()
                    .with_max_level(log::LevelFilter::Trace) // limit log level
                    .with_tag("aw-server-rust"), // logs will show under mytag tag
                                                 //.with_filter( // configure messages for specific crate
                                                 //    FilterBuilder::new()
                                                 //        .parse("debug,hello::crate=error")
                                                 //        .build())
            );
            debug!("aw-server-rust 日志记录器初始化完成");
        } else {
            info!("aw-server-rust 已经初始化");
            debug!("aw-server-rust 初始化检查完成，状态：已初始化");
        }
        INITIALIZED = true;
        debug!("标记 aw-server-rust 为已初始化状态");
        // Without this it might not work due to weird error probably arising from Rust optimizing away the JNIEnv:
        //  JNI DETECTED ERROR IN APPLICATION: use of deleted weak global reference
        string_to_jstring(&env, "test".to_string());
        debug!("字符串转换操作完成");
    }

    #[no_mangle]
    pub unsafe extern "C" fn Java_net_activitywatch_android_RustInterface_setDataDir(
        env: JNIEnv,
        _: JClass,
        java_dir: JString,
    ) {
        if !INITIALIZED {
            debug!("aw-server-rust 尚未初始化，开始初始化日志记录器...");
            android_logger::init_once(
                Config::default()
                    .with_max_level(log::LevelFilter::Trace) // limit log level
                    .with_tag("aw-server-rust"), // logs will show under mytag tag
                                                 //.with_filter( // configure messages for specific crate
                                                 //    FilterBuilder::new()
                                                 //        .parse("debug,hello::crate=error")
                                                 //        .build())
            );
            debug!("aw-server-rust 日志记录器初始化完成");
        }

        error!("ee开始获取 Android 数据目录路径...");
        debug!("setDataDir - JNIEnv 指针: {:?}", &env as *const _);
        //debug!("setDataDir - java_dir (转换为 Rust String): {}", java_dir);
        //debug!("setDataDir - java_dir JNI 引用地址: {:p}", java_dir);
        info!("开始获取 Android 数据目录路径...");
        // 对应/data/user/0/net.activitywatch.android.debug/files
        let path = &jstring_to_string(&env, java_dir); //TODO-这个出错了
        info!("成功获取 Android 数据目录路径: {}", path);
        debug!("开始设置 Android 数据目录...");
        dirs::set_android_data_dir(path);
        info!("Android 数据目录设置完成");
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
