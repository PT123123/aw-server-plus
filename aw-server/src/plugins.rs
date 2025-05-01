/// 注册所有插件（目前只集成 aw-inbox）
use aw_inbox_rust::SharedDb;

pub fn register_all_plugins(rocket: rocket::Rocket<rocket::Build>, db: SharedDb) -> rocket::Rocket<rocket::Build> {
    use rocket::figment::Figment;
    let figment: &Figment = rocket.figment();
    let port: u16 = figment.extract_inner("port").unwrap_or(5600);
    let address: String = figment.extract_inner("address").unwrap_or("127.0.0.1".to_string());
    println!(
        "[INFO] aw-inbox-rust 已挂载到主服务，访问入口: http://{}:{}/inbox",
        address, port
    );
    aw_inbox_rust::mount_rocket(rocket, db)
}
