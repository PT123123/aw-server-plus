use gethostname::gethostname;
use rocket::fairing::Fairing;
use rocket::fs::FileServer;
use rocket::http::Header;
use rocket::serde::json::Json;
use rocket::State;

use crate::config::AWConfig;

use aw_datastore::Datastore;
use aw_models::Info;

// The Datastore is just a cheap handle to the DB worker thread (a crossbeam
// channel sender), which serializes all DB access internally. No mutex is
// needed here — wrapping it in one would serialize all HTTP requests, letting
// a slow query block every heartbeat.
pub struct ServerState {
    pub datastore: Datastore,
    pub device_id: String,
}

#[macro_use]
mod util;
mod apikey;
mod bucket;
mod cors;
mod export;
mod extension_cors;
mod hostcheck;
mod import;
mod settings;

pub use util::HttpErrorJson;

// CSP Fairing
pub struct CSPFairing;

#[rocket::async_trait]
impl Fairing for CSPFairing {
    fn info(&self) -> rocket::fairing::Info {
        rocket::fairing::Info {
            name: "Content Security Policy",
            kind: rocket::fairing::Kind::Response,
        }
    }

    async fn on_response<'r>(
        &self,
        _request: &'r rocket::Request<'_>,
        response: &mut rocket::Response<'r>,
    ) {
        response.set_header(Header::new(
            "Content-Security-Policy",
            "default-src 'self'; connect-src 'self' http://localhost:5600 http://localhost:5600 ws://localhost:5600 ws://localhost:5600 http://127.0.0.1:5600 http://127.0.0.1:5600 http://10.0.2.2:5600 ws://10.0.2.2:5600; script-src 'self' 'unsafe-eval'; img-src 'self' blob: data: http://127.0.0.1:5600 http://10.0.2.2:5600; style-src 'self' 'unsafe-inline'; font-src 'self'; frame-src 'self'; manifest-src 'self'; upgrade-insecure-requests; block-all-mixed-content;",
        ));
    }
}

#[get("/")]
fn server_info(config: &State<AWConfig>, state: &State<ServerState>) -> Json<Info> {
    #[allow(clippy::or_fun_call)]
    let hostname = gethostname().into_string().unwrap_or("unknown".to_string());
    const VERSION: Option<&'static str> = option_env!("CARGO_PKG_VERSION");

    Json(Info {
        hostname,
        version: format!("v{} (rust)", VERSION.unwrap_or("(unknown)")),
        testing: config.testing,
        device_id: state.device_id.clone(),
    })
}

pub fn build_rocket(server_state: ServerState, config: AWConfig) -> rocket::Rocket<rocket::Build> {
    info!(
        "Starting aw-server-rust at {}:{}",
        config.address, config.port
    );
    let cors = cors::cors(&config);
    let extension_cors = extension_cors::ExtensionCorsScope::new(&config);
    let hostcheck = hostcheck::HostCheck::new(&config);
    let apikey = apikey::ApiKeyCheck::new(&config);
    let custom_static = config.custom_static.clone();

    let mut rocket = rocket::custom(config.to_rocket_config())
        .attach(cors.clone())
        // Attached before the other request fairings so a blocked extension
        // request is rewritten to the 403 route before they inspect the path.
        .attach(extension_cors)
        .attach(hostcheck)
        .attach(CSPFairing)
        .manage(cors)
        .manage(server_state)
        .manage(config)
        .mount("/api/0/info", routes![server_info])
        .mount(
            "/api/0/buckets",
            routes![
                bucket::bucket_new,
                bucket::bucket_delete,
                bucket::buckets_get,
                bucket::bucket_get,
                bucket::bucket_events_get,
                bucket::bucket_events_create,
                bucket::bucket_events_heartbeat,
                bucket::bucket_event_count,
                bucket::bucket_events_get_single,
                bucket::bucket_events_delete_by_id,
                bucket::bucket_export
            ],
        )
        .mount(
            "/api/0/import",
            routes![import::bucket_import_json, import::bucket_import_form],
        )
        .mount("/api/0/export", routes![export::buckets_export])
        .mount(
            "/api/0/settings",
            routes![
                settings::setting_get,
                settings::setting_set,
                settings::setting_delete,
                settings::settings_get,
            ],
        )
        .mount("/", rocket_cors::catch_all_options_routes());

    // for each custom static directory, mount it at the given name
    for (name, dir) in custom_static {
        info!(
            "Serving /pages/{} custom static directory from {}",
            name, dir
        );
        rocket = rocket.mount(&format!("/pages/{name}"), FileServer::from(dir));
    }
    rocket
}
