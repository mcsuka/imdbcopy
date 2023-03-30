#[macro_use]
extern crate rocket;

use rocket::serde::json::Json;

use rocket_okapi::gen::OpenApiGenerator;
use rocket_okapi::request::{OpenApiFromRequest, RequestHeaderInput};
use rocket_okapi::settings::UrlObject;
use rocket_okapi::{openapi, openapi_get_routes, rapidoc::*, swagger_ui::*};

use rocket_db_pools::sqlx;
use rocket_db_pools::Database;

mod repo;
mod schemas;

#[derive(Database)]
#[database("imdb_db")]
struct DbPool(sqlx::PgPool);

impl<'r> OpenApiFromRequest<'r> for &'r DbPool {
    fn from_request_input(
        _gen: &mut OpenApiGenerator,
        _name: String,
        _required: bool,
    ) -> rocket_okapi::Result<RequestHeaderInput> {
        Ok(RequestHeaderInput::None)
    }
}

/// Search a film or other moving picture by a title fragment
#[openapi(tag = "Async Process")]
#[get("/imdb/title?<title_fragment>")]
async fn titles(db: &DbPool, title_fragment: &str) -> Json<Vec<schemas::TitleBasics>> {
    Json(repo::titles_by_name(&db.0, title_fragment).await)
}

#[launch]
fn rocket() -> _ {
    rocket::build()
        .attach(DbPool::init())
        .mount("/", openapi_get_routes![titles])
        .mount(
            "/swagger-ui/",
            make_swagger_ui(&SwaggerUIConfig {
                url: "../openapi.json".to_owned(),
                ..Default::default()
            }),
        )
        .mount(
            "/rapidoc/",
            make_rapidoc(&RapiDocConfig {
                title: Some("Sandbox Webserver with Rust/Rocket".to_owned()),
                general: GeneralConfig {
                    spec_urls: vec![UrlObject::new("General", "../openapi.json")],
                    ..Default::default()
                },
                hide_show: HideShowConfig {
                    allow_spec_url_load: false,
                    allow_spec_file_load: false,
                    ..Default::default()
                },
                ..Default::default()
            }),
        )
}
