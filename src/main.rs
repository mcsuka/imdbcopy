#[macro_use]
extern crate rocket;

use std::collections::HashSet;
use std::time::{Duration, SystemTime};

use dashmap::DashSet;
use rocket::fairing::{Fairing, Info, Kind};
use rocket::http::Status;
use rocket::serde::json::Json;
use rocket::{Orbit, Rocket, State};

use rocket_okapi::gen::OpenApiGenerator;
use rocket_okapi::okapi::schemars;
use rocket_okapi::request::{OpenApiFromRequest, RequestHeaderInput};
use rocket_okapi::settings::UrlObject;
use rocket_okapi::{openapi, openapi_get_routes, rapidoc::*, swagger_ui::*};

use rocket_db_pools::sqlx;
use rocket_db_pools::Database;
use serde::Serialize;

mod kevinbacon;
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

/// Search films or other moving pictures by a title fragment
#[openapi(tag = "IMDB")]
#[get("/imdb/title?<title_fragment>")]
async fn titles(
    db: &DbPool,
    title_fragment: &str,
) -> Result<Json<Vec<schemas::TitleDetails>>, (Status, String)> {
    let result = repo::titles_by_name(&db.0, title_fragment).await?;
    Ok(Json(result))
}

/// Search for contributors by name
#[openapi(tag = "IMDB")]
#[get("/imdb/principal?<name>")]
async fn contributor(
    db: &DbPool,
    cache: &State<schemas::TitlePrincipalCache>,
    name: &str,
) -> Result<Json<Vec<schemas::NameBasics>>, String> {
    let result = repo::basics_for_name(&db.0, cache, name).await;
    match result {
        Ok(names) => Ok(Json(names)),
        Err(err) => Err(format!("{:?}", err)),
    }
}

fn busiest_actor(
    cache: &State<schemas::TitlePrincipalCache>,
    nconsts: Vec<String>,
) -> Option<String> {
    if nconsts.is_empty() {
        None
    } else {
        let mut busiest: (String, usize) = (nconsts[0].clone(), 0);
        for nconst in nconsts {
            let score = cache.p_to_t(&nconst).map_or(0, |x| x.len());
            if score > busiest.1 {
                busiest = (nconst, score);
            }
        }
        Some(busiest.0.to_string())
    }
}

/// Search the **shortest path between 2 actors**, identified by name.<br/>
/// In case two actors have the same name, the one with the most films will be used
#[openapi(tag = "IMDB")]
#[get("/imdb/distance?<name1>&<name2>&<parallel>")]
async fn name_distance(
    db_pool: &DbPool,
    cache: &State<schemas::TitlePrincipalCache>,
    name1: &str,
    name2: &str,
    parallel: bool,
) -> Result<Json<DistanceResult>, (Status, String)> {
    if let Some(nconst1) = busiest_actor(cache, repo::nconst_for_name(&db_pool.0, name1).await?) {
        if let Some(nconst2) = busiest_actor(cache, repo::nconst_for_name(&db_pool.0, name2).await?)
        {
            distance(db_pool, cache, &nconst1, &nconst2, parallel).await
        } else {
            Err((
                Status::NotFound,
                format!("Could not find Contributor {}", name2),
            ))
        }
    } else {
        Err((
            Status::NotFound,
            format!("Could not find Contributor {}", name1),
        ))
    }
}

#[derive(Serialize, schemars::JsonSchema)]
#[serde(crate = "rocket::serde")]
struct DistanceResult {
    separation_degree: i32,
    response_time: Duration,
    connection_path: Vec<schemas::TitleToNames>,
}

/// Search the shortest path between 2 actors, identified by their id
#[openapi(tag = "IMDB")]
#[get("/imdb/distance/principal/<nconst1>?<nconst2>&<parallel>")]
async fn distance(
    db_pool: &DbPool,
    cache: &State<schemas::TitlePrincipalCache>,
    nconst1: &str,
    nconst2: &str,
    parallel: bool,
) -> Result<Json<DistanceResult>, (Status, String)> {
    let start_time = SystemTime::now();
    let mut visited_titles: DashSet<String> = DashSet::with_capacity(100000);
    let mut visited_names: DashSet<String> = DashSet::with_capacity(100000);
    let first_level = vec![("".to_owned(), HashSet::from([nconst1.to_string()]))];
    let result = kevinbacon::search_titles(
        parallel,
        cache,
        &mut visited_titles,
        &mut visited_names,
        &first_level,
        nconst2,
        1,
    );

    match result {
        Ok(route) => {
            let response_time = start_time.elapsed().unwrap();
            println!("Response time: {:?}", response_time);

            if route.is_empty() {
                Ok(Json(DistanceResult {
                    separation_degree: -1,
                    response_time,
                    connection_path: vec![],
                }))
            } else {
                let separation_degree = (route.len() - 1) / 2;
                let mut connection_path: Vec<schemas::TitleToNames> = Vec::new();
                for i in 0..separation_degree {
                    let idx: usize = (i * 2).try_into().unwrap();
                    let step = repo::title_to_names(
                        db_pool,
                        &route[idx + 1],
                        &route[idx],
                        &route[idx + 2],
                    )
                    .await?;
                    connection_path.push(step);
                }

                Ok(Json(DistanceResult {
                    separation_degree: separation_degree.try_into().unwrap(),
                    response_time,
                    connection_path,
                }))
            }
        }
        Err(err) => Err((
            Status::NotFound,
            format!("Could not find actor with ID {}", err.0),
        )),
    }
}

struct TitlePrincipalCacheLoader;

impl TitlePrincipalCacheLoader {
    fn init() -> TitlePrincipalCacheLoader {
        TitlePrincipalCacheLoader
    }
}

#[rocket::async_trait]
impl Fairing for TitlePrincipalCacheLoader {
    fn info(&self) -> Info {
        Info {
            name: "Retrieve Title to Actor mappings to a memory cache",
            kind: Kind::Liftoff,
        }
    }

    async fn on_liftoff(&self, rocket: &Rocket<Orbit>) {
        let state: Option<&State<schemas::TitlePrincipalCache>> = State::get(rocket);

        if let Some(cache) = state {
            let start_time = SystemTime::now();
            // cache.insert("some".to_string(), "thing".to_string());
            if let Some(db_pool) = DbPool::fetch(rocket) {
                repo::titles_to_principals(db_pool, &cache).await;
            }

            let (size_t, size_p) = cache.len();
            println!(
                "Inserted ({}, {}) rows to the cache in {:?} time",
                size_t,
                size_p,
                start_time.elapsed().unwrap()
            );
        }
    }
}

#[launch]
fn rocket() -> _ {
    rocket::build()
        .manage(schemas::TitlePrincipalCache::new())
        .attach(DbPool::init())
        .attach(TitlePrincipalCacheLoader::init())
        .mount(
            "/",
            openapi_get_routes![titles, contributor, name_distance, distance],
        )
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
