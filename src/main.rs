#[macro_use]
extern crate rocket;

use std::collections::HashSet;
use std::time::SystemTime;

use rocket::fairing::{Fairing, Info, Kind};
use rocket::serde::json::Json;
use rocket::{Orbit, Rocket, State};

use rocket_okapi::gen::OpenApiGenerator;
use rocket_okapi::request::{OpenApiFromRequest, RequestHeaderInput};
use rocket_okapi::settings::UrlObject;
use rocket_okapi::{openapi, openapi_get_routes, rapidoc::*, swagger_ui::*};

use rocket_db_pools::sqlx;
use rocket_db_pools::Database;
use schemas::TitlePrincipalCache;

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
#[openapi(tag = "IMDB")]
#[get("/imdb/title?<title_fragment>")]
async fn titles(db: &DbPool, title_fragment: &str) -> Json<Vec<schemas::TitleBasics>> {
    Json(repo::titles_by_name(&db.0, title_fragment).await)
}

fn search_names(
    cache: &State<TitlePrincipalCache>,
    tconst: &str,
    nconst2: &str,
) -> (bool, HashSet<String>) {
    let mut names_to_visit: HashSet<String> = HashSet::new();
    if let Some(names) = cache.t_to_p(tconst) {
        for nconst_i in names.value() {
            if nconst_i == nconst2 {
                return (true, names_to_visit);
            } else {
                names_to_visit.insert(nconst_i.to_string());
            }
        }
    }
    (false, names_to_visit)
}

fn search_titles(
    cache: &State<TitlePrincipalCache>,
    visited_titles: &mut HashSet<String>,
    this_level: &Vec<(String, HashSet<String>)>,
    nconst2: &str,
    level: usize,
) -> Vec<String> {
    let mut next_level: Vec<(String, HashSet<String>)> = Vec::new();

    for (route, names) in this_level {
        for nconst in names {
            if let Some(titles) = cache.p_to_t(nconst) {
                let mut titles_to_visit: HashSet<String> = HashSet::new();
                for tconst in titles.value() {
                    if !visited_titles.contains(tconst) {
                        titles_to_visit.insert(tconst.to_string());
                        visited_titles.insert(tconst.to_string());

                        let (success, names_to_visit) = search_names(cache, &tconst, nconst2);
                        if success {
                            println!(
                                "connections: {}: {} {} {} {}",
                                level, route, nconst, tconst, nconst2
                            );
                            let mut route2: Vec<String> = if route == "" {
                                Vec::new()
                            } else {
                                route
                                    .split_whitespace()
                                    .map(|x| x.to_string())
                                    .collect::<Vec<String>>()
                            };
                            route2.append(&mut vec![
                                nconst.to_string(),
                                tconst.to_string(),
                                nconst2.to_string(),
                            ]);
                            return route2;
                        } else {
                            let new_route: String = format!("{} {} {}", route, nconst, tconst);
                            next_level.push((new_route, names_to_visit));
                        }
                    }
                }
            }
        }
    }

    if level > 9 || next_level.is_empty() {
        vec![]
    } else {
        search_titles(cache, visited_titles, &next_level, nconst2, level + 1)
    }
}

#[openapi(tag = "IMDB")]
#[get("/imdb/distance/principal/<nconst>?<nconst2>")]
fn distance(cache: &State<TitlePrincipalCache>, nconst: &str, nconst2: &str) -> String {
    let start_time = SystemTime::now();
    let mut visited_titles: HashSet<String> = HashSet::with_capacity(100000);
    let first_level = vec![("".to_owned(), HashSet::from([nconst.to_string()]))];
    let result = search_titles(cache, &mut visited_titles, &first_level, nconst2, 1);

    let mut txt = String::new();
    if result.is_empty() {
        txt.push_str(&format!(
            "**** no result for {} -> {} ****",
            nconst, nconst2
        ));
        println!("{txt}");
    } else {
        txt.push_str(&format!("steps: {} ", (result.len() - 1) / 2));
        txt.push_str(
            &result
                .iter()
                .map(|x| format!("{} -> ", x))
                .collect::<String>(),
        );
    }
    println!("search time: {:?}", start_time.elapsed().unwrap());

    txt
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
        let state: Option<&State<TitlePrincipalCache>> = State::get(rocket);

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
        .manage(TitlePrincipalCache::new())
        .attach(DbPool::init())
        .attach(TitlePrincipalCacheLoader::init())
        .mount("/", openapi_get_routes![titles, distance])
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
