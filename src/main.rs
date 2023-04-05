#[macro_use]
extern crate rocket;

use std::collections::HashSet;
use std::sync::mpsc;
use std::thread;
use std::time::SystemTime;

use dashmap::DashSet;
use rocket::fairing::{Fairing, Info, Kind};
use rocket::serde::json::Json;
use rocket::{Orbit, Rocket, State};

use rocket_okapi::gen::OpenApiGenerator;
use rocket_okapi::request::{OpenApiFromRequest, RequestHeaderInput};
use rocket_okapi::settings::UrlObject;
use rocket_okapi::{openapi, openapi_get_routes, rapidoc::*, swagger_ui::*};

use rocket_db_pools::sqlx;
use rocket_db_pools::Database;

use crate::schemas::TitlePrincipalCache;

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
    ignored_names: &DashSet<String>,
    tconst: &str,
    nconst2: &str,
) -> (bool, HashSet<String>) {
    let mut names_to_visit: HashSet<String> = HashSet::new();
    if let Some(names) = cache.t_to_p(tconst) {
        for nconst_i in names.value() {
            if nconst_i == nconst2 {
                return (true, names_to_visit);
            } else if !ignored_names.contains(nconst_i) {
                names_to_visit.insert(nconst_i.to_string());
            }
        }
    }
    (false, names_to_visit)
}

struct NextRoute {
    success_route: Option<Vec<String>>,
    visited_titles: HashSet<String>,
    next_level: Vec<(String, HashSet<String>)>,
}

impl NextRoute {
    fn found(route: Vec<String>) -> NextRoute {
        NextRoute {
            success_route: Some(route),
            visited_titles: HashSet::from([]),
            next_level: Vec::from([]),
        }
    }

    fn search_further(
        visited_titles: HashSet<String>,
        next_level: Vec<(String, HashSet<String>)>,
    ) -> NextRoute {
        NextRoute {
            success_route: None,
            visited_titles: visited_titles,
            next_level: next_level,
        }
    }
}

fn search_route(
    cache: &State<TitlePrincipalCache>,
    ignored_titles: &DashSet<String>,
    ignored_names: &DashSet<String>,
    route: &str,
    names_to_visit: &HashSet<String>,
    nconst2: &str,
) -> NextRoute {
    let mut visited_titles: HashSet<String> = HashSet::new();
    let mut next_level: Vec<(String, HashSet<String>)> = Vec::new();

    for nconst in names_to_visit {
        if !ignored_names.contains(nconst) {
            if let Some(titles) = cache.p_to_t(nconst) {
                let mut titles_to_visit: HashSet<String> = HashSet::new();
                for tconst in titles.value() {
                    if !ignored_titles.contains(tconst) && !visited_titles.contains(tconst) {
                        titles_to_visit.insert(tconst.to_string());

                        let (success, names_to_visit2) =
                            search_names(cache, ignored_names, &tconst, nconst2);
                        if success {
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
                            return NextRoute::found(route2);
                        } else {
                            let route2: String = format!("{} {} {}", route, nconst, tconst);
                            // visited_names.extend(names_to_visit.clone());
                            next_level.push((route2, names_to_visit2));
                        }
                    }
                }
                visited_titles.extend(titles_to_visit);
            }
        }
    }

    NextRoute::search_further(visited_titles, next_level)
}

fn search_titles(
    cache: &State<TitlePrincipalCache>,
    visited_titles: &mut DashSet<String>,
    visited_names: &mut DashSet<String>,
    this_level: &Vec<(String, HashSet<String>)>,
    nconst2: &str,
    level: usize,
) -> Vec<String> {
    let mut next_level: Vec<(String, HashSet<String>)> = Vec::new();
    let batch_size = 16;

    if this_level.len() > batch_size {
        let mut success_route: Option<Vec<String>> = None;

        let chunks = this_level.chunks(batch_size);

        for chunk in chunks {
            let mut new_visited_titles: HashSet<String> = HashSet::new();
            let mut new_visited_names: HashSet<String> = HashSet::new();

            thread::scope(|scope| {
                let mut thread_cnt = 0;
                let (tx, rx) = mpsc::channel();
                for (route, names) in chunk {
                    let tx = tx.clone();

                    let a: &DashSet<String> = visited_titles;
                    let b: &DashSet<String> = visited_names;

                    scope.spawn(move || {
                        let next_route = search_route(cache, a, b, route, names, nconst2);
                        tx.send(next_route)
                            .expect("error sending message on channel!");
                    });

                    thread_cnt += 1;
                    new_visited_names.extend(names.clone());
                }
                for _ in 0..thread_cnt {
                    match rx.recv() {
                        Ok(next_route) => {
                            if next_route.success_route.is_some() {
                                success_route = next_route.success_route;
                            } else {
                                next_level.extend(next_route.next_level);
                                new_visited_titles.extend(next_route.visited_titles);
                            }
                        }
                        Err(err) => println!("Error receiving message: {:?}", err),
                    }
                }
            });

            if let Some(result) = success_route {
                return result;
            }
            visited_names.extend(new_visited_names);
            visited_titles.extend(new_visited_titles);
        }
    } else {
        for (route, names) in this_level {
            let next_route =
                search_route(cache, visited_titles, visited_names, route, names, nconst2);
            if let Some(success_route) = next_route.success_route {
                return success_route;
            } else {
                next_level.extend(next_route.next_level);
                visited_titles.extend(next_route.visited_titles);
                visited_names.extend(names.clone());
            }
        }
    }

    if level > 9 || next_level.is_empty() {
        vec![]
    } else {
        search_titles(
            cache,
            visited_titles,
            visited_names,
            &next_level,
            nconst2,
            level + 1,
        )
    }
}

fn mk_string(list: &Vec<String>, delimiter: &str) -> String {
    let mut str = String::new();
    let mut dl = "";
    for s in list {
        str.push_str(dl);
        str.push_str(&s);
        dl = delimiter;
    }
    str
}

#[openapi(tag = "IMDB")]
#[get("/imdb/distance/principal/<nconst>?<nconst2>")]
fn distance(cache: &State<TitlePrincipalCache>, nconst: &str, nconst2: &str) -> String {
    let start_time = SystemTime::now();
    let mut visited_titles: DashSet<String> = DashSet::with_capacity(100000);
    let mut visited_names: DashSet<String> = DashSet::with_capacity(100000);
    let first_level = vec![("".to_owned(), HashSet::from([nconst.to_string()]))];
    let result = search_titles(
        cache,
        &mut visited_titles,
        &mut visited_names,
        &first_level,
        nconst2,
        1,
    );

    let mut txt = String::new();
    if result.is_empty() {
        txt.push_str(&format!(
            "**** no result for {} -> {} ****",
            nconst, nconst2
        ));
    } else {
        let str = mk_string(&result, " -> ");
        txt.push_str(&format!("steps: {} ", (result.len() - 1) / 2));
        txt.push_str(&str);
    }
    println!("{txt}");
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
