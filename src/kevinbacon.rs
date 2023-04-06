use std::collections::HashSet;

use dashmap::DashSet;
use rayon::prelude::*;
use rocket::State;

use crate::schemas::TitlePrincipalCache;

struct NextRoute {
    success_route: Option<Vec<String>>,
    next_level: Vec<(String, HashSet<String>)>,
}

pub struct NameNotFound(pub String);

impl NextRoute {
    fn new() -> NextRoute {
        NextRoute {
            success_route: None,
            next_level: Vec::from([]),
        }
    }

    fn found(route: Vec<String>) -> NextRoute {
        NextRoute {
            success_route: Some(route),
            next_level: Vec::from([]),
        }
    }

    fn search_further(next_level: Vec<(String, HashSet<String>)>) -> NextRoute {
        NextRoute {
            success_route: None,
            next_level: next_level,
        }
    }
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
                ignored_names.insert(nconst_i.to_string());
            }
        }
    }
    (false, names_to_visit)
}

fn search_route(
    cache: &State<TitlePrincipalCache>,
    ignored_titles: &DashSet<String>,
    ignored_names: &DashSet<String>,
    route: &str,
    names_to_visit: &HashSet<String>,
    nconst2: &str,
) -> Result<NextRoute, NameNotFound> {
    let mut next_level: Vec<(String, HashSet<String>)> = Vec::new();

    for nconst in names_to_visit {
        if let Some(titles) = cache.p_to_t(nconst) {
            for tconst in titles.value() {
                if !ignored_titles.contains(tconst) {
                    ignored_titles.insert(tconst.to_string());

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
                        return Ok(NextRoute::found(route2));
                    } else {
                        let route2: String = format!("{} {} {}", route, nconst, tconst);
                        // visited_names.extend(names_to_visit.clone());
                        next_level.push((route2, names_to_visit2));
                    }
                }
            }
        } else {
            return Err(NameNotFound(nconst.to_string()));
        }
    }

    Ok(NextRoute::search_further(next_level))
}

fn par_search_titles(
    cache: &State<TitlePrincipalCache>,
    visited_titles: &mut DashSet<String>,
    visited_names: &mut DashSet<String>,
    this_level: &Vec<(String, HashSet<String>)>,
    nconst2: &str,
    level: usize,
) -> Result<Vec<String>, NameNotFound> {
    let mut next_level: Vec<(String, HashSet<String>)> = Vec::new();
    let batch_size = 1000;

    for chunk in this_level.chunks(batch_size) {
        let next_route_result = chunk
            .par_iter()
            .map(|(route, names)| {
                let next_route =
                    search_route(cache, visited_titles, visited_names, route, names, nconst2);
                next_route
            })
            .try_reduce(
                || NextRoute::new(),
                |mut all_routes, next_route| {
                    all_routes.next_level.extend(next_route.next_level);
                    Ok(NextRoute {
                        success_route: all_routes
                            .success_route
                            .clone()
                            .or(next_route.success_route),
                        next_level: all_routes.next_level,
                    })
                },
            );
        match next_route_result {
            Ok(next_route) => {
                if let Some(route) = next_route.success_route {
                    return Ok(route);
                } else {
                    next_level.extend(next_route.next_level);
                }
            }
            Err(err) => return Err(err),
        }
    }

    if level > 9 || next_level.is_empty() {
        Ok(vec![])
    } else {
        par_search_titles(
            cache,
            visited_titles,
            visited_names,
            &next_level,
            nconst2,
            level + 1,
        )
    }
}

pub fn search_titles(
    do_parallel: bool,
    cache: &State<TitlePrincipalCache>,
    visited_titles: &mut DashSet<String>,
    visited_names: &mut DashSet<String>,
    this_level: &Vec<(String, HashSet<String>)>,
    nconst2: &str,
    level: usize,
) -> Result<Vec<String>, NameNotFound> {
    let mut next_level: Vec<(String, HashSet<String>)> = Vec::new();

    for (route, names) in this_level {
        let next_route = search_route(cache, visited_titles, visited_names, route, names, nconst2)?;
        if let Some(success_route) = next_route.success_route {
            return Ok(success_route);
        } else {
            next_level.extend(next_route.next_level);
        }
    }

    if level > 9 || next_level.is_empty() {
        Ok(vec![])
    } else if do_parallel && level > 1 {
        par_search_titles(
            cache,
            visited_titles,
            visited_names,
            &next_level,
            nconst2,
            level + 1,
        )
    } else {
        search_titles(
            do_parallel,
            cache,
            visited_titles,
            visited_names,
            &next_level,
            nconst2,
            level + 1,
        )
    }
}
