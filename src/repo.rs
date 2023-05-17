use std::future;

use rocket::futures::StreamExt;
use rocket::http::Status;
//use rocket_db_pools::sqlx::postgres::PgRow;
use rocket_db_pools::sqlx::{self, Error, Row};

use crate::schemas::{
    NameBasics, TitleBasics, TitleDetails, TitlePrincipal, TitlePrincipalCache, TitleToNames,
};

pub async fn titles_by_name(
    db_pool: &sqlx::PgPool,
    title_name: &str,
) -> Result<Vec<TitleDetails>, (Status, String)> {
    let title_match = format!("%{}%", title_name);
    let sql = "SELECT tb.*, tr.numvotes, tr.averagerating FROM title_basics tb
    JOIN title_ratings tr ON tr.tconst = tb.tconst
    WHERE tb.titletype = 'movie' AND (tb.primarytitle ilike $1 or tb.originaltitle ilike $2)
    ORDER BY startyear";
    let titles = sqlx::query(sql)
        .bind(&title_match)
        .bind(&title_match)
        .fetch_all(db_pool)
        .await
        .and_then(|rows| {
            let title_vec = rows
                .iter()
                .map(|r| TitleDetails::from_db_row(r))
                .collect::<Vec<TitleDetails>>();

            Ok(title_vec)
        });

    match titles {
        Ok(title_vec) => {
            if title_vec.is_empty() {
                Err((
                    Status::NotFound,
                    format!("Could not find Title {}", title_name),
                ))
            } else {
                let mut titles_with_principals: Vec<TitleDetails> = vec![];
                for mut t in title_vec {
                    let principals = principals_by_title(db_pool, t.get_title_id()).await?;
                    t.add_principals(&principals);
                    titles_with_principals.push(t);
                }
                Ok(titles_with_principals)
            }
        }
        Err(err) => Err((
            Status::InternalServerError,
            format!("Error querying title {} by name: {:?}", title_name, err),
        )),
    }
}

async fn principals_by_title(
    db_pool: &sqlx::PgPool,
    title_id: &str,
) -> Result<Vec<TitlePrincipal>, (Status, String)> {
    let sql = "SELECT tp.nconst, tp.category, tp.job, tp.characters , nb.primaryname, nb.birthyear, nb.deathyear
    FROM title_principals tp
    JOIN name_basics nb ON nb.nconst = tp.nconst
    WHERE tconst = $1
    ORDER BY tp.ordering";
    sqlx::query(sql)
        .bind(title_id)
        .fetch_all(db_pool)
        .await
        .and_then(|rows| {
            let principals = rows
                .iter()
                .map(|r| TitlePrincipal::from_db_row(r))
                .collect::<Vec<TitlePrincipal>>();

            Ok(principals)
        })
        .map_err(|err| {
            (
                Status::InternalServerError,
                format!(
                    "Error retrieving title principals for title id {}: {:?}",
                    title_id, err
                ),
            )
        })
}

pub async fn titles_to_principals(db_pool: &sqlx::PgPool, cache: &TitlePrincipalCache) {
    let sql = "SELECT tconst, nconst FROM title_principals tp where category = 'actor'";
    sqlx::query(sql)
        .fetch(db_pool)
        .for_each(|result| {
            match result {
                Ok(row) => {
                    let tconst = row.get::<String, usize>(0);
                    let nconst = row.get::<String, usize>(1);
                    cache.insert(tconst, nconst);
                }
                Err(error) => {
                    println!("Error reading DB row: {}", error);
                }
            }
            future::ready(())
        })
        .await;
}

pub async fn title_to_names(
    db_pool: &sqlx::PgPool,
    tconst: &str,
    nconst1: &str,
    nconst2: &str,
) -> Result<TitleToNames, (Status, String)> {
    let sql = "SELECT tb.tconst, tb.primarytitle, tb.startyear, tb.titletype, tp1.nconst nconst1, tp1.characters characters1, nb1.primaryname primaryname1, tp2.nconst nconst2, tp2.characters characters2, nb2.primaryname primaryname2
    FROM title_basics tb 
    JOIN title_principals tp1 ON tp1.tconst = tb.tconst AND tp1.nconst = $1 
    JOIN title_principals tp2 ON tp2.tconst = tb.tconst AND tp2.nconst = $2 
    JOIN name_basics nb1 ON nb1.nconst = tp1.nconst  
    JOIN name_basics nb2 ON nb2.nconst = tp2.nconst    
    WHERE tb.tconst = $3";
    sqlx::query(sql)
        .bind(nconst1)
        .bind(nconst2)
        .bind(tconst)
        .fetch_one(db_pool)
        .await
        .and_then(|r| {
            let title_to_names = TitleToNames::from_db_row(&r);
            Ok(title_to_names)
        })
        .map_err(|err| {
            (
                Status::InternalServerError,
                format!(
                    "Error retrieving title {} and principals {}, {}: {:?}",
                    tconst, nconst1, nconst2, err
                ),
            )
        })
}

pub async fn basics_for_name(
    db_pool: &sqlx::PgPool,
    cache: &TitlePrincipalCache,
    name: &str,
) -> Result<Vec<NameBasics>, Error> {
    let sql = "SELECT nconst, primaryname, primaryprofession, birthyear, deathyear, knownfortitles
    FROM name_basics WHERE primaryname = $1";
    let name_vec = sqlx::query(sql)
        .bind(name)
        .fetch_all(db_pool)
        .await
        .and_then(|rows| {
            let name_vec = rows
                .iter()
                .map(|r| NameBasics::from_db_row(r))
                .collect::<Vec<NameBasics>>();

            Ok(name_vec)
        })?;

    let mut new_name_vec: Vec<NameBasics> = Vec::new();
    for mut name_basics in name_vec {
        let tconsts = name_basics.title_ids();
        // this is prone to SQL injection, but unfortunately sqlx 0.5 has no prepared statement solution for this
        let sql = format!(
            "SELECT * FROM title_basics WHERE tconst in ( {} )",
            mk_string(&tconsts, "'", "', '", "'")
        );

        let titles = sqlx::query(&sql)
            .fetch_all(db_pool)
            .await
            .and_then(|rows| {
                let title_vec = rows
                    .iter()
                    .map(|r| TitleBasics::from_db_row(r))
                    .collect::<Vec<TitleBasics>>();

                Ok(title_vec)
            })?;

        let references = cache.ref_count(&name_basics);
        name_basics.set_details(references, titles);
        new_name_vec.push(name_basics);
    }

    new_name_vec.sort_by(|rec1, rec2| rec2.actorroles.cmp(&rec1.actorroles));

    Ok(new_name_vec)
}

pub async fn nconst_for_name(
    db_pool: &sqlx::PgPool,
    name: &str,
) -> Result<Vec<String>, (Status, String)> {
    let sql = "SELECT nconst FROM name_basics WHERE primaryname = $1";
    let result = sqlx::query(sql)
        .bind(name)
        .fetch_all(db_pool)
        .await
        .and_then(|rows| {
            let name_vec = rows
                .iter()
                .map(|r| r.get::<String, &str>("nconst"))
                .collect::<Vec<String>>();

            Ok(name_vec)
        });
    match result {
        Ok(ok) => Ok(ok),
        Err(err) => Err((
            Status::InternalServerError,
            format!("Error searching for name {}: {:?}", name, err),
        )),
    }
}

fn mk_string_simple(list: &Vec<String>, delimiter: &str) -> String {
    let mut str = String::new();
    let mut dl = "";
    for s in list {
        str.push_str(dl);
        str.push_str(&s);
        dl = delimiter;
    }
    str
}

fn mk_string(list: &Vec<String>, start: &str, delimiter: &str, end: &str) -> String {
    let mut str = String::from(start);
    str.push_str(&mk_string_simple(list, delimiter));
    str.push_str(end);
    str
}
