use std::future;

use rocket::futures::StreamExt;
use rocket_db_pools::sqlx::postgres::PgRow;
use rocket_db_pools::sqlx::{self, Row};

use crate::schemas::{DbRow, TitleBasics, TitlePrincipal, TitlePrincipalCache, TitleToNames};

struct MyPgRow<'a>(&'a PgRow);

impl<'a> From<&'a PgRow> for MyPgRow<'a> {
    fn from(value: &'a PgRow) -> Self {
        MyPgRow(value)
    }
}

impl<'a> DbRow for MyPgRow<'a> {
    fn string(&self, column: &str) -> String {
        self.0.get::<String, &str>(column)
    }
    fn i32(&self, column: &str) -> i32 {
        self.0.get::<i32, &str>(column)
    }
    fn bool(&self, column: &str) -> bool {
        self.0.try_get::<bool, &str>(column).unwrap_or(false)
    }
    fn opt_string(&self, column: &str) -> Option<String> {
        self.0.try_get::<String, &str>(column).ok()
    }
    fn opt_i32(&self, column: &str) -> Option<i32> {
        self.0.try_get::<i32, &str>(column).ok()
    }
}

pub async fn titles_by_name(db_pool: &sqlx::PgPool, title_name: &str) -> Vec<TitleBasics> {
    let title_match = format!("%{}%", title_name);
    let sql = "SELECT * FROM title_basics 
    WHERE titletype = 'movie' AND (primarytitle ilike $1 or originaltitle ilike $2)
    ORDER BY startyear";
    let titles = sqlx::query(sql)
        .bind(&title_match)
        .bind(&title_match)
        .fetch_all(db_pool)
        .await
        .and_then(|rows| {
            let title_vec = rows
                .iter()
                .map(|r| TitleBasics::from_db_row(&MyPgRow::from(r)))
                .collect::<Vec<TitleBasics>>();

            Ok(title_vec)
        })
        .ok();

    let mut titles_with_principals: Vec<TitleBasics> = vec![];
    if let Some(title_vec) = titles {
        for mut t in title_vec {
            let principals = principals_by_title(db_pool, t.get_title_id()).await;
            if let Some(p_vec) = principals {
                t.add_principals(&p_vec);
            }
            titles_with_principals.push(t);
        }
    }

    titles_with_principals
}

async fn principals_by_title(
    db_pool: &sqlx::PgPool,
    title_id: &str,
) -> Option<Vec<TitlePrincipal>> {
    let sql = "SELECT tp.nconst, tp.category, tp.job, tp.characters , nb.primaryname, nb.birthyear, nb.deathyear
    FROM title_principals tp
    JOIN name_basics nb ON nb.nconst = tp.nconst
    WHERE tconst = $1";
    sqlx::query(sql)
        .bind(title_id)
        .fetch_all(db_pool)
        .await
        .and_then(|rows| {
            let principals = rows
                .iter()
                .map(|r| TitlePrincipal::from_db_row(&MyPgRow::from(r)))
                .collect::<Vec<TitlePrincipal>>();

            Ok(principals)
        })
        .ok()
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
) -> Option<TitleToNames> {
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
            let title_to_names = TitleToNames::from_db_row(&MyPgRow::from(&r));
            Ok(title_to_names)
        })
        .ok()
}
