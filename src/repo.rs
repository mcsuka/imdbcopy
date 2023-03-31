
use rocket_db_pools::sqlx::{self, Row};
use rocket_db_pools::sqlx::postgres::PgRow;

use crate::schemas::{DbRow, TitleBasics, TitlePrincipal};

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
