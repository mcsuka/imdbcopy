use rocket::serde::{Deserialize, Serialize};

use rocket_okapi::okapi::schemars;
use rocket_okapi::okapi::schemars::JsonSchema;

use rocket_db_pools::sqlx::{postgres::PgRow, Row};

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(crate = "rocket::serde")]
pub struct TitleBasics {
    tconst: String,
    titletype: String,
    primarytitle: String,
    originaltitle: String,
    startyear: i32,
    runtimeminutes: i32,
    genres: String,
    isadult: bool,
    principals: Vec<TitlePrincipal>,
}

impl TitleBasics {
    pub fn from_db_row(r: &PgRow) -> TitleBasics {
        TitleBasics {
            tconst: r.get::<String, &str>("tconst"),
            titletype: r
                .try_get::<String, &str>("titletype")
                .unwrap_or("".to_string()),
            primarytitle: r
                .try_get::<String, &str>("primarytitle")
                .unwrap_or("".to_string()),
            originaltitle: r
                .try_get::<String, &str>("originaltitle")
                .unwrap_or("".to_string()),
            startyear: r.try_get::<i32, &str>("startyear").unwrap_or(0),
            runtimeminutes: r.try_get::<i32, &str>("runtimeminutes").unwrap_or(0),
            genres: r
                .try_get::<String, &str>("genres")
                .unwrap_or("".to_string()),
            isadult: r.try_get::<bool, &str>("isadult").unwrap_or(false),
            principals: vec![],
        }
    }

    pub fn get_title_id(&self) -> &str {
        &self.tconst
    }

    pub fn add_principals(&mut self, principals: &Vec<TitlePrincipal>) {
        self.principals = principals.clone();
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(crate = "rocket::serde")]
pub struct TitlePrincipal {
    nconst: String,
    category: String,
    characters: String,
    primaryname: String,
    birthyear: i32,
    deathyear: i32,
}

impl TitlePrincipal {
    pub fn from_db_row(r: &PgRow) -> TitlePrincipal {
        TitlePrincipal {
            nconst: r.get::<String, &str>("nconst"),
            category: r
                .try_get::<String, &str>("category")
                .unwrap_or("".to_string()),
            characters: r
                .try_get::<String, &str>("characters")
                .unwrap_or("".to_string()),
            primaryname: r
                .try_get::<String, &str>("primaryname")
                .unwrap_or("".to_string()),
            birthyear: r.try_get::<i32, &str>("birthyear").unwrap_or(0),
            deathyear: r.try_get::<i32, &str>("deathyear").unwrap_or(0),
        }
    }
}
