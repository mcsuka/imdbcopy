use rocket::serde::{Deserialize, Serialize};

use rocket_okapi::okapi::schemars;
use rocket_okapi::okapi::schemars::JsonSchema;

pub trait DbRow {
    fn string(&self, column: &str) -> String;
    fn i32(&self, column: &str) -> i32;
    fn bool(&self, column: &str) -> bool;
    fn opt_string(&self, column: &str) -> Option<String>;
    fn opt_i32(&self, column: &str) -> Option<i32>;
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(crate = "rocket::serde")]
pub struct TitleBasics {
    tconst: String,
    titletype: Option<String>,
    primarytitle: Option<String>,
    originaltitle: Option<String>,
    startyear: Option<i32>,
    runtimeminutes: Option<i32>,
    genres: Option<String>,
    isadult: bool,
    principals: Vec<TitlePrincipal>,
}

impl TitleBasics {
    pub fn from_db_row(r: &dyn DbRow) -> TitleBasics {
        TitleBasics {
            tconst: r.string("tconst"),
            titletype: r.opt_string("titletype"),
            primarytitle: r.opt_string("primarytitle"),
            originaltitle: r.opt_string("originaltitle"),
            startyear: r.opt_i32("startyear"),
            runtimeminutes: r.opt_i32("runtimeminutes"),
            genres: r.opt_string("genres"),
            isadult: r.bool("isadult"),
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
    category: Option<String>,
    characters: Option<String>,
    primaryname: Option<String>,
    birthyear: Option<i32>,
    deathyear: Option<i32>,
}

impl TitlePrincipal {
    pub fn from_db_row(r: &dyn DbRow) -> TitlePrincipal {
        TitlePrincipal {
            nconst: r.string("nconst"),
            category: r.opt_string("category"),
            characters: r.opt_string("characters"),
            primaryname: r.opt_string("primaryname"),
            birthyear: r.opt_i32("birthyear"),
            deathyear: r.opt_i32("deathyear"),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::schemas::DbRow;


    #[test]
    fn dummy() {
        
    }
}