use dashmap::DashMap;

use dashmap::mapref::one::Ref;
use rocket::serde::{Deserialize, Serialize};

use rocket_okapi::okapi::schemars;
use rocket_okapi::okapi::schemars::JsonSchema;

use std::collections::HashSet;
use std::sync::atomic::{AtomicUsize, Ordering};

pub trait DbRow {
    fn string(&self, column: &str) -> String;
    fn i32(&self, column: &str) -> i32;
    fn bool(&self, column: &str) -> bool;
    fn opt_string(&self, column: &str) -> Option<String>;
    fn opt_i32(&self, column: &str) -> Option<i32>;
    fn opt_f32(&self, column: &str) -> Option<f32>;
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(crate = "rocket::serde")]
pub struct TitleBasics {
    tconst: String,
    titletype: Option<String>,
    primarytitle: Option<String>,
    startyear: Option<i32>,
}

impl TitleBasics {
    pub fn from_db_row(r: &dyn DbRow) -> TitleBasics {
        TitleBasics {
            tconst: r.string("tconst"),
            titletype: r.opt_string("titletype"),
            primarytitle: r.opt_string("primarytitle"),
            startyear: r.opt_i32("startyear"),
        }
    }

    pub fn from_tconst(tconst: &str) -> TitleBasics {
        TitleBasics {
            tconst: tconst.to_string(),
            titletype: None,
            primarytitle: None,
            startyear: None,
        }
    }

}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(crate = "rocket::serde")]
pub struct TitleDetails {
    tconst: String,
    titletype: Option<String>,
    primarytitle: Option<String>,
    originaltitle: Option<String>,
    averagerating: Option<f32>,
    numvotes: Option<i32>,
    startyear: Option<i32>,
    runtimeminutes: Option<i32>,
    genres: Option<String>,
    isadult: bool,
    principals: Vec<TitlePrincipal>,
}

impl TitleDetails {
    pub fn from_db_row(r: &dyn DbRow) -> TitleDetails {
        TitleDetails {
            tconst: r.string("tconst"),
            titletype: r.opt_string("titletype"),
            primarytitle: r.opt_string("primarytitle"),
            originaltitle: r.opt_string("originaltitle"),
            averagerating: r.opt_f32("averagerating"),
            numvotes: r.opt_i32("numvotes"),
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

#[derive(Clone, Debug, Serialize, JsonSchema)]
#[serde(crate = "rocket::serde")]
pub struct NameToTitle {
    nconst: String,
    primaryname: Option<String>,
    characters: Option<String>,
}

#[derive(Clone, Debug, Serialize, JsonSchema)]
#[serde(crate = "rocket::serde")]
pub struct TitleToNames {
    tconst: String,
    primarytitle: Option<String>,
    titletype: Option<String>,
    startyear: Option<i32>,
    actor1: NameToTitle,
    actor2: NameToTitle,
}

impl TitleToNames {
    pub fn from_db_row(r: &dyn DbRow) -> TitleToNames {
        TitleToNames {
            tconst: r.string("tconst"),
            primarytitle: r.opt_string("primarytitle"),
            titletype: r.opt_string("titletype"),
            startyear: r.opt_i32("startyear"),
            actor1: NameToTitle {
                nconst: r.string("nconst1"),
                primaryname: r.opt_string("primaryname1"),
                characters: r.opt_string("characters1"),
            },
            actor2: NameToTitle {
                nconst: r.string("nconst2"),
                primaryname: r.opt_string("primaryname2"),
                characters: r.opt_string("characters2"),
            },
        }
    }
}

#[derive(Clone, Debug, Serialize, JsonSchema)]
#[serde(crate = "rocket::serde")]
pub struct NameBasics {
    nconst: String,
    pub actorroles: usize,
    primaryname: Option<String>,
    primaryprofession: Option<String>,
    birthyear: Option<i32>,
    deathyear: Option<i32>,
    knownfortitles: Vec<TitleBasics>,
}

impl NameBasics {
    pub fn from_db_row(r: &dyn DbRow) -> NameBasics {
        let mut knownfortitles: Vec<TitleBasics> = vec![];
        if let Some(kt) = r.opt_string("knownfortitles") {
            kt.split(",")
                .for_each(|tconst| knownfortitles.push(TitleBasics::from_tconst(tconst)));
        }
        NameBasics {
            nconst: r.string("nconst"),
            actorroles: 0,
            primaryname: r.opt_string("primaryname"),
            primaryprofession: r.opt_string("primaryprofession"),
            birthyear: r.opt_i32("birthyear"),
            deathyear: r.opt_i32("deathyear"),
            knownfortitles: knownfortitles,
        }
    }

    pub fn title_ids(&self) -> Vec<String> {
        self.knownfortitles
            .iter()
            .map(|t| t.tconst.to_string())
            .collect()
    }

    pub fn set_details(&mut self, references: usize, titles: Vec<TitleBasics>) {
        self.actorroles = references;
        self.knownfortitles = titles;
    }
}

#[derive(Debug)]
pub struct TitlePrincipalCache {
    insert_counter: AtomicUsize,
    t_to_p: DashMap<String, HashSet<String>>,
    p_to_t: DashMap<String, HashSet<String>>,
}

impl TitlePrincipalCache {
    pub fn new() -> TitlePrincipalCache {
        TitlePrincipalCache {
            insert_counter: AtomicUsize::new(0),
            t_to_p: DashMap::with_capacity(1_500_000),
            p_to_t: DashMap::with_capacity(1_500_000),
        }
    }

    pub fn insert(&self, tconst: String, nconst: String) {
        let mut entry = self.t_to_p.get_mut(&tconst);
        if let Some(set) = entry.as_deref_mut() {
            set.insert(nconst.clone());
        } else {
            let set: HashSet<String> = HashSet::from([nconst.clone()]);
            self.t_to_p.insert(tconst.clone(), set);
        }

        let mut entry = self.p_to_t.get_mut(&nconst);
        if let Some(set) = entry.as_deref_mut() {
            set.insert(tconst);
        } else {
            let set: HashSet<String> = HashSet::from([tconst]);
            self.p_to_t.insert(nconst, set);
        }

        let cntr = self.insert_counter.fetch_add(1, Ordering::Relaxed);
        if cntr % 100000 == 0 {
            println!("Processed {} mappings", cntr);
        }
    }

    pub fn t_to_p(&self, tconst: &str) -> Option<Ref<'_, String, HashSet<String>>> {
        self.t_to_p.get(tconst)
    }

    pub fn p_to_t(&self, nconst: &str) -> Option<Ref<'_, String, HashSet<String>>> {
        self.p_to_t.get(nconst)
    }

    pub fn len(&self) -> (usize, usize) {
        (self.t_to_p.len(), self.p_to_t.len())
    }

    pub fn ref_count(&self, name_basics: &NameBasics) -> usize {
        self.p_to_t.get(&name_basics.nconst).map_or(0, |x| x.len())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use crate::schemas::{DbRow, TitleDetails, TitlePrincipal};

    struct TestDbRow<'r> {
        map: HashMap<&'static str, &'r str>,
    }

    impl<'r> DbRow for TestDbRow<'r> {
        fn string(&self, column: &str) -> String {
            self.map.get(column).unwrap().to_string()
        }

        fn i32(&self, column: &str) -> i32 {
            self.map
                .get(column)
                .map(|x| x.parse::<i32>().unwrap())
                .unwrap()
        }

        fn bool(&self, column: &str) -> bool {
            self.map.get(column).map(|x| *x == "true").unwrap_or(false)
        }

        fn opt_string(&self, column: &str) -> Option<String> {
            self.map.get(column).map(|x| x.to_string())
        }

        fn opt_i32(&self, column: &str) -> Option<i32> {
            self.map.get(column).map(|x| x.parse::<i32>().unwrap())
        }

        fn opt_f32(&self, column: &str) -> Option<f32> {
            self.map.get(column).map(|x| x.parse::<f32>().unwrap())
        }
    }

    const TCONST: &str = "value1";
    const TITLETYPE: &str = "value2";
    const PRIMARYTITLE: &str = "value3";
    const ORIGINALTITLE: &str = "value4";
    const STARTYEAR: i32 = 1234;
    const RUNTIMEMINUTES: i32 = 234;
    const GENRES: &str = "[value5, value6]";
    const ISADULT: bool = true;
    const AVERAGERATING: f32 = 3.33;
    const NUMVOTES: i32 = 666;

    const NCONST: &str = "value7";
    const CATEGORY: &str = "value8";
    const CHARACTERS: &str = "[value9]";
    const PRIMARYNAME: &str = "valueA";
    const BIRTHYEAR: i32 = 1922;
    const DEATHYEAR: i32 = 1999;

    #[test]
    fn title_details_from_db_row_optionals() {
        let title_basics = TitleDetails {
            tconst: TCONST.to_string(),
            titletype: Some(TITLETYPE.to_string()),
            primarytitle: Some(PRIMARYTITLE.to_string()),
            originaltitle: Some(ORIGINALTITLE.to_string()),
            averagerating: Some(AVERAGERATING),
            numvotes: Some(NUMVOTES),
            startyear: Some(STARTYEAR),
            runtimeminutes: Some(RUNTIMEMINUTES),
            genres: Some(GENRES.to_string()),
            isadult: ISADULT,
            principals: vec![],
        };

        let startyear = STARTYEAR.to_string();
        let runtimeminutes = RUNTIMEMINUTES.to_string();
        let isadult = ISADULT.to_string();
        let averagerating = AVERAGERATING.to_string();
        let numvotes = NUMVOTES.to_string();
        let map: HashMap<&'static str, &str> = HashMap::from([
            ("tconst", TCONST),
            ("titletype", TITLETYPE),
            ("primarytitle", PRIMARYTITLE),
            ("originaltitle", ORIGINALTITLE),
            ("averagerating", &averagerating),
            ("numvotes", &numvotes),
            ("startyear", &startyear),
            ("runtimeminutes", &runtimeminutes),
            ("genres", &GENRES),
            ("isadult", &isadult),
        ]);
        let row = TestDbRow { map };
        let new_title_basics = TitleDetails::from_db_row(&row);

        assert_eq!(new_title_basics.tconst, title_basics.tconst);
        assert_eq!(new_title_basics.titletype, title_basics.titletype);
        assert_eq!(new_title_basics.primarytitle, title_basics.primarytitle);
        assert_eq!(new_title_basics.originaltitle, title_basics.originaltitle);
        assert_eq!(new_title_basics.startyear, title_basics.startyear);
        assert_eq!(new_title_basics.runtimeminutes, title_basics.runtimeminutes);
        assert_eq!(new_title_basics.isadult, title_basics.isadult);
        assert!(new_title_basics.principals.is_empty());
    }

    #[test]
    fn title_basics_from_db_row_mandatory() {
        let title_basics = TitleDetails {
            tconst: TCONST.to_string(),
            titletype: None,
            primarytitle: None,
            originaltitle: None,
            averagerating: None,
            numvotes: None,
            startyear: None,
            runtimeminutes: None,
            genres: None,
            isadult: false,
            principals: vec![],
        };

        let map: HashMap<&'static str, &str> = HashMap::from([("tconst", TCONST)]);
        let row = TestDbRow { map };
        let new_title_basics = TitleDetails::from_db_row(&row);

        assert_eq!(new_title_basics.tconst, title_basics.tconst);
        assert!(new_title_basics.titletype.is_none());
        assert!(new_title_basics.primarytitle.is_none());
        assert!(new_title_basics.originaltitle.is_none());
        assert!(new_title_basics.startyear.is_none());
        assert!(new_title_basics.runtimeminutes.is_none());
        assert!(new_title_basics.isadult == false);
        assert!(new_title_basics.principals.is_empty());
    }

    #[test]
    fn title_principal_from_db_row_optionals() {
        let title_principal = TitlePrincipal {
            nconst: NCONST.to_string(),
            category: Some(CATEGORY.to_string()),
            characters: Some(CHARACTERS.to_string()),
            primaryname: Some(PRIMARYNAME.to_string()),
            birthyear: Some(BIRTHYEAR),
            deathyear: Some(DEATHYEAR),
        };

        let birthyear = BIRTHYEAR.to_string();
        let deathyear = DEATHYEAR.to_string();
        let map = HashMap::from([
            ("nconst", NCONST),
            ("category", CATEGORY),
            ("characters", CHARACTERS),
            ("primaryname", PRIMARYNAME),
            ("birthyear", &birthyear),
            ("deathyear", &deathyear),
        ]);
        let row = TestDbRow { map };
        let new_title_principal = TitlePrincipal::from_db_row(&row);

        assert_eq!(new_title_principal.nconst, title_principal.nconst);
        assert_eq!(new_title_principal.category, title_principal.category);
        assert_eq!(new_title_principal.characters, title_principal.characters);
        assert_eq!(new_title_principal.primaryname, title_principal.primaryname);
        assert_eq!(new_title_principal.birthyear, title_principal.birthyear);
        assert_eq!(new_title_principal.deathyear, title_principal.deathyear);
    }

    #[test]
    fn title_principal_from_db_row_mandatory() {
        let title_principal = TitlePrincipal {
            nconst: NCONST.to_string(),
            category: None,
            characters: None,
            primaryname: None,
            birthyear: None,
            deathyear: None,
        };

        let map = HashMap::from([("nconst", NCONST)]);
        let row = TestDbRow { map };
        let new_title_principal = TitlePrincipal::from_db_row(&row);

        assert_eq!(new_title_principal.nconst, title_principal.nconst);
        assert!(new_title_principal.category.is_none());
        assert!(new_title_principal.characters.is_none());
        assert!(new_title_principal.primaryname.is_none());
        assert!(new_title_principal.birthyear.is_none());
        assert!(new_title_principal.deathyear.is_none());
    }
}
