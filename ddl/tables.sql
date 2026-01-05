create table title_basics (
  tconst varchar(255) not null,
  titletype varchar(255) not null,
  primarytitle varchar(255) not null,
  originaltitle varchar(255),
  isadult bool,
  startyear integer,
  endyear integer,
  runtimeminutes integer,
  genres varchar(255),
  CONSTRAINT title_basics_pkey PRIMARY KEY (tconst)
);

create table name_basics (
  nconst varchar(255) not null,
  primaryname varchar(255) not null,
  birthyear integer,
  deathyear integer,
  primaryprofession varchar(255),
  knownfortitles varchar(255),
  CONSTRAINT name_basics_pkey PRIMARY KEY (nconst)
);

create table title_principals (
  tconst varchar(255) not null,
  ordering integer not null,
  nconst varchar(255) not null,
  category varchar(255),
  job varchar(255),
  characters varchar(255),
  CONSTRAINT title_principals_pkey PRIMARY KEY (tconst, nconst),
  CONSTRAINT title_principals_fk1 FOREIGN KEY (tconst) REFERENCES title_basics("tconst") ON DELETE cascade,
  CONSTRAINT title_principals_fk2 FOREIGN KEY (nconst) REFERENCES name_basics("nconst") ON DELETE cascade
);

create table title_ratings (
  tconst varchar(255) not null,
  averagerating real not null,
  numvotes integer null,
  CONSTRAINT title_ratings_pkey PRIMARY KEY (tconst),
  CONSTRAINT title_ratings_fk1 FOREIGN KEY (tconst) REFERENCES title_basics("tconst") ON DELETE cascade
);

