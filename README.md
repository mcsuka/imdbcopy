# IMDB copycat + degrees of separation
Search movie data in a PostgreSQL database, using Rust, Rocket, Rocket-OKAPI, Rayon, DashMap.\
The project was meant for me to learn Rust and Rocket, there is very little unit testing done and there is no warranty whatsoever it is working correctly.\
The IMDB data can be downloaded from https://datasets.imdbws.com/ as tab-separated files.

## Search for a title
Search for films (movie, documentary, series, ...) by a title fragment

## Search for a contributor
Search for contributors (actors, actresses, directors, ...) by name

## Degrees of separation
Find the shortest distance between 2 actors, via common titles, using Breadth First Search.\
For best performance, compile with `--release` flag.

