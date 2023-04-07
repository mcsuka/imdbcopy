# IMDB copycat + degrees of separation
Search movie data in a Postgres database, using Rust, Rocket, Rocket-OKAPI, Rayon, DashMap.\
The project was meant for me to learn Rust and Rocket, there is very little unit testing done and there is no warranty whatsoever it is working correctly.

## Search for a title
Search for films (movie, documentary, series, ...) by a title fragment

## Search for a contributor
Search for contributors (actors, ctresses, directors, ...) by name

## Degrees of separation
Find the shortest distance between 2 actors, via common titles, using Breadth First Search.\
For best performance, compile with `--release` flag.

