SHOW search_path;
CREATE SCHEMA IF NOT EXISTS content;
SET search_path TO content,public;
CREATE TABLE IF NOT EXISTS content.film_work (
    id uuid PRIMARY KEY,
    title TEXT NOT NULL,
    description TEXT,
    creation_date DATE,
    file_path TEXT,
    rating FLOAT,
    type TEXT NOT NULL,
    created timestamp with time zone,
    modified timestamp with time zone
);
CREATE TABLE IF NOT EXISTS content.genre (
    id uuid PRIMARY KEY,
    name varchar NOT NULL,
    description TEXT,
    created timestamp with time zone,
    modified timestamp with time zone
);
CREATE TABLE IF NOT EXISTS content.person (
    id uuid PRIMARY KEY,
    full_name TEXT NOT NULL,
    created timestamp with time zone,
    modified timestamp with time zone
);
CREATE TABLE IF NOT EXISTS content.genre_film_work
(
    id           uuid PRIMARY KEY,
    genre_id     uuid NOT NULL REFERENCES genre (id) ON DELETE CASCADE,
    film_work_id uuid NOT NULL REFERENCES film_work (id) ON DELETE CASCADE,
    created      timestamp with time zone
);
CREATE TABLE IF NOT EXISTS content.person_film_work
(
    id           uuid PRIMARY KEY,
    person_id    uuid NOT NULL REFERENCES person (id) ON DELETE CASCADE,
    film_work_id uuid NOT NULL REFERENCES film_work (id) ON DELETE CASCADE,
    role         TEXT NOT NULL,
    created      timestamp with time zone
);
CREATE UNIQUE INDEX film_work_person ON content.person_film_work (film_work_id, person_id, role);
CREATE UNIQUE INDEX film_work_genre ON content.genre_film_work (film_work_id, genre_id);
CREATE INDEX film_work_creation_date_idx ON content.film_work (creation_date);

