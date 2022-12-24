DROP TABLE IF EXISTS callback_urls;

CREATE TABLE callback_urls (
    id INTEGER PRIMARY KEY,
    url VARCHAR UNIQUE
);
