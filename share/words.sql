DROP TABLE IF EXISTS correct_words;
DROP TABLE IF EXISTS valid_words;

CREATE TABLE valid_words(
    valid_word_id INTEGER PRIMARY KEY,
    valid_word VARCHAR NOT NULL
);

CREATE TABLE correct_words(
    correct_word_id INTEGER PRIMARY KEY,
    correct_word VARCHAR NOT NULL
);