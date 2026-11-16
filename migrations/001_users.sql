CREATE TABLE users (
    user_id    INTEGER PRIMARY KEY,
    home_city  TEXT NOT NULL
);

-- Registered home cities, recorded once at "account opening".
-- Copied from the producer's home_city(): CITIES[user_id % 5].
INSERT INTO users (user_id, home_city) VALUES
    (1, 'Mumbai'),  (2, 'Bangalore'),  (3, 'London'),  (4, 'Dubai'),   (5, 'Delhi'),
    (6, 'Mumbai'),  (7, 'Bangalore'),  (8, 'London'),  (9, 'Dubai'),   (10, 'Delhi'),
    (11, 'Mumbai'), (12, 'Bangalore'), (13, 'London'), (14, 'Dubai'),  (15, 'Delhi'),
    (16, 'Mumbai'), (17, 'Bangalore'), (18, 'London'), (19, 'Dubai'),  (20, 'Delhi');