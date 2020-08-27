CREATE KEYSPACE spark_db WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
USE spark_db;
CREATE TABLE users(Login_id text PRIMARY KEY, user_name text, last_login timestamp);

INSERT INTO users (Login_id, user_name, last_login) VALUES( '100001', 'Prashant', '2019-02-05 10:05:00');
INSERT INTO users (Login_id, user_name, last_login) VALUES( '100009', 'Alisha', '2019-03-07 11:03:00');
INSERT INTO users (Login_id, user_name, last_login) VALUES( '100087', 'Abdul', '2019-06-12 09:43:00');

INSERT INTO users (Login_id, user_name, last_login) VALUES( '100091', 'New User', '2019-06-12 09:43:00');