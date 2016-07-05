CREATE KEYSPACE it WITH replication = {'class':'SimpleStrategy', 'replication_factor':3};
CREATE TABLE it.users (user_id text PRIMARY KEY, first_name text, last_name text, age int, emails set<text>, top_places list<text>, todo map<timestamp, text>);
CREATE TABLE it.simple_types (id int PRIMARY KEY, name text, flt float, dbl double, bl bool, ts timestamp, dt date);
