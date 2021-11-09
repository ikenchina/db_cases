
reate database test_sql_case110;

\c test_sql_case110;


create type token_type as enum('default', 'invalid', 'deleted');
create table users(id bigserial primary key, user_id integer, token token_type);
create index idx_user_token on users(user_id, token);


# normal
DO $FN$
DECLARE
  user_token_count integer := 1;
BEGIN
  FOR i IN 200000..250000 LOOP
    select random() * 100 into user_token_count;
    insert into users(user_id, token) 
        select i + generate_series(1, user_token_count), 'default';
  END LOOP;
END;
$FN$;


# special
DO $FN$
DECLARE
  user_token_count integer := 1;
BEGIN
  FOR i IN 10000..10080 LOOP
    select random() * 1000 into user_token_count;
    insert into users(user_id, token) 
        select i + generate_series(1, user_token_count), 'default';
  END LOOP;
END;
$FN$;



# normal
DO $FN$
DECLARE
  user_token_count integer := 1;
BEGIN
  FOR i IN 10080..11080 LOOP
    select random() * 100 into user_token_count;
    insert into users(user_id, token) 
        select i + generate_series(1, user_token_count), 'default';
  END LOOP;
END;
$FN$;


analyze users;

