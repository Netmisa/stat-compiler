-- Table: stat_compiled.users

DROP TABLE IF EXISTS stat_compiled.users;

CREATE TABLE stat_compiled.users
(
    id integer NOT NULL,
    user_name text,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);

INSERT INTO stat_compiled.users
(id, user_name)
SELECT DISTINCT user_id, user_name
FROM (
    SELECT user_id, first_value(user_name) over (partition by user_id order by request_date DESC) as user_name
    FROM (
        SELECT user_id, user_name, MIN(request_date) as request_date
        FROM stat.requests
        GROUP BY user_id, user_name
    ) B
) A
ORDER BY user_id
;
