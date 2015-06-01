-- Table: stat_compiled.error_stats

DROP TABLE IF EXISTS stat_compiled.error_stats CASCADE;

CREATE TABLE stat_compiled.error_stats
(
  region_id text NOT NULL,
  api text NOT NULL,
  request_date date NOT NULL,
  user_id integer NOT NULL,
  app_name text NOT NULL,
  err_id text NOT NULL,
  is_internal_call integer,
  nb_req bigint,
  nb_without_journey bigint,
  CONSTRAINT error_stats_pkey PRIMARY KEY (region_id, api, request_date, user_id, app_name, err_id)
)
WITH (
  OIDS=FALSE
);

CREATE OR REPLACE FUNCTION error_stats_insert_trigger()
RETURNS TRIGGER AS $$
DECLARE
  schema VARCHAR(100);
  partition VARCHAR(100);
BEGIN
  schema := 'stat_compiled';
  partition := 'error_stats' || '_' || to_char(NEW.request_date, '"y"YYYY"m"MM');
  IF NOT EXISTS(SELECT 1 FROM pg_tables WHERE tablename=partition and schemaname=schema) THEN
    RAISE NOTICE 'A partition has been created %',partition;
    EXECUTE 'CREATE TABLE IF NOT EXISTS ' || schema || '.' || partition || 
            ' (check (request_date >= DATE ''' || to_char(NEW.request_date, 'YYYY-MM-01') || ''' 
                      AND request_date < DATE ''' || to_char(NEW.request_date + interval '1 month', 'YYYY-MM-01') || ''') ) ' || 
            'INHERITS (' || schema || '.error_stats);';
  END IF;
  EXECUTE 'INSERT INTO ' || schema || '.' || partition || ' SELECT(' || schema || '.error_stats' || ' ' || quote_literal(NEW) || ').*;';
  RETURN NULL;
END;
$$
LANGUAGE plpgsql;

CREATE TRIGGER insert_error_stats_trigger
    BEFORE INSERT ON stat_compiled.error_stats
    FOR EACH ROW EXECUTE PROCEDURE error_stats_insert_trigger();

DROP TABLE IF EXISTS stat_compiled.error_stats_y2014m05;
DROP TABLE IF EXISTS stat_compiled.error_stats_y2014m06;
DROP TABLE IF EXISTS stat_compiled.error_stats_y2014m07;
DROP TABLE IF EXISTS stat_compiled.error_stats_y2014m08;
DROP TABLE IF EXISTS stat_compiled.error_stats_y2014m09;
DROP TABLE IF EXISTS stat_compiled.error_stats_y2014m10;
DROP TABLE IF EXISTS stat_compiled.error_stats_y2014m11;
DROP TABLE IF EXISTS stat_compiled.error_stats_y2014m12;
DROP TABLE IF EXISTS stat_compiled.error_stats_y2015m01;
DROP TABLE IF EXISTS stat_compiled.error_stats_y2015m02;
DROP TABLE IF EXISTS stat_compiled.error_stats_y2015m03;
DROP TABLE IF EXISTS stat_compiled.error_stats_y2015m04;
DROP TABLE IF EXISTS stat_compiled.error_stats_y2015m05;
DROP TABLE IF EXISTS stat_compiled.error_stats_y2015m06;

CREATE TABLE stat_compiled.error_stats_y2014m05 ( CHECK (request_date >= DATE '2014-05-01' AND request_date < DATE '2014-06-01') ) INHERITS (stat_compiled.error_stats);
CREATE TABLE stat_compiled.error_stats_y2014m06 ( CHECK (request_date >= DATE '2014-06-01' AND request_date < DATE '2014-07-01') ) INHERITS (stat_compiled.error_stats);
CREATE TABLE stat_compiled.error_stats_y2014m07 ( CHECK (request_date >= DATE '2014-07-01' AND request_date < DATE '2014-08-01') ) INHERITS (stat_compiled.error_stats);
CREATE TABLE stat_compiled.error_stats_y2014m08 ( CHECK (request_date >= DATE '2014-08-01' AND request_date < DATE '2014-09-01') ) INHERITS (stat_compiled.error_stats);
CREATE TABLE stat_compiled.error_stats_y2014m09 ( CHECK (request_date >= DATE '2014-09-01' AND request_date < DATE '2014-10-01') ) INHERITS (stat_compiled.error_stats);
CREATE TABLE stat_compiled.error_stats_y2014m10 ( CHECK (request_date >= DATE '2014-10-01' AND request_date < DATE '2014-11-01') ) INHERITS (stat_compiled.error_stats);
CREATE TABLE stat_compiled.error_stats_y2014m11 ( CHECK (request_date >= DATE '2014-11-01' AND request_date < DATE '2014-12-01') ) INHERITS (stat_compiled.error_stats);
CREATE TABLE stat_compiled.error_stats_y2014m12 ( CHECK (request_date >= DATE '2014-12-01' AND request_date < DATE '2015-01-01') ) INHERITS (stat_compiled.error_stats);
CREATE TABLE stat_compiled.error_stats_y2015m01 ( CHECK (request_date >= DATE '2015-01-01' AND request_date < DATE '2015-02-01') ) INHERITS (stat_compiled.error_stats);
CREATE TABLE stat_compiled.error_stats_y2015m02 ( CHECK (request_date >= DATE '2015-02-01' AND request_date < DATE '2015-03-01') ) INHERITS (stat_compiled.error_stats);
CREATE TABLE stat_compiled.error_stats_y2015m03 ( CHECK (request_date >= DATE '2015-03-01' AND request_date < DATE '2015-04-01') ) INHERITS (stat_compiled.error_stats);
CREATE TABLE stat_compiled.error_stats_y2015m04 ( CHECK (request_date >= DATE '2015-04-01' AND request_date < DATE '2015-05-01') ) INHERITS (stat_compiled.error_stats);
CREATE TABLE stat_compiled.error_stats_y2015m05 ( CHECK (request_date >= DATE '2015-05-01' AND request_date < DATE '2015-06-01') ) INHERITS (stat_compiled.error_stats);
CREATE TABLE stat_compiled.error_stats_y2015m06 ( CHECK (request_date >= DATE '2015-06-01' AND request_date < DATE '2015-07-01') ) INHERITS (stat_compiled.error_stats);

INSERT INTO stat_compiled.error_stats
  (region_id, api, request_date, user_id, app_name, err_id, is_internal_call, nb_req, nb_without_journey)
SELECT
       cov.region_id AS region_id,
       req.api AS api,
       DATE(req.request_date) AS request_date,
       req.user_id AS user_id,
       req.app_name AS app_name,
       err.id AS err_id,
       CASE
           WHEN req.user_name LIKE '%canaltp%' THEN 1
           ELSE 0
       END AS is_internal_call,
       COUNT(DISTINCT req.id) nb_req,
       SUM(CASE WHEN j.request_id IS NULL THEN 1 ELSE 0 END) AS nb_without_journey
FROM stat.requests req
INNER JOIN stat.coverages cov ON cov.request_id=req.id
INNER JOIN stat.errors err ON err.request_id=req.id
LEFT JOIN stat.journeys j ON j.request_id=req.id
GROUP BY
         region_id,
         api,
         DATE(request_date),
         user_id,
         app_name,
         err_id,
         is_internal_call ;

