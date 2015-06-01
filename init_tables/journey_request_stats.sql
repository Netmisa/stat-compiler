-- Table: stat_compiled.journey_request_stats

DROP TABLE IF EXISTS stat_compiled.journey_request_stats CASCADE;

CREATE TABLE stat_compiled.journey_request_stats
(
  request_id bigint NOT NULL,
  requested_date_time timestamp without time zone,
  request_date timestamp without time zone,
  clockwise boolean,
  departure_insee text,
  departure_admin text,
  arrival_insee text,
  arrival_admin text,
  departure_admin_name text,
  arrival_admin_name text,
  region_id text,
  is_internal_call integer,
  CONSTRAINT journey_request_stats_pkey PRIMARY KEY (request_id)
)
WITH (
  OIDS=FALSE
);

CREATE OR REPLACE FUNCTION journey_request_stats_insert_trigger()
RETURNS TRIGGER AS $$
DECLARE
  schema VARCHAR(100);
  partition VARCHAR(100);
BEGIN
  schema := 'stat_compiled';
  partition := 'journey_request_stats' || '_' || to_char(NEW.request_date, '"y"YYYY"m"MM');
  IF NOT EXISTS(SELECT 1 FROM pg_tables WHERE tablename=partition and schemaname=schema) THEN
    RAISE NOTICE 'A partition has been created %',partition;
    EXECUTE 'CREATE TABLE IF NOT EXISTS ' || schema || '.' || partition || 
            ' (check (request_date >= DATE ''' || to_char(NEW.request_date, 'YYYY-MM-01') || ''' 
                      AND request_date < DATE ''' || to_char(NEW.request_date + interval '1 month', 'YYYY-MM-01') || ''') ) ' || 
            'INHERITS (' || schema || '.journey_request_stats);';
  END IF;
  EXECUTE 'INSERT INTO ' || schema || '.' || partition || ' SELECT(' || schema || '.journey_request_stats' || ' ' || quote_literal(NEW) || ').*;';
  RETURN NULL;
END;
$$
LANGUAGE plpgsql;

CREATE TRIGGER insert_journey_request_stats_trigger
    BEFORE INSERT ON stat_compiled.journey_request_stats
    FOR EACH ROW EXECUTE PROCEDURE journey_request_stats_insert_trigger();

DROP TABLE IF EXISTS stat_compiled.journey_request_stats_y2014m06;
DROP TABLE IF EXISTS stat_compiled.journey_request_stats_y2014m07;
DROP TABLE IF EXISTS stat_compiled.journey_request_stats_y2014m08;
DROP TABLE IF EXISTS stat_compiled.journey_request_stats_y2014m09;
DROP TABLE IF EXISTS stat_compiled.journey_request_stats_y2014m10;
DROP TABLE IF EXISTS stat_compiled.journey_request_stats_y2014m11;
DROP TABLE IF EXISTS stat_compiled.journey_request_stats_y2014m12;
DROP TABLE IF EXISTS stat_compiled.journey_request_stats_y2015m01;
DROP TABLE IF EXISTS stat_compiled.journey_request_stats_y2015m02;
DROP TABLE IF EXISTS stat_compiled.journey_request_stats_y2015m03;
DROP TABLE IF EXISTS stat_compiled.journey_request_stats_y2015m04;
DROP TABLE IF EXISTS stat_compiled.journey_request_stats_y2015m05;
DROP TABLE IF EXISTS stat_compiled.journey_request_stats_y2015m06;

CREATE TABLE stat_compiled.journey_request_stats_y2014m06 ( CHECK (request_date >= DATE '2014-06-01' AND request_date < DATE '2014-07-01') ) INHERITS (stat_compiled.journey_request_stats);
CREATE TABLE stat_compiled.journey_request_stats_y2014m07 ( CHECK (request_date >= DATE '2014-07-01' AND request_date < DATE '2014-08-01') ) INHERITS (stat_compiled.journey_request_stats);
CREATE TABLE stat_compiled.journey_request_stats_y2014m08 ( CHECK (request_date >= DATE '2014-08-01' AND request_date < DATE '2014-09-01') ) INHERITS (stat_compiled.journey_request_stats);
CREATE TABLE stat_compiled.journey_request_stats_y2014m09 ( CHECK (request_date >= DATE '2014-09-01' AND request_date < DATE '2014-10-01') ) INHERITS (stat_compiled.journey_request_stats);
CREATE TABLE stat_compiled.journey_request_stats_y2014m10 ( CHECK (request_date >= DATE '2014-10-01' AND request_date < DATE '2014-11-01') ) INHERITS (stat_compiled.journey_request_stats);
CREATE TABLE stat_compiled.journey_request_stats_y2014m11 ( CHECK (request_date >= DATE '2014-11-01' AND request_date < DATE '2014-12-01') ) INHERITS (stat_compiled.journey_request_stats);
CREATE TABLE stat_compiled.journey_request_stats_y2014m12 ( CHECK (request_date >= DATE '2014-12-01' AND request_date < DATE '2015-01-01') ) INHERITS (stat_compiled.journey_request_stats);
CREATE TABLE stat_compiled.journey_request_stats_y2015m01 ( CHECK (request_date >= DATE '2015-01-01' AND request_date < DATE '2015-02-01') ) INHERITS (stat_compiled.journey_request_stats);
CREATE TABLE stat_compiled.journey_request_stats_y2015m02 ( CHECK (request_date >= DATE '2015-02-01' AND request_date < DATE '2015-03-01') ) INHERITS (stat_compiled.journey_request_stats);
CREATE TABLE stat_compiled.journey_request_stats_y2015m03 ( CHECK (request_date >= DATE '2015-03-01' AND request_date < DATE '2015-04-01') ) INHERITS (stat_compiled.journey_request_stats);
CREATE TABLE stat_compiled.journey_request_stats_y2015m04 ( CHECK (request_date >= DATE '2015-04-01' AND request_date < DATE '2015-05-01') ) INHERITS (stat_compiled.journey_request_stats);
CREATE TABLE stat_compiled.journey_request_stats_y2015m05 ( CHECK (request_date >= DATE '2015-05-01' AND request_date < DATE '2015-06-01') ) INHERITS (stat_compiled.journey_request_stats);
CREATE TABLE stat_compiled.journey_request_stats_y2015m06 ( CHECK (request_date >= DATE '2015-06-01' AND request_date < DATE '2015-07-01') ) INHERITS (stat_compiled.journey_request_stats);

INSERT INTO stat_compiled.journey_request_stats
(
  request_id,
  requested_date_time,
  request_date,
  clockwise,
  departure_insee,
  departure_admin,
  arrival_insee,
  arrival_admin,
  departure_admin_name,
  arrival_admin_name,
  region_id,
  is_internal_call
)
SELECT DISTINCT jr.request_id,
                jr.requested_date_time,
                req.request_date,
                jr.clockwise,
                jr.departure_insee,
                jr.departure_admin,
                jr.arrival_insee,
                jr.arrival_admin,
                jr.departure_admin_name,
                jr.arrival_admin_name,
                first_value(cov.region_id) OVER (PARTITION BY jr.request_id) AS region_id,
                CASE WHEN req.user_name LIKE '%canaltp%' THEN 1 ELSE 0 END as is_internal_call
FROM stat.journey_request jr
INNER JOIN stat.requests req ON req.id=jr.request_id
INNER JOIN stat.coverages cov ON req.id=cov.request_id;
