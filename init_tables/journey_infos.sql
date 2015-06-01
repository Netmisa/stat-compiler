-- Table: stat_compiled.journey_stop_areas

DROP TABLE IF EXISTS stat_compiled.journey_infos CASCADE;

CREATE TABLE stat_compiled.journey_infos
(
  id bigint NOT NULL,
  request_id bigint,
  region_id text,
  user_id integer,
  app_name text,
  is_internal_call integer,
  request_date timestamp without time zone,
  requested_date_time timestamp without time zone,
  nb_transfers integer,
  duration integer,
  CONSTRAINT journey_infos_pkey PRIMARY KEY (id)
)
WITH (
  OIDS=FALSE
);

CREATE OR REPLACE FUNCTION journey_infos_insert_trigger()
RETURNS TRIGGER AS $$
DECLARE
  schema VARCHAR(100);
  partition VARCHAR(100);
BEGIN
  schema := 'stat_compiled';
  partition := 'journey_infos' || '_' || to_char(NEW.request_date, '"y"YYYY"m"MM');
  IF NOT EXISTS(SELECT 1 FROM pg_tables WHERE tablename=partition and schemaname=schema) THEN
    RAISE NOTICE 'A partition has been created %',partition;
    EXECUTE 'CREATE TABLE IF NOT EXISTS ' || schema || '.' || partition || 
            ' (check (request_date >= DATE ''' || to_char(NEW.request_date, 'YYYY-MM-01') || ''' 
                      AND request_date < DATE ''' || to_char(NEW.request_date + interval '1 month', 'YYYY-MM-01') || ''') ) ' || 
            'INHERITS (' || schema || '.journey_infos);';
  END IF;
  EXECUTE 'INSERT INTO ' || schema || '.' || partition || ' SELECT(' || schema || '.journey_infos' || ' ' || quote_literal(NEW) || ').*;';
  RETURN NULL;
END;
$$
LANGUAGE plpgsql;

CREATE TRIGGER insert_journey_infos_trigger
    BEFORE INSERT ON stat_compiled.journey_infos
    FOR EACH ROW EXECUTE PROCEDURE journey_infos_insert_trigger();

DROP TABLE IF EXISTS stat_compiled.journey_infos_y2014m06;
DROP TABLE IF EXISTS stat_compiled.journey_infos_y2014m07;
DROP TABLE IF EXISTS stat_compiled.journey_infos_y2014m08;
DROP TABLE IF EXISTS stat_compiled.journey_infos_y2014m09;
DROP TABLE IF EXISTS stat_compiled.journey_infos_y2014m10;
DROP TABLE IF EXISTS stat_compiled.journey_infos_y2014m11;
DROP TABLE IF EXISTS stat_compiled.journey_infos_y2014m12;
DROP TABLE IF EXISTS stat_compiled.journey_infos_y2015m01;
DROP TABLE IF EXISTS stat_compiled.journey_infos_y2015m02;
DROP TABLE IF EXISTS stat_compiled.journey_infos_y2015m03;
DROP TABLE IF EXISTS stat_compiled.journey_infos_y2015m04;
DROP TABLE IF EXISTS stat_compiled.journey_infos_y2015m05;
DROP TABLE IF EXISTS stat_compiled.journey_infos_y2015m06;

CREATE TABLE stat_compiled.journey_infos_y2014m06 ( CHECK (request_date >= DATE '2014-06-01' AND request_date < DATE '2014-07-01') ) INHERITS (stat_compiled.journey_infos);
CREATE TABLE stat_compiled.journey_infos_y2014m07 ( CHECK (request_date >= DATE '2014-07-01' AND request_date < DATE '2014-08-01') ) INHERITS (stat_compiled.journey_infos);
CREATE TABLE stat_compiled.journey_infos_y2014m08 ( CHECK (request_date >= DATE '2014-08-01' AND request_date < DATE '2014-09-01') ) INHERITS (stat_compiled.journey_infos);
CREATE TABLE stat_compiled.journey_infos_y2014m09 ( CHECK (request_date >= DATE '2014-09-01' AND request_date < DATE '2014-10-01') ) INHERITS (stat_compiled.journey_infos);
CREATE TABLE stat_compiled.journey_infos_y2014m10 ( CHECK (request_date >= DATE '2014-10-01' AND request_date < DATE '2014-11-01') ) INHERITS (stat_compiled.journey_infos);
CREATE TABLE stat_compiled.journey_infos_y2014m11 ( CHECK (request_date >= DATE '2014-11-01' AND request_date < DATE '2014-12-01') ) INHERITS (stat_compiled.journey_infos);
CREATE TABLE stat_compiled.journey_infos_y2014m12 ( CHECK (request_date >= DATE '2014-12-01' AND request_date < DATE '2015-01-01') ) INHERITS (stat_compiled.journey_infos);
CREATE TABLE stat_compiled.journey_infos_y2015m01 ( CHECK (request_date >= DATE '2015-01-01' AND request_date < DATE '2015-02-01') ) INHERITS (stat_compiled.journey_infos);
CREATE TABLE stat_compiled.journey_infos_y2015m02 ( CHECK (request_date >= DATE '2015-02-01' AND request_date < DATE '2015-03-01') ) INHERITS (stat_compiled.journey_infos);
CREATE TABLE stat_compiled.journey_infos_y2015m03 ( CHECK (request_date >= DATE '2015-03-01' AND request_date < DATE '2015-04-01') ) INHERITS (stat_compiled.journey_infos);
CREATE TABLE stat_compiled.journey_infos_y2015m04 ( CHECK (request_date >= DATE '2015-04-01' AND request_date < DATE '2015-05-01') ) INHERITS (stat_compiled.journey_infos);
CREATE TABLE stat_compiled.journey_infos_y2015m05 ( CHECK (request_date >= DATE '2015-05-01' AND request_date < DATE '2015-06-01') ) INHERITS (stat_compiled.journey_infos);
CREATE TABLE stat_compiled.journey_infos_y2015m06 ( CHECK (request_date >= DATE '2015-06-01' AND request_date < DATE '2015-07-01') ) INHERITS (stat_compiled.journey_infos);

INSERT INTO stat_compiled.journey_infos
(
  id,
  request_id,
  region_id,
  user_id,
  app_name,
  is_internal_call,
  request_date,
  requested_date_time,
  nb_transfers,
  duration
)
SELECT DISTINCT
  j.id,
  j.request_id,
  first_value(cov.region_id) OVER (PARTITION BY j.id) AS region_id,
  req.user_id,
  app_name,
  CASE WHEN req.user_name LIKE '%canaltp%' THEN 1 ELSE 0 END as is_internal_call,
  req.request_date,
  j.requested_date_time,
  j.nb_transfers,
  j.duration
FROM
  stat.journeys j
  inner join stat.requests req on j.request_id=req.id
  inner join stat.coverages cov on j.request_id=cov.request_id
;
