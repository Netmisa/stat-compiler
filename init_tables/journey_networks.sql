-- Table: stat_compiled.journey_networks

DROP TABLE IF EXISTS stat_compiled.journey_networks CASCADE;

CREATE TABLE stat_compiled.journey_networks
(
  journey_id bigint NOT NULL,
  network_id text NOT NULL,
  network_name text,
  rank int,
  request_date timestamp without time zone,
  CONSTRAINT journey_networks_pkey PRIMARY KEY (journey_id, network_id, rank)
)
WITH (
  OIDS=FALSE
);

CREATE OR REPLACE FUNCTION journey_networks_insert_trigger()
RETURNS TRIGGER AS $$
DECLARE
  schema VARCHAR(100);
  partition VARCHAR(100);
BEGIN
  schema := 'stat_compiled';
  partition := 'journey_networks' || '_' || to_char(NEW.request_date, '"y"YYYY"m"MM');
  IF NOT EXISTS(SELECT 1 FROM pg_tables WHERE tablename=partition and schemaname=schema) THEN
    RAISE NOTICE 'A partition has been created %',partition;
    EXECUTE 'CREATE TABLE IF NOT EXISTS ' || schema || '.' || partition || 
            ' (check (request_date >= DATE ''' || to_char(NEW.request_date, 'YYYY-MM-01') || ''' 
                      AND request_date < DATE ''' || to_char(NEW.request_date + interval '1 month', 'YYYY-MM-01') || ''') ) ' || 
            'INHERITS (' || schema || '.journey_networks);';
  END IF;
  EXECUTE 'INSERT INTO ' || schema || '.' || partition || ' SELECT(' || schema || '.journey_networks' || ' ' || quote_literal(NEW) || ').*;';
  RETURN NULL;
END;
$$
LANGUAGE plpgsql;

CREATE TRIGGER insert_journey_networks_trigger
    BEFORE INSERT ON stat_compiled.journey_networks
    FOR EACH ROW EXECUTE PROCEDURE journey_networks_insert_trigger();

DROP TABLE IF EXISTS stat_compiled.journey_networks_y2014m06;
DROP TABLE IF EXISTS stat_compiled.journey_networks_y2014m07;
DROP TABLE IF EXISTS stat_compiled.journey_networks_y2014m08;
DROP TABLE IF EXISTS stat_compiled.journey_networks_y2014m09;
DROP TABLE IF EXISTS stat_compiled.journey_networks_y2014m10;
DROP TABLE IF EXISTS stat_compiled.journey_networks_y2014m11;
DROP TABLE IF EXISTS stat_compiled.journey_networks_y2014m12;
DROP TABLE IF EXISTS stat_compiled.journey_networks_y2015m01;
DROP TABLE IF EXISTS stat_compiled.journey_networks_y2015m02;
DROP TABLE IF EXISTS stat_compiled.journey_networks_y2015m03;
DROP TABLE IF EXISTS stat_compiled.journey_networks_y2015m04;
DROP TABLE IF EXISTS stat_compiled.journey_networks_y2015m05;
DROP TABLE IF EXISTS stat_compiled.journey_networks_y2015m06;

CREATE TABLE stat_compiled.journey_networks_y2014m06 ( CHECK (request_date >= DATE '2014-06-01' AND request_date < DATE '2014-07-01') ) INHERITS (stat_compiled.journey_networks);
CREATE TABLE stat_compiled.journey_networks_y2014m07 ( CHECK (request_date >= DATE '2014-07-01' AND request_date < DATE '2014-08-01') ) INHERITS (stat_compiled.journey_networks);
CREATE TABLE stat_compiled.journey_networks_y2014m08 ( CHECK (request_date >= DATE '2014-08-01' AND request_date < DATE '2014-09-01') ) INHERITS (stat_compiled.journey_networks);
CREATE TABLE stat_compiled.journey_networks_y2014m09 ( CHECK (request_date >= DATE '2014-09-01' AND request_date < DATE '2014-10-01') ) INHERITS (stat_compiled.journey_networks);
CREATE TABLE stat_compiled.journey_networks_y2014m10 ( CHECK (request_date >= DATE '2014-10-01' AND request_date < DATE '2014-11-01') ) INHERITS (stat_compiled.journey_networks);
CREATE TABLE stat_compiled.journey_networks_y2014m11 ( CHECK (request_date >= DATE '2014-11-01' AND request_date < DATE '2014-12-01') ) INHERITS (stat_compiled.journey_networks);
CREATE TABLE stat_compiled.journey_networks_y2014m12 ( CHECK (request_date >= DATE '2014-12-01' AND request_date < DATE '2015-01-01') ) INHERITS (stat_compiled.journey_networks);
CREATE TABLE stat_compiled.journey_networks_y2015m01 ( CHECK (request_date >= DATE '2015-01-01' AND request_date < DATE '2015-02-01') ) INHERITS (stat_compiled.journey_networks);
CREATE TABLE stat_compiled.journey_networks_y2015m02 ( CHECK (request_date >= DATE '2015-02-01' AND request_date < DATE '2015-03-01') ) INHERITS (stat_compiled.journey_networks);
CREATE TABLE stat_compiled.journey_networks_y2015m03 ( CHECK (request_date >= DATE '2015-03-01' AND request_date < DATE '2015-04-01') ) INHERITS (stat_compiled.journey_networks);
CREATE TABLE stat_compiled.journey_networks_y2015m04 ( CHECK (request_date >= DATE '2015-04-01' AND request_date < DATE '2015-05-01') ) INHERITS (stat_compiled.journey_networks);
CREATE TABLE stat_compiled.journey_networks_y2015m05 ( CHECK (request_date >= DATE '2015-05-01' AND request_date < DATE '2015-06-01') ) INHERITS (stat_compiled.journey_networks);
CREATE TABLE stat_compiled.journey_networks_y2015m06 ( CHECK (request_date >= DATE '2015-06-01' AND request_date < DATE '2015-07-01') ) INHERITS (stat_compiled.journey_networks);

INSERT INTO stat_compiled.journey_networks
(
  journey_id,
  network_id,
  network_name,
  rank,
  request_date
)
SELECT js.journey_id,
       js.network_id,
       js.network_name,
       row_number() over (order by js.journey_id, js.id),
       req.request_date
FROM stat.journey_sections js
INNER JOIN stat.requests req ON req.id = js.request_id
WHERE network_id <> '' ;
