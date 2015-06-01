-- Table: stat_compiled.journey_lines

DROP TABLE IF EXISTS stat_compiled.journey_lines CASCADE;

CREATE TABLE stat_compiled.journey_lines
(
  journey_id bigint NOT NULL,
  type text NOT NULL,
  line_id text NOT NULL,
  line_code text,
  network_id text NOT NULL,
  network_name text,
  request_date timestamp without time zone,
  CONSTRAINT journey_lines_pkey PRIMARY KEY (journey_id, type, line_id, network_id)
)
WITH (
  OIDS=FALSE
);

CREATE OR REPLACE FUNCTION journey_lines_insert_trigger()
RETURNS TRIGGER AS $$
DECLARE
  schema VARCHAR(100);
  partition VARCHAR(100);
BEGIN
  schema := 'stat_compiled';
  partition := 'journey_lines' || '_' || to_char(NEW.request_date, '"y"YYYY"m"MM');
  IF NOT EXISTS(SELECT 1 FROM pg_tables WHERE tablename=partition and schemaname=schema) THEN
    RAISE NOTICE 'A partition has been created %',partition;
    EXECUTE 'CREATE TABLE IF NOT EXISTS ' || schema || '.' || partition || 
            ' (check (request_date >= DATE ''' || to_char(NEW.request_date, 'YYYY-MM-01') || ''' 
                      AND request_date < DATE ''' || to_char(NEW.request_date + interval '1 month', 'YYYY-MM-01') || ''') ) ' || 
            'INHERITS (' || schema || '.journey_lines);';
  END IF;
  EXECUTE 'INSERT INTO ' || schema || '.' || partition || ' SELECT(' || schema || '.journey_lines' || ' ' || quote_literal(NEW) || ').*;';
  RETURN NULL;
END;
$$
LANGUAGE plpgsql;

CREATE TRIGGER insert_journey_lines_trigger
    BEFORE INSERT ON stat_compiled.journey_lines
    FOR EACH ROW EXECUTE PROCEDURE journey_lines_insert_trigger();

DROP TABLE IF EXISTS stat_compiled.journey_lines_y2014m06;
DROP TABLE IF EXISTS stat_compiled.journey_lines_y2014m07;
DROP TABLE IF EXISTS stat_compiled.journey_lines_y2014m08;
DROP TABLE IF EXISTS stat_compiled.journey_lines_y2014m09;
DROP TABLE IF EXISTS stat_compiled.journey_lines_y2014m10;
DROP TABLE IF EXISTS stat_compiled.journey_lines_y2014m11;
DROP TABLE IF EXISTS stat_compiled.journey_lines_y2014m12;
DROP TABLE IF EXISTS stat_compiled.journey_lines_y2015m01;
DROP TABLE IF EXISTS stat_compiled.journey_lines_y2015m02;
DROP TABLE IF EXISTS stat_compiled.journey_lines_y2015m03;
DROP TABLE IF EXISTS stat_compiled.journey_lines_y2015m04;
DROP TABLE IF EXISTS stat_compiled.journey_lines_y2015m05;
DROP TABLE IF EXISTS stat_compiled.journey_lines_y2015m06;

CREATE TABLE stat_compiled.journey_lines_y2014m06 ( CHECK (request_date >= DATE '2014-06-01' AND request_date < DATE '2014-07-01') ) INHERITS (stat_compiled.journey_lines);
CREATE TABLE stat_compiled.journey_lines_y2014m07 ( CHECK (request_date >= DATE '2014-07-01' AND request_date < DATE '2014-08-01') ) INHERITS (stat_compiled.journey_lines);
CREATE TABLE stat_compiled.journey_lines_y2014m08 ( CHECK (request_date >= DATE '2014-08-01' AND request_date < DATE '2014-09-01') ) INHERITS (stat_compiled.journey_lines);
CREATE TABLE stat_compiled.journey_lines_y2014m09 ( CHECK (request_date >= DATE '2014-09-01' AND request_date < DATE '2014-10-01') ) INHERITS (stat_compiled.journey_lines);
CREATE TABLE stat_compiled.journey_lines_y2014m10 ( CHECK (request_date >= DATE '2014-10-01' AND request_date < DATE '2014-11-01') ) INHERITS (stat_compiled.journey_lines);
CREATE TABLE stat_compiled.journey_lines_y2014m11 ( CHECK (request_date >= DATE '2014-11-01' AND request_date < DATE '2014-12-01') ) INHERITS (stat_compiled.journey_lines);
CREATE TABLE stat_compiled.journey_lines_y2014m12 ( CHECK (request_date >= DATE '2014-12-01' AND request_date < DATE '2015-01-01') ) INHERITS (stat_compiled.journey_lines);
CREATE TABLE stat_compiled.journey_lines_y2015m01 ( CHECK (request_date >= DATE '2015-01-01' AND request_date < DATE '2015-02-01') ) INHERITS (stat_compiled.journey_lines);
CREATE TABLE stat_compiled.journey_lines_y2015m02 ( CHECK (request_date >= DATE '2015-02-01' AND request_date < DATE '2015-03-01') ) INHERITS (stat_compiled.journey_lines);
CREATE TABLE stat_compiled.journey_lines_y2015m03 ( CHECK (request_date >= DATE '2015-03-01' AND request_date < DATE '2015-04-01') ) INHERITS (stat_compiled.journey_lines);
CREATE TABLE stat_compiled.journey_lines_y2015m04 ( CHECK (request_date >= DATE '2015-04-01' AND request_date < DATE '2015-05-01') ) INHERITS (stat_compiled.journey_lines);
CREATE TABLE stat_compiled.journey_lines_y2015m05 ( CHECK (request_date >= DATE '2015-05-01' AND request_date < DATE '2015-06-01') ) INHERITS (stat_compiled.journey_lines);
CREATE TABLE stat_compiled.journey_lines_y2015m06 ( CHECK (request_date >= DATE '2015-06-01' AND request_date < DATE '2015-07-01') ) INHERITS (stat_compiled.journey_lines);

INSERT INTO stat_compiled.journey_lines
(
  journey_id,
  type,
  line_id,
  line_code,
  network_id,
  network_name,
  request_date
)
SELECT DISTINCT journey_id,
                type,
                line_id,
                line_code,
                network_id,
                network_name,
                request_date
FROM stat.journey_sections js
INNER JOIN stat.requests req ON req.id = js.request_id
WHERE line_id <> '' ;
