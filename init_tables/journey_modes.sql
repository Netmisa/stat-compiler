-- Table: stat_compiled.journey_modes

DROP TABLE IF EXISTS stat_compiled.journey_modes CASCADE;

CREATE TABLE stat_compiled.journey_modes
(
  journey_id bigint NOT NULL,
  type text NOT NULL,
  mode text NOT NULL,
  commercial_mode_id text NOT NULL,
  commercial_mode_name text,
  request_date timestamp without time zone,
  CONSTRAINT journey_modes_pkey PRIMARY KEY (journey_id, type, mode, commercial_mode_id)
)
WITH (
  OIDS=FALSE
);

CREATE OR REPLACE FUNCTION journey_modes_insert_trigger()
RETURNS TRIGGER AS $$
DECLARE
  schema VARCHAR(100);
  partition VARCHAR(100);
BEGIN
  schema := 'stat_compiled';
  partition := 'journey_modes' || '_' || to_char(NEW.request_date, '"y"YYYY"m"MM');
  IF NOT EXISTS(SELECT 1 FROM pg_tables WHERE tablename=partition and schemaname=schema) THEN
    RAISE NOTICE 'A partition has been created %',partition;
    EXECUTE 'CREATE TABLE IF NOT EXISTS ' || schema || '.' || partition || 
            ' (check (request_date >= DATE ''' || to_char(NEW.request_date, 'YYYY-MM-01') || ''' 
                      AND request_date < DATE ''' || to_char(NEW.request_date + interval '1 month', 'YYYY-MM-01') || ''') ) ' || 
            'INHERITS (' || schema || '.journey_modes);';
    EXECUTE 'CREATE INDEX ' || partition || '_mode_idx ON ' || schema || '.' || partition || ' (mode);';
    EXECUTE 'CREATE INDEX ' || partition || '_type_idx ON ' || schema || '.' || partition || ' (type);';
    EXECUTE 'CREATE INDEX ' || partition || '_commercial_mode_id_idx ON ' || schema || '.' || partition || ' (commercial_mode_id);';
  END IF;
  EXECUTE 'INSERT INTO ' || schema || '.' || partition || ' SELECT(' || schema || '.journey_modes' || ' ' || quote_literal(NEW) || ').*;';
  RETURN NULL;
END;
$$
LANGUAGE plpgsql;

CREATE TRIGGER insert_journey_modes_trigger
    BEFORE INSERT ON stat_compiled.journey_modes
    FOR EACH ROW EXECUTE PROCEDURE journey_modes_insert_trigger();

DROP TABLE IF EXISTS stat_compiled.journey_modes_y2014m06;
DROP TABLE IF EXISTS stat_compiled.journey_modes_y2014m07;
DROP TABLE IF EXISTS stat_compiled.journey_modes_y2014m08;
DROP TABLE IF EXISTS stat_compiled.journey_modes_y2014m09;
DROP TABLE IF EXISTS stat_compiled.journey_modes_y2014m10;
DROP TABLE IF EXISTS stat_compiled.journey_modes_y2014m11;
DROP TABLE IF EXISTS stat_compiled.journey_modes_y2014m12;
DROP TABLE IF EXISTS stat_compiled.journey_modes_y2015m01;
DROP TABLE IF EXISTS stat_compiled.journey_modes_y2015m02;
DROP TABLE IF EXISTS stat_compiled.journey_modes_y2015m03;
DROP TABLE IF EXISTS stat_compiled.journey_modes_y2015m04;
DROP TABLE IF EXISTS stat_compiled.journey_modes_y2015m05;
DROP TABLE IF EXISTS stat_compiled.journey_modes_y2015m06;

CREATE TABLE stat_compiled.journey_modes_y2014m06 ( CHECK (request_date >= DATE '2014-06-01' AND request_date < DATE '2014-07-01') ) INHERITS (stat_compiled.journey_modes);
CREATE TABLE stat_compiled.journey_modes_y2014m07 ( CHECK (request_date >= DATE '2014-07-01' AND request_date < DATE '2014-08-01') ) INHERITS (stat_compiled.journey_modes);
CREATE TABLE stat_compiled.journey_modes_y2014m08 ( CHECK (request_date >= DATE '2014-08-01' AND request_date < DATE '2014-09-01') ) INHERITS (stat_compiled.journey_modes);
CREATE TABLE stat_compiled.journey_modes_y2014m09 ( CHECK (request_date >= DATE '2014-09-01' AND request_date < DATE '2014-10-01') ) INHERITS (stat_compiled.journey_modes);
CREATE TABLE stat_compiled.journey_modes_y2014m10 ( CHECK (request_date >= DATE '2014-10-01' AND request_date < DATE '2014-11-01') ) INHERITS (stat_compiled.journey_modes);
CREATE TABLE stat_compiled.journey_modes_y2014m11 ( CHECK (request_date >= DATE '2014-11-01' AND request_date < DATE '2014-12-01') ) INHERITS (stat_compiled.journey_modes);
CREATE TABLE stat_compiled.journey_modes_y2014m12 ( CHECK (request_date >= DATE '2014-12-01' AND request_date < DATE '2015-01-01') ) INHERITS (stat_compiled.journey_modes);
CREATE TABLE stat_compiled.journey_modes_y2015m01 ( CHECK (request_date >= DATE '2015-01-01' AND request_date < DATE '2015-02-01') ) INHERITS (stat_compiled.journey_modes);
CREATE TABLE stat_compiled.journey_modes_y2015m02 ( CHECK (request_date >= DATE '2015-02-01' AND request_date < DATE '2015-03-01') ) INHERITS (stat_compiled.journey_modes);
CREATE TABLE stat_compiled.journey_modes_y2015m03 ( CHECK (request_date >= DATE '2015-03-01' AND request_date < DATE '2015-04-01') ) INHERITS (stat_compiled.journey_modes);
CREATE TABLE stat_compiled.journey_modes_y2015m04 ( CHECK (request_date >= DATE '2015-04-01' AND request_date < DATE '2015-05-01') ) INHERITS (stat_compiled.journey_modes);
CREATE TABLE stat_compiled.journey_modes_y2015m05 ( CHECK (request_date >= DATE '2015-05-01' AND request_date < DATE '2015-06-01') ) INHERITS (stat_compiled.journey_modes);
CREATE TABLE stat_compiled.journey_modes_y2015m06 ( CHECK (request_date >= DATE '2015-06-01' AND request_date < DATE '2015-07-01') ) INHERITS (stat_compiled.journey_modes);

CREATE INDEX journey_modes_y2014m06_mode_idx ON stat_compiled.journey_modes_y2014m06 (mode);
CREATE INDEX journey_modes_y2014m07_mode_idx ON stat_compiled.journey_modes_y2014m07 (mode);
CREATE INDEX journey_modes_y2014m08_mode_idx ON stat_compiled.journey_modes_y2014m08 (mode);
CREATE INDEX journey_modes_y2014m09_mode_idx ON stat_compiled.journey_modes_y2014m09 (mode);
CREATE INDEX journey_modes_y2014m10_mode_idx ON stat_compiled.journey_modes_y2014m10 (mode);
CREATE INDEX journey_modes_y2014m11_mode_idx ON stat_compiled.journey_modes_y2014m11 (mode);
CREATE INDEX journey_modes_y2014m12_mode_idx ON stat_compiled.journey_modes_y2014m12 (mode);
CREATE INDEX journey_modes_y2015m01_mode_idx ON stat_compiled.journey_modes_y2015m01 (mode);
CREATE INDEX journey_modes_y2015m02_mode_idx ON stat_compiled.journey_modes_y2015m02 (mode);
CREATE INDEX journey_modes_y2015m03_mode_idx ON stat_compiled.journey_modes_y2015m03 (mode);
CREATE INDEX journey_modes_y2015m04_mode_idx ON stat_compiled.journey_modes_y2015m04 (mode);
CREATE INDEX journey_modes_y2015m05_mode_idx ON stat_compiled.journey_modes_y2015m05 (mode);
CREATE INDEX journey_modes_y2015m06_mode_idx ON stat_compiled.journey_modes_y2015m06 (mode);

CREATE INDEX journey_modes_y2014m06_type_idx ON stat_compiled.journey_modes_y2014m06 (type);
CREATE INDEX journey_modes_y2014m07_type_idx ON stat_compiled.journey_modes_y2014m07 (type);
CREATE INDEX journey_modes_y2014m08_type_idx ON stat_compiled.journey_modes_y2014m08 (type);
CREATE INDEX journey_modes_y2014m09_type_idx ON stat_compiled.journey_modes_y2014m09 (type);
CREATE INDEX journey_modes_y2014m10_type_idx ON stat_compiled.journey_modes_y2014m10 (type);
CREATE INDEX journey_modes_y2014m11_type_idx ON stat_compiled.journey_modes_y2014m11 (type);
CREATE INDEX journey_modes_y2014m12_type_idx ON stat_compiled.journey_modes_y2014m12 (type);
CREATE INDEX journey_modes_y2015m01_type_idx ON stat_compiled.journey_modes_y2015m01 (type);
CREATE INDEX journey_modes_y2015m02_type_idx ON stat_compiled.journey_modes_y2015m02 (type);
CREATE INDEX journey_modes_y2015m03_type_idx ON stat_compiled.journey_modes_y2015m03 (type);
CREATE INDEX journey_modes_y2015m04_type_idx ON stat_compiled.journey_modes_y2015m04 (type);
CREATE INDEX journey_modes_y2015m05_type_idx ON stat_compiled.journey_modes_y2015m05 (type);
CREATE INDEX journey_modes_y2015m06_type_idx ON stat_compiled.journey_modes_y2015m06 (type);

CREATE INDEX journey_modes_y2014m06_commercial_mode_id_idx ON stat_compiled.journey_modes_y2014m06 (commercial_mode_id);
CREATE INDEX journey_modes_y2014m07_commercial_mode_id_idx ON stat_compiled.journey_modes_y2014m07 (commercial_mode_id);
CREATE INDEX journey_modes_y2014m08_commercial_mode_id_idx ON stat_compiled.journey_modes_y2014m08 (commercial_mode_id);
CREATE INDEX journey_modes_y2014m09_commercial_mode_id_idx ON stat_compiled.journey_modes_y2014m09 (commercial_mode_id);
CREATE INDEX journey_modes_y2014m10_commercial_mode_id_idx ON stat_compiled.journey_modes_y2014m10 (commercial_mode_id);
CREATE INDEX journey_modes_y2014m11_commercial_mode_id_idx ON stat_compiled.journey_modes_y2014m11 (commercial_mode_id);
CREATE INDEX journey_modes_y2014m12_commercial_mode_id_idx ON stat_compiled.journey_modes_y2014m12 (commercial_mode_id);
CREATE INDEX journey_modes_y2015m01_commercial_mode_id_idx ON stat_compiled.journey_modes_y2015m01 (commercial_mode_id);
CREATE INDEX journey_modes_y2015m02_commercial_mode_id_idx ON stat_compiled.journey_modes_y2015m02 (commercial_mode_id);
CREATE INDEX journey_modes_y2015m03_commercial_mode_id_idx ON stat_compiled.journey_modes_y2015m03 (commercial_mode_id);
CREATE INDEX journey_modes_y2015m04_commercial_mode_id_idx ON stat_compiled.journey_modes_y2015m04 (commercial_mode_id);
CREATE INDEX journey_modes_y2015m05_commercial_mode_id_idx ON stat_compiled.journey_modes_y2015m05 (commercial_mode_id);
CREATE INDEX journey_modes_y2015m06_commercial_mode_id_idx ON stat_compiled.journey_modes_y2015m06 (commercial_mode_id);

INSERT INTO stat_compiled.journey_modes
(
  journey_id,
  type,
  mode,
  commercial_mode_id,
  commercial_mode_name,
  request_date
)
SELECT DISTINCT js.journey_id,
                js.type,
                js.mode,
                js.commercial_mode_id,
                js.commercial_mode_name,
                req.request_date
FROM stat.journey_sections js
INNER JOIN stat.requests req ON req.id = js.request_id
;
