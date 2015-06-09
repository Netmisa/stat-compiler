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
