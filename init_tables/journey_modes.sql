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
