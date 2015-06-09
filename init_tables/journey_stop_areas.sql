-- Table: stat_compiled.journey_stop_areas

DROP TABLE IF EXISTS stat_compiled.journey_stop_areas CASCADE;

CREATE TABLE stat_compiled.journey_stop_areas
(
  journey_id bigint,
  stop_area_id text,
  stop_area_name text,
  city_id text,
  city_name text,
  city_insee text,
  department_code text,
  request_date timestamp without time zone,
  is_start_stop_area boolean,
  is_end_stop_area boolean,
  CONSTRAINT journey_stop_areas_pkey PRIMARY KEY (journey_id, stop_area_id)
)
WITH (
  OIDS=FALSE
);

CREATE OR REPLACE FUNCTION journey_stop_areas_insert_trigger()
RETURNS TRIGGER AS $$
DECLARE
  schema VARCHAR(100);
  partition VARCHAR(100);
BEGIN
  schema := 'stat_compiled';
  partition := 'journey_stop_areas' || '_' || to_char(NEW.request_date, '"y"YYYY"m"MM');
  IF NOT EXISTS(SELECT 1 FROM pg_tables WHERE tablename=partition and schemaname=schema) THEN
    RAISE NOTICE 'A partition has been created %',partition;
    EXECUTE 'CREATE TABLE IF NOT EXISTS ' || schema || '.' || partition || 
            ' (check (request_date >= DATE ''' || to_char(NEW.request_date, 'YYYY-MM-01') || ''' 
                      AND request_date < DATE ''' || to_char(NEW.request_date + interval '1 month', 'YYYY-MM-01') || ''') ) ' || 
            'INHERITS (' || schema || '.journey_stop_areas);';
  END IF;
  EXECUTE 'INSERT INTO ' || schema || '.' || partition || ' SELECT(' || schema || '.journey_stop_areas' || ' ' || quote_literal(NEW) || ').*;';
  RETURN NULL;
END;
$$
LANGUAGE plpgsql;

CREATE TRIGGER insert_journey_stop_areas_trigger
    BEFORE INSERT ON stat_compiled.journey_stop_areas
    FOR EACH ROW EXECUTE PROCEDURE journey_stop_areas_insert_trigger();

INSERT INTO stat_compiled.journey_stop_areas
(
  journey_id,
  stop_area_id,
  stop_area_name,
  city_id,
  city_name,
  city_insee,
  department_code,
  request_date,
  is_start_stop_area,
  is_end_stop_area
)
SELECT DISTINCT
    A.journey_id,
    A.stop_area_id,
    A.stop_area_name,
    A.city_id,
    A.city_name,
    A.city_insee,
    A.department_code,
    A.request_date,
    CASE WHEN A.stop_area_id=B.dep_stop_area_id THEN TRUE ELSE FALSE END AS is_start_stop_area,
    CASE WHEN A.stop_area_id=B.arr_stop_area_id THEN TRUE ELSE FALSE END AS is_end_stop_area
FROM (
    SELECT
        journey_id,
        request_date,
        to_id as stop_area_id,
        to_name as stop_area_name,
        to_admin_id as city_id,
        to_admin_name as city_name,
        to_admin_insee as city_insee,
        substring(to_admin_insee, 1, 2) as department_code
    FROM
        stat.journey_sections js
        INNER JOIN stat.requests req ON req.id = js.request_id
    WHERE
        type = 'public_transport'
    UNION
    SELECT
        journey_id,
        request_date,
        from_id as stop_area_id,
        from_name as stop_area_name,
        from_admin_id as city_id,
        from_admin_name as city_name,
        from_admin_insee as city_insee,
        substring(from_admin_insee, 1, 2) as department_code
    FROM
        stat.journey_sections js
        INNER JOIN stat.requests req ON req.id = js.request_id
    WHERE
        type = 'public_transport'
) A,
(
    SELECT DISTINCT dep.journey_id AS journey_id,
                    dep.from_id AS dep_stop_area_id,
                    arr.to_id AS arr_stop_area_id
    FROM stat.journey_sections dep
    INNER JOIN
      (SELECT js.journey_id,
              MIN(js.id) AS dep_id,
              MAX(js.id) AS arr_id
       FROM stat.journey_sections js
       WHERE js.type = 'public_transport'
       GROUP BY js.journey_id) od ON (dep.journey_id = od.journey_id
                                      AND dep.id = od.dep_id)
    INNER JOIN stat.journey_sections arr ON (od.journey_id = arr.journey_id
                                             AND od.arr_id = arr.id)
) B
WHERE A.journey_id = B.journey_id
;
