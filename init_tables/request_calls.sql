-- Table: stat_compiled.requests_calls

DROP TABLE IF EXISTS stat_compiled.requests_calls CASCADE;

CREATE TABLE stat_compiled.requests_calls
(
  region_id text NOT NULL,
  api text NOT NULL,
  user_id integer NOT NULL,
  app_name text NOT NULL,
  is_internal_call integer,
  request_date date NOT NULL,
  nb bigint,
  nb_without_journey bigint,
  CONSTRAINT requests_calls_pkey PRIMARY KEY (region_id, api, request_date, user_id, app_name)
)
WITH (
  OIDS=FALSE
);

CREATE OR REPLACE FUNCTION requests_calls_insert_trigger()
RETURNS TRIGGER AS $$
DECLARE
  schema VARCHAR(100);
  partition VARCHAR(100);
BEGIN
  schema := 'stat_compiled';
  partition := 'requests_calls' || '_' || to_char(NEW.request_date, '"y"YYYY"m"MM');
  IF NOT EXISTS(SELECT 1 FROM pg_tables WHERE tablename=partition and schemaname=schema) THEN
    RAISE NOTICE 'A partition has been created %',partition;
    EXECUTE 'CREATE TABLE IF NOT EXISTS ' || schema || '.' || partition || 
            ' (check (request_date >= DATE ''' || to_char(NEW.request_date, 'YYYY-MM-01') || ''' 
                      AND request_date < DATE ''' || to_char(NEW.request_date + interval '1 month', 'YYYY-MM-01') || ''') ) ' || 
            'INHERITS (' || schema || '.requests_calls);';
  END IF;
  EXECUTE 'INSERT INTO ' || schema || '.' || partition || ' SELECT(' || schema || '.requests_calls' || ' ' || quote_literal(NEW) || ').*;';
  RETURN NULL;
END;
$$
LANGUAGE plpgsql;

CREATE TRIGGER insert_requests_calls_trigger
    BEFORE INSERT ON stat_compiled.requests_calls
    FOR EACH ROW EXECUTE PROCEDURE requests_calls_insert_trigger();

INSERT INTO stat_compiled.requests_calls
(
  region_id,
  api,
  user_id,
  app_name,
  is_internal_call,
  request_date,
  nb,
  nb_without_journey
)
SELECT
    cov.region_id,
    req.api,
    user_id,
    app_name,
    CASE WHEN req.user_name LIKE '%canaltp%' THEN 1 ELSE 0 END as is_internal_call,
    DATE(req.request_date) AS request_date,
    COUNT(DISTINCT req.id) AS nb,
    SUM(CASE WHEN j.request_id IS NULL THEN 1 ELSE 0 END) AS nb_without_journey
FROM
    stat.requests req
    INNER JOIN stat.coverages cov ON cov.request_id=req.id
    LEFT JOIN stat.journeys j ON j.request_id=req.id
GROUP BY
    cov.region_id,
    req.api,
    user_id,
    app_name,
    is_internal_call,
    DATE(req.request_date)
;
