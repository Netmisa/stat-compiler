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

