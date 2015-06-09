<?php
namespace CanalTP\StatCompiler\Updater;

class RequestCallsUpdater extends AbstractUpdater
{
    public function getAffectedTable()
    {
        return 'requests_calls';
    }

    protected function getDeleteQuery()
    {
        return "DELETE FROM stat_compiled.requests_calls WHERE request_date >= (:start_date :: date) and request_date < (:end_date :: date) + interval '1 day'";
    }

    protected function getInsertQuery()
    {
        $insertQuery = <<<EOT
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
WHERE
    req.request_date >= (:start_date :: date)
    AND req.request_date < (:end_date :: date) + interval '1 day'
GROUP BY
    cov.region_id,
    req.api,
    user_id,
    app_name,
    is_internal_call,
    DATE(req.request_date)
;
EOT;

        return $insertQuery;
    }

    protected function getInitQuery()
    {
        $initQuery = <<<EOT
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
EOT;
        return $initQuery;
    }
}
