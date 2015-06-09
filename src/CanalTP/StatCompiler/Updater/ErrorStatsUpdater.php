<?php
namespace CanalTP\StatCompiler\Updater;

use Psr\Log\LoggerAwareTrait;

class ErrorStatsUpdater extends AbstractUpdater
{
    public function getAffectedTable()
    {
        return 'error_stats';
    }

    protected function getDeleteQuery()
    {
        return "DELETE FROM stat_compiled.error_stats WHERE request_date >= (:start_date :: date) and request_date < (:end_date :: date) + interval '1 day'";
    }

    protected function getInsertQuery()
    {
        $insertQuery = <<<EOT
INSERT INTO stat_compiled.error_stats (region_id, api, request_date, user_id, app_name, err_id, is_internal_call, nb_req, nb_without_journey)
SELECT cov.region_id AS region_id,
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
WHERE
req.request_date >= (:start_date :: date)
AND req.request_date < (:end_date :: date) + interval '1 day'
GROUP BY region_id,
         api,
         DATE(request_date),
         user_id,
         app_name,
         err_id,
         is_internal_call ;
EOT;

        return $insertQuery;
    }

    protected function getInitQuery()
    {
        $initQuery = <<<EOT
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
EOT;
        return $initQuery;
    }
}
