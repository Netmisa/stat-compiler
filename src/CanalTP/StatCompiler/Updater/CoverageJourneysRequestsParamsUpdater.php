<?php
namespace CanalTP\StatCompiler\Updater;

class CoverageJourneysRequestsParamsUpdater extends AbstractUpdater
{
    public function getAffectedTable()
    {
        return 'coverage_journeys_requests_params';
    }

    protected function getDeleteQuery()
    {
        return "DELETE FROM stat_compiled.coverage_journeys_requests_params WHERE request_date >= (:start_date :: date) and request_date < (:end_date :: date) + interval '1 day'";
    }

    protected function getInsertQuery()
    {
        $insertQuery = <<<EOT
INSERT INTO stat_compiled.coverage_journeys_requests_params
(
  request_date,
  region_id,
  is_internal_call,
  nb_wheelchair
)
SELECT req.request_date::date,
       cov.region_id,
       CASE WHEN req.user_name LIKE '%canaltp%' THEN 1 ELSE 0 END as is_internal_call,
       COUNT(p.*) AS nb_wheelchair
FROM stat.journey_request jr
INNER JOIN stat.requests req ON req.id = jr.request_id
INNER JOIN stat.coverages cov ON cov.request_id = req.id
INNER JOIN stat.parameters p ON p.request_id = req.id
WHERE p.param_key = 'wheelchair'
AND p.param_value = 'true'
AND req.request_date >= (:start_date :: date)
AND req.request_date < (:end_date :: date) + interval '1 day'
GROUP BY req.request_date::date, cov.region_id, is_internal_call
;
EOT;

        return $insertQuery;
    }

    protected function getInitQuery()
    {
        $initQuery = <<<EOT
INSERT INTO stat_compiled.coverage_journeys_requests_params
(
  request_date,
  region_id,
  is_internal_call,
  nb_wheelchair
)
SELECT req.request_date::date,
       cov.region_id,
       CASE WHEN req.user_name LIKE '%canaltp%' THEN 1 ELSE 0 END as is_internal_call,
       COUNT(p.*) AS nb_wheelchair
FROM stat.journey_request jr
INNER JOIN stat.requests req ON req.id = jr.request_id
INNER JOIN stat.coverages cov ON cov.request_id = req.id
INNER JOIN stat.parameters p ON p.request_id = req.id
WHERE p.param_key = 'wheelchair'
AND p.param_value = 'true'
GROUP BY req.request_date::date, cov.region_id, is_internal_call
;
EOT;
        return $initQuery;
    }
}
