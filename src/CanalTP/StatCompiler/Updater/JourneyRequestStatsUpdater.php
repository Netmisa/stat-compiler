<?php
namespace CanalTP\StatCompiler\Updater;

use Psr\Log\LoggerAwareTrait;

class JourneyRequestStatsUpdater extends AbstractUpdater
{
    public function getAffectedTable()
    {
        return 'journey_request_stats';
    }

    protected function getDeleteQuery()
    {
        return "DELETE FROM stat_compiled.journey_request_stats WHERE request_date >= (:start_date :: date) and request_date < (:end_date :: date) + interval '1 day'";
    }

    protected function getInsertQuery()
    {
        $insertQuery = <<<EOT
INSERT INTO stat_compiled.journey_request_stats
(
  request_id,
  requested_date_time,
  request_date,
  clockwise,
  departure_insee,
  departure_admin,
  arrival_insee,
  arrival_admin,
  departure_admin_name,
  arrival_admin_name,
  region_id,
  is_internal_call
)
SELECT DISTINCT jr.request_id,
                jr.requested_date_time,
                req.request_date,
                jr.clockwise,
                jr.departure_insee,
                jr.departure_admin,
                jr.arrival_insee,
                jr.arrival_admin,
                jr.departure_admin_name,
                jr.arrival_admin_name,
                first_value(cov.region_id) OVER (PARTITION BY jr.request_id) AS region_id,
                CASE WHEN req.user_name LIKE '%canaltp%' THEN 1 ELSE 0 END as is_internal_call
FROM stat.journey_request jr
INNER JOIN stat.requests req ON req.id=jr.request_id
INNER JOIN stat.coverages cov ON req.id=cov.request_id
WHERE
req.request_date >= (:start_date :: date)
AND req.request_date < (:end_date :: date) + interval '1 day';
EOT;

        return $insertQuery;
    }
}
