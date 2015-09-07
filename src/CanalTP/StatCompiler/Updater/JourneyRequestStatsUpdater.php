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
  departure_admin_name,
  departure_department_code,
  arrival_insee,
  arrival_admin,
  arrival_admin_name,
  arrival_department_code,
  region_id,
  is_internal_call,
  from_id,
  from_type,
  to_id,
  to_type
)
SELECT DISTINCT jr.request_id,
                jr.requested_date_time,
                req.request_date,
                jr.clockwise,
                jr.departure_insee,
                jr.departure_admin,
                jr.departure_admin_name,
                substring(departure_insee, 1, 2) as departure_department_code,
                jr.arrival_insee,
                jr.arrival_admin,
                jr.arrival_admin_name,
                substring(arrival_insee, 1, 2) as arrival_department_code,
                first_value(cov.region_id) OVER (PARTITION BY jr.request_id) AS region_id,
                CASE WHEN req.user_name LIKE '%canaltp%' THEN 1 ELSE 0 END as is_internal_call,
                from_param.param_value as from_id,
                CASE WHEN from_param.param_value LIKE '%:%' THEN substring(from_param.param_value from '^([^\:]*):') ELSE 'address' END as from_type,
                to_param.param_value as to_id,
                CASE WHEN to_param.param_value LIKE '%:%' THEN substring(to_param.param_value from '^([^\:]*):') ELSE 'address' END as to_type
FROM stat.journey_request jr
INNER JOIN stat.requests req ON req.id=jr.request_id
INNER JOIN stat.coverages cov ON req.id=cov.request_id
LEFT JOIN stat.parameters from_param ON req.id=from_param.request_id AND from_param.param_key='from'
LEFT JOIN stat.parameters to_param ON req.id=to_param.request_id AND to_param.param_key='to'
WHERE
req.request_date >= (:start_date :: date)
AND req.request_date < (:end_date :: date) + interval '1 day';
EOT;

        return $insertQuery;
    }

    protected function getInitQuery()
    {
        $initQuery = <<<EOT
INSERT INTO stat_compiled.journey_request_stats
(
  request_id,
  requested_date_time,
  request_date,
  clockwise,
  departure_insee,
  departure_admin,
  departure_admin_name,
  departure_department_code,
  arrival_insee,
  arrival_admin,
  arrival_admin_name,
  arrival_department_code,
  region_id,
  is_internal_call,
  from_id,
  from_type,
  to_id,
  to_type
)
SELECT DISTINCT jr.request_id,
                jr.requested_date_time,
                req.request_date,
                jr.clockwise,
                jr.departure_insee,
                jr.departure_admin,
                jr.departure_admin_name,
                substring(departure_insee, 1, 2) as departure_department_code,
                jr.arrival_insee,
                jr.arrival_admin,
                jr.arrival_admin_name,
                substring(arrival_insee, 1, 2) as arrival_department_code,
                first_value(cov.region_id) OVER (PARTITION BY jr.request_id) AS region_id,
                CASE WHEN req.user_name LIKE '%canaltp%' THEN 1 ELSE 0 END as is_internal_call,
                from_param.param_value as from_id,
                CASE WHEN from_param.param_value LIKE '%:%' THEN substring(from_param.param_value from '^([^\:]*):') ELSE 'address' END as from_type,
                to_param.param_value as to_id,
                CASE WHEN to_param.param_value LIKE '%:%' THEN substring(to_param.param_value from '^([^\:]*):') ELSE 'address' END as to_type
FROM stat.journey_request jr
INNER JOIN stat.requests req ON req.id=jr.request_id
INNER JOIN stat.coverages cov ON req.id=cov.request_id
LEFT JOIN stat.parameters from_param ON req.id=from_param.request_id AND from_param.param_key='from'
LEFT JOIN stat.parameters to_param ON req.id=to_param.request_id AND to_param.param_key='to';
EOT;
        return $initQuery;
    }
}
