<?php
namespace CanalTP\StatCompiler\Updater;

use Psr\Log\LoggerAwareTrait;

class JourneyInfosUpdater extends AbstractUpdater
{
    public function getAffectedTable()
    {
        return 'journey_infos';
    }

    protected function getDeleteQuery()
    {
        return "DELETE FROM stat_compiled.journey_infos WHERE request_date >= (:start_date :: date) and request_date < (:end_date :: date) + interval '1 day'";
    }

    protected function getInsertQuery()
    {
        $insertQuery = <<<EOT
INSERT INTO stat_compiled.journey_infos
(
  id,
  request_id,
  region_id,
  user_id,
  app_name,
  is_internal_call,
  request_date,
  requested_date_time,
  nb_transfers,
  duration
)
SELECT DISTINCT
  j.id,
  j.request_id,
  first_value(cov.region_id) OVER (PARTITION BY j.id) AS region_id,
  req.user_id,
  app_name,
  CASE WHEN req.user_name LIKE '%canaltp%' THEN 1 ELSE 0 END as is_internal_call,
  req.request_date,
  j.requested_date_time,
  j.nb_transfers,
  j.duration
FROM
  stat.journeys j
  inner join stat.requests req on j.request_id=req.id
  inner join stat.coverages cov on j.request_id=cov.request_id
WHERE
req.request_date >= (:start_date :: date)
AND req.request_date < (:end_date :: date) + interval '1 day';
EOT;

        return $insertQuery;
    }
}
