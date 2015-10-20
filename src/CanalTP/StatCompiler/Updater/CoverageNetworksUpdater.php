<?php
namespace CanalTP\StatCompiler\Updater;

use Psr\Log\LoggerAwareTrait;

class CoverageNetworksUpdater extends AbstractUpdater
{
    public function getAffectedTable()
    {
        return 'coverage_networks';
    }

    protected function getDeleteQuery()
    {
        return "DELETE FROM stat_compiled.coverage_networks WHERE request_date >= (:start_date :: date) and request_date < (:end_date :: date) + interval '1 day'";
    }

    protected function getInsertQuery()
    {
        $insertQuery = <<<EOT
INSERT INTO stat_compiled.coverage_networks
(
  request_date,
  region_id,
  network_id,
  network_name,
  is_internal_call,
  nb
)
SELECT req.request_date::date,
       cov.region_id,
       js.network_id,
       js.network_name,
       CASE WHEN req.user_name LIKE '%canaltp%' THEN 1 ELSE 0 END as is_internal_call,
       COUNT(DISTINCT js.journey_id) AS nb
FROM stat.journey_sections js
INNER JOIN stat.requests req ON req.id = js.request_id
INNER JOIN stat.coverages cov ON cov.request_id = req.id
WHERE network_id <> ''
AND req.request_date >= (:start_date :: date)
AND req.request_date < (:end_date :: date) + interval '1 day'
GROUP BY req.request_date::date, cov.region_id, js.network_id, js.network_name, is_internal_call
;
EOT;

        return $insertQuery;
    }

    protected function getInitQuery()
    {
        $initQuery = <<<EOT
INSERT INTO stat_compiled.coverage_networks
(
  request_date,
  region_id,
  network_id,
  network_name,
  is_internal_call,
  nb
)
SELECT req.request_date::date,
       cov.region_id,
       js.network_id,
       js.network_name,
       CASE WHEN req.user_name LIKE '%canaltp%' THEN 1 ELSE 0 END as is_internal_call,
       COUNT(DISTINCT js.journey_id) AS nb
FROM stat.journey_sections js
INNER JOIN stat.requests req ON req.id = js.request_id
INNER JOIN stat.coverages cov ON cov.request_id = req.id
GROUP BY req.request_date::date, cov.region_id, js.network_id, js.network_name, is_internal_call
;
EOT;
        return $initQuery;
    }
}
