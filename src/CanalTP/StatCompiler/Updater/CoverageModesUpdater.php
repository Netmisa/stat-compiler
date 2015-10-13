<?php
namespace CanalTP\StatCompiler\Updater;

use Psr\Log\LoggerAwareTrait;

class CoverageModesUpdater extends AbstractUpdater
{
    public function getAffectedTable()
    {
        return 'coverage_modes';
    }

    protected function getDeleteQuery()
    {
        return "DELETE FROM stat_compiled.coverage_modes WHERE request_date >= (:start_date :: date) and request_date < (:end_date :: date) + interval '1 day'";
    }

    protected function getInsertQuery()
    {
        $insertQuery = <<<EOT
INSERT INTO stat_compiled.coverage_modes
(
  request_date,
  region_id,
  type,
  mode,
  commercial_mode_id,
  commercial_mode_name,
  is_internal_call,
  nb
)
SELECT req.request_date::date,
       cov.region_id,
       js.type,
       js.mode,
       js.commercial_mode_id,
       js.commercial_mode_name,
       CASE WHEN req.user_name LIKE '%canaltp%' THEN 1 ELSE 0 END as is_internal_call,
       COUNT(DISTINCT js.journey_id) AS nb
FROM stat.journey_sections js
INNER JOIN stat.requests req ON req.id = js.request_id
INNER JOIN stat.coverages cov ON cov.request_id = req.id
WHERE req.request_date >= (:start_date :: date)
AND req.request_date < (:end_date :: date) + interval '1 day'
GROUP BY req.request_date::date, cov.region_id, js.type, js.mode, js.commercial_mode_id, js.commercial_mode_name, is_internal_call
;
EOT;

        return $insertQuery;
    }

    protected function getInitQuery()
    {
        $initQuery = <<<EOT
INSERT INTO stat_compiled.coverage_modes
(
  request_date,
  region_id,
  type,
  mode,
  commercial_mode_id,
  commercial_mode_name,
  is_internal_call,
  nb
)
SELECT req.request_date::date AS request_date,
       cov.region_id,
       js.type,
       js.mode,
       js.commercial_mode_id,
       js.commercial_mode_name,
       CASE WHEN req.user_name LIKE '%canaltp%' THEN 1 ELSE 0 END as is_internal_call,
       COUNT(DISTINCT js.journey_id) AS nb
FROM stat.journey_sections js
INNER JOIN stat.requests req ON req.id = js.request_id
INNER JOIN stat.coverages cov ON cov.request_id = req.id
GROUP BY request_date, cov.region_id, js.type, js.mode, js.commercial_mode_id, js.commercial_mode_name, is_internal_call
;
EOT;
        return $initQuery;
    }
}
