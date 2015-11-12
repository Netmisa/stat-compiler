<?php
namespace CanalTP\StatCompiler\Updater;

use Psr\Log\LoggerAwareTrait;

class CoverageJourneysTransfersUpdater extends AbstractUpdater
{
    public function getAffectedTable()
    {
        return 'coverage_journeys_transfers';
    }

    protected function getDeleteQuery()
    {
        return "DELETE FROM stat_compiled.coverage_journeys_transfers WHERE request_date >= (:start_date :: date) and request_date < (:end_date :: date) + interval '1 day'";
    }

    protected function getInsertQuery()
    {
        $insertQuery = <<<EOT
INSERT INTO stat_compiled.coverage_journeys_transfers
(
  request_date,
  region_id,
  is_internal_call,
  nb_transfers,
  nb
)
SELECT req.request_date::date,
       cov.region_id,
       CASE WHEN req.user_name LIKE '%canaltp%' THEN 1 ELSE 0 END as is_internal_call,
       j.nb_transfers,
       COUNT(DISTINCT j.id) AS nb
FROM stat.journeys j
INNER JOIN stat.requests req ON req.id = j.request_id
INNER JOIN stat.coverages cov ON cov.request_id = req.id
WHERE req.request_date >= (:start_date :: date)
AND req.request_date < (:end_date :: date) + interval '1 day'
GROUP BY req.request_date::date, cov.region_id, is_internal_call, j.nb_transfers
;
EOT;

        return $insertQuery;
    }

    protected function getInitQuery()
    {
        $initQuery = <<<EOT
INSERT INTO stat_compiled.coverage_journeys_transfers
(
  request_date,
  region_id,
  is_internal_call,
  nb_transfers,
  nb
)
SELECT req.request_date::date,
       cov.region_id,
       CASE WHEN req.user_name LIKE '%canaltp%' THEN 1 ELSE 0 END as is_internal_call,
       j.nb_transfers,
       COUNT(DISTINCT j.id) AS nb
FROM stat.journeys j
INNER JOIN stat.requests req ON req.id = j.request_id
INNER JOIN stat.coverages cov ON cov.request_id = req.id
GROUP BY req.request_date::date, cov.region_id, is_internal_call, j.nb_transfers
;
EOT;
        return $initQuery;
    }
}
