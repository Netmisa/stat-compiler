<?php
namespace CanalTP\StatCompiler\Updater;

use Psr\Log\LoggerAwareTrait;

class JourneyModesUpdater extends AbstractUpdater
{
    public function getAffectedTable()
    {
        return 'journey_modes';
    }

    protected function getDeleteQuery()
    {
        return "DELETE FROM stat_compiled.journey_modes WHERE request_date >= (:start_date :: date) and request_date < (:end_date :: date) + interval '1 day'";
    }

    protected function getInsertQuery()
    {
        $insertQuery = <<<EOT
INSERT INTO stat_compiled.journey_modes
(
  journey_id,
  type,
  mode,
  commercial_mode_id,
  commercial_mode_name,
  request_date
)
SELECT DISTINCT js.journey_id,
                js.type,
                js.mode,
                js.commercial_mode_id,
                js.commercial_mode_name,
                req.request_date
FROM stat.journey_sections js
INNER JOIN stat.requests req ON req.id = js.request_id
WHERE req.request_date >= (:start_date :: date)
AND req.request_date < (:end_date :: date) + interval '1 day';
EOT;

        return $insertQuery;
    }

    protected function getInitQuery()
    {
        $initQuery = <<<EOT
INSERT INTO stat_compiled.journey_modes
(
  journey_id,
  type,
  mode,
  commercial_mode_id,
  commercial_mode_name,
  request_date
)
SELECT DISTINCT js.journey_id,
                js.type,
                js.mode,
                js.commercial_mode_id,
                js.commercial_mode_name,
                req.request_date
FROM stat.journey_sections js
INNER JOIN stat.requests req ON req.id = js.request_id
;
EOT;
        return $initQuery;
    }
}
