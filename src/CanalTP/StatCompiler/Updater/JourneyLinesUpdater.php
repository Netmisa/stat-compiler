<?php
namespace CanalTP\StatCompiler\Updater;

use Psr\Log\LoggerAwareTrait;

class JourneyLinesUpdater extends AbstractUpdater
{
    public function getAffectedTable()
    {
        return 'journey_lines';
    }

    protected function getDeleteQuery()
    {
        return "DELETE FROM stat_compiled.journey_lines WHERE request_date >= (:start_date :: date) and request_date < (:end_date :: date) + interval '1 day'";
    }

    protected function getInsertQuery()
    {
        $insertQuery = <<<EOT
INSERT INTO stat_compiled.journey_lines
(
  journey_id,
  type,
  line_id,
  line_code,
  network_id,
  network_name,
  request_date
)
SELECT DISTINCT journey_id,
                type,
                line_id,
                line_code,
                network_id,
                network_name,
                request_date
FROM stat.journey_sections js
INNER JOIN stat.requests req ON req.id = js.request_id
WHERE line_id <> ''
AND req.request_date >= (:start_date :: date)
AND req.request_date < (:end_date :: date) + interval '1 day';
EOT;

        return $insertQuery;
    }

    protected function getInitQuery()
    {
        $initQuery = <<<EOT
INSERT INTO stat_compiled.journey_lines
(
  journey_id,
  type,
  line_id,
  line_code,
  network_id,
  network_name,
  request_date
)
SELECT DISTINCT journey_id,
                type,
                line_id,
                line_code,
                network_id,
                network_name,
                request_date
FROM stat.journey_sections js
INNER JOIN stat.requests req ON req.id = js.request_id
WHERE line_id <> '' ;
EOT;
        return $initQuery;
    }
}
