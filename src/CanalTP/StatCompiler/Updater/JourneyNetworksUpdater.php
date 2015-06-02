<?php
namespace CanalTP\StatCompiler\Updater;

use Psr\Log\LoggerAwareTrait;

class JourneyNetworksUpdater extends AbstractUpdater
{
    public function getAffectedTable()
    {
        return 'journey_networks';
    }

    protected function getDeleteQuery()
    {
        return "DELETE FROM stat_compiled.journey_networks WHERE request_date >= (:start_date :: date) and request_date < (:end_date :: date) + interval '1 day'";
    }

    protected function getInsertQuery()
    {
        $insertQuery = <<<EOT
INSERT INTO stat_compiled.journey_networks
(
  journey_id,
  network_id,
  network_name,
  rank,
  request_date
)
SELECT js.journey_id,
       js.network_id,
       js.network_name,
       row_number() over (order by js.journey_id, js.id),
       req.request_date
FROM stat.journey_sections js
INNER JOIN stat.requests req ON req.id = js.request_id
WHERE network_id <> ''
AND req.request_date >= (:start_date :: date)
AND req.request_date < (:end_date :: date) + interval '1 day';
EOT;

        return $insertQuery;
    }
}
