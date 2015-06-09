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
  request_date,
  is_start_network,
  is_end_network
)
SELECT js.journey_id,
       js.network_id,
       js.network_name,
       row_number() over (PARTITION BY js.journey_id ORDER BY js.journey_id, js.id) as rank,
       req.request_date,
       CASE WHEN row_number() over (PARTITION BY js.journey_id ORDER BY js.journey_id, js.id) = 1 THEN true ELSE false END as is_start_network,
       CASE WHEN row_number() over (PARTITION BY js.journey_id ORDER BY js.journey_id, js.id) = count(1) over (PARTITION BY js.journey_id) THEN true ELSE false END as is_end_network
FROM stat.journey_sections js
INNER JOIN stat.requests req ON req.id = js.request_id
WHERE network_id <> ''
AND req.request_date >= (:start_date :: date)
AND req.request_date < (:end_date :: date) + interval '1 day';
EOT;

        return $insertQuery;
    }

    protected function getInitQuery()
    {
        $initQuery = <<<EOT
INSERT INTO stat_compiled.journey_networks
(
  journey_id,
  network_id,
  network_name,
  rank,
  request_date,
  is_start_network,
  is_end_network
)
SELECT js.journey_id,
       js.network_id,
       js.network_name,
       row_number() over (PARTITION BY js.journey_id ORDER BY js.journey_id, js.id) as rank,
       req.request_date,
       CASE WHEN row_number() over (PARTITION BY js.journey_id ORDER BY js.journey_id, js.id) = 1 THEN true ELSE false END as is_start_network,
       CASE WHEN row_number() over (PARTITION BY js.journey_id ORDER BY js.journey_id, js.id) = count(1) over (PARTITION BY js.journey_id) THEN true ELSE false END as is_end_network
FROM stat.journey_sections js
INNER JOIN stat.requests req ON req.id = js.request_id
WHERE network_id <> '' ;
EOT;
        return $initQuery;
    }
}
