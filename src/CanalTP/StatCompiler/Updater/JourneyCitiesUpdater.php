<?php
namespace CanalTP\StatCompiler\Updater;

use Psr\Log\LoggerAwareTrait;

class JourneyCitiesUpdater extends AbstractUpdater
{
    public function getAffectedTable()
    {
        return 'journey_cities';
    }

    protected function getDeleteQuery()
    {
        return "DELETE FROM stat_compiled.journey_cities WHERE request_date >= (:start_date :: date) and request_date < (:end_date :: date) + interval '1 day'";
    }

    protected function getInsertQuery()
    {
        $insertQuery = <<<EOT
INSERT INTO stat_compiled.journey_cities
(
  journey_id,
  city_id,
  city_insee,
  city_name,
  department_code,
  request_date,
  is_start_city,
  is_end_city
)
SELECT DISTINCT
    A.journey_id,
    A.city_id,
    A.city_insee,
    A.city_name,
    A.department_code,
    A.request_date,
    CASE WHEN A.city_id=B.dep_city_id THEN TRUE ELSE FALSE END AS is_start_city,
    CASE WHEN A.city_id=B.arr_city_id THEN TRUE ELSE FALSE END AS is_end_city
FROM (
    SELECT
        journey_id,
        request_date,
        to_admin_id as city_id,
        to_admin_name as city_name,
        to_admin_insee as city_insee,
        substring(to_admin_insee, 1, 2) as department_code
    FROM
        stat.journey_sections js
        INNER JOIN stat.requests req ON req.id = js.request_id
    WHERE
        to_admin_id <> ''
        AND req.request_date >= (:start_date :: date)
        AND req.request_date < (:end_date :: date) + interval '1 day'
    UNION ALL
    SELECT
        journey_id,
        request_date,
        from_admin_id as city_id,
        from_admin_name as city_name,
        from_admin_insee as city_insee,
        substring(from_admin_insee, 1, 2) as department_code
    FROM
        stat.journey_sections js
        INNER JOIN stat.requests req ON req.id = js.request_id
    WHERE
        from_admin_id <> ''
        AND req.request_date >= (:start_date :: date)
        AND req.request_date < (:end_date :: date) + interval '1 day'
) A,
(
    SELECT DISTINCT dep.journey_id AS journey_id,
                    dep.from_admin_id AS dep_city_id,
                    arr.to_admin_id AS arr_city_id
    FROM stat.journey_sections dep
    INNER JOIN
      (SELECT js.journey_id,
              MIN(js.id) AS dep_id,
              MAX(js.id) AS arr_id
       FROM stat.journey_sections js
       INNER JOIN stat.requests req ON req.id = js.request_id
       WHERE js.type = 'public_transport'
        AND req.request_date >= (:start_date :: date)
        AND req.request_date < (:end_date :: date) + interval '1 day'
       GROUP BY js.journey_id) od ON (dep.journey_id = od.journey_id
                                      AND dep.id = od.dep_id)
    INNER JOIN stat.journey_sections arr ON (od.journey_id = arr.journey_id
                                             AND od.arr_id = arr.id)
) B
WHERE A.journey_id = B.journey_id;
EOT;

        return $insertQuery;
    }
}
