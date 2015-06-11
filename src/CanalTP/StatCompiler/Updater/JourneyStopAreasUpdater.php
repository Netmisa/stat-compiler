<?php
namespace CanalTP\StatCompiler\Updater;

use Psr\Log\LoggerAwareTrait;

class JourneyStopAreasUpdater extends AbstractUpdater
{
    public function getAffectedTable()
    {
        return 'journey_stop_areas';
    }

    protected function getDeleteQuery()
    {
        return "DELETE FROM stat_compiled.journey_stop_areas WHERE request_date >= (:start_date :: date) and request_date < (:end_date :: date) + interval '1 day'";
    }

    protected function getInsertQuery()
    {
        $insertQuery = <<<EOT
INSERT INTO stat_compiled.journey_stop_areas
(
  journey_id,
  stop_area_id,
  stop_area_name,
  city_id,
  city_name,
  city_insee,
  department_code,
  request_date,
  is_start_stop_area,
  is_end_stop_area
)
SELECT DISTINCT
    A.journey_id,
    A.stop_area_id,
    A.stop_area_name,
    A.city_id,
    A.city_name,
    A.city_insee,
    A.department_code,
    A.request_date,
    CASE WHEN A.stop_area_id=B.dep_stop_area_id AND A.city_id=B.dep_city_id THEN TRUE ELSE FALSE END AS is_start_stop_area,
    CASE WHEN A.stop_area_id=B.arr_stop_area_id AND A.city_id=B.arr_city_id THEN TRUE ELSE FALSE END AS is_end_stop_area
FROM (
    SELECT
        journey_id,
        request_date,
        to_id as stop_area_id,
        to_name as stop_area_name,
        to_admin_id as city_id,
        to_admin_name as city_name,
        to_admin_insee as city_insee,
        substring(to_admin_insee, 1, 2) as department_code
    FROM
        stat.journey_sections js
        INNER JOIN stat.requests req ON req.id = js.request_id
    WHERE
        type = 'public_transport'
        AND req.request_date >= (:start_date :: date)
        AND req.request_date < (:end_date :: date) + interval '1 day'
    UNION ALL
    SELECT
        journey_id,
        request_date,
        from_id as stop_area_id,
        from_name as stop_area_name,
        from_admin_id as city_id,
        from_admin_name as city_name,
        from_admin_insee as city_insee,
        substring(from_admin_insee, 1, 2) as department_code
    FROM
        stat.journey_sections js
        INNER JOIN stat.requests req ON req.id = js.request_id
    WHERE
        type = 'public_transport'
        AND req.request_date >= (:start_date :: date)
        AND req.request_date < (:end_date :: date) + interval '1 day'
) A,
(
    SELECT DISTINCT dep.journey_id AS journey_id,
                    dep.from_id AS dep_stop_area_id,
                    dep.from_admin_id AS dep_city_id,
                    arr.to_id AS arr_stop_area_id,
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

    protected function getInitQuery()
    {
        $initQuery = <<<EOT
INSERT INTO stat_compiled.journey_stop_areas
(
  journey_id,
  stop_area_id,
  stop_area_name,
  city_id,
  city_name,
  city_insee,
  department_code,
  request_date,
  is_start_stop_area,
  is_end_stop_area
)
SELECT DISTINCT
    A.journey_id,
    A.stop_area_id,
    A.stop_area_name,
    A.city_id,
    A.city_name,
    A.city_insee,
    A.department_code,
    A.request_date,
    CASE WHEN A.stop_area_id=B.dep_stop_area_id AND A.city_id=B.dep_city_id THEN TRUE ELSE FALSE END AS is_start_stop_area,
    CASE WHEN A.stop_area_id=B.arr_stop_area_id AND A.city_id=B.arr_city_id THEN TRUE ELSE FALSE END AS is_end_stop_area
FROM (
    SELECT
        journey_id,
        request_date,
        to_id as stop_area_id,
        to_name as stop_area_name,
        to_admin_id as city_id,
        to_admin_name as city_name,
        to_admin_insee as city_insee,
        substring(to_admin_insee, 1, 2) as department_code
    FROM
        stat.journey_sections js
        INNER JOIN stat.requests req ON req.id = js.request_id
    WHERE
        type = 'public_transport'
    UNION
    SELECT
        journey_id,
        request_date,
        from_id as stop_area_id,
        from_name as stop_area_name,
        from_admin_id as city_id,
        from_admin_name as city_name,
        from_admin_insee as city_insee,
        substring(from_admin_insee, 1, 2) as department_code
    FROM
        stat.journey_sections js
        INNER JOIN stat.requests req ON req.id = js.request_id
    WHERE
        type = 'public_transport'
) A,
(
    SELECT DISTINCT dep.journey_id AS journey_id,
                    dep.from_id AS dep_stop_area_id,
                    dep.from_admin_id AS dep_city_id,
                    arr.to_id AS arr_stop_area_id,
                    arr.to_admin_id AS arr_city_id
    FROM stat.journey_sections dep
    INNER JOIN
      (SELECT js.journey_id,
              MIN(js.id) AS dep_id,
              MAX(js.id) AS arr_id
       FROM stat.journey_sections js
       WHERE js.type = 'public_transport'
       GROUP BY js.journey_id) od ON (dep.journey_id = od.journey_id
                                      AND dep.id = od.dep_id)
    INNER JOIN stat.journey_sections arr ON (od.journey_id = arr.journey_id
                                             AND od.arr_id = arr.id)
) B
WHERE A.journey_id = B.journey_id;
EOT;
        return $initQuery;
    }
}
