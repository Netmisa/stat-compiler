<?php
namespace CanalTP\StatCompiler\Updater;

use Psr\Log\LoggerAwareTrait;

class CoverageStopAreasUpdater extends AbstractUpdater
{
    public function getAffectedTable()
    {
        return 'coverage_stop_areas';
    }

    protected function getDeleteQuery()
    {
        return "DELETE FROM stat_compiled.coverage_stop_areas WHERE request_date >= (:start_date :: date) and request_date < (:end_date :: date) + interval '1 day'";
    }

    protected function getInsertQuery()
    {
        $insertQuery = <<<EOT
INSERT INTO stat_compiled.coverage_stop_areas
(
  request_date,
  region_id,
  stop_area_id,
  stop_area_name,
  city_id,
  city_name,
  city_insee,
  department_code,
  is_internal_call,
  nb
)
SELECT DISTINCT
    A.request_date::date,
    cov.region_id,
    A.stop_area_id,
    A.stop_area_name,
    A.city_id,
    A.city_name,
    A.city_insee,
    A.department_code,
    CASE WHEN req.user_name LIKE '%canaltp%' THEN 1 ELSE 0 END as is_internal_call,
    COUNT(DISTINCT A.journey_id) AS nb
FROM (
    SELECT
        request_id,
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
        request_id,
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
) A
INNER JOIN stat.requests req ON req.id = A.request_id
INNER JOIN stat.coverages cov ON cov.request_id = req.id
GROUP BY A.request_date::date, cov.region_id, A.stop_area_id, A.stop_area_name,  A.city_id, A.city_name, A.city_insee, A.department_code, is_internal_call
;
EOT;

        return $insertQuery;
    }

    protected function getInitQuery()
    {
        $initQuery = <<<EOT
INSERT INTO stat_compiled.coverage_stop_areas
(
  request_date,
  region_id,
  stop_area_id,
  stop_area_name,
  city_id,
  city_name,
  city_insee,
  department_code,
  is_internal_call,
  nb
)
SELECT DISTINCT
    A.request_date::date,
    cov.region_id,
    A.stop_area_id,
    A.stop_area_name,
    A.city_id,
    A.city_name,
    A.city_insee,
    A.department_code,
    CASE WHEN req.user_name LIKE '%canaltp%' THEN 1 ELSE 0 END as is_internal_call,
    COUNT(DISTINCT A.journey_id) AS nb
FROM (
    SELECT
        request_id,
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
    UNION ALL
    SELECT
        request_id,
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
) A
INNER JOIN stat.requests req ON req.id = A.request_id
INNER JOIN stat.coverages cov ON cov.request_id = req.id
GROUP BY A.request_date::date, cov.region_id, A.stop_area_id, A.stop_area_name,  A.city_id, A.city_name, A.city_insee, A.department_code, is_internal_call
;
EOT;
        return $initQuery;
    }
}
