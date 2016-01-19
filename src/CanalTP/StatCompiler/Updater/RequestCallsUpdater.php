<?php
namespace CanalTP\StatCompiler\Updater;

class RequestCallsUpdater extends AbstractUpdater
{
    public function getAffectedTable()
    {
        return 'requests_calls';
    }

    protected function getDeleteQuery()
    {
        return "DELETE FROM stat_compiled.requests_calls WHERE request_date >= (:start_date :: date) and request_date < (:end_date :: date) + interval '1 day'";
    }

    protected function getInsertQuery()
    {
        $insertQuery = <<<EOT
INSERT INTO stat_compiled.requests_calls
(
  end_point_id,
  region_id,
  api,
  user_id,
  app_name,
  is_internal_call,
  request_date,
  nb,
  nb_without_journey,
  object_count
)
SELECT
    end_point_id,
    cov.region_id,
    req.api,
    user_id,
    app_name,
    CASE WHEN req.user_name LIKE '%canaltp%' THEN 1 ELSE 0 END as is_internal_call,
    DATE(req.request_date) AS request_date,
    COUNT(DISTINCT req.id) AS nb,
    COUNT(DISTINCT CASE WHEN j.request_id IS NULL THEN req.id ELSE null END) AS nb_without_journey,
    SUM(CASE WHEN object_count IS NULL THEN 0 ELSE object_count END) AS object_count
FROM
    stat.requests req
    INNER JOIN stat.coverages cov ON cov.request_id=req.id
    LEFT JOIN stat.journeys j ON j.request_id=req.id
    LEFT JOIN stat.info_response ir ON ir.request_id = req.id
WHERE
    req.request_date >= (:start_date :: date)
    AND req.request_date < (:end_date :: date) + interval '1 day'
GROUP BY
    end_point_id,
    cov.region_id,
    req.api,
    user_id,
    app_name,
    is_internal_call,
    DATE(req.request_date)
;
EOT;

        return $insertQuery;
    }

    protected function getInitQuery()
    {
        $initQuery = <<<EOT
INSERT INTO stat_compiled.requests_calls
(
  end_point_id,
  region_id,
  api,
  user_id,
  app_name,
  is_internal_call,
  request_date,
  nb,
  nb_without_journey,
  object_count
)
SELECT
    end_point_id,
    cov.region_id,
    req.api,
    user_id,
    app_name,
    CASE WHEN req.user_name LIKE '%canaltp%' THEN 1 ELSE 0 END as is_internal_call,
    DATE(req.request_date) AS request_date,
    COUNT(DISTINCT req.id) AS nb,
    COUNT(DISTINCT CASE WHEN j.request_id IS NULL THEN req.id ELSE null END) AS nb_without_journey,
    SUM(object_count)
FROM
    stat.requests req
    INNER JOIN stat.coverages cov ON cov.request_id=req.id
    LEFT JOIN stat.journeys j ON j.request_id=req.id
    LEFT JOIN stat.info_response ir ON ir.request_id = req.id
GROUP BY
    end_point_id,
    cov.region_id,
    req.api,
    user_id,
    app_name,
    is_internal_call,
    DATE(req.request_date)
;
EOT;
        return $initQuery;
    }
}
