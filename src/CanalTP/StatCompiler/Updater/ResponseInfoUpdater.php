<?php

namespace CanalTP\StatCompiler\Updater;

class ResponseInfoUpdater extends AbstractUpdater
{
    public function getAffectedTable()
    {
        return 'response_info';
    }

    protected function getDeleteQuery()
    {
        return "DELETE FROM stat_compiled.response_info;";
    }

    protected function getInsertQuery()
    {
        $insertQuery = <<<EOT
INSERT INTO stat_compiled.response_info
(
    datetime,
    user_id,
    object_type,
    object_count
)
SELECT
    date_trunc('day', request_date) as request_date,
	user_id,
	api,
	SUM(object_count)
FROM
    stat.info_response res
    INNER JOIN stat.requests req ON req.id = res.request_id
WHERE
    req.request_date >= (:start_date :: date)
    AND req.request_date < (:end_date :: date) + interval '1 day'
GROUP BY
    date_trunc('day', request_date), user_id, api;
EOT;

        return $insertQuery;
    }

    protected function getInitQuery()
    {
        $initQuery = <<<EOT
INSERT INTO stat_compiled.response_info
(
    datetime,
    user_id,
    object_type,
    object_count
)
SELECT
    date_trunc('day', request_date) as request_date,
	user_id,
	api,
	SUM(object_count)
FROM
    stat.info_response res
    INNER JOIN stat.requests req ON req.id = res.request_id
GROUP BY
    date_trunc('day', request_date), user_id, api;
EOT;

        return $initQuery;
    }
}
