<?php
namespace CanalTP\StatCompiler\Updater;

use Psr\Log\LoggerAwareTrait;

class TokenStatsUpdater extends AbstractUpdater
{
    public function getAffectedTable()
    {
        return 'token_stats';
    }

    protected function getDeleteQuery()
    {
        $deleteQuery = "
            DELETE FROM stat_compiled.token_stats
            WHERE request_date >= (:start_date :: date)
            AND request_date < (:end_date :: date) + interval '1 day';
        ";

        return $deleteQuery;
    }

    protected function getInsertQuery()
    {
        $insertQuery = "
            INSERT INTO stat_compiled.token_stats (token, request_date, nb_req)
            SELECT
                req.token,
                DATE(req.request_date) AS request_date,
                COUNT(DISTINCT req.id) nb_req
            FROM stat.requests req
            WHERE
                req.request_date >= (:start_date :: date)
                AND req.request_date < (:end_date :: date) + interval '1 day'
            GROUP BY
                token,
                DATE(request_date);
        ";

        return $insertQuery;
    }

    protected function getInitQuery()
    {
        $initQuery = "
            INSERT INTO stat_compiled.token_stats (token, request_date, nb_req)
            SELECT
                req.token,
                DATE(req.request_date) AS request_date,
                COUNT(DISTINCT req.id) nb_req
            FROM stat.requests req
            GROUP BY
                token,
                DATE(request_date);
        ";

        return $initQuery;
    }
}
