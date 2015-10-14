<?php
namespace CanalTP\StatCompiler\Updater;

use Psr\Log\LoggerAwareTrait;
use Psr\Log\NullLogger;

class UsersUpdater implements UpdaterInterface
{
    use LoggerAwareTrait;

    protected $dbConnection;

    public function __construct(\PDO $dbConnection)
    {
        $this->dbConnection = $dbConnection;
        $this->logger = new NullLogger();
    }

    public function getAffectedTable()
    {
        return 'users';
    }

    public function update(\DateTime $startDate, \DateTime $endDate)
    {
        $this->dbConnection->beginTransaction();

        try {

            $this->dbConnection->exec(
                'CREATE TEMPORARY TABLE tmp_users AS ' .
                'SELECT DISTINCT user_id as id, user_name ' .
                'FROM ( ' .
                    'SELECT user_id, first_value(user_name) over (partition by user_id order by request_date DESC) as user_name ' .
                    'FROM ( ' .
                        'SELECT user_id, user_name, MIN(request_date) as request_date ' .
                        'FROM stat.requests ' .
                        "WHERE request_date >= ('" . $startDate->format('Y-m-d') . "' :: date) " .
                        "AND request_date < ('" . $endDate->format('Y-m-d') . "' :: date) + interval '1 day' " .
                        'GROUP BY user_id, user_name ' .
                    ') B ' .
                ') A '
            );

            $this->dbConnection->exec('DELETE FROM stat_compiled.users WHERE id in (select id from tmp_users)');

            $this->dbConnection->exec('INSERT INTO stat_compiled.users (id, user_name) SELECT id, user_name FROM tmp_users');

            $this->dbConnection->exec('DROP TABLE tmp_users');
            $this->dbConnection->commit();
        } catch(\PDOException $e) {
            $this->dbConnection->rollBack();
            throw new \RuntimeException("Exception occurred during update", 1, $e);
        }
    }

    public function init()
    {
        $sqlToRun = array(
            'TRUNCATE TABLE stat_compiled.users',
            'INSERT INTO stat_compiled.users
            (id, user_name)
            SELECT DISTINCT user_id, user_name
            FROM (
                SELECT user_id, first_value(user_name) over (partition by user_id order by request_date DESC) as user_name
                FROM (
                    SELECT user_id, user_name, MIN(request_date) as request_date
                    FROM stat.requests
                    GROUP BY user_id, user_name
                ) B
            ) A',
        );
        foreach ($sqlToRun as $sql) {
            $this->logger->debug('Query = ' . $sql);
            $this->dbConnection->exec($sql);
        }
    }
}
