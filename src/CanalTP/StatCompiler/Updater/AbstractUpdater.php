<?php
namespace CanalTP\StatCompiler\Updater;

use Psr\Log\LoggerAwareTrait;
use Psr\Log\NullLogger;

abstract class AbstractUpdater implements UpdaterInterface
{
    use LoggerAwareTrait;

    protected $dbConnection;

    public function __construct(\PDO $dbConnection)
    {
        $this->dbConnection = $dbConnection;
        $this->logger = new NullLogger();
    }

    protected abstract function getDeleteQuery();
    protected abstract function getInsertQuery();
    protected abstract function getInitQuery();

    public function update(\DateTime $startDate, \DateTime $endDate)
    {
        $this->dbConnection->beginTransaction();

        try {
            $this->logger->info("Deleting from " . $this->getAffectedTable());
            $this->logger->debug("Query = " . $this->getDeleteQuery(), array('start_date' => $startDate, 'end_date' => $endDate));
            $deleteStmt = $this->dbConnection->prepare($this->getDeleteQuery());
            $deleteStmt->bindValue('start_date', $startDate->format('Y-m-d'));
            $deleteStmt->bindValue('end_date', $endDate->format('Y-m-d'));
            $deleteStmt->execute();

            $this->logger->debug("Nb lines deleted = " . $deleteStmt->rowCount());

            $this->logger->info("Inserting into " . $this->getAffectedTable());
            $this->logger->debug("Query = " . $this->getInsertQuery(), array('start_date' => $startDate, 'end_date' => $endDate));
            $insertStmt = $this->dbConnection->prepare($this->getInsertQuery());
            $insertStmt->bindValue('start_date', $startDate->format('Y-m-d'));
            $insertStmt->bindValue('end_date', $endDate->format('Y-m-d'));
            $insertStmt->execute();

            $this->logger->debug("Nb lines inserted = " . $insertStmt->rowCount());

            $this->dbConnection->commit();
        } catch(\PDOException $e) {
            $this->dbConnection->rollBack();
            throw new \RuntimeException("Exception occurred during update", 1, $e);
        }
    }

    public function init()
    {
        $this->logger->info("Truncating table " . $this->getAffectedTable());
        $truncateQuery = 'TRUNCATE TABLE stat_compiled.' . $this->getAffectedTable();
        $this->logger->debug("Query = " . $truncateQuery);
        $this->dbConnection->exec($truncateQuery);

        $this->logger->info("Loading initial data into table " . $this->getAffectedTable());
        $this->logger->debug("Query = " . $this->getInitQuery());
        $this->dbConnection->exec($this->getInitQuery());
        $this->logger->info("Initial data loaded into table " . $this->getAffectedTable());
    }
}
