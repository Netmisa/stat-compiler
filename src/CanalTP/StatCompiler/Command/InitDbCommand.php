<?php
namespace CanalTP\StatCompiler\Command;

use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Output\OutputInterface;
use Symfony\Component\Console\Input\InputArgument;

use CanalTP\StatCompiler\Updater\UpdaterInterface;
use CanalTP\StatCompiler\Updater\ErrorStatsUpdater;
use CanalTP\StatCompiler\Updater\RequestCallsUpdater;

use Psr\Log\LoggerAwareTrait;

use Monolog\Logger;
use Monolog\Handler\StreamHandler;

class InitDbCommand extends Command
{
    use LoggerAwareTrait;

    private $updaters = array();
    private $dbConnection;

    protected function configure()
    {
        $this
            ->setName('initdb')
            ->setDescription('Initialize stat tables from all raw data')
        ;
    }

    protected function execute(InputInterface $input, OutputInterface $output)
    {
        $this->logger->info('Starting init');
        foreach ($this->updaters as $upd) {
            $this->logger->info("Launching " . get_class($upd));
            $upd->init();
        }
        $this->logger->info('Init ended');
    }

    public function addUpdater(UpdaterInterface $updater)
    {
        $this->updaters[] = $updater;
    }
}
