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

class UpdateDbCommand extends Command
{
    use LoggerAwareTrait;

    private $updaters = array();
    private $dbConnection;

    protected function configure()
    {
        $this
            ->setName('updatedb')
            ->setDescription('Update compiled stat tables')
            ->addArgument('start_date', InputArgument::REQUIRED, 'Consolidation start date (YYYY-MM-DD)')
            ->addArgument('end_date', InputArgument::REQUIRED, 'Consolidation end date (YYYY-MM-DD)')
        ;
    }

    protected function execute(InputInterface $input, OutputInterface $output)
    {
        $startDate = \DateTime::createFromFormat('Y-m-d', $input->getArgument('start_date'));
        $endDate = \DateTime::createFromFormat('Y-m-d', $input->getArgument('end_date'));

        if (false === $startDate) {
            throw new \RuntimeException('Wrong start date format (' . $input->getArgument('start_date') . ') expecting YYYY-MM-DD');
        }

        if (false === $endDate) {
            throw new \RuntimeException('Wrong end date format (' . $input->getArgument('end_date') . ') expecting YYYY-MM-DD');
        }

        $this->logger->info('Starting update', array('start_date' => $startDate, 'end_date' => $endDate));
        foreach ($this->updaters as $upd) {
            $output->writeln("Launching " . get_class($upd));
            $upd->update($startDate, $endDate);
        }
        $this->logger->info('Update ended');
    }

    public function addUpdater(UpdaterInterface $updater)
    {
        $this->updaters[] = $updater;
    }
}
