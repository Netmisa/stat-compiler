<?php
namespace CanalTP\StatCompiler\Command;

use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Output\OutputInterface;
use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Input\InputOption;

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

    protected function configure()
    {
        $this
            ->setName('updatedb')
            ->setDescription('Update compiled stat tables')
            ->addArgument(
                'start_date',
                InputArgument::OPTIONAL,
                'Consolidation start date (YYYY-MM-DD). Defaults to yesterday.',
                date('Y-m-d', time() - 24 * 3600)
            )
            ->addArgument(
                'end_date',
                InputArgument::OPTIONAL,
                'Consolidation end date (YYYY-MM-DD). Defaults to yesterday.',
                date('Y-m-d', time() - 24 * 3600)
            )
            ->addOption(
                'only-update',
                null,
                InputOption::VALUE_REQUIRED,
                'Limit update to given tables',
                ''
            );
            
    }

    protected function execute(InputInterface $input, OutputInterface $output)
    {
        $startDate = \DateTime::createFromFormat('!Y-m-d', $input->getArgument('start_date'));
        $endDate = \DateTime::createFromFormat('!Y-m-d', $input->getArgument('end_date'));

        if (false === $startDate) {
            throw new \RuntimeException('Wrong start date format (' . $input->getArgument('start_date') . ') expecting YYYY-MM-DD');
        }

        if (false === $endDate) {
            throw new \RuntimeException('Wrong end date format (' . $input->getArgument('end_date') . ') expecting YYYY-MM-DD');
        }

        if ($startDate > $endDate) {
            throw new \RuntimeException('Start date (' . $input->getArgument('start_date') . ') must be before end date (' . $input->getArgument('end_date') . ')');
        }

        $this->logger->info('Starting update', array('start_date' => $startDate, 'end_date' => $endDate));
        $tables = array();
        if('' !== $input->getOption('only-update')){
            $tables = explode(',', $input->getOption('only-update'));
        } 
        
        foreach ($this->updaters as $upd) {
            if(empty($tables) || in_array($upd->getAffectedTable(), $tables)){
                $this->logger->info("Launching " . get_class($upd));
                $upd->update($startDate, $endDate);
            }
        }
        $this->logger->info('Update ended');
    }

    public function addUpdater(UpdaterInterface $updater)
    {
        $this->updaters[] = $updater;
    }
}
