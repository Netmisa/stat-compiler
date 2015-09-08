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

class InitDbCommand extends Command
{
    use LoggerAwareTrait;

    private $updaters = array();

    protected function configure()
    {
        $this
            ->setName('initdb')
            ->setDescription('Initialize stat tables from all raw data')
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
        $this->logger->info('Starting init');

        $tables = array();
        if('' !== $input->getOption('only-update')){
            $tables = explode(',', $input->getOption('only-update'));
        }

        foreach ($this->updaters as $upd) {
            if(empty($tables) || in_array($upd->getAffectedTable(), $tables)){
                $this->logger->info("Launching " . get_class($upd));
                $upd->init();
            }
        }
        $this->logger->info('Init ended');
    }

    public function addUpdater(UpdaterInterface $updater)
    {
        $this->updaters[] = $updater;
    }
}
