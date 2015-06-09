<?php
namespace CanalTP\StatCompiler\Updater;

interface UpdaterInterface
{
    public function getAffectedTable();

    public function init();

    public function update(\DateTime $startDate, \DateTime $endDate);
}