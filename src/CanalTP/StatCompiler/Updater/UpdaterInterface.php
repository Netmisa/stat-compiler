<?php
namespace CanalTP\StatCompiler\Updater;

interface UpdaterInterface
{
    public function getAffectedTable();

    public function update(\DateTime $startDate, \DateTime $endDate);
}