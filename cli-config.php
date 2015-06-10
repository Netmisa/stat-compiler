<?php

$params = require __DIR__ . '/conf/migrations/migrations-db.php';

$config = new \Doctrine\DBAL\Configuration();
$config->setFilterSchemaAssetsExpression("/^(stat_compiled\.|doctrine_).*/");

$conn = \Doctrine\DBAL\DriverManager::getConnection($params, $config);

$GLOBALS['doctrine-helper-set'] = new \Symfony\Component\Console\Helper\HelperSet(array(
    'db' => new \Doctrine\DBAL\Tools\Console\Helper\ConnectionHelper($conn),
));
