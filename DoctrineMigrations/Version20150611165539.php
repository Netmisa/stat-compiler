<?php

namespace DoctrineMigrations;

use Symfony\Component\DependencyInjection\ContainerAwareInterface;
use Symfony\Component\DependencyInjection\ContainerInterface;
use Doctrine\DBAL\Migrations\AbstractMigration;
use Doctrine\DBAL\Schema\Schema;

/**
 * Grant select rights to Metrics Dashboard user on all main tables
 */
class Version20150611165539 extends AbstractMigration implements ContainerAwareInterface
{
    private $container = null;
    private $userBdd = null;

    public function setContainer(ContainerInterface $container = null)
    {
        $this->container = $container;
    }

    /**
     * @param Schema $schema
     */
    public function postUp(Schema $schema)
    {
        $this->userBdd = $this->container->getParameter('db.user');
    }

    /**
     * @param Schema $schema
     */
    public function up(Schema $schema)
    {
        $this->addSql('GRANT USAGE ON SCHEMA stat_compiled TO ' . $this->userBdd . ';');
        $this->addSql('GRANT SELECT ON stat_compiled.error_stats TO ' . $this->userBdd . ';');
        $this->addSql('GRANT SELECT ON stat_compiled.journey_cities TO ' . $this->userBdd . ';');
        $this->addSql('GRANT SELECT ON stat_compiled.journey_infos TO ' . $this->userBdd . ';');
        $this->addSql('GRANT SELECT ON stat_compiled.journey_lines TO ' . $this->userBdd . ';');
        $this->addSql('GRANT SELECT ON stat_compiled.journey_modes TO ' . $this->userBdd . ';');
        $this->addSql('GRANT SELECT ON stat_compiled.journey_networks TO ' . $this->userBdd . ';');
        $this->addSql('GRANT SELECT ON stat_compiled.journey_request_stats TO ' . $this->userBdd . ';');
        $this->addSql('GRANT SELECT ON stat_compiled.journey_stop_areas TO ' . $this->userBdd . ';');
        $this->addSql('GRANT SELECT ON stat_compiled.requests_calls TO ' . $this->userBdd . ';');
        $this->addSql('GRANT SELECT ON stat_compiled.users TO ' . $this->userBdd . ';');
    }

    /**
     * @param Schema $schema
     */
    public function down(Schema $schema)
    {
        $this->addSql('REVOKE SELECT ON stat_compiled.error_stats FROM ' . $this->userBdd . ';');
        $this->addSql('REVOKE SELECT ON stat_compiled.journey_cities FROM ' . $this->userBdd . ';');
        $this->addSql('REVOKE SELECT ON stat_compiled.journey_infos FROM ' . $this->userBdd . ';');
        $this->addSql('REVOKE SELECT ON stat_compiled.journey_lines FROM ' . $this->userBdd . ';');
        $this->addSql('REVOKE SELECT ON stat_compiled.journey_modes FROM ' . $this->userBdd . ';');
        $this->addSql('REVOKE SELECT ON stat_compiled.journey_networks FROM ' . $this->userBdd . ';');
        $this->addSql('REVOKE SELECT ON stat_compiled.journey_request_stats FROM ' . $this->userBdd . ';');
        $this->addSql('REVOKE SELECT ON stat_compiled.journey_stop_areas FROM ' . $this->userBdd . ';');
        $this->addSql('REVOKE SELECT ON stat_compiled.requests_calls FROM ' . $this->userBdd . ';');
        $this->addSql('REVOKE SELECT ON stat_compiled.users FROM ' . $this->userBdd . ';');

        $this->addSql('REVOKE USAGE ON SCHEMA stat_compiled FROM ' . $this->userBdd . ';');
    }
}
