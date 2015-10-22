<?php

namespace DoctrineMigrations;

use Symfony\Component\DependencyInjection\ContainerAwareInterface;
use Symfony\Component\DependencyInjection\ContainerInterface;
use Doctrine\DBAL\Migrations\AbstractMigration;
use Doctrine\DBAL\Schema\Schema;

/**
 * Auto-generated Migration: Please modify to your needs!
 */
class Version20151014164621 extends AbstractMigration implements ContainerAwareInterface
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
        $this->addSql('grant select on stat_compiled.coverage_modes to ' . $this->userBdd . ';');
    }

    /**
     * @param Schema $schema
     */
    public function down(Schema $schema)
    {
        $this->addSql('revoke select on stat_compiled.coverage_modes from ' . $this->userBdd . ';');
    }
}
