<?php

namespace DoctrineMigrations;

use Doctrine\DBAL\Migrations\AbstractMigration;
use Doctrine\DBAL\Schema\Schema;

/**
 * Auto-generated Migration: Please modify to your needs!
 */
class Version20151021122022 extends AbstractMigration
{
    /**
     * @param Schema $schema
     */
    public function up(Schema $schema)
    {
        $this->addSql('ALTER TABLE stat_compiled.requests_calls DROP COLUMN host;');
        $this->addSql('ALTER TABLE stat_compiled.requests_calls ADD end_point_id integer;');
    }

    /**
     * @param Schema $schema
     */
    public function down(Schema $schema)
    {
        $this->addSql('ALTER TABLE stat_compiled.requests_calls ADD host text NOT NULL DEFAULT \'\';');
        $this->addSql('ALTER TABLE stat_compiled.requests_calls DROP COLUMN end_point_id;');
    }
}
