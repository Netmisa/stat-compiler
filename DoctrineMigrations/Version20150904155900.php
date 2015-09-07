<?php

namespace DoctrineMigrations;

use Doctrine\DBAL\Migrations\AbstractMigration;
use Doctrine\DBAL\Schema\Schema;

/**
 * Auto-generated Migration: Please modify to your needs!
 */
class Version20150904155900 extends AbstractMigration
{
    /**
     * @param Schema $schema
     */
    public function up(Schema $schema)
    {
        $this->addSql('ALTER TABLE stat_compiled.journey_request_stats ADD from_id text, ADD from_type text, ADD to_id text, ADD to_type text;');
    }

    /**
     * @param Schema $schema
     */
    public function down(Schema $schema)
    {
        $this->addSql('ALTER TABLE stat_compiled.journey_request_stats DROP COLUMN from_id, DROP COLUMN from_type, DROP COLUMN to_id, DROP COLUMN to_type;');
    }
}
