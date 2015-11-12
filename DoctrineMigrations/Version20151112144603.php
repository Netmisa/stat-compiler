<?php

namespace DoctrineMigrations;

use Doctrine\DBAL\Migrations\AbstractMigration;
use Doctrine\DBAL\Schema\Schema;

/**
 * Auto-generated Migration: Please modify to your needs!
 */
class Version20151112144603 extends AbstractMigration
{
    /**
     * @param Schema $schema
     */
    public function up(Schema $schema)
    {
        $this->addSql('
            CREATE TABLE stat_compiled.coverage_journeys_transfers
            (
              request_date timestamp without time zone,
              region_id text NOT NULL,
              is_internal_call integer,
              nb_transfers integer,
              nb integer,
              CONSTRAINT coverage_journeys_transfers_pkey PRIMARY KEY (request_date, region_id, is_internal_call, nb_transfers)
            )
            WITH (
              OIDS=FALSE
            );
        ');

        $this->addSql('
            CREATE OR REPLACE FUNCTION coverage_journeys_transfers_insert_trigger()
            RETURNS TRIGGER AS $$
            DECLARE
              schema VARCHAR(100);
              partition VARCHAR(100);
            BEGIN
              schema := \'stat_compiled\';
              partition := \'coverage_journeys_transfers\' || \'_\' || to_char(NEW.request_date, \'"y"YYYY"m"MM\');
              IF NOT EXISTS(SELECT 1 FROM pg_tables WHERE tablename=partition and schemaname=schema) THEN
                RAISE NOTICE \'A partition has been created %\',partition;
                EXECUTE \'CREATE TABLE IF NOT EXISTS \' || schema || \'.\' || partition ||
                        \' (CONSTRAINT \' || partition || \'_pkey PRIMARY KEY (request_date, region_id, is_internal_call, nb_transfers),
                          check (request_date >= DATE \'\'\' || to_char(NEW.request_date, \'YYYY-MM-01\') || \'\'\'
                                  AND request_date < DATE \'\'\' || to_char(NEW.request_date + interval \'1 month\', \'YYYY-MM-01\') || \'\'\') ) \' ||
                        \'INHERITS (\' || schema || \'.coverage_journeys_transfers);\';
              END IF;
              EXECUTE \'INSERT INTO \' || schema || \'.\' || partition || \' SELECT(\' || schema || \'.coverage_journeys_transfers\' || \' \' || quote_literal(NEW) || \').*;\';
              RETURN NULL;
            END;
            $$
            LANGUAGE plpgsql;
        ');

        $this->addSql('
            CREATE TRIGGER insert_coverage_journeys_transfers_trigger
                BEFORE INSERT ON stat_compiled.coverage_journeys_transfers
                FOR EACH ROW EXECUTE PROCEDURE coverage_journeys_transfers_insert_trigger();
        ');

        $this->addSql('grant select on stat_compiled.coverage_journeys_transfers to usrsql_nmp_stat;');
    }

    /**
     * @param Schema $schema
     */
    public function down(Schema $schema)
    {
        $this->addSql('DROP TABLE IF EXISTS stat_compiled.coverage_journeys_transfers CASCADE;');
        $this->addSql('DROP FUNCTION IF EXISTS coverage_journeys_transfers_insert_trigger();');
    }
}
