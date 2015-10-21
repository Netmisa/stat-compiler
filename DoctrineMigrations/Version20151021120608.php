<?php

namespace DoctrineMigrations;

use Doctrine\DBAL\Migrations\AbstractMigration;
use Doctrine\DBAL\Schema\Schema;

/**
 * Auto-generated Migration: Please modify to your needs!
 */
class Version20151021120608 extends AbstractMigration
{
    /**
     * @param Schema $schema
     */
    public function up(Schema $schema)
    {
        $this->addSql('
            CREATE TABLE stat_compiled.coverage_stop_areas
            (
              request_date timestamp without time zone,
              region_id text NOT NULL,
              stop_area_id text,
              stop_area_name text,
              city_id text,
              city_name text,
              city_insee text,
              department_code text,
              is_internal_call integer,
              nb integer,
              CONSTRAINT coverage_stop_areas_pkey PRIMARY KEY (request_date, region_id, stop_area_id, city_id, is_internal_call)
            )
            WITH (
              OIDS=FALSE
            );
        ');

        $this->addSql('
            CREATE OR REPLACE FUNCTION coverage_stop_areas_insert_trigger()
            RETURNS TRIGGER AS $$
            DECLARE
              schema VARCHAR(100);
              partition VARCHAR(100);
            BEGIN
              schema := \'stat_compiled\';
              partition := \'coverage_stop_areas\' || \'_\' || to_char(NEW.request_date, \'"y"YYYY"m"MM\');
              IF NOT EXISTS(SELECT 1 FROM pg_tables WHERE tablename=partition and schemaname=schema) THEN
                RAISE NOTICE \'A partition has been created %\',partition;
                EXECUTE \'CREATE TABLE IF NOT EXISTS \' || schema || \'.\' || partition ||
                        \' (
                          check (request_date >= DATE \'\'\' || to_char(NEW.request_date, \'YYYY-MM-01\') || \'\'\'
                                  AND request_date < DATE \'\'\' || to_char(NEW.request_date + interval \'1 month\', \'YYYY-MM-01\') || \'\'\') ) \' ||
                        \'INHERITS (\' || schema || \'.coverage_stop_areas);\';
              END IF;
              EXECUTE \'INSERT INTO \' || schema || \'.\' || partition || \' SELECT(\' || schema || \'.coverage_stop_areas\' || \' \' || quote_literal(NEW) || \').*;\';
              RETURN NULL;
            END;
            $$
            LANGUAGE plpgsql;
        ');

        $this->addSql('
            CREATE TRIGGER insert_coverage_stop_areas_trigger
                BEFORE INSERT ON stat_compiled.coverage_stop_areas
                FOR EACH ROW EXECUTE PROCEDURE coverage_stop_areas_insert_trigger();
        ');

        $this->addSql('grant select on stat_compiled.coverage_stop_areas to usrsql_nmp_stat;');

    }

    /**
     * @param Schema $schema
     */
    public function down(Schema $schema)
    {
        $this->addSql('DROP TABLE IF EXISTS stat_compiled.coverage_stop_areas CASCADE;');
        $this->addSql('DROP FUNCTION IF EXISTS coverage_stop_areas_insert_trigger();');

    }
}
