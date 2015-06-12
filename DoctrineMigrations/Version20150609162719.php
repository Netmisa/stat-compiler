<?php

namespace DoctrineMigrations;

use Doctrine\DBAL\Migrations\AbstractMigration;
use Doctrine\DBAL\Schema\Schema;

/**
 * Creation of the journey_stop_areas table and associated trigger for partitions
 */
class Version20150609162719 extends AbstractMigration
{
    /**
     * @param Schema $schema
     */
    public function up(Schema $schema)
    {
        $this->addSql('
            CREATE TABLE stat_compiled.journey_stop_areas
            (
              journey_id bigint,
              stop_area_id text,
              stop_area_name text,
              city_id text,
              city_name text,
              city_insee text,
              department_code text,
              request_date timestamp without time zone,
              is_start_stop_area boolean,
              is_end_stop_area boolean,
              CONSTRAINT journey_stop_areas_pkey PRIMARY KEY (journey_id, stop_area_id, city_id)
            )
            WITH (
              OIDS=FALSE
            );
        ');

        $this->addSql('
            CREATE OR REPLACE FUNCTION journey_stop_areas_insert_trigger()
            RETURNS TRIGGER AS $$
            DECLARE
              schema VARCHAR(100);
              partition VARCHAR(100);
            BEGIN
              schema := \'stat_compiled\';
              partition := \'journey_stop_areas\' || \'_\' || to_char(NEW.request_date, \'"y"YYYY"m"MM\');
              IF NOT EXISTS(SELECT 1 FROM pg_tables WHERE tablename=partition and schemaname=schema) THEN
                RAISE NOTICE \'A partition has been created %\',partition;
                EXECUTE \'CREATE TABLE IF NOT EXISTS \' || schema || \'.\' || partition || 
                        \' (CONSTRAINT \' || partition || \'_pkey PRIMARY KEY (journey_id, stop_area_id, city_id),
                          check (request_date >= DATE \'\'\' || to_char(NEW.request_date, \'YYYY-MM-01\') || \'\'\' 
                                  AND request_date < DATE \'\'\' || to_char(NEW.request_date + interval \'1 month\', \'YYYY-MM-01\') || \'\'\') ) \' || 
                        \'INHERITS (\' || schema || \'.journey_stop_areas);\';
              END IF;
              EXECUTE \'INSERT INTO \' || schema || \'.\' || partition || \' SELECT(\' || schema || \'.journey_stop_areas\' || \' \' || quote_literal(NEW) || \').*;\';
              RETURN NULL;
            END;
            $$
            LANGUAGE plpgsql;
        ');

        $this->addSql('
            CREATE TRIGGER insert_journey_stop_areas_trigger
                BEFORE INSERT ON stat_compiled.journey_stop_areas
                FOR EACH ROW EXECUTE PROCEDURE journey_stop_areas_insert_trigger();
        ');
    }

    /**
     * @param Schema $schema
     */
    public function down(Schema $schema)
    {
        $this->addSql('DROP TABLE IF EXISTS stat_compiled.journey_stop_areas CASCADE;');
        $this->addSql('DROP FUNCTION IF EXISTS journey_stop_areas_insert_trigger();');
    }
}
