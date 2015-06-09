<?php

namespace DoctrineMigrations;

use Doctrine\DBAL\Migrations\AbstractMigration;
use Doctrine\DBAL\Schema\Schema;

/**
 * Creation of the journey_cities table and associated trigger for partitions
 */
class Version20150609153909 extends AbstractMigration
{
    /**
     * @param Schema $schema
     */
    public function up(Schema $schema)
    {
        $this->addSql('
            CREATE TABLE stat_compiled.journey_cities
            (
              journey_id bigint NOT NULL,
              city_id text NOT NULL,
              city_insee text,
              city_name text,
              department_code text,
              request_date timestamp without time zone,
              is_start_city boolean,
              is_end_city boolean,
              CONSTRAINT journey_cities_pkey PRIMARY KEY (journey_id, city_id)
            )
            WITH (
              OIDS=FALSE
            );
        ');

        $this->addSql('
            CREATE INDEX journey_cities_end_city
              ON stat_compiled.journey_cities
              USING btree
              (journey_id, is_end_city);
        ');

        $this->addSql('
            CREATE INDEX journey_cities_start_city
              ON stat_compiled.journey_cities
              USING btree
              (journey_id, is_start_city);
        ');


        $this->addSql('
            CREATE OR REPLACE FUNCTION journey_cities_insert_trigger()
            RETURNS TRIGGER AS $$
            DECLARE
              schema VARCHAR(100);
              partition VARCHAR(100);
            BEGIN
              schema := \'stat_compiled\';
              partition := \'journey_cities\' || \'_\' || to_char(NEW.request_date, \'"y"YYYY"m"MM\');
              IF NOT EXISTS(SELECT 1 FROM pg_tables WHERE tablename=partition and schemaname=schema) THEN
                RAISE NOTICE \'A partition has been created %\',partition;
                EXECUTE \'CREATE TABLE IF NOT EXISTS \' || schema || \'.\' || partition || 
                        \' ( CONSTRAINT \' || partition || \'_pkey PRIMARY KEY (journey_id, city_id), \' ||
                          \'check (request_date >= DATE \'\'\' || to_char(NEW.request_date, \'YYYY-MM-01\') || \'\'\' 
                                  AND request_date < DATE \'\'\' || to_char(NEW.request_date + interval \'1 month\', \'YYYY-MM-01\') || \'\'\') ) \' || 
                        \'INHERITS (\' || schema || \'.journey_cities);\';
              END IF;
              EXECUTE \'INSERT INTO \' || schema || \'.\' || partition || \' SELECT(\' || schema || \'.journey_cities\' || \' \' || quote_literal(NEW) || \').*;\';
              RETURN NULL;
            END;
            $$
            LANGUAGE plpgsql;
        ');

        $this->addSql('
            CREATE TRIGGER insert_journey_cities_trigger
                BEFORE INSERT ON stat_compiled.journey_cities
                FOR EACH ROW EXECUTE PROCEDURE journey_cities_insert_trigger();
        ');

    }

    /**
     * @param Schema $schema
     */
    public function down(Schema $schema)
    {
        $this->addSql('DROP TABLE IF EXISTS stat_compiled.journey_cities CASCADE;');
        $this->addSql('DROP FUNCTION IF EXISTS journey_cities_insert_trigger();');
    }
}
