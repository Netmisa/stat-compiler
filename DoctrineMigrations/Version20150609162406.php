<?php

namespace DoctrineMigrations;

use Doctrine\DBAL\Migrations\AbstractMigration;
use Doctrine\DBAL\Schema\Schema;

/**
 * Creation of the journey_request_stats table and associated trigger for partitions
 */
class Version20150609162406 extends AbstractMigration
{
    /**
     * @param Schema $schema
     */
    public function up(Schema $schema)
    {
        $this->addSql('
            CREATE TABLE stat_compiled.journey_request_stats
            (
              request_id bigint NOT NULL,
              requested_date_time timestamp without time zone,
              request_date timestamp without time zone,
              clockwise boolean,
              departure_insee text,
              departure_admin text,
              departure_admin_name text,
              departure_department_code text,
              arrival_insee text,
              arrival_admin text,
              arrival_admin_name text,
              arrival_department_code text,
              region_id text,
              is_internal_call integer,
              CONSTRAINT journey_request_stats_pkey PRIMARY KEY (request_id)
            )
            WITH (
              OIDS=FALSE
            );
        ');

        $this->addSql('
            CREATE OR REPLACE FUNCTION journey_request_stats_insert_trigger()
            RETURNS TRIGGER AS $$
            DECLARE
              schema VARCHAR(100);
              partition VARCHAR(100);
            BEGIN
              schema := \'stat_compiled\';
              partition := \'journey_request_stats\' || \'_\' || to_char(NEW.request_date, \'"y"YYYY"m"MM\');
              IF NOT EXISTS(SELECT 1 FROM pg_tables WHERE tablename=partition and schemaname=schema) THEN
                RAISE NOTICE \'A partition has been created %\',partition;
                EXECUTE \'CREATE TABLE IF NOT EXISTS \' || schema || \'.\' || partition || 
                        \' (check (request_date >= DATE \'\'\' || to_char(NEW.request_date, \'YYYY-MM-01\') || \'\'\' 
                                  AND request_date < DATE \'\'\' || to_char(NEW.request_date + interval \'1 month\', \'YYYY-MM-01\') || \'\'\') ) \' || 
                        \'INHERITS (\' || schema || \'.journey_request_stats);\';
              END IF;
              EXECUTE \'INSERT INTO \' || schema || \'.\' || partition || \' SELECT(\' || schema || \'.journey_request_stats\' || \' \' || quote_literal(NEW) || \').*;\';
              RETURN NULL;
            END;
            $$
            LANGUAGE plpgsql;
        ');

        $this->addSql('
            CREATE TRIGGER insert_journey_request_stats_trigger
                BEFORE INSERT ON stat_compiled.journey_request_stats
                FOR EACH ROW EXECUTE PROCEDURE journey_request_stats_insert_trigger();
        ');
    }

    /**
     * @param Schema $schema
     */
    public function down(Schema $schema)
    {
        $this->addSql('DROP TABLE IF EXISTS stat_compiled.journey_request_stats CASCADE;');
        $this->addSql('DROP FUNCTION IF EXISTS journey_request_stats_insert_trigger();');
    }
}
