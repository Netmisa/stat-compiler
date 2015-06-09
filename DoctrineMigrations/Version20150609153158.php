<?php

namespace DoctrineMigrations;

use Doctrine\DBAL\Migrations\AbstractMigration;
use Doctrine\DBAL\Schema\Schema;

/**
 * Creation of the error_stats table and associated trigger for partitions
 */
class Version20150609153158 extends AbstractMigration
{
    /**
     * @param Schema $schema
     */
    public function up(Schema $schema)
    {
        $this->addSql('
            CREATE TABLE stat_compiled.error_stats
            (
              region_id text NOT NULL,
              api text NOT NULL,
              request_date date NOT NULL,
              user_id integer NOT NULL,
              app_name text NOT NULL,
              err_id text NOT NULL,
              is_internal_call integer,
              nb_req bigint,
              nb_without_journey bigint,
              CONSTRAINT error_stats_pkey PRIMARY KEY (region_id, api, request_date, user_id, app_name, err_id)
            );
        ');

        $this->addSql('
            CREATE OR REPLACE FUNCTION error_stats_insert_trigger()
            RETURNS TRIGGER AS $$
            DECLARE
              schema VARCHAR(100);
              partition VARCHAR(100);
            BEGIN
              schema := \'stat_compiled\';
              partition := \'error_stats\' || \'_\' || to_char(NEW.request_date, \'"y"YYYY"m"MM\');
              IF NOT EXISTS(SELECT 1 FROM pg_tables WHERE tablename=partition and schemaname=schema) THEN
                RAISE NOTICE \'A partition has been created %\',partition;
                EXECUTE \'CREATE TABLE IF NOT EXISTS \' || schema || \'.\' || partition || 
                        \' (check (request_date >= DATE \'\'\' || to_char(NEW.request_date, \'YYYY-MM-01\') || \'\'\' 
                                  AND request_date < DATE \'\'\' || to_char(NEW.request_date + interval \'1 month\', \'YYYY-MM-01\') || \'\'\') ) \' || 
                        \'INHERITS (\' || schema || \'.error_stats);\';
              END IF;
              EXECUTE \'INSERT INTO \' || schema || \'.\' || partition || \' SELECT(\' || schema || \'.error_stats\' || \' \' || quote_literal(NEW) || \').*;\';
              RETURN NULL;
            END;
            $$
            LANGUAGE plpgsql;
        ');

        $this->addSql('
            CREATE OR REPLACE FUNCTION error_stats_insert_trigger()
            RETURNS TRIGGER AS $$
            DECLARE
              schema VARCHAR(100);
              partition VARCHAR(100);
            BEGIN
              schema := \'stat_compiled\';
              partition := \'error_stats\' || \'_\' || to_char(NEW.request_date, \'"y"YYYY"m"MM\');
              IF NOT EXISTS(SELECT 1 FROM pg_tables WHERE tablename=partition and schemaname=schema) THEN
                RAISE NOTICE \'A partition has been created %\',partition;
                EXECUTE \'CREATE TABLE IF NOT EXISTS \' || schema || \'.\' || partition || 
                        \' (check (request_date >= DATE \'\'\' || to_char(NEW.request_date, \'YYYY-MM-01\') || \'\'\' 
                                  AND request_date < DATE \'\'\' || to_char(NEW.request_date + interval \'1 month\', \'YYYY-MM-01\') || \'\'\') ) \' || 
                        \'INHERITS (\' || schema || \'.error_stats);\';
              END IF;
              EXECUTE \'INSERT INTO \' || schema || \'.\' || partition || \' SELECT(\' || schema || \'.error_stats\' || \' \' || quote_literal(NEW) || \').*;\';
              RETURN NULL;
            END;
            $$
            LANGUAGE plpgsql;
        ');

        $this->addSql('
            CREATE TRIGGER insert_error_stats_trigger
                BEFORE INSERT ON stat_compiled.error_stats
                FOR EACH ROW EXECUTE PROCEDURE error_stats_insert_trigger();
        ');
    }

    /**
     * @param Schema $schema
     */
    public function down(Schema $schema)
    {
        $this->addSql('DROP TABLE IF EXISTS stat_compiled.error_stats CASCADE;');
        $this->addSql('DROP FUNCTION IF EXISTS error_stats_insert_trigger();');
    }
}
