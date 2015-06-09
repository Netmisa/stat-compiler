<?php

namespace DoctrineMigrations;

use Doctrine\DBAL\Migrations\AbstractMigration;
use Doctrine\DBAL\Schema\Schema;

/**
 * Creation of the requests_calls table and associated trigger for partitions
 */
class Version20150609162930 extends AbstractMigration
{
    /**
     * @param Schema $schema
     */
    public function up(Schema $schema)
    {
        $this->addSql('
            CREATE TABLE stat_compiled.requests_calls
            (
              region_id text NOT NULL,
              api text NOT NULL,
              user_id integer NOT NULL,
              app_name text NOT NULL,
              is_internal_call integer,
              request_date date NOT NULL,
              nb bigint,
              nb_without_journey bigint,
              CONSTRAINT requests_calls_pkey PRIMARY KEY (region_id, api, request_date, user_id, app_name)
            )
            WITH (
              OIDS=FALSE
            );
        ');

        $this->addSql('
            CREATE OR REPLACE FUNCTION requests_calls_insert_trigger()
            RETURNS TRIGGER AS $$
            DECLARE
              schema VARCHAR(100);
              partition VARCHAR(100);
            BEGIN
              schema := \'stat_compiled\';
              partition := \'requests_calls\' || \'_\' || to_char(NEW.request_date, \'"y"YYYY"m"MM\');
              IF NOT EXISTS(SELECT 1 FROM pg_tables WHERE tablename=partition and schemaname=schema) THEN
                RAISE NOTICE \'A partition has been created %\',partition;
                EXECUTE \'CREATE TABLE IF NOT EXISTS \' || schema || \'.\' || partition || 
                        \' (check (request_date >= DATE \'\'\' || to_char(NEW.request_date, \'YYYY-MM-01\') || \'\'\' 
                                  AND request_date < DATE \'\'\' || to_char(NEW.request_date + interval \'1 month\', \'YYYY-MM-01\') || \'\'\') ) \' || 
                        \'INHERITS (\' || schema || \'.requests_calls);\';
              END IF;
              EXECUTE \'INSERT INTO \' || schema || \'.\' || partition || \' SELECT(\' || schema || \'.requests_calls\' || \' \' || quote_literal(NEW) || \').*;\';
              RETURN NULL;
            END;
            $$
            LANGUAGE plpgsql;
        ');

        $this->addSql('
            CREATE TRIGGER insert_requests_calls_trigger
                BEFORE INSERT ON stat_compiled.requests_calls
                FOR EACH ROW EXECUTE PROCEDURE requests_calls_insert_trigger();
        ');
    }

    /**
     * @param Schema $schema
     */
    public function down(Schema $schema)
    {
        $this->addSql('DROP TABLE IF EXISTS stat_compiled.requests_calls CASCADE;');
        $this->addSql('DROP FUNCTION IF EXISTS requests_calls_insert_trigger();');
    }
}
