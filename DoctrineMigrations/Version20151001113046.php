<?php

namespace DoctrineMigrations;

use Doctrine\DBAL\Migrations\AbstractMigration;
use Doctrine\DBAL\Schema\Schema;

/**
 * Auto-generated Migration: Please modify to your needs!
 */
class Version20151001113046 extends AbstractMigration
{
    /**
     * @param Schema $schema
     */
    public function up(Schema $schema)
    {
        // Update Primary key on requests_calls table (host added)
        $this->addSql('
          CREATE UNIQUE INDEX add_host_id_temp_idx ON stat_compiled.requests_calls (region_id, api, request_date, user_id, app_name, host);
        ');
        $this->addSql('
          ALTER TABLE stat_compiled.requests_calls DROP CONSTRAINT requests_calls_pkey, ADD CONSTRAINT requests_calls_pkey PRIMARY KEY USING INDEX add_host_id_temp_idx;
        ');

        // New version of the partition auto-creation for requests_calls
        $this->addSql('
          CREATE OR REPLACE FUNCTION requests_calls_insert_trigger()
            RETURNS trigger AS $$
              DECLARE
                schema VARCHAR(100);
                partition VARCHAR(100);
              BEGIN
                schema := \'stat_compiled\';
                partition := \'requests_calls\' || \'_\' || to_char(NEW.request_date, \'"y"YYYY"m"MM\');
                IF NOT EXISTS(SELECT 1 FROM pg_tables WHERE tablename=partition and schemaname=schema) THEN
                  RAISE NOTICE \'A partition has been created %\',partition;
                  EXECUTE \'CREATE TABLE IF NOT EXISTS \' || schema || \'.\' || partition ||
                          \' (CONSTRAINT \' || partition || \'_pkey PRIMARY KEY (region_id, api, request_date, user_id, app_name, host),
                            check (request_date >= DATE \'\'\' || to_char(NEW.request_date, \'YYYY-MM-01\') || \'\'\'
                                    AND request_date < DATE \'\'\' || to_char(NEW.request_date + interval \'1 month\', \'YYYY-MM-01\') || \'\'\') ) \' ||
                          \'INHERITS (\' || schema || \'.requests_calls);\';
                END IF;
                EXECUTE \'INSERT INTO \' || schema || \'.\' || partition || \' SELECT(\' || schema || \'.requests_calls\' || \' \' || quote_literal(NEW) || \').*;\';
                RETURN NULL;
              END;
              $$
            LANGUAGE plpgsql;
        ');
    }

    /**
     * @param Schema $schema
     */
    public function down(Schema $schema)
    {
        // Update Primary key on requests_calls table (host added)
        $this->addSql('
          CREATE UNIQUE INDEX remove_host_id_temp_idx ON stat_compiled.requests_calls (region_id, api, request_date, user_id, app_name);
        ');
        $this->addSql('
          ALTER TABLE stat_compiled.requests_calls DROP CONSTRAINT requests_calls_pkey, ADD CONSTRAINT requests_calls_pkey PRIMARY KEY USING INDEX remove_host_id_temp_idx;
        ');

        // New version of the partition auto-creation for requests_calls
        $this->addSql('
          CREATE OR REPLACE FUNCTION requests_calls_insert_trigger()
            RETURNS trigger AS $$
              DECLARE
                schema VARCHAR(100);
                partition VARCHAR(100);
              BEGIN
                schema := \'stat_compiled\';
                partition := \'requests_calls\' || \'_\' || to_char(NEW.request_date, \'"y"YYYY"m"MM\');
                IF NOT EXISTS(SELECT 1 FROM pg_tables WHERE tablename=partition and schemaname=schema) THEN
                  RAISE NOTICE \'A partition has been created %\',partition;
                  EXECUTE \'CREATE TABLE IF NOT EXISTS \' || schema || \'.\' || partition ||
                          \' (CONSTRAINT \' || partition || \'_pkey PRIMARY KEY (region_id, api, request_date, user_id, app_name),
                            check (request_date >= DATE \'\'\' || to_char(NEW.request_date, \'YYYY-MM-01\') || \'\'\'
                                    AND request_date < DATE \'\'\' || to_char(NEW.request_date + interval \'1 month\', \'YYYY-MM-01\') || \'\'\') ) \' ||
                          \'INHERITS (\' || schema || \'.requests_calls);\';
                END IF;
                EXECUTE \'INSERT INTO \' || schema || \'.\' || partition || \' SELECT(\' || schema || \'.requests_calls\' || \' \' || quote_literal(NEW) || \').*;\';
                RETURN NULL;
              END;
              $$
            LANGUAGE plpgsql;
        ');
    }
}
