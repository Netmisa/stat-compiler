<?php

namespace DoctrineMigrations;

use Doctrine\DBAL\Migrations\AbstractMigration;
use Doctrine\DBAL\Schema\Schema;

/**
 * Auto-generated Migration: Please modify to your needs!
 */
class Version20151016185153 extends AbstractMigration
{
    /**
     * @param Schema $schema
     */
    public function up(Schema $schema)
    {
        $this->addSql('
            ALTER TABLE stat_compiled.requests_calls ADD COLUMN object_count integer;
        ');

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
                          \' (CONSTRAINT \' || partition || \'_pkey PRIMARY KEY (region_id, api, request_date, user_id, app_name, host, object_count),
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
        $this->addSql('ALTER TABLE stat_compiled.requests_calls DROP COLUMN object_count;');

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
