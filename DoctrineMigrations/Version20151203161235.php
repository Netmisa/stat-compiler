<?php

namespace DoctrineMigrations;

use Doctrine\DBAL\Migrations\AbstractMigration;
use Doctrine\DBAL\Schema\Schema;

/**
 * Auto-generated Migration: Please modify to your needs!
 */
class Version20151203161235 extends AbstractMigration
{
    /**
     * @param Schema $schema
     */
    public function up(Schema $schema)
    {
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
                          \' (check (request_date >= DATE \'\'\' || to_char(NEW.request_date, \'YYYY-MM-01\') || \'\'\'
                              AND request_date < DATE \'\'\' || to_char(NEW.request_date + interval \'1 month\', \'YYYY-MM-01\') || \'\'\') ) \' ||
                          \'INHERITS (\' || schema || \'.requests_calls);\';
                  EXECUTE \'CREATE INDEX \' || partition || \'_region_id_api_request_date_idx ON \' || schema || \'.\' || partition || \' (region_id, api, request_date);\';
                  EXECUTE \'CREATE INDEX \' || partition || \'_user_id_request_date_idx ON \' || schema || \'.\' || partition || \' (user_id, request_date);\';
                  EXECUTE \'CREATE INDEX \' || partition || \'_end_point_id_request_date_idx ON \' || schema || \'.\' || partition || \' (end_point_id, request_date);\';
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
}
