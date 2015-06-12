<?php

namespace DoctrineMigrations;

use Doctrine\DBAL\Migrations\AbstractMigration;
use Doctrine\DBAL\Schema\Schema;

/**
 * Creation of the journey_modes table and associated trigger for partitions
 */
class Version20150609161952 extends AbstractMigration
{
    /**
     * @param Schema $schema
     */
    public function up(Schema $schema)
    {
        $this->addSql('
            CREATE TABLE stat_compiled.journey_modes
            (
              journey_id bigint NOT NULL,
              type text NOT NULL,
              mode text NOT NULL,
              commercial_mode_id text NOT NULL,
              commercial_mode_name text,
              request_date timestamp without time zone,
              CONSTRAINT journey_modes_pkey PRIMARY KEY (journey_id, type, mode, commercial_mode_id, commercial_mode_name)
            )
            WITH (
              OIDS=FALSE
            );
        ');

        $this->addSql('
            CREATE OR REPLACE FUNCTION journey_modes_insert_trigger()
            RETURNS TRIGGER AS $$
            DECLARE
              schema VARCHAR(100);
              partition VARCHAR(100);
            BEGIN
              schema := \'stat_compiled\';
              partition := \'journey_modes\' || \'_\' || to_char(NEW.request_date, \'"y"YYYY"m"MM\');
              IF NOT EXISTS(SELECT 1 FROM pg_tables WHERE tablename=partition and schemaname=schema) THEN
                RAISE NOTICE \'A partition has been created %\',partition;
                EXECUTE \'CREATE TABLE IF NOT EXISTS \' || schema || \'.\' || partition || 
                        \' (CONSTRAINT \' || partition || \'_pkey PRIMARY KEY (journey_id, type, mode, commercial_mode_id, commercial_mode_name),
                          check (request_date >= DATE \'\'\' || to_char(NEW.request_date, \'YYYY-MM-01\') || \'\'\' 
                                  AND request_date < DATE \'\'\' || to_char(NEW.request_date + interval \'1 month\', \'YYYY-MM-01\') || \'\'\') ) \' || 
                        \'INHERITS (\' || schema || \'.journey_modes);\';
                EXECUTE \'CREATE INDEX \' || partition || \'_mode_idx ON \' || schema || \'.\' || partition || \' (mode);\';
                EXECUTE \'CREATE INDEX \' || partition || \'_type_idx ON \' || schema || \'.\' || partition || \' (type);\';
                EXECUTE \'CREATE INDEX \' || partition || \'_commercial_mode_id_idx ON \' || schema || \'.\' || partition || \' (commercial_mode_id);\';
              END IF;
              EXECUTE \'INSERT INTO \' || schema || \'.\' || partition || \' SELECT(\' || schema || \'.journey_modes\' || \' \' || quote_literal(NEW) || \').*;\';
              RETURN NULL;
            END;
            $$
            LANGUAGE plpgsql;
        ');

        $this->addSql('
            CREATE TRIGGER insert_journey_modes_trigger
                BEFORE INSERT ON stat_compiled.journey_modes
                FOR EACH ROW EXECUTE PROCEDURE journey_modes_insert_trigger();
        ');
    }

    /**
     * @param Schema $schema
     */
    public function down(Schema $schema)
    {
        $this->addSql('DROP TABLE IF EXISTS stat_compiled.journey_modes CASCADE;');
        $this->addSql('DROP FUNCTION IF EXISTS journey_modes_insert_trigger();');
    }
}
