<?php

namespace DoctrineMigrations;

use Doctrine\DBAL\Migrations\AbstractMigration;
use Doctrine\DBAL\Schema\Schema;

/**
 * Auto-generated Migration: Please modify to your needs!
 */
class Version20150803105250 extends AbstractMigration
{
    /**
     * @param Schema $schema
     */
    public function up(Schema $schema)
    {
        // New version of the partition auto-creation for journey_infos
        $this->addSql('
            CREATE OR REPLACE FUNCTION journey_infos_insert_trigger()
            RETURNS TRIGGER AS $$
            DECLARE
              schema VARCHAR(100);
              partition VARCHAR(100);
            BEGIN
              schema := \'stat_compiled\';
              partition := \'journey_infos\' || \'_\' || to_char(NEW.request_date, \'"y"YYYY"m"MM\');
              IF NOT EXISTS(SELECT 1 FROM pg_tables WHERE tablename=partition and schemaname=schema) THEN
                RAISE NOTICE \'A partition has been created %\',partition;
                EXECUTE \'CREATE TABLE IF NOT EXISTS \' || schema || \'.\' || partition || 
                        \' (CONSTRAINT \' || partition || \'_pkey PRIMARY KEY (id),
                            check (request_date >= DATE \'\'\' || to_char(NEW.request_date, \'YYYY-MM-01\') || \'\'\' 
                                  AND request_date < DATE \'\'\' || to_char(NEW.request_date + interval \'1 month\', \'YYYY-MM-01\') || \'\'\') ) \' || 
                        \'INHERITS (\' || schema || \'.journey_infos);\';
                EXECUTE \'CREATE INDEX \' || partition || \'_region_id_idx ON \' || schema || \'.\' || partition || \' (region_id);\';
              END IF;
              EXECUTE \'INSERT INTO \' || schema || \'.\' || partition || \' SELECT(\' || schema || \'.journey_infos\' || \' \' || quote_literal(NEW) || \').*;\';
              RETURN NULL;
            END;
            $$
            LANGUAGE plpgsql;
        ');

        // Add index on all existing partitions of journey_infos
        $this->addSql('
            DO $$DECLARE
              journey_infos_partitions CURSOR FOR SELECT tablename FROM pg_tables WHERE tablename like \'journey_infos_%\' and schemaname=\'stat_compiled\' ORDER BY tablename;
            BEGIN
              RAISE NOTICE \'Starting ...\';
              FOR partition IN journey_infos_partitions LOOP
                RAISE NOTICE \'Partition: %s ...\', quote_ident(partition.tablename);
                EXECUTE \'CREATE INDEX \' || partition.tablename || \'_region_id_idx ON stat_compiled.\' || partition.tablename || \' (region_id);\';
              END LOOP;
            END$$;
        ');
    }

    /**
     * @param Schema $schema
     */
    public function down(Schema $schema)
    {
       // Restore previous version of the partition auto-creation for journey_infos
        $this->addSql('
            CREATE OR REPLACE FUNCTION journey_infos_insert_trigger()
            RETURNS TRIGGER AS $$
            DECLARE
              schema VARCHAR(100);
              partition VARCHAR(100);
            BEGIN
              schema := \'stat_compiled\';
              partition := \'journey_infos\' || \'_\' || to_char(NEW.request_date, \'"y"YYYY"m"MM\');
              IF NOT EXISTS(SELECT 1 FROM pg_tables WHERE tablename=partition and schemaname=schema) THEN
                RAISE NOTICE \'A partition has been created %\',partition;
                EXECUTE \'CREATE TABLE IF NOT EXISTS \' || schema || \'.\' || partition || 
                        \' (CONSTRAINT \' || partition || \'_pkey PRIMARY KEY (id),
                            check (request_date >= DATE \'\'\' || to_char(NEW.request_date, \'YYYY-MM-01\') || \'\'\' 
                                  AND request_date < DATE \'\'\' || to_char(NEW.request_date + interval \'1 month\', \'YYYY-MM-01\') || \'\'\') ) \' || 
                        \'INHERITS (\' || schema || \'.journey_infos);\';
              END IF;
              EXECUTE \'INSERT INTO \' || schema || \'.\' || partition || \' SELECT(\' || schema || \'.journey_infos\' || \' \' || quote_literal(NEW) || \').*;\';
              RETURN NULL;
            END;
            $$
            LANGUAGE plpgsql;
        ');

        // Drop indexes on all existing partitions of journey_infos
        $this->addSql('
            DO $$DECLARE
              journey_infos_partitions CURSOR FOR SELECT tablename FROM pg_tables WHERE tablename like \'journey_infos_%\' and schemaname=\'stat_compiled\' ORDER BY tablename;
            BEGIN
              RAISE NOTICE \'Starting ...\';
              FOR partition IN journey_infos_partitions LOOP
                RAISE NOTICE \'Partition: %s ...\', quote_ident(partition.tablename);
                EXECUTE \'DROP INDEX IF EXISTS stat_compiled.\' || partition.tablename || \'_region_id_idx\';
              END LOOP;
            END$$;
        ');
    }
}
