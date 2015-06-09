<?php

namespace DoctrineMigrations;

use Doctrine\DBAL\Migrations\AbstractMigration;
use Doctrine\DBAL\Schema\Schema;

/**
 * Creation of the journey_networks table and associated trigger for partitions
 */
class Version20150609162156 extends AbstractMigration
{
    /**
     * @param Schema $schema
     */
    public function up(Schema $schema)
    {
        $this->addSql('
            CREATE TABLE stat_compiled.journey_networks
            (
              journey_id bigint NOT NULL,
              network_id text NOT NULL,
              network_name text,
              rank int,
              request_date timestamp without time zone,
              is_start_network boolean,
              is_end_network boolean,
              CONSTRAINT journey_networks_pkey PRIMARY KEY (journey_id, network_id, rank)
            )
            WITH (
              OIDS=FALSE
            );
        ');

        $this->addSql('
            CREATE OR REPLACE FUNCTION journey_networks_insert_trigger()
            RETURNS TRIGGER AS $$
            DECLARE
              schema VARCHAR(100);
              partition VARCHAR(100);
            BEGIN
              schema := \'stat_compiled\';
              partition := \'journey_networks\' || \'_\' || to_char(NEW.request_date, \'"y"YYYY"m"MM\');
              IF NOT EXISTS(SELECT 1 FROM pg_tables WHERE tablename=partition and schemaname=schema) THEN
                RAISE NOTICE \'A partition has been created %\',partition;
                EXECUTE \'CREATE TABLE IF NOT EXISTS \' || schema || \'.\' || partition || 
                        \' ( CONSTRAINT \' || partition || \'_pkey PRIMARY KEY (journey_id, network_id, rank), \' ||
                          \'check (request_date >= DATE \'\'\' || to_char(NEW.request_date, \'YYYY-MM-01\') || \'\'\' 
                                  AND request_date < DATE \'\'\' || to_char(NEW.request_date + interval \'1 month\', \'YYYY-MM-01\') || \'\'\') ) \' || 
                        \'INHERITS (\' || schema || \'.journey_networks);\';
                EXECUTE \'CREATE INDEX \' || partition || \'_journey_start_idx ON \' || schema || \'.\' || partition || \' (journey_id, is_start_network);\';
                EXECUTE \'CREATE INDEX \' || partition || \'_journey_end_idx ON \' || schema || \'.\' || partition || \' (journey_id, is_end_network);\';
              END IF;
              EXECUTE \'INSERT INTO \' || schema || \'.\' || partition || \' SELECT(\' || schema || \'.journey_networks\' || \' \' || quote_literal(NEW) || \').*;\';
              RETURN NULL;
            END;
            $$
            LANGUAGE plpgsql;
        ');

        $this->addSql('
            CREATE TRIGGER insert_journey_networks_trigger
                BEFORE INSERT ON stat_compiled.journey_networks
                FOR EACH ROW EXECUTE PROCEDURE journey_networks_insert_trigger();
        ');
    }

    /**
     * @param Schema $schema
     */
    public function down(Schema $schema)
    {
        $this->addSql('DROP TABLE IF EXISTS stat_compiled.journey_networks CASCADE;');
        $this->addSql('DROP FUNCTION IF EXISTS journey_networks_insert_trigger();');
    }
}
