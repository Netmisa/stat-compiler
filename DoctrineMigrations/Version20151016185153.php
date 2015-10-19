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
            CREATE TABLE stat_compiled.response_info
            (
                datetime date NOT NULL,
                user_id integer NOT NULL,
                object_type character varying(100) NOT NULL,
                object_count integer,
                CONSTRAINT pk_response_info PRIMARY KEY (datetime, user_id, object_type)
            )
            WITH (
                OIDS=FALSE
            );
        ');

        $this->addSql('
            CREATE OR REPLACE FUNCTION response_info_insert_trigger()
            RETURNS TRIGGER AS $$
            DECLARE
                schema VARCHAR(100);
                partition VARCHAR(100);
            BEGIN
                schema := \'stat_compiled\';
                partition := \'response_info\' || \'_\' || to_char(NEW.datetime, \'"y"YYYY"m"MM\');
                IF NOT EXISTS(SELECT 1 FROM pg_tables WHERE tablename=partition and schemaname=schema) THEN
                    RAISE NOTICE \'A partition has been created %\',partition;
                    EXECUTE \'CREATE TABLE IF NOT EXISTS \' || schema || \'.\' || partition ||
                        \' (CONSTRAINT \' || partition || \'_pkey PRIMARY KEY (datetime, user_id, object_type),
                          check (datetime >= DATE \'\'\' || to_char(NEW.datetime, \'YYYY-MM-01\') || \'\'\'
                                  AND datetime < DATE \'\'\' || to_char(NEW.datetime + interval \'1 month\', \'YYYY-MM-01\') || \'\'\') ) \' ||
                        \'INHERITS (\' || schema || \'.response_info);\';
                END IF;
                EXECUTE \'INSERT INTO \' || schema || \'.\' || partition || \' SELECT(\' || schema || \'.response_info\' || \' \' || quote_literal(NEW) || \').*;\';
                RETURN NULL;
            END;
            $$
            LANGUAGE plpgsql;
        ');

        $this->addSql('
            CREATE TRIGGER insert_response_info_trigger
                BEFORE INSERT ON stat_compiled.response_info
                FOR EACH ROW EXECUTE PROCEDURE response_info_insert_trigger();
        ');

        $this->addSql('GRANT SELECT ON stat_compiled.response_info TO usrsql_nmp_stat;');
    }

    /**
     * @param Schema $schema
     */
    public function down(Schema $schema)
    {
        $this->addSql('DROP TABLE IF EXISTS stat_compiled.response_info CASCADE;');
        $this->addSql('DROP FUNCTION IF EXISTS response_info_insert_trigger();');
    }
}
