<?php

namespace DoctrineMigrations;

use Doctrine\DBAL\Migrations\AbstractMigration;
use Doctrine\DBAL\Schema\Schema;

/**
 * Auto-generated Migration: Please modify to your needs!
 */
class Version20160104151400 extends AbstractMigration
{
    /**
     * @param Schema $schema
     */
    public function up(Schema $schema)
    {
        $this->addSql("
            CREATE TABLE stat_compiled.token_stats
            (
                id integer NOT NULL,
                token text,
                request_date date NOT NULL,
                nb_req bigint,
                CONSTRAINT token_stats_pkey PRIMARY KEY (id)
            );
        ");

        $this->addSql("
            CREATE SEQUENCE stat_compiled.token_id_seq
                INCREMENT 1
                MINVALUE 1;
        ");

        $this->addSql("
            ALTER TABLE stat_compiled.token_stats
                ALTER COLUMN id SET DEFAULT nextval('stat_compiled.token_id_seq'::regclass);
        ");

        $this->addSql('
            CREATE OR REPLACE FUNCTION token_stats_insert_trigger()
            RETURNS TRIGGER AS $$
            DECLARE
                schema VARCHAR(100);
                partition VARCHAR(100);
            BEGIN
                schema := \'stat_compiled\';
                partition := \'token_stats\' || \'_\' || to_char(NEW.request_date, \'"y"YYYY"m"MM\');
                IF NOT EXISTS(SELECT 1 FROM pg_tables WHERE tablename=partition and schemaname=schema) THEN
                RAISE NOTICE \'A partition has been created %\',partition;
                EXECUTE \'CREATE TABLE IF NOT EXISTS \' || schema || \'.\' || partition ||
                        \' (CONSTRAINT \' || partition || \'_pkey PRIMARY KEY (id),
                          check (request_date >= DATE \'\'\' || to_char(NEW.request_date, \'YYYY-MM-01\') || \'\'\'
                                  AND request_date < DATE \'\'\' || to_char(NEW.request_date + interval \'1 month\', \'YYYY-MM-01\') || \'\'\') ) \' ||
                        \'INHERITS (\' || schema || \'.token_stats);\';
                END IF;
                EXECUTE \'INSERT INTO \' || schema || \'.\' || partition || \' SELECT(\' || schema || \'.token_stats\' || \' \' || quote_literal(NEW) || \').*;\';
                RETURN NULL;
            END;
            $$
            LANGUAGE plpgsql;
        ');

        $this->addSql('
            CREATE TRIGGER insert_token_stats_trigger
                BEFORE INSERT ON stat_compiled.token_stats
                FOR EACH ROW EXECUTE PROCEDURE token_stats_insert_trigger();
        ');
    }

    /**
     * @param Schema $schema
     */
    public function down(Schema $schema)
    {
        $this->addSql('DROP TABLE IF EXISTS stat_compiled.token_stats CASCADE;');
        $this->addSql('DROP FUNCTION IF EXISTS token_stats_insert_trigger();');
    }
}
