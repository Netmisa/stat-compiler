<?php

namespace DoctrineMigrations;

use Doctrine\DBAL\Migrations\AbstractMigration;
use Doctrine\DBAL\Schema\Schema;

/**
 * Grant select rights to Metrics Dashboard user on all main tables
 */
class Version20150611165539 extends AbstractMigration
{
    /**
     * @param Schema $schema
     */
    public function up(Schema $schema)
    {
        $this->addSql('GRANT USAGE ON SCHEMA stat_compiled TO usrsql_nmp_stat;');

        $this->addSql('GRANT SELECT ON stat_compiled.error_stats TO usrsql_nmp_stat;');
        $this->addSql('GRANT SELECT ON stat_compiled.journey_cities TO usrsql_nmp_stat;');
        $this->addSql('GRANT SELECT ON stat_compiled.journey_infos TO usrsql_nmp_stat;');
        $this->addSql('GRANT SELECT ON stat_compiled.journey_lines TO usrsql_nmp_stat;');
        $this->addSql('GRANT SELECT ON stat_compiled.journey_modes TO usrsql_nmp_stat;');
        $this->addSql('GRANT SELECT ON stat_compiled.journey_networks TO usrsql_nmp_stat;');
        $this->addSql('GRANT SELECT ON stat_compiled.journey_request_stats TO usrsql_nmp_stat;');
        $this->addSql('GRANT SELECT ON stat_compiled.journey_stop_areas TO usrsql_nmp_stat;');
        $this->addSql('GRANT SELECT ON stat_compiled.requests_calls TO usrsql_nmp_stat;');
        $this->addSql('GRANT SELECT ON stat_compiled.users TO usrsql_nmp_stat;');
    }

    /**
     * @param Schema $schema
     */
    public function down(Schema $schema)
    {
        $this->addSql('REVOKE SELECT ON stat_compiled.error_stats FROM usrsql_nmp_stat;');
        $this->addSql('REVOKE SELECT ON stat_compiled.journey_cities FROM usrsql_nmp_stat;');
        $this->addSql('REVOKE SELECT ON stat_compiled.journey_infos FROM usrsql_nmp_stat;');
        $this->addSql('REVOKE SELECT ON stat_compiled.journey_lines FROM usrsql_nmp_stat;');
        $this->addSql('REVOKE SELECT ON stat_compiled.journey_modes FROM usrsql_nmp_stat;');
        $this->addSql('REVOKE SELECT ON stat_compiled.journey_networks FROM usrsql_nmp_stat;');
        $this->addSql('REVOKE SELECT ON stat_compiled.journey_request_stats FROM usrsql_nmp_stat;');
        $this->addSql('REVOKE SELECT ON stat_compiled.journey_stop_areas FROM usrsql_nmp_stat;');
        $this->addSql('REVOKE SELECT ON stat_compiled.requests_calls FROM usrsql_nmp_stat;');
        $this->addSql('REVOKE SELECT ON stat_compiled.users FROM usrsql_nmp_stat;');
        
        $this->addSql('REVOKE USAGE ON SCHEMA stat_compiled FROM usrsql_nmp_stat;');
    }
}
