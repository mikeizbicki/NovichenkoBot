/*
 * This file creates a tablespace for fast data accesses 
 * and moves appropriate tables/indexes into the tablespace.
 * It should only be called to create the production environment
 * and not the dev/test environments.
 */

CREATE TABLESPACE fastdata LOCATION '/data-fast/postgresql';

/*
 * All of the following tables are created by postgres for managing the database.
 * These tables are tiny, and it's quite obnoxious to have a command stall reading these tables,
 * so we move them to the fast tablespace.
 *
 * These commands were generated with the command
 *
 *   SELECT ' ALTER TABLE '||schemaname||'.'||tablename||' SET TABLESPACE fastdata;'
 *   FROM pg_tables
 *   WHERE schemaname IN ('pg_catalog', 'information_schema') order by schemaname,tablename;
 *
 * FIXME: move pg_catalog as well
 */
ALTER TABLE information_schema.sql_features SET TABLESPACE fastdata;
ALTER TABLE information_schema.sql_implementation_info SET TABLESPACE fastdata;
ALTER TABLE information_schema.sql_languages SET TABLESPACE fastdata;
ALTER TABLE information_schema.sql_packages SET TABLESPACE fastdata;
ALTER TABLE information_schema.sql_parts SET TABLESPACE fastdata;
ALTER TABLE information_schema.sql_sizing SET TABLESPACE fastdata;
ALTER TABLE information_schema.sql_sizing_profiles SET TABLESPACE fastdata;


/*
 * primary key indices
 */
ALTER INDEX urls_pkey SET TABLESPACE fastdata;
ALTER INDEX urls_scheme_hostname_port_path_params_query_fragment_other_key SET TABLESPACE fastdata;
ALTER INDEX articles_pkey SET TABLESPACE fastdata;
ALTER INDEX frontier_pkey SET TABLESPACE fastdata;
ALTER INDEX responses_pkey SET TABLESPACE fastdata;
ALTER INDEX sentences_pkey SET TABLESPACE fastdata;
ALTER INDEX labels_pkey SET TABLESPACE fastdata;

/*
 * other commonly used indices
 */
ALTER INDEX urls_index_hostname_path SET TABLESPACE fastdata;
ALTER INDEX frontier_index_urls SET TABLESPACE fastdata;
ALTER INDEX frontier_index_hostnamereversed SET TABLESPACE fastdata;
--ALTER INDEX frontier_index_timestamp_received SET TABLESPACE fastdata;
ALTER INDEX frontier_index_nextrequest2 SET TABLESPACE fastdata;
--ALTER INDEX frontier_index_nextrequest_alt SET TABLESPACE fastdata;
ALTER INDEX responses_index_timestamp_received SET TABLESPACE fastdata;
-- FIXME: ALTER INDEX responses_index_hostnametwistedhttp SET TABLESPACE fastdata;
ALTER INDEX responses_index_frontier SET TABLESPACE fastdata;
ALTER INDEX responses_index_timestamp_processed SET TABLESPACE fastdata;
ALTER INDEX articles_index_hostname_time SET TABLESPACE fastdata;
ALTER INDEX articles_index_urls SET TABLESPACE fastdata;
-- FIXME: ALTER INDEX articles_index_hostname_tsvtitle_en SET TABLESPACE fastdata;
-- FIXME: ALTER INDEX articles_title_tsv SET TABLESPACE fastdata;
-- FIXME: ALTER INDEX articles_text_tsv SET TABLESPACE fastdata;
ALTER INDEX sentences_ids_tsv SET TABLESPACE fastdata;

ALTER INDEX articles_summary2_pkey SET TABLESPACE fastdata;
ALTER INDEX frontier_hostname_pkey SET TABLESPACE fastdata;
ALTER INDEX responses_timestamp_hostname_pkey SET TABLESPACE fastdata;
ALTER INDEX requests_hostname_pkey SET TABLESPACE fastdata;
ALTER INDEX articles_lang_pkey SET TABLESPACE fastdata;
ALTER INDEX rollups_pkey SET TABLESPACE fastdata;
ALTER INDEX requests_pkey SET TABLESPACE fastdata;
ALTER INDEX keywords_pkey SET TABLESPACE fastdata;
ALTER INDEX requests_id_frontier_key SET TABLESPACE fastdata;
ALTER INDEX responses_summary_pkey SET TABLESPACE fastdata;
ALTER INDEX crawlable_hostnames_pkey SET TABLESPACE fastdata;

/*
 * important tables
 * FIXME: this is a test
 */

ALTER TABLE requests SET TABLESPACE fastdata;

/*
 * rollup tables
 */
ALTER TABLE responses_summary SET TABLESPACE fastdata;
ALTER TABLE responses_timestamp_hostname SET TABLESPACE fastdata;
ALTER TABLE articles_lang SET TABLESPACE fastdata;
--ALTER TABLE articles_summary2 SET TABLESPACE fastdata;
--ALTER TABLE frontier_hostnameSET TABLESPACE fastdata;
--ALTER TABLE requests_hostnameSET TABLESPACE fastdata;

