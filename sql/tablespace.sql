/*
 * This file creates a tablespace for fast data accesses 
 * and moves appropriate tables/indexes into the tablespace.
 * It should only be called to create the production environment
 * and not the dev/test environments.
 */

CREATE TABLESPACE fastdata LOCATION '/data-fast/postgresql';

ALTER INDEX urls_pkey SET TABLESPACE fastdata;
ALTER INDEX urls_scheme_hostname_port_path_params_query_fragment_other_key SET TABLESPACE fastdata;
ALTER INDEX articles_pkey SET TABLESPACE fastdata;
ALTER INDEX frontier_pkey SET TABLESPACE fastdata;
ALTER INDEX responses_pkey SET TABLESPACE fastdata;
ALTER INDEX sentences_pkey SET TABLESPACE fastdata;
ALTER INDEX labels_pkey SET TABLESPACE fastdata;

ALTER INDEX urls_index_hostname_path SET TABLESPACE fastdata;
ALTER INDEX frontier_index_urls SET TABLESPACE fastdata;
ALTER INDEX frontier_index_nextrequest SET TABLESPACE fastdata;
ALTER INDEX responses_index_timestamp_received SET TABLESPACE fastdata;
ALTER INDEX responses_index_hostnametwistedhttp SET TABLESPACE fastdata;
ALTER INDEX articles_index_hostname_time SET TABLESPACE fastdata;
ALTER INDEX responses_index_frontier SET TABLESPACE fastdata;
ALTER INDEX articles_index_urls SET TABLESPACE fastdata;
ALTER INDEX frontier_index_timestamp_received SET TABLESPACE fastdata;
ALTER INDEX responses_index_timestamp_processed SET TABLESPACE fastdata;
ALTER INDEX sentences_ids_tsv SET TABLESPACE fastdata;
