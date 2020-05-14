CREATE EXTENSION cube;

/*********************************************************************************
 * Bert Title
 *
 * FIXME: This should be deleted once the generic is confirmed working
 */
CREATE TABLE articles_title_bert(
    id_articles BIGINT PRIMARY KEY,
    embedding cube NOT NULL
    --FOREIGN KEY (id_articles) REFERENCES articles(id_articles)
);
CREATE INDEX index_articles_title_bert_embedding on articles_title_bert using GIST (embedding);

SELECT x.*,title FROM (
    SELECT id_articles, embedding <-> get_embedding(209000006) AS distance
    FROM articles_title_bert 
) AS x
INNER JOIN articles on x.id_articles=articles.id_articles
ORDER BY distance ASC LIMIT 10;

CREATE OR REPLACE FUNCTION get_embedding(id integer) RETURNS cube AS
$$
DECLARE
    e cube;
BEGIN
    select embedding into e from articles_title_bert where articles_title_bert.id_articles=id;
    return cube(e);
END;
$$
LANGUAGE 'plpgsql' IMMUTABLE;

/*********************************************************************************
 * Bert Generic
 */
CREATE SCHEMA embeddings;
CREATE TABLE embeddings.sources (
    id_sources SMALLINT PRIMARY KEY,
    table_schema NAME,
    table_name NAME,
    column_name NAME

    -- NOTE:
    -- Conceptually, this table should satisfy the following foreign key constraint.
    -- But because information_schema is a view, postgres cannot create the constraint.
    -- FOREGIN KEY (table_schema,table_name,column_name) REFERENCES information_schema.columns(table_schema,table_name,column_name)
);
INSERT INTO embeddings.sources VALUES (0,'public','articles','title');
INSERT INTO embeddings.sources VALUES (1,'public','articles','text');
INSERT INTO embeddings.sources VALUES (2,'twitter','tweets','text');

CREATE TABLE embeddings.bert (
    embedding cube NOT NULL,
    id BIGINT,
    id_sources SMALLINT REFERENCES embeddings.sources(id_sources),
    PRIMARY KEY (id_sources,id)

    -- NOTE:
    -- Conceptually, every id must be the primary key in the table specified by id_sources.
    -- Postgres has no functionality to enforce such constraints.
    -- Table inheritance seems like it might work,
    -- but I haven't been able to figure out how.
);

-- FIXME:
-- Ideally, the indexes over the where clauses could be replaced by a single multicolumn GIST index,
-- but I can't find any details on how multicolumn GIST indexes work.
CREATE INDEX bert_index ON embeddings.bert USING GIST (embedding);
CREATE INDEX bert_index0 ON embeddings.bert USING GIST (embedding) WHERE id_sources=0;
CREATE INDEX bert_index1 ON embeddings.bert USING GIST (embedding) WHERE id_sources=1;
CREATE INDEX bert_index2 ON embeddings.bert USING GIST (embedding) WHERE id_sources=2;

