CREATE EXTENSION cube;

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
