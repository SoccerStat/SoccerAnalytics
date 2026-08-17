drop function if exists analytics.resolve_entity;

CREATE OR REPLACE FUNCTION analytics.resolve_entity(
    schema_name TEXT,
    table_name TEXT,
    name_col TEXT,
    query_text TEXT,
    query_embedding vector(384)
)
RETURNS TABLE(id VARCHAR, name VARCHAR, score FLOAT, trgm_score FLOAT, vector_score FLOAT, lev_score FLOAT) AS $$
BEGIN
    --TODO: check quality: 'schema_name.table_name' must exist
    RETURN QUERY EXECUTE format(
        'WITH trgm_matches AS (
            SELECT id, %I AS name, similarity(%I, $1) AS trgm_score
            FROM %I.%I
            WHERE similarity(%I, $1) > 0.25
            ORDER BY trgm_score DESC
            LIMIT 10
        ),
        vector_matches AS (
            SELECT id, %I AS name, 1 - (embedding <=> $2) AS vector_score
            FROM %I.%I
            WHERE embedding IS NOT NULL
            ORDER BY embedding <=> $2
            LIMIT 10
        ),
        candidates AS (
            SELECT t.id, t.name, t.trgm_score, v.vector_score
            FROM trgm_matches t
            FULL OUTER JOIN vector_matches v
            ON t.id = v.id
        ),
        scored AS (
            SELECT
                c.id,
                c.name,
                COALESCE(c.trgm_score, 0) AS trgm_score,
                COALESCE(c.vector_score, 0) AS vector_score,
                GREATEST(0, 1.0 - (levenshtein(lower(c.name), lower($1))::float / GREATEST(length(c.name), length($1))))
                    AS lev_score
            FROM candidates c
            JOIN %I.%I t ON t.id = c.id
        )
        SELECT
            id,
            name,
            (0.35 * trgm_score) + (0.4 * vector_score) + (0.25 * lev_score)::FLOAT AS score,
            trgm_score::FLOAT,
            vector_score::FLOAT,
            lev_score::FLOAT
        FROM scored
        ORDER BY score DESC',
        name_col, name_col, schema_name, table_name, name_col,
        name_col, schema_name, table_name,
        schema_name, table_name
    ) USING query_text, query_embedding;
END;
$$ LANGUAGE plpgsql;