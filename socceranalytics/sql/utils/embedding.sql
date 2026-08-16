drop function if exists analytics.resolve_entity;

CREATE OR REPLACE FUNCTION analytics.resolve_entity(
    schema_name TEXT,
    table_name TEXT,
    name_col TEXT,
    query_text TEXT,
    query_embedding vector(384)
)
RETURNS TABLE(id varchar, name varchar, score FLOAT, source varchar) AS $$
BEGIN
    RETURN QUERY EXECUTE format(
        'WITH trgm_matches AS (
            SELECT id, %I AS name, similarity(%I, $1) AS score, ''trgm'' AS source
            FROM %I.%I
            WHERE similarity(%I, $1) > 0.25
            ORDER BY score DESC
            LIMIT 5
        ),
        vector_matches AS (
            SELECT id, %I AS name, 1 - (embedding <=> $2) AS score, ''vector'' AS source
            FROM %I.%I
            WHERE embedding IS NOT NULL
            ORDER BY embedding <=> $2
            LIMIT 5
        )
        SELECT * FROM trgm_matches
        UNION ALL
        SELECT * FROM vector_matches
        ORDER BY score DESC',
        name_col, name_col, schema_name, table_name, name_col,
        name_col, schema_name, table_name
    ) USING query_text, query_embedding;
END;
$$ LANGUAGE plpgsql;