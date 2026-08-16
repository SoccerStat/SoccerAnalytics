CREATE OR REPLACE FUNCTION resolve_entity(
    table_name TEXT,
    name_col TEXT,
    query_text TEXT,
    query_embedding vector(384)
)
RETURNS TABLE(id INT, nom TEXT, score FLOAT, source TEXT) AS $$
BEGIN
    RETURN QUERY EXECUTE format(
        'WITH trgm_matches AS (
            SELECT id, %I AS nom, similarity(%I, $1) AS score, ''trgm'' AS source
            FROM %I
            WHERE similarity(%I, $1) > 0.25
            ORDER BY score DESC
            LIMIT 5
        ),
        vector_matches AS (
            SELECT id, %I AS nom, 1 - (embedding <=> $2) AS score, ''vector'' AS source
            FROM %I
            WHERE embedding IS NOT NULL
            ORDER BY embedding <=> $2
            LIMIT 5
        )
        SELECT * FROM trgm_matches
        UNION ALL
        SELECT * FROM vector_matches
        ORDER BY score DESC',
        name_col, name_col, table_name, name_col,
        name_col, table_name
    ) USING query_text, query_embedding;
END;
$$ LANGUAGE plpgsql;