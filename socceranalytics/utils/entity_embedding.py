from socceranalytics.postgres.postgres_querying import PostgresQuerying


def embed_table(db: PostgresQuerying, schema, table, name_col, id_col="id"):
    cursor = db.execute_query(query=f"SELECT {id_col}, {name_col} FROM {schema}.{table} WHERE embedding IS NULL", return_cursor=True)
    if not cursor:
        print(f"{table}: Nothing to embed")
        return

    rows = cursor.fetchall()
    cursor.close()

    if not rows:
        print(f"{table}: Nothing to embed")
        return

    ids, names = zip(*rows)
    embeddings = list(model.embed(list(names)))

    for row_id, emb in zip(ids, embeddings):
        db.execute_query(
            query=f"UPDATE {schema}.{table} SET embedding = %s WHERE {id_col} = %s",
            params=(emb.tolist(), row_id),
        )

    print(f"{table}: {len(rows)} embeddings completed")