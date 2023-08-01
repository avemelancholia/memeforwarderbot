def get_sql_query(name, vars):
    table = vars["table"]
    sql_create_memes_table = f"""
    CREATE TABLE IF NOT EXISTS {table}(
        id INTEGER PRIMARY KEY,
        date TEXT,
        hash TEXT
        )
    """
    sql_select_hashed = f"SELECT * from {table} WHERE {table}.hash = "
    sql_get_ids_by_id = f"SELECT id from {table} ORDER BY id DESC"
    sql_get_ids_by_ts = f"SELECT id from {table} ORDER BY date DESC"
    sql_insert = f"INSERT INTO {table} VALUES (?, ?, ?)"
    sql_delete_ids = f"DELETE from {table} WHERE id = ?"

    queries = {
        "create_memes": sql_create_memes_table,
        "select_hashed": sql_select_hashed,
        "get_ids_by_ids": sql_get_ids_by_id,
        "get_ids_by_timestamp": sql_get_ids_by_ts,
        "delete_ids": sql_delete_ids,
        "insert": sql_insert,
    }
    return queries[name]
