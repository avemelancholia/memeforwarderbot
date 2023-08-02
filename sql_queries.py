def get_sql_query(name, vars):
    table = vars["table"]
    sql_create_memes_table = f"""
    CREATE TABLE IF NOT EXISTS {table}_meme(
        id INTEGER PRIMARY KEY,
        date INTEGER,
        hash TEXT
        )
    """
    sql_create_user_table = f"""CREATE TABLE IF NOT EXISTS {table}_user(
        id INTEGER PRIMARY KEY,
        date INTEGER,
        abuse INTEGER
        )
    """
    sql_select_hashed = f"SELECT * from {table}_meme WHERE {table}_meme.hash = "
    sql_select_user = f"SELECT * from {table}_user WHERE {table}_user.id = "
    sql_get_ids_by_id = f"SELECT id from {table}_meme ORDER BY id DESC"
    sql_get_ids_by_ts = f"SELECT id from {table}_meme ORDER BY date DESC"
    sql_insert_meme = f"INSERT INTO {table}_meme VALUES (?, ?, ?)"
    sql_insert_user = f"INSERT INTO {table}_user VALUES (?, ?, ?)"
    sql_delete_ids = f"DELETE from {table}_meme WHERE id = ?"
    sql_delete_users = f"DELETE from {table}_user WHERE id = ?"

    queries = {
        "create_memes": sql_create_memes_table,
        "create_users": sql_create_user_table,
        "select_hashed": sql_select_hashed,
        "select_user": sql_select_user,
        "get_ids_by_ids": sql_get_ids_by_id,
        "get_ids_by_timestamp": sql_get_ids_by_ts,
        "delete_ids": sql_delete_ids,
        "delete_users": sql_delete_users,
        "insert_meme": sql_insert_meme,
        "insert_user": sql_insert_user,
    }
    return queries[name]
