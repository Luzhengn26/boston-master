"""Wrapper for datalib Database class."""
from utils import setup
from datalib.database import Database, prepare_sql_query
from sqlalchemy import text


def create_engine(database):
    """Creates the engine for a database."""
    db = Database(database, setup.config_file, setup.vault_section)
    db.create_engine()
    return db


def result_to_dict(result):
    """Convert a result from sqlalchemy to a list of dicts"""
    # converting to list of dicts
    return [
        {column: value for column, value in rowproxy.items()} for rowproxy in result
    ]


def df_from_sql(database, query, *args, **kwargs):
    """Wrap database selection and engine creation for df_to_sql function."""
    db = create_engine(database)
    query = str(query)
    with db.engine.begin() as conn:
        return db.df_from_sql(query, connection=conn, *args, **kwargs)


def df_to_db(
    database,
    df,
    table_name,
    is_redshift="infer",
    schema=None,
    insert_method="append",
    chunksize=None,
    method="multi",
):
    """Wrap df_to_db function to create engine and write to database table."""
    db = create_engine(database)
    if is_redshift == "infer":
        is_redshift = database.startswith("redshift")
    if is_redshift:
        df.to_sql(
            table_name,
            con=db.engine.engine,
            index=False,
            if_exists=insert_method,
            schema=schema,
            chunksize=chunksize,
            method=method,
        )
    else:
        with db.engine.begin() as conn:
            db.df_to_db(df, table_name=table_name, connection=conn)


def run_raw_sql(database, query, return_as_dict=False, *args, **kwargs):
    """Run a sql command as is."""
    db = create_engine(database)
    query = str(query)
    query = prepare_sql_query(query)
    query = query.format(*args, **kwargs)
    query = text(query)
    conn = db.engine.connect()
    with conn.begin() as trans:
        try:
            result = conn.execute(query)
            trans.commit()
        except Exception:
            trans.rollback()
            raise
    conn.close()
    # if the query we ran returned rows convert them to a list of dicts and return
    # otherwise we don't return anything, as there's no result
    if result.returns_rows and return_as_dict:
        return result_to_dict(result)
    else:
        return result
