from datasette import hookimpl
import sqlite_xsv

@hookimpl
def prepare_connection(conn):
    conn.enable_load_extension(True)
    sqlite_xsv.load(conn)
    conn.enable_load_extension(False)