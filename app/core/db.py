import psycopg2
from psycopg2.extras import Json
from .config import settings
from contextlib import contextmanager

@contextmanager
def get_db_connection():
    conn = None
    try:
        conn = psycopg2.connect(settings.DATABASE_URL)
        yield conn
    except Exception as e:
        print(f"Database connection error: {e}")
        raise
    finally:
        if conn:
            conn.close()

def execute_query(query, params=None, fetch_one=False, fetch_all=False, commit=False):
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, params)
            if commit:
                conn.commit()
                return cur.rowcount # Or some other indicator of success
            if fetch_one:
                return cur.fetchone()
            if fetch_all:
                return cur.fetchall()
            return None # For queries like INSERT without RETURNING if commit is False
