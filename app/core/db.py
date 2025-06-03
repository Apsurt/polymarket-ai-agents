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

            result = None
            if fetch_one:
                result = cur.fetchone()
            elif fetch_all:
                result = cur.fetchall()

            if commit:
                conn.commit()
                # If a fetch operation was performed, return its result.
                # Otherwise (commit only), return rowcount as an indicator of affected rows.
                if fetch_one or fetch_all:
                    return result
                return cur.rowcount

            # If not committing, but a fetch operation was performed, return its result.
            return result
