import psycopg2
import psycopg2.extras
from config import DB_URL

class SQLiteCompatibleCursor:
    def __init__(self, cursor):
        self._cursor = cursor
        self.rowcount = -1
        self.lastrowid = None 
    
    @property
    def description(self):
        """Expose cursor description for pandas compatibility"""
        return self._cursor.description

    def execute(self, query, params=None):
        # Convert SQLite ? placeholders to Postgres %s
        pg_query = query.replace('?', '%s')
        
        # Fix for "INSERT OR IGNORE" (SQLite) -> "INSERT ... ON CONFLICT DO NOTHING" (Postgres)
        has_conflict = False
        if "INSERT OR IGNORE" in pg_query.upper():
            pg_query = pg_query.replace("INSERT OR IGNORE", "INSERT")
            pg_query += " ON CONFLICT DO NOTHING"
            has_conflict = True
        
        # Fix for AUTOINCREMENT / lastrowid
        # SQLite sets cursor.lastrowid after INSERT. Postgres needs RETURNING id.
        # Only add RETURNING if it's an INSERT and doesn't already have it
        is_insert = pg_query.strip().upper().startswith("INSERT")
        needs_returning = is_insert and "RETURNING" not in pg_query.upper() and not has_conflict
        
        if needs_returning:
             pg_query += " RETURNING id"
            
        try:
            self._cursor.execute(pg_query, params)
            self.rowcount = self._cursor.rowcount
            
            if needs_returning:
                 # Postgres returns the ID if we asked for it
                 try:
                     res = self._cursor.fetchone()
                     if res:
                         self.lastrowid = res[0]
                 except psycopg2.ProgrammingError:
                     # No results returned
                     pass
        except Exception as e:
            print(f"Postgres Error in query: {pg_query}")
            print(f"Params: {params}")
            raise e
            
    def fetchone(self):
        return self._cursor.fetchone()
        
    def fetchall(self):
        return self._cursor.fetchall()

    def close(self):
        self._cursor.close()

class PostgresConnection:
    def __init__(self, conn):
        self._conn = conn
        self.row_factory = None 
    
    def cursor(self):
        # Use DictCursor so rows behave like dictionaries (compat with sqlite3.Row)
        return SQLiteCompatibleCursor(self._conn.cursor(cursor_factory=psycopg2.extras.DictCursor))
        
    def commit(self):
        self._conn.commit()
    
    def rollback(self):
        self._conn.rollback()
        
    def close(self):
        self._conn.close()
        
    def execute(self, query, params=None):
        c = self.cursor()
        c.execute(query, params)
        return c

def get_postgres_connection():
    try:
        conn = psycopg2.connect(DB_URL)
        return PostgresConnection(conn)
    except Exception as e:
        print(f"Failed to connect to Postgres: {e}")
        raise e
