import sqlite3

def view_database(db_path):
    # Connect to the SQLite database
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Fetch and print all table names
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()
        
        print("Tables in the database:")
        for table in tables:
            print(f"- {table[0]}")
        
        # Loop through tables and print their contents
        for table in tables:
            print(f"\nContents of table {table[0]}:")
            cursor.execute(f"SELECT * FROM {table[0]};")
            rows = cursor.fetchall()

            for row in rows:
                print(row)
        
    except sqlite3.Error as e:
        print(f"Error connecting to database: {e}")
    finally:
        if conn:
            conn.close()

# Path to your database
db_path = r'c:\Users\dionh\Downloads\sportspredictorv2\nba_betting.db'
view_database(db_path)