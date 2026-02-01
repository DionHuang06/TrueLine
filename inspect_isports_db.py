import sqlite3
import os

db_path = 'ingestion/isportsapi.db' 
# Step 11 showed 'ingestion/isportsapi.db' sizeBytes 11059200
# Wait, Step 11 output:
# {"name":"ingestion","isDir":true,"numChildren":7}
# {"name":"isportsapi.db","sizeBytes":"11059200"} 
# It was in the ROOT of 'ingestion' subfolder? No, look at indentation.
# list_dir output is flat json lines.
# "ingestion" is a dir.
# "isportsapi.db" followed "inspect_api_data.py".
# It seems "isportsapi.db" is inside "ingestion" folder based on context of previous file listing tools often listing children.
# But list_dir output format is: "relative path to the directory".
# Wait. Step 11 output:
# {"name":"ingestion","isDir":true,"numChildren":7}
# {"name":"inspect_api_data.py","sizeBytes":"605"}
# {"name":"isportsapi.db","sizeBytes":"11059200"}
# These follow 'ingestion' but the tool description says "For each child in the directory".
# Step 11 was `list_dir` of ROOT.
# So `isportsapi.db` is in `ingestion/isportsapi.db`.
# Let's verify.

def check_isports_db():
    path = "ingestion/isportsapi.db"
    if not os.path.exists(path):
        print(f"File not found at {path}")
        # try root just in case
        path = "isportsapi.db" 
        if not os.path.exists(path):
             print(f"File not found at {path} either.")
             return

    print(f"Inspecting {path}...")
    try:
        conn = sqlite3.connect(path)
        cursor = conn.cursor()
        
        # List Tables
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = cursor.fetchall()
        print("Tables found:", [t[0] for t in tables])
        
        for table_name in tables:
            t = table_name[0]
            print(f"\n--- Table: {t} ---")
            cursor.execute(f"PRAGMA table_info({t})")
            columns = cursor.fetchall()
            print("Columns:", [c[1] for c in columns])
            
            cursor.execute(f"SELECT COUNT(*) FROM {t}")
            count = cursor.fetchone()[0]
            print(f"Row Count: {count}")
            
            # Sample 1
            cursor.execute(f"SELECT * FROM {t} LIMIT 1")
            print("Sample Row:", cursor.fetchone())
            
        conn.close()
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    check_isports_db()
