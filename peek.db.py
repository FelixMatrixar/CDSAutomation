import sqlite3

DB_NAME = "requests.db"

def peek_database():
    print(f"--- Peeking into {DB_NAME} ---")
    
    try:
        conn = sqlite3.connect(DB_NAME)
        c = conn.cursor()

        # --- 1. Show Status Summary ---
        print("\n--- Summary by Status ---")
        c.execute("SELECT status, COUNT(*) FROM requests GROUP BY status")
        status_counts = c.fetchall()
        
        if not status_counts:
            print("Database is empty.")
            conn.close()
            return
        
        for status, count in status_counts:
            print(f"{status.title()}:\t{count}")
            
        # --- 2. Show 10 Most Recent Requests ---
        print("\n--- 10 Most Recent Requests (All Data) ---")
        
        # Get column names
        c.execute("PRAGMA table_info(requests)")
        col_names = [info[1] for info in c.fetchall()]
        print(f"Columns: {col_names}\n")

        # Fetch and print data
        c.execute("SELECT * FROM requests ORDER BY created_at DESC LIMIT 10")
        recent_requests = c.fetchall()
        
        for i, row in enumerate(recent_requests):
            print(f"--- Entry {i+1} ---")
            for col, val in zip(col_names, row):
                print(f"  {col}: {val}")
            print("-" * 15)

    except sqlite3.Error as e:
        print(f"An error occurred: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        print("Is the 'requests' table created? Try running submit.py first.")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    peek_database()