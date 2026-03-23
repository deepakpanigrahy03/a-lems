# scripts/migrations/v2_llm_schema.py
import sqlite3

def migrate():
    conn = sqlite3.connect("data/experiments.db")
    cursor = conn.cursor()
    
    # Rename column
    try:
        cursor.execute("ALTER TABLE llm_interactions RENAME COLUMN throughput_kbps TO app_throughput_kbps")
        print("✅ Renamed throughput_kbps → app_throughput_kbps")
    except Exception as e:
        print(f"⚠️ Rename failed: {e}")
    
    # Add new columns
    new_columns = [
        "total_time_ms REAL",
        "preprocess_ms REAL", 
        "non_local_ms REAL",
        "local_compute_ms REAL",
        "postprocess_ms REAL",
        "cpu_percent_during_wait REAL"
    ]
    
    for col in new_columns:
        try:
            cursor.execute(f"ALTER TABLE llm_interactions ADD COLUMN {col}")
            print(f"✅ Added column: {col.split()[0]}")
        except Exception as e:
            print(f"⚠️ Column {col.split()[0]} already exists: {e}")
    
    conn.commit()
    conn.close()
    print("Migration complete!")

if __name__ == "__main__":
    migrate()
