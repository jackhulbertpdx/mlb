"""
Test script to verify MotherDuck connection with your database.
"""
import duckdb
import os

# Your MotherDuck token
MOTHERDUCK_TOKEN = "${MOTHERDUCK_TOKEN}"

# Set the token as an environment variable (for dlt to use later)
os.environ["MOTHERDUCK_TOKEN"] = MOTHERDUCK_TOKEN

print("Testing MotherDuck connection...")

try:
    # Connect to MotherDuck
    print("\nConnecting to MotherDuck database...")
    conn_str = f"motherduck:motherduck_database?motherduck_token={MOTHERDUCK_TOKEN}"
    conn = duckdb.connect(conn_str)
    
    # Test basic query
    result = conn.execute("SELECT 1 AS test").fetchall()
    print(f"Basic query test successful: {result}")
    
    # Try to access mlb_data schema
    print("\nChecking for mlb_data schema and tables...")
    try:
        # Check for any tables in the mlb_data schema
        tables = conn.execute("SELECT * FROM information_schema.tables WHERE table_schema = 'mlb_data'").fetchall()
        
        if tables:
            print(f"Found {len(tables)} tables in mlb_data schema:")
            for table in tables:
                table_name = table[2]  # table_name is typically the 3rd column
                print(f"  - {table_name}")
                
                # Try to get row count
                try:
                    count = conn.execute(f"SELECT COUNT(*) FROM mlb_data.{table_name}").fetchone()[0]
                    print(f"    Records: {count}")
                except Exception as e:
                    print(f"    Error counting records: {e}")
        else:
            print("No tables found in mlb_data schema")
            
        # Look specifically for mlb_team_rosters
        try:
            count = conn.execute("SELECT COUNT(*) FROM mlb_data.mlb_team_rosters").fetchone()[0]
            print(f"\nFound mlb_team_rosters table with {count} records")
            
            # Show a sample
            if count > 0:
                sample = conn.execute("SELECT * FROM mlb_data.mlb_team_rosters LIMIT 3").fetchall()
                print("\nSample data:")
                for row in sample:
                    print(f"  {row}")
        except Exception as e:
            print(f"\nError accessing mlb_team_rosters: {e}")
            
    except Exception as e:
        print(f"Error checking mlb_data schema: {e}")
        
    # Now let's create a dlt config for this connection
    print("\nBased on this successful connection, your dlt configuration should be:")
    print("""
import dlt
import os

# Set MotherDuck token
os.environ["MOTHERDUCK_TOKEN"] = "your_token_here"

# Create pipeline
pipeline = dlt.pipeline(
    pipeline_name="mlb_team_rosters",
    destination="duckdb",
    dataset_name="mlb_data"
)

# For your version of dlt, don't use destination_options
# The environment variable should be picked up automatically
    """)
    
    conn.close()
    print("\nConnection test completed successfully!")
    
except Exception as e:
    print(f"Connection error: {e}")
