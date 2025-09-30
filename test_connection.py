import os
import snowflake.connector

# MAKE SURE THERE ARE NO WHITE SPACES IN .ENV AFTER YOUR VARIABLES (this can happen when you copy-paste)

# --- Verifying credentials from environment variables ---
account = os.getenv("SF_BASE_ACCOUNT")
user = os.getenv("SF_USER_USERNAME")
password = os.getenv("SF_USER_PASSWORD")

print("--- Credentials being used for connection ---")
print(f"Account: '{account}'")
print(f"User: '{user}'")
# This is for temporary debugging. Be careful printing passwords.
print(f"Password: '{password}'")
print("---------------------------------------------")
# --- End of verification ---

try:
    print("\nAttempting to connect to Snowflake...")
    conn = snowflake.connector.connect(
        user=user,
        password=password,
        account=account,
        warehouse=os.getenv("SF_BASE_WAREHOUSE"),
        database=os.getenv("SF_USER_DATABASE"),
        schema=os.getenv("SF_USER_SCHEMA"),
        role=os.getenv("SF_USER_ROLE"),
    )
    print("✅ Connection successful!")
    
    cs = conn.cursor()
    try:
        print("Executing 'SELECT 1'...")
        cs.execute("SELECT 1")
        one_row = cs.fetchone()
        print(f"✅ Query successful. Result: {one_row[0]}")
    finally:
        cs.close()

except Exception as e:
    print("❌ Connection failed.")
    print(f"Error: {e}")

finally:
    if 'conn' in locals() and conn:
        conn.close()
        print("Connection closed.")