import psycopg2
from dotenv import load_dotenv
import os

# Ensure environment variables are loaded
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
env_path = os.path.join(BASE_DIR, ".env")
load_dotenv(env_path)

DATABASE_URL = os.getenv("DATABASE_URL")

try:
    # Explicitly testing connection with psycopg2 and a 5-second timeout
    conn = psycopg2.connect(DATABASE_URL, connect_timeout=5)
    print("Connected successfully")
    conn.close()
except Exception as e:
    print(f"Connection failed:\n{e}")
