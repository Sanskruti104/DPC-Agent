from db import engine

try:
    with engine.connect() as connection:
        print("OK: Connection successful!")
except Exception as e:
    print(f"FAILED: Connection failed: {e}")
