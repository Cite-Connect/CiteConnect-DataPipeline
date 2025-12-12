"""Debug script to test DATABASE_URL parsing"""
import os
from dotenv import load_dotenv
from urllib.parse import urlparse

load_dotenv()

database_url = os.getenv('DATABASE_URL')

print(f"DATABASE_URL: {database_url[:50]}..." if database_url else "DATABASE_URL: NOT SET")
print()

if database_url:
    try:
        parsed = urlparse(database_url)
        
        print("Parsed components:")
        print(f"  Scheme: {parsed.scheme}")
        print(f"  Username: {parsed.username}")
        print(f"  Password: {'***' if parsed.password else 'None'}")
        print(f"  Hostname: {parsed.hostname}")
        print(f"  Port: {parsed.port}")
        print(f"  Path: {parsed.path}")
        print(f"  Database: {parsed.path.lstrip('/').split('?')[0]}")
        
        print("\nDatabase config dict:")
        config = {
            'host': parsed.hostname,
            'port': parsed.port or 5432,
            'database': parsed.path.lstrip('/').split('?')[0],
            'user': parsed.username,
            'password': parsed.password,
        }
        
        for key, value in config.items():
            if key == 'password':
                print(f"  {key}: {'***' if value else 'None'}")
            else:
                print(f"  {key}: {value}")
                
        if not parsed.username:
            print("\n❌ ERROR: Username is None!")
            print("   Your DATABASE_URL might be malformed")
            print("   Expected format: postgresql://user:pass@host:port/db")
            
    except Exception as e:
        print(f"❌ Error parsing: {e}")
else:
    print("❌ DATABASE_URL not set in environment")

