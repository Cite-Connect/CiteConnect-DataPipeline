"""Quick script to debug .env loading"""
import os
from dotenv import load_dotenv

# Load .env
env_path = '.env'
load_dotenv(env_path)

print(f"📁 .env file: {os.path.abspath(env_path)}")
print(f"   Exists: {os.path.exists(env_path)}\n")

# Check each required variable
required_vars = {
    'DATABASE_URL': 'PostgreSQL connection string (postgresql://user:pass@host:port/db)',
    'SUPABASE_URL': 'Supabase project URL (https://xxx.supabase.co)',
    'GCS_BUCKET_NAME': 'GCS bucket name'
}

# Optional individual variables (if DATABASE_URL not provided)
optional_vars = {
    'SUPABASE_DB_HOST': 'Supabase host',
    'SUPABASE_DB_PORT': 'Database port',
    'SUPABASE_DB_NAME': 'Database name',
    'SUPABASE_DB_USER': 'Database user',
    'SUPABASE_DB_PASSWORD': 'Database password',
}

print("🔍 Checking REQUIRED variables:\n")

all_good = True
for var, description in required_vars.items():
    value = os.getenv(var)
    
    if value:
        # Mask passwords and full URLs
        if 'PASSWORD' in var or 'URL' in var:
            if var == 'DATABASE_URL':
                # Show just host part
                import re
                match = re.search(r'@([^:]+):', value)
                if match:
                    display = f"postgresql://***@{match.group(1)}:****/****"
                else:
                    display = "postgresql://***"
            elif 'URL' in var:
                display = value[:30] + "..." if len(value) > 30 else value
            else:
                display = f"{value[:3]}{'*' * (len(value) - 3)}" if len(value) > 3 else "***"
        else:
            display = value
        
        print(f"✅ {var}")
        print(f"   Value: {display}")
    else:
        print(f"❌ {var} - NOT SET")
        print(f"   Expected: {description}")
        all_good = False
    print()

# Check optional individual variables
print("\n📋 Optional individual database variables (used if DATABASE_URL not set):\n")
for var, description in optional_vars.items():
    value = os.getenv(var)
    if value:
        if 'PASSWORD' in var:
            display = f"{value[:3]}{'*' * (len(value) - 3)}" if len(value) > 3 else "***"
        else:
            display = value
        print(f"   ℹ️  {var}: {display}")

if all_good:
    print("\n✅ All REQUIRED variables configured correctly!")
    print("\n💡 Your setup uses DATABASE_URL (standard format)")
else:
    print("\n❌ Some required variables are missing.")
    print("\nMake sure your .env file has these:")
    print("=" * 60)
    for var, desc in required_vars.items():
        print(f"{var}=your_value_here  # {desc}")
    print("=" * 60)

