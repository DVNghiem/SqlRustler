"""
Verification script for Migration System.
"""
import os
import sys
import shutil
from sqlrustler import DatabaseConfig, DatabaseConnection, DatabaseType
from sqlrustler.migrations import makemigrations, migrate

def setup_db():
    print("Setting up database...")
    config = DatabaseConfig(
        driver=DatabaseType.Sqlite,
        url="sqlite::memory:",
        max_connections=5,
        min_connections=1,
        idle_timeout=30,
    )
    DatabaseConnection.connect(config, "default")

def clean_migrations():
    if os.path.exists("migrations"):
        shutil.rmtree("migrations")
    os.makedirs("migrations")
    with open("migrations/__init__.py", "w") as f:
        f.write("")

def verify_tables_exist():
    # We can't easily check tables with raw SQL yet without a raw execute that returns results easily
    # But we can try to insert data using the models.
    # If tables don't exist, this will fail.
    
    print("Verifying tables...")
    from models import User, Post
    
    try:
        user = User(username="testuser", email="test@example.com")
        user.save()
        print("  ✅ User table exists and insert worked")
        
        post = Post(title="Hello", content="World")
        post.save()
        print("  ✅ Post table exists and insert worked")
        return True
    except Exception as e:
        print(f"  ❌ FAILED: {e}")
        return False

def main():
    print("=" * 60)
    print("SqlRustler Migration System Verification")
    print("=" * 60)
    
    # 1. Clean up
    clean_migrations()
    
    # 2. Setup DB
    setup_db()
    
    # 3. Make migrations
    print("\nRunning makemigrations...")
    try:
        makemigrations("initial")
        if os.path.exists("migrations"):
            files = [f for f in os.listdir("migrations") if f.endswith(".py") and f != "__init__.py"]
            if files:
                print(f"  ✅ Created migration file: {files[0]}")
            else:
                print("  ❌ FAILED: No migration file created")
                return 1
        else:
            print("  ❌ FAILED: migrations directory not created")
            return 1
    except Exception as e:
        print(f"  ❌ FAILED: {e}")
        import traceback
        traceback.print_exc()
        return 1
        
    # 4. Migrate
    print("\nRunning migrate...")
    try:
        migrate()
        print("  ✅ Migrate command completed")
    except Exception as e:
        print(f"  ❌ FAILED: {e}")
        import traceback
        traceback.print_exc()
        return 1
        
    # 5. Verify
    if verify_tables_exist():
        print("\n🎉 Migration system verified successfully!")
        return 0
    else:
        print("\n⚠️  Verification failed")
        return 1

if __name__ == "__main__":
    exit(main())
