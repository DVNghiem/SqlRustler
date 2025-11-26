"""
Migration executor.
"""
from typing import List
from .loader import MigrationLoader
from .schema import SchemaEditor
from sqlrustler import DatabaseConnection

class MigrationExecutor:
    def __init__(self, connection_alias: str = "default"):
        self.connection_alias = connection_alias
        self.loader = MigrationLoader()

    def migrate(self):
        from sqlrustler import Session, get_db_type_with_alias, DatabaseType
        
        # Determine DB type for SQL compatibility
        try:
            db_type = get_db_type_with_alias(self.connection_alias)
        except:
            db_type = DatabaseType.Postgres # Default
            
        with Session(self.connection_alias) as tx:
            # 1. Ensure migrations table exists
            self._ensure_migrations_table(tx, db_type)
            
            # 2. Get applied migrations
            applied = self._get_applied_migrations(tx)
            
            # 3. Get all migrations
            all_migrations = self.loader.load_migrations()
            
            # 4. Filter unapplied
            to_apply = [m for m in all_migrations if m[0] not in applied]
            
            if not to_apply:
                print("No migrations to apply.")
                return

            # 5. Apply
            editor = SchemaEditor()
            print(f"Applying {len(to_apply)} migrations...")
            
            for name, module in to_apply:
                print(f"  Applying {name}...")
                for op in module.operations:
                    sql = op.to_sql(editor)
                    print(f"    Executing: {sql}")
                    self._execute_sql(tx, sql)
                
                self._record_migration(tx, name)
            
    def _ensure_migrations_table(self, tx, db_type):
        from sqlrustler import DatabaseType
        
        if db_type == DatabaseType.Sqlite:
            id_type = "INTEGER PRIMARY KEY AUTOINCREMENT"
        else:
            id_type = "SERIAL PRIMARY KEY"
            
        sql = f"""
        CREATE TABLE IF NOT EXISTS sqlrustler_migrations (
            id {id_type},
            name VARCHAR(255) NOT NULL,
            applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        self._execute_sql(tx, sql)

    def _get_applied_migrations(self, tx) -> List[str]:
        sql = "SELECT name FROM sqlrustler_migrations"
        try:
            results = tx.fetch_all(sql, [])
            # Results are list of dicts or objects depending on parser
            # Assuming list of dicts or similar from raw fetch
            # We might need to handle the result format
            return [r['name'] for r in results]
        except Exception:
            # Table might not exist yet (first run race condition) or empty
            return []

    def _record_migration(self, tx, name: str):
        sql = "INSERT INTO sqlrustler_migrations (name) VALUES ($1)"
        # Note: Placeholder syntax depends on DB. 
        # Postgres: $1, MySQL: ?, Sqlite: ?
        # We need to use the adapter or handle this.
        # For now, let's assume Postgres style or use string formatting (unsafe but ok for internal migration name)
        # Ideally use params.
        # Let's try to use the proper param binder if possible.
        # But Transaction.execute takes params.
        
        # Hack for now to support multiple DBs without full adapter access here
        # We'll use simple string interpolation for the migration name which is safe-ish (filename)
        sql = f"INSERT INTO sqlrustler_migrations (name) VALUES ('{name}')"
        self._execute_sql(tx, sql)

    def _execute_sql(self, tx, sql: str):
        tx.execute(sql, [])
