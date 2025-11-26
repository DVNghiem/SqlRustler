"""
Migration loader.
"""
import os
import importlib.util
from typing import List, Tuple

class MigrationLoader:
    def __init__(self, migration_dir: str = "migrations"):
        self.migration_dir = migration_dir

    def load_migrations(self) -> List[Tuple[str, object]]:
        migrations = []
        if not os.path.exists(self.migration_dir):
            return migrations

        for filename in sorted(os.listdir(self.migration_dir)):
            if filename.endswith(".py") and filename != "__init__.py":
                name = filename[:-3]
                path = os.path.join(self.migration_dir, filename)
                
                spec = importlib.util.spec_from_file_location(name, path)
                module = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(module)
                
                migrations.append((name, module))
        
        return migrations
