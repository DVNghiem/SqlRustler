"""
Migration manager CLI.
"""
import os
import sys
import importlib
import importlib.util
import inspect
import time
from typing import Dict, Type
from sqlrustler.model import Model
from .autodetector import AutoDetector
from .writer import MigrationWriter

def get_all_models() -> Dict[str, Type[Model]]:
    """
    Scan the project for models. 
    This is a simplified version that assumes models are imported or can be found.
    In a real app, we'd need an 'app' concept or configuration.
    For now, we'll scan modules in the current directory.
    """
    models = {}
    # This is a placeholder. In reality, the user needs to register apps or we scan recursively.
    # For demonstration, we'll assume the user passes the module name or we check known locations.
    
    # Check if 'models' module exists
    try:
        if os.path.exists("models.py"):
            spec = importlib.util.spec_from_file_location("models", "models.py")
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            
            for name, obj in inspect.getmembers(module):
                if inspect.isclass(obj) and issubclass(obj, Model) and obj != Model:
                    models[name] = obj
    except Exception as e:
        print(f"Error scanning models: {e}")
        
    return models

def makemigrations(name: str = "initial"):
    """
    Create a new migration based on changes.
    """
    # 1. Load current models
    current_models = get_all_models()
    
    # 2. Load old state (from previous migrations)
    # For now, we assume no previous state (initial migration)
    old_models = {} 
    
    # 3. Detect changes
    detector = AutoDetector(old_models, current_models)
    ops = detector.detect_changes()
    
    if not ops:
        print("No changes detected.")
        return

    # 4. Write migration file
    writer = MigrationWriter(name, ops)
    content = writer.as_string()
    
    os.makedirs("migrations", exist_ok=True)
    timestamp = int(time.time())
    filename = f"migrations/{timestamp}_{name}.py"
    
    with open(filename, "w") as f:
        f.write(content)
        
    print(f"Created migration {filename}")

def migrate():
    """
    Apply migrations.
    """
    from .executor import MigrationExecutor
    executor = MigrationExecutor()
    executor.migrate()

if __name__ == "__main__":
    import time
    if len(sys.argv) > 1:
        cmd = sys.argv[1]
        if cmd == "makemigrations":
            makemigrations(sys.argv[2] if len(sys.argv) > 2 else "initial")
        elif cmd == "migrate":
            migrate()
        else:
            print("Unknown command")
    else:
        print("Usage: python -m sqlrustler.migrations.manager [makemigrations|migrate]")
