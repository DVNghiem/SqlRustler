"""
Auto-detector for schema changes.
"""
from typing import List, Dict, Type
from sqlrustler.model import Model
from .operations import Operation, CreateTable

class AutoDetector:
    def __init__(self, old_state: Dict[str, Type[Model]], new_state: Dict[str, Type[Model]]):
        self.old_state = old_state
        self.new_state = new_state

    def detect_changes(self) -> List[Operation]:
        operations = []
        
        # Detect new models
        for name, model in self.new_state.items():
            if name not in self.old_state:
                operations.append(CreateTable(
                    name=model.table_name(),
                    fields=model._fields
                ))
        
        # TODO: Detect deleted models
        # TODO: Detect field changes
        
        return operations
