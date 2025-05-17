from .express import Expression
from typing import List, Any

class F:
    def __init__(self, field: str, params: List[Any] = None):
        self.field = field.replace("__", ".")
        self.params = params or []

    def __add__(self, other):
        if isinstance(other, F):
            return Expression(f"{self.field} + {other.field}", [])
        return Expression(f"{self.field} + %s", [other])

    def __sub__(self, other):
        if isinstance(other, F):
            return Expression(f"{self.field} - {other.field}", [])
        return Expression(f"{self.field} - %s", [other])

    def __mul__(self, other):
        if isinstance(other, F):
            return Expression(f"{self.field} * {other.field}", [])
        return Expression(f"{self.field} * %s", [other])

    def __truediv__(self, other):
        if isinstance(other, F):
            return Expression(f"{self.field} / {other.field}", [])
        return Expression(f"{self.field} / %s", [other])

    def sum(self):
        return Expression(f"SUM({self.field})", [])

    def avg(self):
        return Expression(f"AVG({self.field})", [])

    def count(self):
        return Expression(f"COUNT({self.field})", [])

    def max(self):
        return Expression(f"MAX({self.field})", [])

    def min(self):
        return Expression(f"MIN({self.field})", [])

    def lag(self, offset=1, default=None):
        if default is None:
            return Expression(f"LAG({self.field}, {offset})", [])
        return Expression(f"LAG({self.field}, {offset}, %s)", [default])

    def lead(self, offset=1, default=None):
        if default is None:
            return Expression(f"LEAD({self.field}, {offset})", [])
        return Expression(f"LEAD({self.field}, {offset}, %s)", [default])

    def row_number(self):
        return Expression("ROW_NUMBER()", [])

    def rank(self):
        return Expression("RANK()", [])

    def dense_rank(self):
        return Expression("DENSE_RANK()", [])