class Q:
    def __init__(self, *args, **kwargs):
        self.children = list(args)
        self.connector = "AND"
        self.negated = False

        if kwargs:
            for key, value in kwargs.items():
                condition = {key: value}
                self.children.append(condition)

    def __and__(self, other):
        if getattr(other, "connector", "AND") == "AND" and not other.negated:
            clone = self._clone()
            clone.children.extend(other.children)
            return clone
        else:
            q = Q()
            q.connector = "AND"
            q.children = [self, other]
            return q

    def __or__(self, other):
        if getattr(other, "connector", "OR") == "OR" and not other.negated:
            clone = self._clone()
            clone.connector = "OR"
            clone.children.extend(other.children)
            return clone
        else:
            q = Q()
            q.connector = "OR"
            q.children = [self, other]
            return q

    def __invert__(self):
        clone = self._clone()
        clone.negated = not self.negated
        return clone

    def _clone(self):
        clone = Q()
        clone.connector = self.connector
        clone.negated = self.negated
        clone.children = self.children[:]
        return clone

    def add(self, child, connector):
        if connector != self.connector:
            self.children = [Q(*self.children, connector=self.connector)]
            self.connector = connector

        if isinstance(child, Q):
            if child.connector == connector and not child.negated:
                self.children.extend(child.children)
            else:
                self.children.append(child)
        else:
            self.children.append(child)

    def _combine(self, other, connector):
        if not other:
            return self._clone()

        if not self:
            return other._clone() if isinstance(other, Q) else Q(other)

        q = Q()
        q.connector = connector
        q.children = [self, other]
        return q

    def __bool__(self):
        return bool(self.children)

    def __str__(self):
        if self.negated:
            return f"NOT ({self._str_inner()})"
        return self._str_inner()

    def _str_inner(self):
        if not self.children:
            return ""

        children_str = []
        for child in self.children:
            if isinstance(child, Q):
                child_str = str(child)
            elif isinstance(child, dict):
                child_str = " AND ".join(f"{k}={v}" for k, v in child.items())
            else:
                child_str = str(child)
            children_str.append(f"({child_str})")

        return f" {self.connector} ".join(children_str)
