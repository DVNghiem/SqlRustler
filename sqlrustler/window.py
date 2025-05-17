class Window:
    def __init__(self, name: str, partition_by=None, order_by=None, frame=None):
        self.name = name
        self.partition_by = partition_by
        self.order_by = order_by
        self.frame = frame

    def to_sql(self):
        parts = [f"{self.name} AS ("]
        clauses = []

        if self.partition_by:
            if isinstance(self.partition_by, str):
                self.partition_by = [self.partition_by]
            formatted_fields = [f.replace("__", ".") for f in self.partition_by]
            clauses.append(f"PARTITION BY {', '.join(formatted_fields)}")

        if self.order_by:
            if isinstance(self.order_by, str):
                self.order_by = [self.order_by]
            formatted_order = []
            for field in self.order_by:
                if field.startswith("-"):
                    field = f"{field[1:].replace('__', '.')} DESC"
                elif field.startswith("+"):
                    field = f"{field[1:].replace('__', '.')} ASC"
                else:
                    field = field.replace("__", ".")
                formatted_order.append(field)
            clauses.append(f"ORDER BY {', '.join(formatted_order)}")

        if self.frame:
            if isinstance(self.frame, str):
                clauses.append(self.frame)
            elif isinstance(self.frame, (list, tuple)):
                frame_type = "ROWS"
                if len(self.frame) == 3 and self.frame[0].upper() in (
                    "ROWS",
                    "RANGE",
                    "GROUPS",
                ):
                    frame_type = self.frame[0].upper()
                    frame = self.frame[1:]
                frame_clause = f"{frame_type} BETWEEN {frame[0]} AND {frame[1]}"
                clauses.append(frame_clause)

        parts.append(" ".join(clauses))
        parts.append(")")
        return " ".join(parts)
