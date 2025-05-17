class Expression:
    def __init__(self, sql: str, params: list):
        self.sql = sql
        self.params = params

    def over(self, partition_by=None, order_by=None, frame=None, window_name=None):
        if window_name:
            self.sql = f"{self.sql} OVER {window_name}"
            return self

        parts = ["OVER("]
        clauses = []

        if partition_by:
            if isinstance(partition_by, str):
                partition_by = [partition_by]
            formatted_fields = [f.replace("__", ".") for f in partition_by]
            clauses.append(f"PARTITION BY {', '.join(formatted_fields)}")

        if order_by:
            if isinstance(order_by, str):
                order_by = [order_by]
            formatted_order = []
            for field in order_by:
                if isinstance(field, str):
                    if field.startswith("-"):
                        field = f"{field[1:]} DESC"
                    elif field.startswith("+"):
                        field = f"{field[1:]} ASC"
                    if "__" in field:
                        field = field.replace("__", ".")
                formatted_order.append(field)
            clauses.append(f"ORDER BY {', '.join(formatted_order)}")

        if frame:
            if isinstance(frame, str):
                clauses.append(frame)
            elif isinstance(frame, (list, tuple)):
                frame_type = "ROWS"
                if len(frame) == 3 and frame[0].upper() in ("ROWS", "RANGE", "GROUPS"):
                    frame_type = frame[0].upper()
                    frame = frame[1:]
                frame_clause = f"{frame_type} BETWEEN {frame[0]} AND {frame[1]}"
                clauses.append(frame_clause)

        parts.append(" ".join(clauses))
        parts.append(")")
        self.sql = f"{self.sql} {' '.join(parts)}"
        return self
