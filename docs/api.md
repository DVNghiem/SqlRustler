# API Reference

## Key Classes

- **QuerySet**: Main interface for building and executing queries.
  - Methods: `filter`, `exclude`, `select`, `annotate`, `aggregate`, `select_related`, `prefetch_related`, `raw`, `values`, `execute`, etc.
- **Model**: Base class for defining database tables.
- **Fields**: `IntegerField`, `TextField`, `ForeignKeyField`, etc., for defining model attributes.
- **DatabaseConfig**: Configures database connections.

## Notable Methods

- `QuerySet.raw()`: Return raw dictionaries instead of model instances.
- `QuerySet.values(*fields)`: Return dictionaries for specified fields.
- `QuerySet.annotate(**annotations)`: Add computed fields (e.g., `row_num`).
- `QuerySet.select_related(*fields)`: Eagerly load foreign key relationships.
- `QuerySet.bulk_create(objs)`: Efficiently insert multiple records.

::: sqlrustler.model
::: sqlrustler.field
::: sqlrustler.queryset