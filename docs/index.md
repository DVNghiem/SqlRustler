# Sqlrustler

**Sqlrustler** is a lightweight, high-performance Object-Relational Mapping (ORM) library for Python, designed to simplify database interactions with PostgreSQL and MySQL. Built with Rust bindings using `maturin` for speed and reliability, it offers a Django-inspired API with a focus on modularity, extensibility, and ease of use. Whether you're querying complex relationships, performing bulk operations, or annotating results, `sqlrustler` provides a robust and intuitive interface.

## Features

- **ORM with Model Support**: Define database tables as Python classes with fields like `IntegerField`, `TextField`, and `ForeignKeyField`...
- **Query Builder**: Chainable query methods (`filter`, `select`, `annotate`, etc.) for expressive SQL generation.
- **Database Support**: Compatible with PostgreSQL and MySQL, with extensible adapters for other databases.
- **Performance**: Leverages Rust for critical operations, ensuring fast query execution and result parsing.
- **Modular Design**: Separates query construction, result parsing, and expression handling for maintainability.
- **Raw and Flexible Results**: Supports both model instances and raw dictionaries for non-standard queries.
- **Annotations and Aggregations**: Easily add computed fields (e.g., `ROW_NUMBER`) or aggregate functions (e.g., `COUNT`, `SUM`).
- **Foreign Key Handling**: Seamless `select_related` and `prefetch_related` for efficient relationship queries.
- **Error Handling**: Graceful fallback to raw results when model parsing fails, with detailed logging.