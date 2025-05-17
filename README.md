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

## Installation

### Prerequisites

- Python 3.8+
- Rust (for building the library)
- PostgreSQL or MySQL server
- `maturin` for Rust-Python integration

### Install sqlrustler

1. Clone the repository:
   ```bash
   git clone https://github.com/DVNghiem/SqlRustler.git
   cd SqlRustler
   ```
2. Install Python dependencies (if any):
   ```bash
   pip3 install poetry maturin[patchelf]
   poetry install
   ```

3. Build and install the library using `maturin`:
   ```bash
   maturin develop
   ```
## Quick Start

### Define Models

Create a `models.py` file to define your database tables:

```python
from sqlrustler.model import Model
from sqlrustler.field import IntegerField, TextField, ForeignKeyField

class Company(Model):
    __tablename__ = "res_company"
    __alias__ = "default"
    id = IntegerField(primary_key=True)
    name = TextField()

class ResPartner(Model):
    __tablename__ = "res_partner"
    __alias__ = "default"
    id = IntegerField(primary_key=True)
    name = TextField()
    email = TextField()
    company_id = ForeignKeyField(Company, related_field="id")
```

### Connect to the Database

Configure and connect to your database:

```python
from sqlrustler.sqlrustler import DatabaseConfig, DatabaseType, DatabaseConnection

config = DatabaseConfig(
    driver=DatabaseType.Postgres,
    url="postgresql://user:password@localhost:5432/mms_stag_v2",
    max_connections=10,
    min_connections=1,
    idle_timeout=30,
)

DatabaseConnection.connect(config)
```

### Query the Database

Perform queries using the ORM:

```python
from sqlrustler import F

# Fetch all partners with row numbers
partners = ResPartner.objects().annotate(row_num=F("id").row_number()).execute()
for partner in partners:
    print(f"Partner: {partner.name}, Row Number: {partner._annotations['row_num']}")

# Filter and select related data
partners = ResPartner.objects().filter(name__contains="John").select_related("company_id").execute()
for partner in partners:
    print(f"Partner: {partner.name}, Company: {partner.company_id.name}")

# Aggregate data
result = ResPartner.objects().aggregate(count=F("id").count())
print(f"Total partners: {result['count']}")

# Raw results for custom queries
results = ResPartner.objects().select("name", "email").raw().execute()
print(results)  # [{'name': 'John Doe', 'email': 'john@example.com'}, ...]
```

## Usage Examples

### Filtering and Ordering

```python
# Filter partners by email and order by name
partners = ResPartner.objects().filter(email__endswith="@example.com").order_by("name").execute()
```

### Bulk Create

```python
company = Company(name="Test Corp")
company.save()
partners = [
    ResPartner(name=f"User {i}", email=f"user{i}@example.com", company_id=company)
    for i in range(3)
]
ResPartner.objects().bulk_create(partners)
```

### Custom Select and Values

```python
# Select specific fields as dictionaries
results = ResPartner.objects().values("name", "email").execute()
# [{'name': 'John Doe', 'email': 'john@example.com'}, ...]

# Flat values list
names = ResPartner.objects().values_list("name", flat=True).execute()
# ['John Doe', 'Jane Smith', ...]
```

### Window Functions

```python
# Rank partners by company
partners = ResPartner.objects().annotate(
    rank=F("id").rank(partition_by=["company_id"])
).execute()
for partner in partners:
    print(f"Partner: {partner.name}, Rank: {partner._annotations['rank']}")
```

## API Reference

### Key Classes

- **QuerySet**: Main interface for building and executing queries.
  - Methods: `filter`, `exclude`, `select`, `annotate`, `aggregate`, `select_related`, `prefetch_related`, `raw`, `values`, `execute`, etc.
- **Model**: Base class for defining database tables.
- **Fields**: `IntegerField`, `TextField`, `ForeignKeyField`, etc., for defining model attributes.
- **DatabaseConfig**: Configures database connections.

### Notable Methods

- `QuerySet.raw()`: Return raw dictionaries instead of model instances.
- `QuerySet.values(*fields)`: Return dictionaries for specified fields.
- `QuerySet.annotate(**annotations)`: Add computed fields (e.g., `row_num`).
- `QuerySet.select_related(*fields)`: Eagerly load foreign key relationships.
- `QuerySet.bulk_create(objs)`: Efficiently insert multiple records.

## Contributing

We welcome contributions! To get started:

1. Fork the repository.
2. Create a feature branch (`git checkout -b feature/your-feature`).
3. Commit your changes (`git commit -m "Add your feature"`).
4. Push to the branch (`git push origin feature/your-feature`).
5. Open a Pull Request.

Please include tests for new features and follow the coding style in the codebase.


## License

This project is licensed under the MIT License. See the `LICENSE` file for details.

## Support

For issues, feature requests, or questions, please open an issue on the [GitHub repository](https://github.com/DVNghiem/SqlRustler).