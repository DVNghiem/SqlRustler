"""
Verification script to test critical bug fixes in SqlRustler ORM.
"""
from sqlrustler import Model, IntegerField, TextField, ForeignKeyField, CharField, DecimalField
from sqlrustler.exceptions import SqlRustlerError, DoesNotExist


def test_field_type_mapper():
    """Test 1: Field type_mapper initialization fix"""
    print("Test 1: Field type_mapper initialization...")
    try:
        # This should NOT raise AttributeError anymore
        field = IntegerField()
        sql_type = field.sql_type()
        print(f"  ✅ IntegerField.sql_type() = {sql_type}")
        
        char_field = CharField(max_length=100)
        char_sql = char_field.sql_type()
        print(f"  ✅ CharField(max_length=100).sql_type() = {char_sql}")
        
        decimal_field = DecimalField(max_digits=10, decimal_places=2)
        decimal_sql = decimal_field.sql_type()
        print(f"  ✅ DecimalField.sql_type() = {decimal_sql}")
        
        return True
    except AttributeError as e:
        print(f"  ❌ FAILED: {e}")
        return False


def test_foreign_key_table_name():
    """Test 2: ForeignKey table name resolution fix"""
    print("\nTest 2: ForeignKey table name resolution...")
    try:
        class Company(Model):
            __tablename__ = "companies"  # Custom table name
            id = IntegerField(primary_key=True)
            name = TextField()
        
        class Employee(Model):
            __tablename__ = "employees"
            id = IntegerField(primary_key=True)
            name = TextField()
            company_id = ForeignKeyField(Company, related_field="id")
        
        # Get the foreign key SQL
        fk_sql = Employee._get_foreign_key_sql("company_id", Employee._fields["company_id"])
        
        # Should use "companies" (from table_name()) not "company" (from __name__.lower())
        if "companies" in fk_sql:
            print(f"  ✅ Foreign key SQL correctly references 'companies' table")
            print(f"     SQL: {fk_sql}")
            return True
        else:
            print(f"  ❌ FAILED: Foreign key SQL doesn't reference correct table")
            print(f"     SQL: {fk_sql}")
            return False
    except Exception as e:
        print(f"  ❌ FAILED: {e}")
        return False


def test_exception_hierarchy():
    """Test 3: Exception hierarchy improvements"""
    print("\nTest 3: Exception hierarchy...")
    try:
        # Test that all exceptions inherit from SqlRustlerError
        assert issubclass(DoesNotExist, SqlRustlerError)
        print("  ✅ DoesNotExist inherits from SqlRustlerError")
        
        # Test that exceptions can be raised and caught
        try:
            raise DoesNotExist("Test error")
        except SqlRustlerError:
            print("  ✅ Can catch DoesNotExist as SqlRustlerError")
        
        return True
    except Exception as e:
        print(f"  ❌ FAILED: {e}")
        return False


def test_model_creation():
    """Test 4: Model creation and field registration"""
    print("\nTest 4: Model creation...")
    try:
        class TestModel(Model):
            __tablename__ = "test_table"
            id = IntegerField(primary_key=True)
            name = TextField()
            description = TextField(null=True)
        
        # Check fields are registered
        assert "id" in TestModel._fields
        assert "name" in TestModel._fields
        assert "description" in TestModel._fields
        print(f"  ✅ Model fields registered: {list(TestModel._fields.keys())}")
        
        # Check table name
        assert TestModel.table_name() == "test_table"
        print(f"  ✅ Table name: {TestModel.table_name()}")
        
        # Create instance
        instance = TestModel(id=1, name="Test", description="A test")
        assert instance._data["id"] == 1
        assert instance._data["name"] == "Test"
        print(f"  ✅ Model instance created successfully")
        
        return True
    except Exception as e:
        print(f"  ❌ FAILED: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    """Run all verification tests"""
    print("=" * 60)
    print("SqlRustler ORM - Bug Fix Verification")
    print("=" * 60)
    
    results = []
    results.append(("Field type_mapper initialization", test_field_type_mapper()))
    results.append(("ForeignKey table name resolution", test_foreign_key_table_name()))
    results.append(("Exception hierarchy", test_exception_hierarchy()))
    results.append(("Model creation", test_model_creation()))
    
    print("\n" + "=" * 60)
    print("VERIFICATION SUMMARY")
    print("=" * 60)
    
    passed = sum(1 for _, result in results if result)
    total = len(results)
    
    for test_name, result in results:
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"{status}: {test_name}")
    
    print(f"\nTotal: {passed}/{total} tests passed")
    
    if passed == total:
        print("\n🎉 All critical bug fixes verified successfully!")
        return 0
    else:
        print(f"\n⚠️  {total - passed} test(s) failed")
        return 1


if __name__ == "__main__":
    exit(main())
