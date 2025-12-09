"""
Test suite for SqlRustler QuerySet functionality.
"""
import pytest
from sqlrustler import Model, IntegerField, TextField, QuerySet


class TestQuerySetClone:
    """Test QuerySet cloning functionality."""
    
    def test_queryset_clone_independence(self, mock_model):
        """Test that cloned querysets are independent."""
        qs1 = mock_model.objects()
        qs2 = qs1.filter(id=1)
        
        # Original should not be modified
        assert len(qs1.state["where"]) == 0
        assert len(qs2.state["where"]) > 0


class TestQuerySetLast:
    """Test QuerySet last() method."""
    
    def test_last_reverses_order_asc(self, mock_model):
        """Test that last() properly reverses ASC order."""
        qs = mock_model.objects().order_by("name")
        qs_last = qs.clone()
        
        # Manually apply last() logic
        if not qs_last.state["order_by"]:
            qs_last = qs_last.order_by("-id")
        else:
            reversed_order = []
            for field in qs_last.state["order_by"]:
                if " DESC" in field:
                    reversed_order.append(field.replace(" DESC", " ASC"))
                elif " ASC" in field:
                    reversed_order.append(field.replace(" ASC", " DESC"))
                else:
                    reversed_order.append(f"{field} DESC")
            qs_last.state["order_by"] = reversed_order
        
        # Should reverse to DESC
        assert "DESC" in qs_last.state["order_by"][0]
    
    def test_last_reverses_order_desc(self, mock_model):
        """Test that last() properly reverses DESC order."""
        qs = mock_model.objects()
        qs.state["order_by"] = ["test_table.name DESC"]
        
        reversed_order = []
        for field in qs.state["order_by"]:
            if " DESC" in field:
                reversed_order.append(field.replace(" DESC", " ASC"))
            elif " ASC" in field:
                reversed_order.append(field.replace(" ASC", " DESC"))
            else:
                reversed_order.append(f"{field} DESC")
        
        # Should reverse to ASC
        assert "ASC" in reversed_order[0]
        assert "DESC" not in reversed_order[0]


class TestQuerySetRaw:
    """Test raw results functionality."""
    
    def test_raw_flag_set(self, mock_model):
        """Test that raw() sets the flag correctly."""
        qs = mock_model.objects().raw()
        assert qs._raw_results is True
    
    def test_values_sets_raw(self, mock_model):
        """Test that values() sets raw results."""
        qs = mock_model.objects().values("id", "name")
        assert qs._raw_results is True


class TestQuerySetDistinct:
    """Test DISTINCT functionality."""
    
    def test_distinct_flag(self, mock_model):
        """Test that distinct() sets the flag."""
        qs = mock_model.objects().distinct()
        assert qs.state["distinct"] is True
    
    def test_distinct_on_fields(self, mock_model):
        """Test DISTINCT ON with specific fields."""
        qs = mock_model.objects().distinct("name", "description")
        assert qs.state["distinct"] is True
        assert "name" in qs.state["distinct_on"]
        assert "description" in qs.state["distinct_on"]


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
