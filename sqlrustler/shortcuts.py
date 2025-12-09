"""
Shortcut functions for common database operations.

Provides Django-style convenience functions to simplify common patterns.
"""
from typing import Any, Dict, List, Optional, Type

from .exceptions import DoesNotExist


class Http404(Exception):
    """Exception raised when an object is not found (similar to Django's Http404)."""
    pass


def get_object_or_404(model_class: Type, **kwargs: Any):
    """Get an object or raise Http404 if not found.
    
    Args:
        model_class: The model class to query
        **kwargs: Filter conditions
        
    Returns:
        The found instance
        
    Raises:
        Http404: If no object matches the query
        
    Example:
        user = get_object_or_404(User, id=1)
    """
    try:
        return model_class.objects().filter(**kwargs).get()
    except DoesNotExist:
        raise Http404(f"{model_class.__name__} matching query does not exist")


def get_list_or_404(model_class: Type, **kwargs: Any) -> List[Any]:
    """Get a list of objects or raise Http404 if empty.
    
    Args:
        model_class: The model class to query
        **kwargs: Filter conditions
        
    Returns:
        List of matching instances
        
    Raises:
        Http404: If no objects match the query
        
    Example:
        users = get_list_or_404(User, is_active=True)
    """
    results = model_class.objects().filter(**kwargs).execute()
    if not results:
        raise Http404(f"No {model_class.__name__} objects found matching the query")
    return results


def bulk_create_or_update(
    model_class: Type,
    objects: List[Any],
    update_fields: Optional[List[str]] = None,
    match_fields: Optional[List[str]] = None
) -> Dict[str, int]:
    """Bulk create or update objects.
    
    Args:
        model_class: The model class
        objects: List of model instances or dictionaries
        update_fields: Fields to update if object exists
        match_fields: Fields to use for matching existing objects (defaults to 'id')
        
    Returns:
        Dictionary with counts: {'created': int, 'updated': int}
        
    Example:
        users = [
            {'email': 'user1@example.com', 'name': 'User 1'},
            {'email': 'user2@example.com', 'name': 'User 2'},
        ]
        result = bulk_create_or_update(User, users, match_fields=['email'])
    """
    if not objects:
        return {'created': 0, 'updated': 0}
    
    match_fields = match_fields or ['id']
    created_count = 0
    updated_count = 0
    
    for obj_data in objects:
        # Convert to dict if it's a model instance
        if hasattr(obj_data, '_data'):
            data = obj_data._data.copy()
        else:
            data = obj_data.copy()
        
        # Extract match criteria
        match_criteria = {field: data[field] for field in match_fields if field in data}
        
        if not match_criteria:
            # No match criteria, just create
            model_class.create(**data)
            created_count += 1
        else:
            # Try to update or create
            defaults = {k: v for k, v in data.items() if k not in match_criteria}
            if update_fields:
                defaults = {k: v for k, v in defaults.items() if k in update_fields}
            
            _, created = model_class.update_or_create(defaults=defaults, **match_criteria)
            if created:
                created_count += 1
            else:
                updated_count += 1
    
    return {'created': created_count, 'updated': updated_count}
