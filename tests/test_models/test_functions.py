
from postmodel.models import functions as fn

def test_functions_1():
    assert fn.Trim('a').field_name == 'a'
    assert fn.Length('a').field_name == 'a'
    assert fn.Coalesce('a').field_name == 'a'
    assert fn.Lower('a').field_name == 'a'
    assert fn.Upper('a').field_name == 'a'
    assert fn.Count('a').field_name == 'a'
    assert fn.Sum('a').field_name == 'a'
    assert fn.Max('a').field_name == 'a'
    assert fn.Min('a').field_name == 'a'
    assert fn.Avg('a').field_name == 'a'