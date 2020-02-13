
from tests.testmodels import (
    IntFieldsModel,

)

def test_int_models():
    model = IntFieldsModel(id=1, intnum=10)
    assert model.intnum_null == None
    fields_map = model._meta.fields_map
    intnum_field = fields_map['intnum']
    assert intnum_field.required == True
    assert intnum_field.to_db_value(None) == None
    assert intnum_field.to_db_value(23) == 23
    assert intnum_field.to_db_value('33') == 33
    assert intnum_field.to_python_value(None) == None
    assert intnum_field.to_python_value(344) == 344
    assert intnum_field.to_python_value('876') == 876
    assert fields_map['intnum_null'].required == False