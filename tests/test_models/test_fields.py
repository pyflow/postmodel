import pytest
from datetime import datetime, date, timedelta
import json
import uuid
import time

from tests.testmodels import (
    IntFieldsModel,
    DataVersionFieldsModel,
    DatetimeFieldsModel,
    DateFieldsModel,
    TimeDeltaFieldsModel,
    JSONFieldsModel,
    UUIDFieldsModel
)

from postmodel.models.fields import (
    BinaryField,
    CharField,
    DecimalField,
    DatetimeField,
    UUIDField
)

from postmodel.exceptions import ConfigurationError, FieldValueError

def test_int_field():
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

def test_wrong_config():
    with pytest.raises(ConfigurationError):
        CharField(max_length = 0)
    with pytest.raises(ConfigurationError):
        DecimalField(max_digits = 0, decimal_places=10)
    with pytest.raises(ConfigurationError):
        DecimalField(max_digits = 128, decimal_places = -1)
    with pytest.raises(ConfigurationError):
        DatetimeField(auto_now=True, auto_now_add=True)

def test_dataversion_field():
    model = DataVersionFieldsModel(id=1)
    assert model.data_ver == 0
    fields_map = model._meta.fields_map
    data_ver = fields_map['data_ver']
    data_ver.auto_value(model)
    assert model.data_ver == 1


def test_datatime_field():
    model = DatetimeFieldsModel(id=1, datetime=datetime.utcnow())
    fields_map = model._meta.fields_map
    field = fields_map['datetime']
    fields_map['datetime_null'].auto_value(model)
    assert model.datetime_null == None
    assert field.to_python_value(None) == None
    dt = datetime.now()
    assert field.to_python_value(dt) == dt
    assert field.to_python_value('2020-01-06 12:12:12') == datetime(2020, 1, 6, 12, 12, 12)
    assert model.datetime_auto == None
    assert model.datetime_add == None
    dt = datetime.utcnow()
    time.sleep(1)
    fields_map['datetime_auto'].auto_value(model)
    fields_map['datetime_add'].auto_value(model)
    assert model.datetime_auto > dt
    assert model.datetime_add > dt

def test_date_field():
    model = DateFieldsModel(id=1, date=date.today())
    fields_map = model._meta.fields_map
    field = fields_map['date']
    assert field.to_python_value(None) == None
    dt = date.today()
    assert field.to_python_value(dt) == dt
    assert field.to_python_value('2020-01-06') == date(2020, 1, 6)

def test_timedelta_field():
    model = TimeDeltaFieldsModel(id=1, timedelta=1000000)
    fields_map = model._meta.fields_map
    field = fields_map['timedelta']
    assert field.to_python_value(None) == None
    assert field.to_python_value(timedelta(20, 0, 0)) == timedelta(20, 0, 0)
    assert field.to_python_value(1000) == timedelta(microseconds=1000)
    assert field.to_db_value(None) == None
    assert field.to_db_value(timedelta(days=1)) == 86400*1000000

def test_json_field():
    model = JSONFieldsModel(id=1, data={'fookey': 'hello', 'key2': 124})
    fields_map = model._meta.fields_map
    field = fields_map['data']
    assert field.to_python_value(None) == None
    assert field.to_python_value(['a', 'b']) == ['a', 'b']
    assert field.to_python_value('["abc", 123, "cde"]') == ["abc", 123, "cde"]

    assert field.to_db_value(None) == None
    assert json.loads(field.to_db_value({'fookey': 'world', 'key2': 223})) == {'fookey': 'world', 'key2': 223}

def test_uuid_field():
    f = UUIDField(pk=True)
    assert f.default == uuid.uuid4
    model = UUIDFieldsModel(data='123e4567-e89b-12d3-a456-426655440000')
    fields_map = model._meta.fields_map
    field = fields_map['data']
    v = uuid.uuid4()
    assert field.to_python_value(None) == None
    assert field.to_python_value(v) == uuid.UUID(str(v))
    assert field.to_python_value(str(v)) == v

    assert field.to_db_value(None) == None
    assert field.to_db_value(v) == str(v)

def test_binary_field():
    f = BinaryField()
    assert f.to_python_value(None) == None
    assert f.to_python_value(b'xxx') == b'xxx'

    assert f.to_db_value(None) == None
    assert f.to_db_value(b'hello') == bytes('hello', 'ascii')
    assert f.to_db_value('hello') == bytes('hello', 'ascii')
    with pytest.raises(FieldValueError):
        assert f.to_db_value(10)