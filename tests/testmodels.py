
from postmodel import models
import uuid

class Foo(models.Model):
    foo_id = models.IntField(pk=True)
    name = models.CharField(max_length=255, index=True)
    tag = models.CharField(max_length=128)
    memo = models.TextField()
    class Meta:
        table = "single_primary_foo"

class FooJsonModel(models.Model):
    foo_id = models.IntField(pk=True)
    value =  models.JSONField()

    class Meta:
        table = "foo_json"

class MultiPrimaryFoo(models.Model):
    foo_id = models.IntField()
    name = models.CharField(max_length=255)
    tag = models.CharField(max_length=128)
    date = models.DateField()
    updated = models.DatetimeField(auto_now=True)
    created = models.DatetimeField(auto_now_add=True)
    class Meta:
        table = "multi_primary_foo"
        primary_key = ('foo_id', 'name')
        unique_together = ('name', 'date')
        indexes = ('name', 'tag')


class Book(models.Model):
    id = models.IntField(pk=True)
    name = models.CharField(max_length=255, index=True)
    description = models.TextField()
    created = models.DatetimeField(auto_now_add=True)
    updated = models.DatetimeField(auto_now=True)
    data_ver = models.DataVersionField()

    class Meta:
        table = "book"

class IntFieldsModel(models.Model):
    id = models.IntField(pk=True)
    intnum = models.IntField()
    intnum_null = models.IntField(null=True)


class BigIntFieldsModel(models.Model):
    id = models.BigIntField(pk=True)
    intnum = models.BigIntField()
    intnum_null = models.BigIntField(null=True)

class AutoFieldsModel(models.Model):
    id = models.AutoField()
    intnum = models.IntField()

class DataVersionFieldsModel(models.Model):
    id = models.IntField(pk=True)
    data_ver = models.DataVersionField()

class SmallIntFieldsModel(models.Model):
    id = models.IntField(pk=True)
    smallintnum = models.SmallIntField()
    smallintnum_null = models.SmallIntField(null=True)


class CharFieldsModel(models.Model):
    id = models.IntField(pk=True)
    char = models.CharField(max_length=255)
    char_null = models.CharField(max_length=255, null=True)


class TextFieldsModel(models.Model):
    id = models.IntField(pk=True)
    text = models.TextField()
    text_null = models.TextField(null=True)


class BooleanFieldsModel(models.Model):
    id = models.IntField(pk=True)
    boolean = models.BooleanField()
    boolean_null = models.BooleanField(null=True)


class BinaryFieldsModel(models.Model):
    id = models.IntField(pk=True)
    binary = models.BinaryField()
    binary_null = models.BinaryField(null=True)


class DecimalFieldsModel(models.Model):
    id = models.IntField(pk=True)
    decimal = models.DecimalField(max_digits=18, decimal_places=4)
    decimal_nodec = models.DecimalField(max_digits=18, decimal_places=0)
    decimal_null = models.DecimalField(max_digits=18, decimal_places=4, null=True)


class DatetimeFieldsModel(models.Model):
    id = models.IntField(pk=True)
    datetime = models.DatetimeField()
    datetime_null = models.DatetimeField(null=True)
    datetime_auto = models.DatetimeField(auto_now=True)
    datetime_add = models.DatetimeField(auto_now_add=True)


class TimeDeltaFieldsModel(models.Model):
    id = models.IntField(pk=True)
    timedelta = models.TimeDeltaField()
    timedelta_null = models.TimeDeltaField(null=True)


class DateFieldsModel(models.Model):
    id = models.IntField(pk=True)
    date = models.DateField()
    date_null = models.DateField(null=True)


class FloatFieldsModel(models.Model):
    id = models.IntField(pk=True)
    floatnum = models.FloatField()
    floatnum_null = models.FloatField(null=True)


class JSONFieldsModel(models.Model):
    id = models.IntField(pk=True)
    data = models.JSONField()
    data_null = models.JSONField(null=True)
    data_default = models.JSONField(default={"a": 1})


class UUIDFieldsModel(models.Model):
    id = models.UUIDField(pk=True, default=uuid.uuid1)
    data = models.UUIDField()
    data_auto = models.UUIDField(default=uuid.uuid4)
    data_null = models.UUIDField(null=True)
