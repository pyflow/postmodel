
from postmodel.model import Model
from postmodel import fields

def test_model_1():
    class Foo(Model):
        foo_id = fields.IntField(pk=True)
        content = fields.TextField()
    
    assert Foo._meta != None
    assert len(Foo._meta.fields_map) == 2
    
    class FooBar(Foo):
        bar_content = fields.TextField()

    assert len(FooBar._meta.fields_map) == 3