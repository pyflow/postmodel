
from postmodel import models

def test_model_1():
    class Foo(models.Model):
        foo_id = models.IntField(pk=True)
        content = models.TextField()
    
    assert Foo._meta != None
    assert len(Foo._meta.fields_map) == 2
    
    class FooBar(Foo):
        bar_content = models.TextField()

    assert len(FooBar._meta.fields_map) == 3

