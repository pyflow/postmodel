
from postmodel.models.query import Q, QuerySet
from postmodel.exceptions import OperationalError, FieldError
from tests.testmodels import Foo
import pytest

def test_q_basic():
    q = Q(moo="cow")
    assert q.children == ()
    assert q.filters == {"moo": "cow"}
    assert q.join_type == "AND"

    with pytest.raises(OperationalError):
        Q(moo="cow", join_type="XOR")

    with pytest.raises(OperationalError):
        Q(object())

    q = Q(Q(moo="cow"), foo="bar")
    assert len(q.children) == 2
    assert len(q.filters) == 0


def test_q_negate():
    q = Q(moo="cow")
    q1 = ~q

    assert q1._is_negated == True

def test_q_compound():
    q1 = Q(moo="cow")
    q2 = Q(moo="bull")
    q = Q(q1, q2, join_type=Q.OR)

    assert q.children == (q1, q2)
    assert q.filters == {}
    assert q.join_type == "OR"

    with pytest.raises(OperationalError):
        q & object()



def test_q_compound_or():
    q1 = Q(moo="cow")
    q2 = Q(moo="bull")
    q = q1 | q2

    assert q.children == (q1, q2)
    assert q.filters == {}
    assert q.join_type == "OR"

    with pytest.raises(OperationalError):
        q | object()

def test_q_compound_and():
    q1 = Q(moo="cow")
    q2 = Q(moo="bull")
    q = q1 & q2

    assert q.children == (q1, q2)
    assert q.filters == {}
    assert q.join_type == "AND"


def test_queryset_1():
    qs = QuerySet(Foo)
    qs_all = qs.all()
    assert qs.model_class == qs_all.model_class
    assert qs.fields == qs_all.fields

    with pytest.raises(FieldError):
        qs.order_by('-wrongname')

    with pytest.raises(TypeError):
        qs.filter(object())

    with pytest.raises(TypeError):
        qs.exclude(object())

    qs_one_or_none = qs.get_or_none()
    assert qs_one_or_none._limit == 1
    assert qs_one_or_none._return_single == 1

    assert qs.db_name == "default"
    qs.using_db("test_db_queryset")
    assert qs.db_name == "test_db_queryset"