
from postmodel.models.query import Q


def test_q_basic():
    q = Q(moo="cow")
    assert q.children == ()
    assert q.filters == {"moo": "cow"}
    assert q.join_type == "AND"



def test_q_compound():
    q1 = Q(moo="cow")
    q2 = Q(moo="bull")
    q = Q(q1, q2, join_type=Q.OR)

    assert q.children == (q1, q2)
    assert q.filters == {}
    assert q.join_type == "OR"



def test_q_compound_or():
    q1 = Q(moo="cow")
    q2 = Q(moo="bull")
    q = q1 | q2

    assert q.children == (q1, q2)
    assert q.filters == {}
    assert q.join_type == "OR"


def test_q_compound_and():
    q1 = Q(moo="cow")
    q2 = Q(moo="bull")
    q = q1 & q2

    assert q.children == (q1, q2)
    assert q.filters == {}
    assert q.join_type == "AND"

