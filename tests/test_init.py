
from postmodel import Postmodel
import pytest

@pytest.mark.asyncio
async def test_init_1():
    await Postmodel.init('postgres://user:pass@localhost:54320/test_db')
    assert len(Postmodel._connections) == 1
    assert Postmodel._inited == True