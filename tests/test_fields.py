from shillelagh.fields import Integer
from shillelagh.fields import Field
from shillelagh.fields import Order
from shillelagh.filters import Equal


def test_comparison():
    field1 = Field(filters=[Equal], order=Order.ASCENDING, exact=True)
    field2 = Field(filters=[Equal], order=Order.ASCENDING, exact=True)
    field3 = Field(filters=[Equal], order=Order.ASCENDING, exact=False)

    assert field1 == field2
    assert field1 != field3
    assert field1 != 42


def test_integer():
    assert Integer.parse(1) == 1
    assert Integer.parse("1") == 1
