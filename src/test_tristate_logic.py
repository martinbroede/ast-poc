import pytest

from core.logic import TriState


@pytest.mark.parametrize(
    "left,right,expected",
    [
        (TriState.TRUE, TriState.TRUE, TriState.TRUE),
        (TriState.TRUE, TriState.FALSE, TriState.FALSE),
        (TriState.TRUE, TriState.UNKNOWN, TriState.UNKNOWN),
        (TriState.FALSE, TriState.TRUE, TriState.FALSE),
        (TriState.FALSE, TriState.FALSE, TriState.FALSE),
        (TriState.FALSE, TriState.UNKNOWN, TriState.FALSE),
        (TriState.UNKNOWN, TriState.TRUE, TriState.UNKNOWN),
        (TriState.UNKNOWN, TriState.FALSE, TriState.FALSE),
        (TriState.UNKNOWN, TriState.UNKNOWN, TriState.UNKNOWN),
    ],
)
def test_tristate_and_operator(left: TriState, right: TriState, expected: TriState) -> None:
    assert (left & right) is expected


@pytest.mark.parametrize(
    "left,right,expected",
    [
        (TriState.TRUE, TriState.TRUE, TriState.TRUE),
        (TriState.TRUE, TriState.FALSE, TriState.TRUE),
        (TriState.TRUE, TriState.UNKNOWN, TriState.TRUE),
        (TriState.FALSE, TriState.TRUE, TriState.TRUE),
        (TriState.FALSE, TriState.FALSE, TriState.FALSE),
        (TriState.FALSE, TriState.UNKNOWN, TriState.UNKNOWN),
        (TriState.UNKNOWN, TriState.TRUE, TriState.TRUE),
        (TriState.UNKNOWN, TriState.FALSE, TriState.UNKNOWN),
        (TriState.UNKNOWN, TriState.UNKNOWN, TriState.UNKNOWN),
    ],
)
def test_tristate_or_operator(left: TriState, right: TriState, expected: TriState) -> None:
    assert (left | right) is expected


@pytest.mark.parametrize(
    "value,expected",
    [
        (TriState.TRUE, TriState.FALSE),
        (TriState.FALSE, TriState.TRUE),
        (TriState.UNKNOWN, TriState.UNKNOWN),
    ],
)
def test_tristate_not_operator(value: TriState, expected: TriState) -> None:
    assert (~value) is expected
