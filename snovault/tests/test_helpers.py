import pytest

from .helpers import call_fixture


def test_call_fixture():

    chronology = []

    @pytest.yield_fixture()
    def adder(n, m):
        chronology.append("adder_before")
        yield n + m
        chronology.append("adder_after")

    with call_fixture(adder, 10, 7) as seventeen:
        chronology.append("adder_in_use")
        assert seventeen == 17

    assert chronology == ["adder_before", "adder_in_use", "adder_after"]

    class SampleTransferOfControl(BaseException):
        pass

    chronology = []

    try:
        with call_fixture(adder, 10, 7) as seventeen:
            chronology.append("adder_in_use")
            raise SampleTransferOfControl()
    except SampleTransferOfControl:
        pass

    # No magic here. If the fixture doesn't clean up after itself, that's all we can do.
    assert chronology == ["adder_before", "adder_in_use"]

    @pytest.yield_fixture()
    def robust_adder(n, m):
        try:
            chronology.append("robust_adder_before")
            yield n + m
        finally:
            chronology.append("robust_adder_after")

    chronology = []

    try:
        with call_fixture(robust_adder, 10, 7) as seventeen:
            chronology.append("robust_adder_in_use")
            raise SampleTransferOfControl()
    except SampleTransferOfControl:
        pass

    assert chronology == ["robust_adder_before", "robust_adder_in_use", "robust_adder_after"]
