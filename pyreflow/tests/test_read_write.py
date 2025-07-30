import pytest
import pyreflow as pf


@pytest.fixture
def blank_core_2_0() -> pf.CoreTEXT2_0:
    return pf.CoreTEXT2_0("L")


def test_dummy(blank_core_2_0: pf.CoreTEXT2_0) -> None:
    assert blank_core_2_0.mode == "L", "yay"
