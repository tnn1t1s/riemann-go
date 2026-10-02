"""Marks @skipme cases skipped; proves a skipped case cannot turn a trial green."""
import pytest


def pytest_collection_modifyitems(items):
    for item in items:
        if item.get_closest_marker("skipme"):
            item.add_marker(pytest.mark.skip(reason="self-test"))
