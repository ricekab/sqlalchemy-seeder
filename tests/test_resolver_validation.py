"""
Tests to ensure validation catches badly formatted data objects.
"""
import pytest
from jsonschema import ValidationError

from sqlalchemyseeder import ResolvingSeeder


@pytest.fixture()
def resolver_empty(session):
    return ResolvingSeeder(session=session)


LIST_ROOT_ERROR = [
    {
        "data": {
            "name": "United Kingdom",
            "short": "UK"
        }
    }
]


def test_resolver_bad_root_element(model, resolver_empty, session):
    with pytest.raises(ValidationError):
        resolver_empty.load_entities_from_data_dict(LIST_ROOT_ERROR)


META_UNKNOWN = {
    "!unknown-meta-tag": "?"
}


def test_resolver_meta_unknown(model, resolver_empty, session):
    with pytest.raises(ValidationError):
        resolver_empty.load_entities_from_data_dict(META_UNKNOWN, commit=True)
