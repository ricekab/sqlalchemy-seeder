"""
Tests for resolver with no additional configuration / registration.
"""

import pytest
from jsonschema import ValidationError

from sqlalchemyseeder import ResolvingSeeder


@pytest.fixture()
def resolver_empty(session):
    return ResolvingSeeder(session=session)


META_ONLY = {
    "!version": 2,
    "!classes": [],
    "!files": []
}


def test_resolver_meta_only(model, resolver_empty, session):
    entities = resolver_empty.load_entities_from_data_dict(META_ONLY)
    assert len(entities) == 0



COUNTRY_INLINE = {
    "conftest.Country": {
        "name": "United Kingdom",
        "short": "UK"
    }
}


def test_resolver_inline(model, resolver_empty, session):
    entities = resolver_empty.load_entities_from_data_dict(COUNTRY_INLINE, commit=True)
    assert len(entities) == 1
    country = entities[0]
    retrieved_countries = session.query(model.Country).all()
    assert len(retrieved_countries) == 1
    assert country in retrieved_countries
    assert country.name == "United Kingdom"
    assert country.short == "UK"
