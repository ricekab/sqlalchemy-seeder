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
    "!models": [],
    "!files": []
}


def test_resolver_meta_only(model, resolver_empty, session):
    entities = resolver_empty.load_entities_from_data_dict(META_ONLY)
    assert len(entities) == 0


COUNTRY_INLINE = {
    "conftest:Country": {
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


MODEL_IMPORT = {
    "!models": ["conftest"],
    "Country": {
        "name": "United Kingdom",
        "short": "UK"
    },
    "Airport": {
        "icao": "EGLL",
        "name": "London Heathrow",
        "country": "!Country?short=UK"
    }
}


def test_resolver_meta_model(model, resolver_empty, session):
    entities = resolver_empty.load_entities_from_data_dict(MODEL_IMPORT, commit=True)
    assert len(entities) == 2
    countries = session.query(model.Country).all()
    assert len(countries) == 1
    assert countries[0].name == "United Kingdom"
    assert countries[0].short == "UK"
    airports = session.query(model.Airport).all()
    assert len(airports) == 1
    assert airports[0].name == "London Heathrow"
    assert airports[0].icao == "EGLL"
    assert airports[0].country == countries[0]
