"""
Tests for resolver with model classes preconfigured.
"""
import json

import pytest
import yaml

from sqlalchemyseeder.exceptions import UnresolvedReferencesError, AmbiguousReferenceError
from sqlalchemyseeder.resolving_seeder import ResolvingSeeder

INITIAL_DATA_YAML = '''
Country:
  - name: United Kingdom
    short: UK
  - name: Belgium
    short: BE
Airport:
  - id: 999
    icao: EGLL
    name: London Heathrow
    country: "!Country?short=UK"
  - icao: EGKK    
    name: London Gatwick
    country: "!Country?short=UK"
  - icao: EBBR
    name: Brussuls Zaventem
    country: "!Country?short=BE"
  - icao: EBAW
    name: Antwerpen Deurne
    country: "!Country?short=BE"
'''


@pytest.fixture()
def resolver(session, model):
    seeder = ResolvingSeeder(session=session)
    seeder.register_class(model.Airport)
    seeder.register_class(model.Country)
    seeder.register_class(model.User)
    seeder.register_class(model.Address)
    data = yaml.load(INITIAL_DATA_YAML)
    seeder.load_entities_from_data_dict(data)
    return seeder


def test_initial_data_state(resolver, session, model):
    assert len(session.query(model.Airport).all()) == 4
    assert len(session.query(model.Country).all()) == 2


ADDING_NEW_DATA = {
    "Country": {
        "name": "Netherlands",
        "short": "NL"
    },
    "Airport": [{
        "icao": "EGCC",
        "name": "Manchester Intl",
        "country": "!Country?short=UK"
    }, {
        "icao": "EHAM",
        "name": "Schiphol Intl",
        "country": "!Country?short=NL"
    }
    ]
}


def test_adding_data(resolver, session, model):
    assert len(session.query(model.Airport).all()) == 4
    assert len(session.query(model.Country).all()) == 2
    new_entities = resolver.load_entities_from_data_dict(ADDING_NEW_DATA)
    assert len(new_entities) == 3
    assert len(session.query(model.Airport).all()) == 6
    assert len(session.query(model.Country).all()) == 3


ADDING_EXISTING_DATA = {
    "Airport": {
        "id": 999,
        "icao": "EGLL",
        "name": "London Heathrow",
        "country": "!Country?short=UK"
    }
}


def test_merge_existing_data(resolver, session, model):
    assert len(session.query(model.Airport).all()) == 4
    assert len(session.query(model.Country).all()) == 2
    resolver.load_entities_from_data_dict(ADDING_EXISTING_DATA)
    assert len(session.query(model.Airport).all()) == 4
    assert len(session.query(model.Country).all()) == 2
