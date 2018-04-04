"""
Tests for resolver with model classes preconfigured.
"""
import pytest
from jsonschema import ValidationError
from sqlalchemyseeder.exceptions import UnresolvedReferencesError, AmbiguousReferenceError
from sqlalchemyseeder.resolving_seeder import ResolvingSeeder


@pytest.fixture()
def resolver_populated(session, model):
    seeder = ResolvingSeeder(session=session)
    seeder.registry.register_class(model.Airport)
    seeder.registry.register_class(model.Country)
    return seeder


COUNTRY_SINGLE_OK = {
    "Country": {
        "name": "United Kingdom",
        "short": "UK"
    }
}


def test_resolver_basic(model, resolver_populated, session):
    entities = resolver_populated.load_entities_from_data_dict(COUNTRY_SINGLE_OK, commit=True)
    assert len(entities) == 1
    country = entities[0]
    retrieved_countries = session.query(model.Country).all()
    assert len(retrieved_countries) == 1
    assert country in retrieved_countries
    assert country.name == "United Kingdom"
    assert country.short == "UK"


COUNTRY_LIST_OK = {
    "Country": [
        {
            "name": "United Kingdom",
            "short": "UK"
        }, {
            "name": "Belgium",
            "short": "BE"
        }
    ]
}


def test_resolver_combined(model, resolver_populated, session):
    entities = resolver_populated.load_entities_from_data_dict(COUNTRY_LIST_OK, commit=True)
    assert len(entities) == 2
    retrieved_countries = session.query(model.Country).all()
    assert len(retrieved_countries) == 2
    for e in entities:
        assert e in retrieved_countries


COUNTRY_LIST_SEPARATE_OK = [
    {
        "target_class": "Country",
        "data":
            {
                "name": "United Kingdom",
                "short": "UK"
            }

    },
    {
        "target_class": "Country",
        "data": {
            "name": "Belgium",
            "short": "BE"
        }
    }
]


def test_resolver_separate(model, resolver_populated, session):
    entities = resolver_populated.load_entities_from_data_dict(COUNTRY_LIST_SEPARATE_OK, commit=True)
    assert len(entities) == 2
    retrieved_countries = session.query(model.Country).all()
    assert len(retrieved_countries) == 2
    for e in entities:
        assert e in retrieved_countries


AIRPORT_COUNTRY_REFERENCE_ENTITY_OK = {
    "Airport": {
        "icao": "EGLL",
        "name": "London Heathrow",
        "!refs": {
            "country": {
                "target_class": "Country",
                "criteria": {
                    "short": "UK"
                }
            }
        }
    }
}


def test_resolver_reference_entity(model, resolver_populated, session):
    country = model.Country(name="United Kingdom", short="UK")
    session.add(country)
    session.commit()
    entities = resolver_populated.load_entities_from_data_dict(AIRPORT_COUNTRY_REFERENCE_ENTITY_OK, commit=True)
    assert len(entities) == 1
    airport = entities[0]
    assert airport.country.id == airport.country_id == country.id
    assert airport.icao == "EGLL"
    assert airport.name == "London Heathrow"
    assert airport.country.short == "UK"
    assert airport.country.name == "United Kingdom"


AIRPORT_COUNTRY_REFERENCE_FIELD_OK = {
    "Airport": {
        "icao": "EGLL",
        "name": "London Heathrow",
        "!refs": {
            "country_id": {
                "target_class": "Country",
                "criteria": {
                    "short": "UK"
                },
                "field": "id"
            }
        }
    }
}


def test_resolver_reference_field(model, resolver_populated, session):
    session.add(model.Country(name="United Kingdom", short="UK"))
    session.commit()
    entities = resolver_populated.load_entities_from_data_dict(AIRPORT_COUNTRY_REFERENCE_FIELD_OK, commit=True)
    assert len(entities) == 1
    airport = entities[0]
    assert airport.country.id == airport.country_id
    assert airport.country.short == "UK"
    assert airport.country.name == "United Kingdom"


AIRPORT_COUNTRY_REFERENCE_FIELD_UNRESOLVABLE = {
    "target_class": "Airport",
    "data": {
        "icao": "EGLL",
        "name": "London Heathrow",
        "!refs": {
            "country_id": {
                "target_class": "Country",
                "criteria": {
                    "short": "UK"
                },
                "field": "id"
            }
        }
    }
}


def test_resolver_bad_reference(model, resolver_populated, session):
    # UK never added
    assert len(session.query(model.Country).all()) == 0
    with pytest.raises(UnresolvedReferencesError):
        entities = resolver_populated.load_entities_from_data_dict(AIRPORT_COUNTRY_REFERENCE_FIELD_UNRESOLVABLE,
                                                                   commit=True)
        assert entities[0].country is None
        assert entities[0].country_id is None


AIRPORT_COUNTRY_REFERENCE_FIELD_AMBIGUOUS = {
    "target_class": "Airport",
    "data": {
        "icao": "EGLL",
        "name": "London Heathrow",
        "!refs": {
            "country_id": {
                "target_class": "Country",
                "criteria": {
                    "short": "UK"
                },
                "field": "id"
            }
        }
    }
}


def test_resolver_ambiguous_reference(model, resolver_populated, session):
    session.add(model.Country(name="United Kingdom", short="UK"))
    session.add(model.Country(name="United Kingdom 2", short="UK"))
    session.commit()
    assert len(session.query(model.Country).filter_by(short="UK").all()) == 2
    with pytest.raises(AmbiguousReferenceError):
        resolver_populated.load_entities_from_data_dict(AIRPORT_COUNTRY_REFERENCE_FIELD_AMBIGUOUS, commit=True)


AIRPORT_COUNTRY_PARALLEL_OK = {
    "Country": {
        "name": "United Kingdom",
        "short": "UK"
    },
    "Airport": {
        "icao": "EGLL",
        "name": "London Heathrow",
        "!refs": {
            "country": {
                "target_class": "Country",
                "criteria": {
                    "short": "UK"
                }
            }
        }
    }
}


def test_resolver_parallel(model, resolver_populated, session):
    entities = resolver_populated.load_entities_from_data_dict(AIRPORT_COUNTRY_PARALLEL_OK, commit=True)
    assert len(entities) == 2
    airport = session.query(model.Airport).first()
    assert airport.country.id == airport.country_id
    assert airport.country.short == "UK"
    assert airport.country.name == "United Kingdom"


AIRPORT_COUNTRY_REFERENCE_BY_ID = {
    "Country": {
        "!id": "country_uk",
        "name": "United Kingdom",
        "short": "UK"
    },
    "Airport": {
        "icao": "EGLL",
        "name": "London Heathrow",
        "country": "#country_uk"
    }
}


def test_resolver_reference_by_id(model, resolver_populated, session):
    entities = resolver_populated.load_entities_from_data_dict(AIRPORT_COUNTRY_REFERENCE_BY_ID, commit=True,
                                                               separate_by_class=True)
    assert len(entities[model.Airport]) == 1
    assert len(entities[model.Country]) == 1


AIRPORT_COUNTRY_REFERENCE_BY_ID_FIELD = {
    "Country": {
        "!id": "country_uk",
        "name": "United Kingdom",
        "short": "UK"
    },
    "Airport": {
        "icao": "EGLL",
        "name": "London Heathrow",
        "country_id": "#country_uk.id"
    }
}


def test_resolver_reference_by_id_field(model, resolver_populated, session):
    entities = resolver_populated.load_entities_from_data_dict(AIRPORT_COUNTRY_REFERENCE_BY_ID_FIELD, commit=True,
                                                               separate_by_class=True)
    assert len(entities[model.Airport]) == 1
    assert len(entities[model.Country]) == 1


AIRPORT_COUNTRY_REFERENCE_BY_ID_UNKNOWN = {
    "Country": {
        "!id": "country_uk",
        "name": "United Kingdom",
        "short": "UK"
    },
    "Airport": {
        "icao": "EGLL",
        "name": "London Heathrow",
        "country": "#country_uk"
    }
}


def test_resolver_reference_by_id_unknown(model, resolver_populated, session):
    with pytest.raises(UnresolvedReferencesError):
        resolver_populated.load_entities_from_data_dict(AIRPORT_COUNTRY_REFERENCE_BY_ID_UNKNOWN, commit=True,
                                                        separate_by_class=True)


JSON_STRING = '''
"Country": [
    {
        "name": "United Kingdom",
        "short": "UK"
    },
    {
        "name": "Belgium",
        "short": "BE"
    }
],
"Airport": {
    "icao": "EGLL",
    "name": "London Heathrow",
    "!refs": {
        "country": {
            "target_class": "Country",
            "criteria": {
                "short": "UK"
            }
        }
    }
}'''


def test_resolver_json_string(model, resolver_populated, session):
    entities = resolver_populated.load_entities_from_json_string(JSON_STRING, commit=True, separate_by_class=True)
    heathrow = session.query(model.Airport).filter_by(icao="EGLL").one()
    assert len(entities[model.Airport]) == len(session.query(model.Airport).all()) == 1
    assert len(entities[model.Country]) == len(session.query(model.Country).all()) == 2
    assert heathrow.name == "London Heathrow"
    assert heathrow.country == session.query(model.Country).filter_by(short="UK").one()


YAML_STRING = '''
- Country:
  - name: United Kingdom
    short: UK
  - name: Belgium
    short: BE
- Airport:
  icao: EGLL
  name: London Heathrow
  "!refs": 
    country:
      target_class: Country
      criteria:
        short: UK
'''


def test_resolver_yaml_string(model, resolver_populated, session):
    entities = resolver_populated.load_entities_from_yaml_string(YAML_STRING, commit=True, separate_by_class=True)
    heathrow = session.query(model.Airport).filter_by(icao="EGLL").one()
    assert len(entities[model.Airport]) == len(session.query(model.Airport).all()) == 1
    assert len(entities[model.Country]) == len(session.query(model.Country).all()) == 2
    assert heathrow.name == "London Heathrow"
    assert heathrow.country == session.query(model.Country).filter_by(short="UK").one()
