import pytest
from jsonschema import ValidationError
from jsonseeder.exceptions import UnresolvedReferencesError, AmbiguousReferenceError
from jsonseeder.resolving_seeder import ResolvingSeeder


@pytest.fixture()
def resolver_empty(session):
    return ResolvingSeeder(session=session)


@pytest.fixture()
def resolver_populated(session, model):
    seeder = ResolvingSeeder(session=session)
    seeder.registry.register_class(model.Airport)
    seeder.registry.register_class(model.Country)
    return seeder


COUNTRY_SINGLE_OK = {
    "target_class": "Country",
    "data": {
        "name": "United Kingdom",
        "short": "UK"
    }
}


def test_resolver_basic(model, resolver_populated, session):
    entities = resolver_populated.load_entities_from_json_dict(COUNTRY_SINGLE_OK, commit=True)
    assert len(entities) == 1
    country = entities[0]
    retrieved_countries = session.query(model.Country).all()
    assert len(retrieved_countries) == 1
    assert country in retrieved_countries
    assert country.name == "United Kingdom"
    assert country.short == "UK"


COUNTRY_SINGLE_BAD_FORMAT = {
    "data": {
        "name": "United Kingdom",
        "short": "UK"
    }
}


def test_resolver_single_bad_format(model, resolver_populated, session):
    with pytest.raises(ValidationError):
        resolver_populated.load_entities_from_json_dict(COUNTRY_SINGLE_BAD_FORMAT)


COUNTRY_LIST_COMBINED_OK = {
    "target_class": "Country",
    "data": [
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
    entities = resolver_populated.load_entities_from_json_dict(COUNTRY_LIST_COMBINED_OK, commit=True)
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
    entities = resolver_populated.load_entities_from_json_dict(COUNTRY_LIST_SEPARATE_OK, commit=True)
    assert len(entities) == 2
    retrieved_countries = session.query(model.Country).all()
    assert len(retrieved_countries) == 2
    for e in entities:
        assert e in retrieved_countries


AIRPORT_COUNTRY_REFERENCE_ENTITY_OK = {
    "target_class": "Airport",
    "data": {
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
    entities = resolver_populated.load_entities_from_json_dict(AIRPORT_COUNTRY_REFERENCE_ENTITY_OK, commit=True)
    assert len(entities) == 1
    airport = entities[0]
    assert airport.country.id == airport.country_id == country.id
    assert airport.icao == "EGLL"
    assert airport.name == "London Heathrow"
    assert airport.country.short == "UK"
    assert airport.country.name == "United Kingdom"


AIRPORT_COUNTRY_REFERENCE_FIELD_OK = {
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


def test_resolver_reference_field(model, resolver_populated, session):
    session.add(model.Country(name="United Kingdom", short="UK"))
    session.commit()
    entities = resolver_populated.load_entities_from_json_dict(AIRPORT_COUNTRY_REFERENCE_FIELD_OK, commit=True)
    assert len(entities) == 1
    airport = entities[0]
    assert airport.country.id == airport.country_id
    assert airport.country.short == "UK"
    assert airport.country.name == "United Kingdom"


AIRPORT_COUNTRY_REFERENCE_FIELD_BAD = {
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
        entities = resolver_populated.load_entities_from_json_dict(AIRPORT_COUNTRY_REFERENCE_FIELD_BAD, commit=True)
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
        resolver_populated.load_entities_from_json_dict(AIRPORT_COUNTRY_REFERENCE_FIELD_AMBIGUOUS, commit=True)


AIRPORT_COUNTRY_PARALLEL_OK = [
    {
        "target_class": "Country",
        "data":
            {
                "name": "United Kingdom",
                "short": "UK"
            }

    },
    {
        "target_class": "Airport",
        "data": {
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
]


def test_resolver_parallel(model, resolver_populated, session):
    entities = resolver_populated.load_entities_from_json_dict(AIRPORT_COUNTRY_PARALLEL_OK, commit=True)
    assert len(entities) == 2
    airport = session.query(model.Airport).first()
    assert airport.country.id == airport.country_id
    assert airport.country.short == "UK"
    assert airport.country.name == "United Kingdom"

# Inline nested structure makes it too complex so the feature is not planned currently.

# AIRPORT_COUNTRY_INLINE_OK = {
#     "target_class": "Airport",
#     "data": {
#         "icao": "EGLL",
#         "name": "London Heathrow",
#         "!inline": {
#             "country": {
#                 "target_class": "Country",
#                 "data": {
#                     "short": "UK",
#                     "name": "United Kingdom"
#                 }
#             }
#         }
#     }
# }
#
#
# def test_resolver_inline(model, resolver_populated, session):
#     entities = resolver_populated.load_entities_from_json_dict(AIRPORT_COUNTRY_INLINE_OK, commit=True)
#     assert len(entities) == 1
#     airport = session.query(model.Airport).first()
#     assert airport.country.id == airport.country_id
#     assert airport.country.short == "UK"
#     assert airport.country.name == "United Kingdom"
#
#
# def test_resolver_inline_uses_existing(model, resolver_populated, session):
#     session.add(model.Country(short="UK", name="United Kingdom"))
#     session.commit()
#     entities = resolver_populated.load_entities_from_json_dict(AIRPORT_COUNTRY_INLINE_OK, commit=True)
#     assert len(session.query(model.Country).all()) == 1
#     assert len(entities) == 1
#     airport = session.query(model.Airport).first()
#     assert airport.country.id == airport.country_id
#     assert airport.country.short == "UK"
#     assert airport.country.name == "United Kingdom"
#
#
# COUNTRY_AIRPORTS_INLINE_MULTIPLE_OK = {
#     "target_class": "Country",
#     "data": {
#         "short": "UK",
#         "name": "United Kingdom",
#         "!inline": {
#             "airports": {
#                 "target_class": "Country",
#                 "data": [
#                     {
#                         "icao": "EGLL",
#                         "name": "London Heathrow"
#                     }, {
#                         "icao": "EGKK",
#                         "name": "London Gatwick"
#                     }
#                 ]
#             }
#         }
#     }
# }
#
#
# def test_resolver_inline_multiple(model, resolver_populated, session):
#     entities = resolver_populated.load_entities_from_json_dict(COUNTRY_AIRPORTS_INLINE_MULTIPLE_OK, commit=True)
#     assert len(entities) == 1
#     assert len(session.query(model.Airport).all()) == 2
#     country = session.query(model.Country).first()
#     assert country.short == "UK"
#     assert country.name == "United Kingdom"
#     assert len(country.airports) == 2
