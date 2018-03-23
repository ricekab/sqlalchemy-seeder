import pytest
from seeder.basic_seeder import BasicSeeder


def test_basic_from_dict(model):
    airport_dict = {
        "icao": "EGLL",
        "name": "London Heathrow"
    }
    airport_entity = BasicSeeder.entity_from_dict(airport_dict, model.Airport)
    assert airport_entity.id is None
    assert airport_entity.icao == "EGLL"
    assert airport_entity.name == "London Heathrow"
    assert airport_entity.country is None


def test_basic_from_dict_invalid_key(model):
    with pytest.raises(TypeError):
        airport_dict = {
            "icao": "EGLL",
            "name": "London Heathrow",
            "bad_key": -1
        }
        BasicSeeder.entity_from_dict(airport_dict, model.Airport)
