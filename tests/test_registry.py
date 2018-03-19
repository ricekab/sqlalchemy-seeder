import pytest
from jsonseeder.resolving_seeder import ClassRegistry


@pytest.fixture()
def registry_empty():
    return ClassRegistry()


def test_retrieval_string_class(model, registry_empty):
    registry_empty.register_class(model.Country)
    retrieved_class = registry_empty.get_class_for_string("Country")
    assert retrieved_class is model.Country


def test_retrieval_string_invalid_class(model, registry_empty):
    with pytest.raises(AttributeError):
        registry_empty.get_class_for_string("NotRegistered")


def test_retrieval_string_class_path(model, registry_empty):
    registry_empty.register_class(model.Country)
    retrieved_class = registry_empty.get_class_for_string("conftest:Country")
    assert retrieved_class is model.Country


def test_retrieval_string_invalid_class_path(model, registry_empty):
    with pytest.raises(AttributeError):
        registry_empty.get_class_for_string("conftest:NotRegistered")


def test_register_with_path(model, registry_empty):
    registry_empty.register('conftest:Airport')
    assert registry_empty.get_class_for_string('conftest:Airport') is model.Airport


def test_register_with_invalid_path(model, registry_empty):
    with pytest.raises(ValueError):
        registry_empty.register('conftest:woops:Airport')


def test_register_with_path_unknown_class(model, registry_empty):
    with pytest.raises(ValueError):
        registry_empty.register('conftest:Weather')
