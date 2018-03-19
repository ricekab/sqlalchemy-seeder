import importlib
import inspect as pyinsp
import json

import jsonschema
import pkg_resources
from sqlalchemy import inspect as sainsp
from sqlalchemy.exc import NoInspectionAvailable

VALIDATION_SCHEMA_RSC = 'resources/resolver.schema.json'


def _is_mappable_class(cls):
    try:
        return pyinsp.isclass(cls) and sainsp(cls).mapper
    except NoInspectionAvailable:
        return False


class ClassRegistry(object):
    def __init__(self):
        self.class_path_cache = {}

    def __getitem__(self, item):
        return self.get_class_for_string(item)

    @property
    def registered_classes(self):
        return self.class_path_cache.values()

    def register(self, target):
        """ 
        Register module or class defined by target. 
        
        If target is a class, it is registered directly.
        If target is a module, it registers all classes in the module that are mappable.
        If target is a string, it is first resolved into either a module path or a class path.
        
        module path: path.to.module
        class path: path.to.module:MyClass
        """
        if type(target) is str:
            if ':' not in target:
                self.register_module(target)
            try:
                target_module, target_class = target.split(':')
                module_ = importlib.import_module(target_module)
                target_class = getattr(module_, target_class)
                self.register_class(target_class)
            except ValueError:
                raise ValueError("Couldn't separate module and class. Too many ':' symbols in '{}'?".format(target))
            except AttributeError:
                raise ValueError("No class '{}' in module '{}' found".format(target_class, target_module))
        if pyinsp.isclass(target):
            self.register_class(target)
        if pyinsp.ismodule(target):
            self.register_module(target)

    def register_class(self, cls):
        if not _is_mappable_class(cls):
            raise ValueError("Class {} does not have an associated mapper.".format(cls.__name__))
        self.class_path_cache[cls.__module__ + ':' + cls.__name__] = cls
        self.registered_classes.append(cls)

    def register_module(self, module_):
        module_attrs = [getattr(module_, attr) for attr in dir(module_) if not attr.startswith('_')]
        mappable_classes = {cls for cls in module_attrs if _is_mappable_class(cls)}
        for cls in mappable_classes:
            self.register_class(cls)

    def get_class_for_string(self, target):
        if ':' not in target:
            for cls in self.registered_classes:
                if cls.__name__ == target:
                    return cls
        if target in self.class_path_cache:
            return self.class_path_cache[target]
        raise AttributeError("No registered class found for '{}'".format(target))


class ResolvingSeeder(object):
    """ Seeder that can resolve references and nested entities. Entity classes must first be registered with the seeder
    so it can retrieve them during the seeding process. 
    
    Entities produced by this seeder are flushed into the provided session to generate ids as they are required. """

    def __init__(self, session):
        self.session = session
        schema_string = pkg_resources.resource_string('jsonseeder', VALIDATION_SCHEMA_RSC)
        self.validation_schema = json.loads(schema_string)
        self.registry = ClassRegistry()

    def load_entities_from_file(self, seed_file, separate_by_class=False, commit=False):
        with open(seed_file, 'rt') as json_file:
            json_string = json_file.read()
        json_dict = json.loads(json_string)
        return self.load_entities_from_json_dict(json_dict, separate_by_class, commit)

    def load_entities_from_json_dict(self, seed_dict, separate_by_class=False, commit=False):
        """ Returns a list of entity objects or a dictionary with a list of objects for each class. """
        jsonschema.validate(seed_dict, self.validation_schema)
