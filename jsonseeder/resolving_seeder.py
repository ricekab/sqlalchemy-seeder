import importlib
import inspect as pyinsp
import json
from collections import defaultdict, namedtuple

import jsonschema
import pkg_resources
from jsonseeder.exceptions import AmbiguousReferenceError, UnresolvedReferencesError, EntityBuildError
from sqlalchemy import inspect as sainsp
from sqlalchemy.exc import NoInspectionAvailable
from sqlalchemy.orm.exc import MultipleResultsFound

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
                return self.register_module(target)
            try:
                target_module, target_class = target.split(':')
                module_ = importlib.import_module(target_module)
                target_class = getattr(module_, target_class)
                return self.register_class(target_class)
            except ValueError:
                raise ValueError("Couldn't separate module and class. Too many ':' symbols in '{}'?".format(target))
            except AttributeError:
                raise ValueError("No class '{}' in module '{}' found".format(target_class, target_module))
        if pyinsp.isclass(target):
            return self.register_class(target)
        if pyinsp.ismodule(target):
            return self.register_module(target)

    def register_class(self, cls):
        if not _is_mappable_class(cls):
            raise ValueError("Class {} does not have an associated mapper.".format(cls.__name__))
        self.class_path_cache[cls.__module__ + ':' + cls.__name__] = cls
        self.registered_classes.append(cls)
        return cls

    def register_module(self, module_):
        module_attrs = [getattr(module_, attr) for attr in dir(module_) if not attr.startswith('_')]
        mappable_classes = {cls for cls in module_attrs if _is_mappable_class(cls)}
        for cls in mappable_classes:
            self.register_class(cls)
        return mappable_classes

    def get_class_for_string(self, target):
        """ Look for class in the cache. If it cannot be found and a classpath is provided, attempt to register it. """
        if ':' not in target:
            for cls in self.registered_classes:
                if cls.__name__ == target:
                    return cls
            raise AttributeError("No registered class found for '{}'".format(target))
        if target in self.class_path_cache:
            return self.class_path_cache[target]
        else:
            return self.register(target)


class ResolvingSeeder(object):
    """ Seeder that can resolve entities with references. Entity classes must first be registered with the seeder
    so it can retrieve them during the seeding process. 
    
    By default each entity is flushed into the provided session when it is created. This is useful if you want to
     reference them by id in other entities.
     
    If this behaviour is not wanted (eg. the created entities from the file are incomplete) you can disable it by
    setting `flush_on_create` to False when loading entities.
    
    Commit is only done internally if `commit` is set to True, by default no commits are issued. """

    def __init__(self, session):
        self.session = session
        schema_string = pkg_resources.resource_string('jsonseeder', VALIDATION_SCHEMA_RSC)
        self.validation_schema = json.loads(schema_string)
        self.registry = ClassRegistry()

    def load_entities_from_file(self, seed_file, separate_by_class=False, flush_on_create=True, commit=False):
        with open(seed_file, 'rt') as json_file:
            json_string = json_file.read()
        json_data = json.loads(json_string)
        return self.load_entities_from_json_dict(json_data, separate_by_class, flush_on_create, commit)

    def load_entities_from_json_dict(self, seed_data, separate_by_class=False, flush_on_create=True, commit=False):
        """
        :param seed_data: The json formatted entity dict or list. This collection can be modified by the resolver.
        :param separate_by_class: Whether the output should separate entities by class (in a dict) 
        :param flush_on_create: Whether entities should be flushed once they are created. Note that the provided session
        could be configured with `autoflush=True` in which case flushes can still happen.
        :param commit: Whether the session should be committed after entities are generated.
        :return: List of entities or a dictionary mapping of classes to a list of entities based on separate_by_class.
        :raise ValidationError: If the provided data does not conform to the expected json structure. 
        """
        jsonschema.validate(seed_data, self.validation_schema)
        resolver = _ReferenceResolver(session=self.session, registry=self.registry, flush_on_create=flush_on_create)
        generated_entities = resolver.generate_entities(seed_data)
        if commit:
            self.session.commit()
        if separate_by_class:
            entity_dict = defaultdict(list)
            for e in generated_entities:
                entity_dict[e.__class__].append(e)
            return entity_dict
        return generated_entities

    def register(self, class_or_module_target):
        return self.registry.register(class_or_module_target)

    def register_class(self, cls):
        return self.registry.register_class(cls)

    def register_module(self, module_):
        return self.registry.register_module(module_)


class _ReferenceResolver(object):
    def __init__(self, session, registry, flush_on_create=False):
        self.session = session
        self.registry = registry
        self.flush_on_create = flush_on_create

    def generate_entities(self, seed_data):
        entity_builders = []
        if isinstance(seed_data, list):
            for group_data in seed_data:
                group_builders = self._generate_builders_from_group(group_data)
                entity_builders.extend(group_builders)
        if isinstance(seed_data, dict):
            group_builders = self._generate_builders_from_group(seed_data)
            entity_builders.extend(group_builders)
        return self._resolve_builders(entity_builders)

    def _generate_builders_from_group(self, entity_group_dict):
        """ Returns the entity or the list of entities that are defined in the group. """
        target_cls = self.registry.get_class_for_string(entity_group_dict["target_class"])
        target_data = entity_group_dict["data"]
        if isinstance(target_data, list):
            return [self._generate_builder_from_data_block(target_cls, data_block) for data_block in target_data]
        return [self._generate_builder_from_data_block(target_cls, target_data)]

    def _generate_builder_from_data_block(self, target_cls, data_dict):
        return _EntityBuilder(session=self.session, registry=self.registry, target_cls=target_cls, data_block=data_dict)

    def _resolve_builders(self, entity_builders):
        entities = []
        previous_builder_count = len(entity_builders)  # Safeguard against unresolvable references
        while len(entity_builders) > 0:
            resolved_builders = []
            for builder in entity_builders:
                if builder.resolve():
                    resolved_builders.append(builder)
            for resolved_builder in resolved_builders:
                entity = resolved_builder.build()
                self.session.add(entity)
                if self.flush_on_create:
                    self.session.flush()
                entities.append(entity)
                entity_builders.remove(resolved_builder)
            if previous_builder_count == len(entity_builders):  # No progress being made
                raise UnresolvedReferencesError(
                    "'{}' builders have unresolvable references.".format(len(entity_builders)))
            previous_builder_count = len(entity_builders)
        return entities


EntityReference = namedtuple("EntityRef", ['src_field', 'ref_cls', 'ref_filter_dict', 'ref_field'])


class _EntityBuilder(object):
    """ A builder corresponds to one entity block and thus can only ever build once. Multiple attempts to build will
     throw a EntityBuildError. """

    def __init__(self, session, registry, target_cls, data_block):
        self.session = session
        self.registry = registry
        self.target_cls = target_cls
        self.refs = self._init_refs(data_block.pop("!refs", {}))
        self.data_dict = data_block
        self.built = False

    def _init_refs(self, refs_block):
        refs = []
        for field, reference in refs_block.items():
            refs.append(EntityReference(src_field=field,
                                        ref_cls=self.registry.get_class_for_string(reference["target_class"]),
                                        ref_filter_dict=reference["criteria"],
                                        ref_field=reference["field"] if "field" in reference else ""))
        return refs

    @property
    def resolved(self):
        """ A builder is resolved if there are no more refs / inlines to resolve """
        return len(self.refs) == 0

    def build(self):
        if not self.resolved:
            raise UnresolvedReferencesError("Entity Builder has unresolved references.")
        if self.built:
            raise EntityBuildError("Entity Builder has already been used.")
        self.built = True
        return self.target_cls(**self.data_dict)

    def resolve(self):
        """ Return True if fully resolved, False otherwise. """
        if self.resolved:
            return True
        resolved_refs = []
        for ref in self.refs:  # type: EntityReference
            try:
                reference_entity = self.session.query(ref.ref_cls).filter_by(**ref.ref_filter_dict).one_or_none()
            except MultipleResultsFound:
                raise AmbiguousReferenceError("Matched more than one entity of class '{}'".format(ref.ref_cls))
            if reference_entity:
                resolved_refs.append(ref)
                if ref.ref_field:
                    self.data_dict[ref.src_field] = getattr(reference_entity, ref.ref_field)
                else:
                    self.data_dict[ref.src_field] = reference_entity
        for resolved in resolved_refs:
            self.refs.remove(resolved)
        return self.resolved
