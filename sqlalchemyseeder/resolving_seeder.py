import importlib
import inspect as pyinsp
import json
import os
from collections import defaultdict, namedtuple

import jsonschema
import pkg_resources
import yaml
from sqlalchemy import inspect as sainsp
from sqlalchemy.exc import NoInspectionAvailable
from sqlalchemy.orm.exc import MultipleResultsFound
from sqlalchemyseeder.exceptions import AmbiguousReferenceError, UnresolvedReferencesError, EntityBuildError
from sqlalchemyseeder.util import UniqueDeque

VALIDATION_SCHEMA_RSC = 'resources/resolver.schema.json'
DEFAULT_VERSION = 1


def _is_mappable_class(cls):
    try:
        return pyinsp.isclass(cls) and sainsp(cls).mapper
    except NoInspectionAvailable:
        return False


class ClassRegistry(object):
    """ A cache of mappable classes used by :class:`~sqlalchemyseeder.resolving_seeder.ResolvingSeeder`. """

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
        
        :param target:
        
            If `target` is a class, it is registered directly using :data:`register_class`.
        
            If `target` is a module, it registers all mappable classes using :data:`register_module`.
        
            If `target` is a string, it is first resolved into either a module or a class. Which look like:
        
                Module path: "path.to.module" or "path.to.module#<depth>" 
                
                Class path: "path.to.module:MyClass"
        
        :raise ValueError: If target string could not be parsed.
        :raise AttributeError: If target string references a class that does not exist.
        """
        if type(target) is str:
            if ':' not in target:
                depth = 1
                if '#' in target:
                    target, depth = target.split('#')
                    depth = int(depth)
                target_module = importlib.import_module(target, depth)
                return self.register_module(target_module)
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
        """
        Registers the given class with its full class path in the cache.
        
        :param cls: The class to register.
        :return: The class that was passed.
        :raise ValueError: If the class is not mappable (no associated SQLAlchemy mapper).
        """
        if not _is_mappable_class(cls):
            raise ValueError("Class {} does not have an associated mapper.".format(cls.__name__))
        self.class_path_cache[cls.__module__ + ':' + cls.__name__] = cls
        return cls

    def register_module(self, module_, depth=0):
        """
        Retrieves all classes from the given module that are mappable. 
        
        :param module_: The module to inspect.
        :param depth: How deep to recurse into the module to search for mappable classes. Default is 0.
        :return: A set of all mappable classes that were found. 
        """
        module_attrs = [getattr(module_, attr) for attr in dir(module_) if not attr.startswith('_')]
        mappable_classes = {cls for cls in module_attrs if _is_mappable_class(cls)}
        if depth > 0:
            for attr in module_attrs:
                if pyinsp.ismodule(attr):
                    mappable_classes = mappable_classes.union(self.register_module(attr, depth=depth - 1))
        for cls in mappable_classes:
            self.register_class(cls)
        return mappable_classes

    def get_class_for_string(self, target):
        """
        Look for class in the cache. If it cannot be found and a full classpath is provided, it is first registered 
        before returning.
        
        :param target: The class name or full classpath.
        :return: The class defined by the target.
        :raise AttributeError: If there is no registered class for the given target.
        """
        if ':' not in target:
            for cls in self.registered_classes:
                if cls.__name__ == target:
                    return cls
            raise AttributeError("No registered class found for '{}'".format(target))
        if target in self.class_path_cache:
            return self.class_path_cache[target]
        else:
            return self.register(target)

MetaData = namedtuple("MetaData", ("version",))

class ResolvingSeeder(object):
    """ Seeder that can resolve entities with references to other entities. 
    
    This requires the data to be formatted in a custom :ref:`data-format` to define the references.
    
    As entities have to define their target class they must be registered so the sqlalchemyseeder can retrieve them during the 
    seeding process. This is typically done using :meth:`~sqlalchemyseeder.resolving_seeder.ClassRegistry.register`, 
    :meth:`~sqlalchemyseeder.resolving_seeder.ClassRegistry.register_class` or 
    :meth:`~sqlalchemyseeder.resolving_seeder.ClassRegistry.register_module` which are
    hoisted methods from :class:`~sqlalchemyseeder.resolving_seeder.ClassRegistry`. If a classpath is encountered but not
    recognized it will be resolved before continuing.
    
    The session passed to this sqlalchemyseeder is used to resolve references. Flushes may occur depending on the session
    configuration and the passed parameters. The default behaviour when loading entities is to perform flushes but not 
    to commit.
    """

    def __init__(self, session):
        self.session = session
        schema_string = pkg_resources.resource_string('sqlalchemyseeder', VALIDATION_SCHEMA_RSC)
        self.validation_schema = json.loads(schema_string)
        self.registry = ClassRegistry()

    def load_entities_from_data_dict(self, seed_data, separate_by_class=False, flush_on_create=True, commit=False):
        """
        Create entities from the given dictionary.
        
        By default each entity is flushed into the provided session when it is created. This is useful if you want to
        reference them by id in other entities.
         
        If this behaviour is not wanted (eg. the created entities are incomplete) you can disable it by setting 
        `flush_on_create` to False when loading entities. The provided session can still flush if it is configured with
        `autoflush=True`.
        
        No commit is issued unless `commit` is set to True.
        
        :param seed_data: The formatted entity dict or list. This collection can be modified by the resolver.
        :param separate_by_class: Whether the output should separate entities by class (in a dict).
        :param flush_on_create: Whether entities should be flushed once they are created.
        :param commit: Whether the session should be committed after entities are generated.
        :return: List of entities or a dictionary mapping of classes to a list of entities based on `separate_by_class`.
        :raise ValidationError: If the provided data does not conform to the expected data structure.
        :raise AmbiguousReferenceError: If one or more references provided refer to more than one entity.
        :raise UnresolvedReferencesError: If one or more references could not be resolved (eg. they don't exist).
        """
        generated_entities = self.generate_entities(seed_data, flush_on_create)
        if commit:
            self.session.commit()
        if separate_by_class:
            entity_dict = defaultdict(list)
            for e in generated_entities:
                entity_dict[e.__class__].append(e)
            return entity_dict
        return generated_entities

    def generate_entities(self, seed_data, flush_on_create=True):
        """ Parses the data dictionary to generate the entities. """
        jsonschema.validate(seed_data, self.validation_schema)
        meta_data = self.parse_meta_tags(seed_data)
        # Todo: Backwards compat support here based on metadata version.
        resolver = ReferenceResolver(session=self.session, registry=self.registry, flush_on_create=flush_on_create)
        return resolver.generate_entities(seed_data)

    def parse_meta_tags(self, seed_data):
        """ Parses meta tags and executes relevant actions. """
        models = seed_data.pop("!models", [])
        for m in models:
            self.register(m)
        version = seed_data.pop("!version", DEFAULT_VERSION)
        return MetaData(version=version)

    def register(self, class_or_module_target):
        return self.registry.register(class_or_module_target)

    def register_class(self, cls):
        return self.registry.register_class(cls)

    def register_module(self, module_):
        return self.registry.register_module(module_)


class ResolvingFileSeeder(ResolvingSeeder):
    """ ResolvingSeeder extension to read from file path(s) """

    def __init__(self, session):
        super(ResolvingFileSeeder, self).__init__(session)
        self.data_queue = UniqueDeque()

    def queue_file(self, file_path):
        """ Reads in the given file and parses relevant metadata. Does not perform any seeding actions yet. """
        with open(file_path, 'rt') as yaml_file:
            data_string = yaml_file.read()
        data = yaml.load(data_string)
        jsonschema.validate(data, self.validation_schema)
        referenced_files = data.pop("!files", [])
        self.data_queue.append(data)
        for file_ in referenced_files:
            self.queue_file(os.path.join(os.path.dirname(file_path), file_))

    def load_entities(self, separate_by_class=False, flush_on_create=True, commit=False):
        """ 
        Load entities from queued up files.
         
        See :any:`load_entities_from_data_dict`
        """
        entities = []
        while self.data_queue:
            entities.extend(self.generate_entities(self.data_queue.pop(), flush_on_create))
        if commit:
            self.session.commit()
        if separate_by_class:
            entity_dict = defaultdict(list)
            for e in entities:
                entity_dict[e.__class__].append(e)
            return entity_dict
        return entities


class ReferenceResolver(object):
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
