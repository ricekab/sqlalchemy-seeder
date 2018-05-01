import json
import logging
import os
from collections import defaultdict, namedtuple

import jsonschema
import pkg_resources
import yaml
from sqlalchemy.orm.exc import MultipleResultsFound
from sqlalchemyseeder.exceptions import AmbiguousReferenceError, UnresolvedReferencesError, EntityBuildError
from sqlalchemyseeder.registry import ClassRegistry
from sqlalchemyseeder.util import UniqueDeque

VALIDATION_SCHEMA_RSC = 'resources/resolver.schema.json'

DEFAULT_FORMAT_VERSION = 1

META_CHARACTER = '!'
INLINE_REF_CHARACTER = '!'
ID_REF_CHARACTER = '#'
CRITERIA_SEPARATOR = '&'
REF_CLS_SEPARATOR = '?'
REF_FIELD_SEPARATOR = ':'
KEY_VALUE_SEPARATOR = '='

_logger = logging.getLogger(__name__)

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

    def load_entities_from_json_file(self, seed_file, separate_by_class=False, flush_on_create=True, commit=False):
        """
        Convenience method to read the given file and parse it as json.
        
        See: :data:`load_entities_from_data_dict`
        """
        with open(seed_file, 'rt') as json_file:
            json_string = json_file.read()
        return self.load_entities_from_json_string(json_string, separate_by_class, flush_on_create, commit)

    def load_entities_from_json_string(self, json_string, separate_by_class=False, flush_on_create=True, commit=False):
        """
        Parse the given string as json.
        
        See: :data:`load_entities_from_data_dict`
        """
        data = json.loads(json_string)
        return self.load_entities_from_data_dict(data, separate_by_class, flush_on_create, commit)

    def load_entities_from_yaml_file(self, seed_file, separate_by_class=False, flush_on_create=True, commit=False):
        """
        Convenience method to read the given file and parse it as yaml.
        
        See: :any:`load_entities_from_data_dict`
        """
        with open(seed_file, 'rt') as yaml_file:
            yaml_string = yaml_file.read()
        return self.load_entities_from_yaml_string(yaml_string, separate_by_class, flush_on_create, commit)

    def load_entities_from_yaml_string(self, yaml_string, separate_by_class=False, flush_on_create=True, commit=False):
        """
        Parse the given string as yaml.
        
        See: :any:`load_entities_from_data_dict`
        """
        data = yaml.load(yaml_string)
        return self.load_entities_from_data_dict(data, separate_by_class, flush_on_create, commit)

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
        version = seed_data.pop("!version", DEFAULT_FORMAT_VERSION)
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
        self.file_set = set()
        self.data_queue = UniqueDeque()

    def queue_file(self, file_path):
        """ Reads in the given file and parses relevant metadata. Does not perform any seeding actions yet. """
        if file_path in self.file_set:
            return  # File has already been traversed
        self.file_set.add(file_path)
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
        self.builder_mapping = {}
        self.flush_on_create = flush_on_create

    def generate_entities(self, seed_data):
        entity_builders = []
        for key, data in seed_data.items():
            if not key.startswith(META_CHARACTER):
                group_builders = self._generate_builders_from_group(key, data)
                entity_builders.extend(group_builders)
        _logger.info("Attempting to generate {} entities.".format(len(entity_builders)))
        return self._resolve_builders(entity_builders)

    def _generate_builders_from_group(self, target_string, target_data):
        """ Returns the entity or the list of entities that are defined in the group. """
        target_cls = self.registry.get_class_for_string(target_string)
        _logger.info("Generating builders for class: ".format(target_cls.__name__))
        if isinstance(target_data, list):
            return [self._generate_builder_from_data_block(target_cls, data_block) for data_block in target_data]
        return [self._generate_builder_from_data_block(target_cls, target_data)]

    def _generate_builder_from_data_block(self, target_cls, data_dict):
        builder = _EntityBuilder(self, target_cls=target_cls, data_block=data_dict)
        if "!id" in data_dict:
            self.builder_mapping[data_dict.pop("!id")] = builder
        return builder

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
EntityIdReference = namedtuple("EntityIdRef", ['src_field', 'ref_id', 'ref_field'])


class _EntityBuilder(object):
    """ A builder corresponds to one entity block and thus can only ever build once. Multiple attempts to build will
     throw a EntityBuildError. """

    def __init__(self, resolver, target_cls, data_block):
        self.resolver = resolver
        self.target_cls = target_cls
        self.refs = []
        self.id_refs = []
        self._init_refs(data_block)
        self.data_dict = data_block
        self.built_entity = None

    def _init_refs(self, data_block):
        for field, reference in data_block.pop("!refs", {}).items():
            _logger.debug("Parsing reference block".format(reference))
            self.refs.append(EntityReference(src_field=field,
                                             ref_cls=self.registry.get_class_for_string(reference["target_class"]),
                                             ref_filter_dict=reference["criteria"],
                                             ref_field=reference["field"] if "field" in reference else ""))
        for field, value in data_block.items():
            if value.startswith(INLINE_REF_CHARACTER):  # Inline reference
                self.refs.append(self._parse_inline_reference(field, value))
            if value.startswith(ID_REF_CHARACTER):  # ID reference
                self.id_refs.append(self._parse_id_reference(field, value))

    def _parse_inline_reference(self, field, reference_string):
        _logger.debug("Parsing inline reference: ".format(reference_string))
        reference_string = reference_string.strip(INLINE_REF_CHARACTER)
        ref_target, slug = reference_string.split(REF_CLS_SEPARATOR)
        ref_field = ""
        if REF_FIELD_SEPARATOR in slug:
            slug, ref_field = slug.split(REF_FIELD_SEPARATOR)
        criteria = {}
        for c in slug.split(CRITERIA_SEPARATOR):
            ref_key, ref_value = c.split(KEY_VALUE_SEPARATOR)
            # Todo: Numeric and other types?
            criteria[ref_key] = ref_value
        return EntityReference(src_field=field,
                               ref_cls=self.registry.get_class_for_string(ref_target),
                               ref_filter_dict=criteria,
                               ref_field=ref_field)

    def _parse_id_reference(self, field, reference_string):
        _logger.debug("Parsing id reference: ".format(reference_string))
        id_ = reference_string.strip(ID_REF_CHARACTER)
        ref_field = ""
        if REF_FIELD_SEPARATOR in id_:
            id_, ref_field = id_.split(REF_FIELD_SEPARATOR)
        return EntityIdReference(src_field=field, ref_id=id_, ref_field=ref_field)

    @property
    def resolved(self):
        """ A builder is resolved if there are no more refs / inlines to resolve """
        return len(self.refs) + len(self.id_refs) == 0

    @property
    def session(self):
        return self.resolver.session

    @property
    def registry(self):
        return self.resolver.registry

    @property
    def builder_mapping(self):
        return self.resolver.builder_mapping

    def build(self):
        if not self.resolved:
            raise UnresolvedReferencesError("Entity Builder has unresolved references.")
        if self.built_entity:
            raise EntityBuildError("Entity Builder has already been used.")
        self.built_entity = self.target_cls(**self.data_dict)
        return self.built_entity

    def resolve(self):
        """ Return True if fully resolved, False otherwise. """
        if self.resolved:
            return True
        self._resolve_refs()
        self._resolve_id_refs()
        return self.resolved

    def _resolve_refs(self):
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

    def _resolve_id_refs(self):
        resolved_refs = []
        for ref in self.id_refs:  # type: EntityIdReference
            if self.builder_mapping[ref.ref_id].built_entity:
                resolved_refs.append(ref)
                ref_entity = self.builder_mapping[ref.ref_id].built_entity
                if ref.ref_field:
                    self.data_dict[ref.src_field] = getattr(ref_entity, ref.ref_field)
                else:
                    self.data_dict[ref.src_field] = ref_entity
        for resolved in resolved_refs:
            self.id_refs.remove(resolved)
