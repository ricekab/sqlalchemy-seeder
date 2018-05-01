import importlib
import inspect as pyinsp
import logging

from sqlalchemy import inspect as sainsp
from sqlalchemy.exc import NoInspectionAvailable

_logger = logging.getLogger(__name__)

MODULE_DEPTH_SEPARATOR = '#'
MODULE_CLASS_SEPARATOR = ':'


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
            if MODULE_CLASS_SEPARATOR not in target:
                depth = 0
                if MODULE_DEPTH_SEPARATOR in target:
                    target, depth = target.split(MODULE_DEPTH_SEPARATOR)
                    depth = int(depth)
                target_module = importlib.import_module(target)
                return self.register_module(target_module, depth)
            try:
                target_module, target_class = target.split(MODULE_CLASS_SEPARATOR)
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
                    _logger.debug("Inspecting submodule '{}' (remaining depth: {})".format(attr.__name__, depth - 1))
                    mappable_classes = mappable_classes.union(self.register_module(attr, depth=depth - 1))
        _logger.debug("Found {} mappable classes in {}".format(len(mappable_classes), module_.__name__))
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
        if MODULE_CLASS_SEPARATOR not in target:
            for cls in self.registered_classes:
                if cls.__name__ == target:
                    return cls
            raise AttributeError("No registered class found for '{}'".format(target))
        if target in self.class_path_cache:
            return self.class_path_cache[target]
        else:
            return self.register(target)
