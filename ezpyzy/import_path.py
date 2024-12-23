
import sys
import pathlib as pl
import importlib as imp
import importlib.util as impu
import os

def get_import_path(cls_or_fn):
    module = cls_or_fn.__module__
    if module is None or module == str.__class__.__module__:
        return cls_or_fn.__qualname__
    elif module == '__main__':
        module_path = pl.Path(sys.modules[module].__file__)
        shortest_common_path = None
        for pathstr in [os.getcwd(), *os.environ.get('PYTHONPATH', '').split(':')]:
            import_from_path = pl.Path(pathstr)
            if module_path.is_relative_to(import_from_path):
                common_path = module_path.relative_to(import_from_path)
                if shortest_common_path is None or len(str(common_path)) < len(str(shortest_common_path)):
                    shortest_common_path = common_path
        module = str(shortest_common_path)[:-len(shortest_common_path.suffix)].replace('/', '.')
    return module + '.' + cls_or_fn.__qualname__

def import_obj_from_path(import_path):
    module, name = import_path.rsplit('.', 1)
    main_module_path = pl.Path(sys.modules['__main__'].__file__).absolute()
    cwd = pl.Path.cwd()
    main_module_path = main_module_path.relative_to(cwd)
    main_module = '.'.join((*main_module_path.parts[:-1], main_module_path.stem))
    if module == main_module:
        module = '__main__'
    if module not in sys.modules:
        sys.modules[module] = imp.import_module(module)
    obj = getattr(sys.modules[module], name)
    return obj