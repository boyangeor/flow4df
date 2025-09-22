import pkgutil
import importlib
import setuptools
from types import ModuleType


def list_modules(package: str) -> list[ModuleType]:
    """Recursively find all modules in a package.

    Parameters
    ----------
    package : str
        The package to be listed e.g. 'flow4df.tools'

    Returns
    -------
    list of ModuleType
        List of all modules
    """
    spec = importlib.util.find_spec(package)
    package_posix_path = spec.submodule_search_locations[0]
    modules = []

    def _append_modules(
        modules: list, package_posix_path: str, sub_pkg: str
    ) -> None:
        for info in pkgutil.iter_modules([package_posix_path]):
            if not info.ispkg:
                name = f'{sub_pkg}.{info.name}'
                m = importlib.import_module(name=name, package=package)
                modules.append(m)

    # find top level modules
    _append_modules(modules, package_posix_path, sub_pkg='')

    # recursively find modules in subpackages, `find_namespace_packages` lists
    # the packages recursively
    for sub_pkg in setuptools.find_namespace_packages(package_posix_path):
        spec = importlib.util.find_spec(f'.{sub_pkg}', package)
        sub_pkg_posix_path = spec.submodule_search_locations[0]
        _append_modules(modules, sub_pkg_posix_path, sub_pkg=f'.{sub_pkg}')

    return modules
