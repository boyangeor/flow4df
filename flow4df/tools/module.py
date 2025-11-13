import os
import pkgutil
import importlib
import itertools
from pathlib import Path
from zipfile import ZipFile
from types import ModuleType


def list_zip_dirs(root_package_abs_path: Path) -> list[Path]:
    zip_part_indices = [
        i for i, part in enumerate(root_package_abs_path.parts)
        if part.endswith('.zip')
    ]
    _m = f'Multiple .zips on `{root_package_abs_path}`'
    assert len(zip_part_indices) == 1, _m
    zip_part_index = zip_part_indices[0]

    zip_abs_path = Path(*root_package_abs_path.parts[:zip_part_index + 1])

    all_zip_dirs = [
        Path(zip_abs_path, d.filename)
        for d in ZipFile(zip_abs_path).infolist() if d.is_dir()
    ]
    # Remove the non relative paths
    return [
        d for d in all_zip_dirs if d.is_relative_to(root_package_abs_path)
    ]


def list_all_subdirs(root_package_abs_path: Path) -> list[Path]:
    # Try with `rglob`, if the root package is in a .zip file it will return an
    # empty list
    all_dirs = [d for d in root_package_abs_path.rglob('**') if d.is_dir]
    if len(all_dirs) > 0:
        return all_dirs

    all_dirs = list_zip_dirs(root_package_abs_path)
    # log WARNING, that no dirs can be found!
    return all_dirs


def find_modules(
    root_package: str,
    root_package_abs_path: Path,
    abs_dir: Path
) -> list:
    modules = []
    module_components = [root_package]
    rel_sub_path = abs_dir.relative_to(root_package_abs_path)
    rel_sub_path_posix = rel_sub_path.as_posix()
    if rel_sub_path_posix == '.':
        rel_sub_path_posix = ''
    else:
        module_components.append(rel_sub_path_posix.replace(os.path.sep, '.'))

    for module_info in pkgutil.iter_modules([abs_dir.as_posix()]):
        if module_info.ispkg:
            continue

        abs_module = '.'.join(module_components + [module_info.name])
        m = importlib.import_module(abs_module)
        modules.append(m)

    return modules


def list_modules(root_package: str) -> list[ModuleType]:
    root_package_spec = importlib.util.find_spec(root_package)
    root_package_abs_path = Path(
        root_package_spec.submodule_search_locations[0]
    )
    all_subdirs = list_all_subdirs(root_package_abs_path)
    _modules = [
        find_modules(
            root_package=root_package,
            root_package_abs_path=root_package_abs_path,
            abs_dir=abs_dir
        )
        for abs_dir in all_subdirs
    ]
    return list(itertools.chain.from_iterable(_modules))
