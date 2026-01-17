import shutil
import tempfile
from pathlib import Path

tmp_dir = Path(tempfile.gettempdir())


def zip_package(package_parent_dir: Path, package_dir_name: str) -> Path:
    """Create a .zip, can be used in --py-files with Spark Applications.

    Parameters
    ----------
    package_parent_dir : Path
    package_dir_name : str

    Returns
    -------
    Path
        Path to the created .zip

    Examples
    --------

    >>> zip_path = zip_package(
    ...     package_parent_dir=Path.home().joinpath('repos', 'flow4df'),
    ...     package_dir_name='flow4df')
    >>> zip_path
    PosixPath('/tmp/dist_flow4df/flow4df_package.zip')
    """
    dist_dir = tmp_dir.joinpath(f'dist_{package_dir_name}')
    shutil.rmtree(dist_dir, ignore_errors=True)  # clean the `dist` dir

    zip_path = dist_dir.joinpath(f'{package_dir_name}_package')
    _path = shutil.make_archive(
        base_name=zip_path.as_posix(),
        format='zip',
        root_dir=package_parent_dir,
        base_dir=package_dir_name
    )
    return Path(_path)
