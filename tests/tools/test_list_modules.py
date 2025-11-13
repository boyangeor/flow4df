from flow4df.tools.module import list_modules


def test_list_modules() -> None:
    tools_modules = list_modules(root_package='flow4df.tools')
    _m = 'At least 1 module under `flow4df.tools` expected!'
    assert len(tools_modules) >= 1, _m
