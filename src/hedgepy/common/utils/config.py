import dotenv
import tomllib
from pathlib import Path
from dataclasses import dataclass


PROJECT_ROOT = dotenv.get_key(
    dotenv_path = dotenv.find_dotenv(),
    key_to_get = 'PROJECT_ROOT'
    )
SOURCE_ROOT = (Path(PROJECT_ROOT) / 'src' / 'hedgepy').resolve()


def _get_env_var(key: str, dotenv_path: str = PROJECT_ROOT) -> str:
    return dotenv.get_key(
        Path(dotenv_path) / '.env',
        key
        )


def _toml_path_from_dir(dir_path: str = PROJECT_ROOT) -> str:
    return Path(dir_path) / 'config.toml'


def _get_toml_vars(toml_path: str = PROJECT_ROOT) -> dict:
    toml_path = _toml_path_from_dir(toml_path)
    with toml_path.open('rb') as file:
        return tomllib.load(file)
    
    
def _get_toml_var(args: tuple[str], toml_path: str = PROJECT_ROOT) -> str:
    var = _get_toml_vars(toml_path)
    for arg in args:
        var = var[arg.strip("$")]
    return var
    

def get(*args: str, toml_path: str = PROJECT_ROOT) -> str:      
    if len(args) == 1:
        args = args[0].split('.')
    toml_var = _get_toml_var(args, toml_path)
    if isinstance(toml_var, str):
        if toml_var.startswith('$'):
            toml_var = _get_env_var(toml_var[1:])
    return toml_var


def _replace_str(value: str):
    if value.startswith('$'):
        return get(value[1:])
    return value


def _replace_tuple(tup: tuple) -> tuple:
    tup_out = ()
    for value in tup: 
        if isinstance(value, str):
            tup_out += (_replace_str(value),)
        elif isinstance(value, tuple):
            tup_out += (_replace_tuple(value),)
    return tup_out


def _replace_dict(di: dict) -> dict:
    for key, value in di.items():
        if isinstance(value, str):
            di[key] = _replace_str(value)
        elif isinstance(value, dict):
            di[key] = _replace_dict(value)
        elif isinstance(value, tuple):
            di[key] = _replace_tuple(value)
    return di


def replace(value: str | tuple | dict) -> str | tuple | dict:
    if isinstance(value, str):
        return _replace_str(value)
    elif isinstance(value, tuple):
        return _replace_tuple(value)
    elif isinstance(value, dict):
        return _replace_dict(value)
    else:
        raise ValueError(f"Unsupported type: {type(value)}")


@dataclass
class EnvironmentVariable:
    name: str
    value: str
    
    @classmethod
    def from_config(cls, key: str):
        return cls(name=key, value=get(key))
    