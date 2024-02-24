import inspect
from time import time, sleep
from datetime import timedelta
from dataclasses import dataclass
from typing import Callable, Literal
from types import ModuleType

LIMIT_RES_NONE = timedelta(seconds=0)
LIMIT_BUFFER_MS = 100

@dataclass
class EnvironmentVariable:
    value: str

@dataclass
class EventLoop:
    target: Callable


class _Function:
    def __init__(self, target: Callable):
        self.target = target
        self.signature = inspect.signature(target)

    def __call__(self, **kwargs):
        return self.target(**kwargs)


class _FunctionWithLimiter(_Function):
    history = []

    def __init__(self, target: Callable, limit_n: int = 0, limit_res: timedelta = LIMIT_RES_NONE):
        super().__init__(target)
        self.limit_n = limit_n
        self.limit_res = limit_res

    def __call__(self, **kwargs):
        if self.limit_n > 0:
            now = time()
            self.history.append(now)
            if len(self.history) >= self.limit_n:
                nth_request_time = self.history[-self.limit_n]
                elapsed = now - nth_request_time
                if remaining := (self.limit_res - elapsed) > 0:
                    sleep(remaining + LIMIT_BUFFER_MS/1000)

        return super().__call__(**kwargs)

    def clear_history(self):
        self.history = []


class Endpoint(_FunctionWithLimiter):
    def __init__(self,
                 target,
                 format_function: Callable | None,
                 return_type: Literal['long', 'wide'] = 'long',
                 limit_n: None | int = 0,
                 limit_res: timedelta = LIMIT_RES_NONE):
        super().__init__(target, limit_n, limit_res)
        self.format_function = format_function
        self.return_type = return_type

    def __call__(self, **kwargs):
        res = super().__call__(**kwargs)
        if self.format_function is not None:
            res = self.format_function(res)
        return res


class UtilityFunction(_FunctionWithLimiter):
    def __init__(self, target: Callable, run_first: bool = True):
        super().__init__(target)
        self.run_first = run_first


class Vendor:
    def __init__(self, mod: ModuleType):
        self.endpoints = {}
        for attr in dir(mod):
            value = getattr(mod, attr)
            print(value)
            if attr.isupper():
                setattr(self, attr, value)
            elif value.__class__.__name__ == 'Endpoint':
                self.endpoints[attr] = value

    def __getitem__(self, item):
        if item in self.endpoints:
            return self.endpoints[item]
        else:
            return getattr(self, item)
