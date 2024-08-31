import functools
import time
from abc import abstractmethod
from typing import Any, Callable, TypeVar

from .parser import get_config

T = TypeVar("T")


def execution_time(fn: Callable[[], T]) -> tuple[float, T]:
    start = time.perf_counter()
    result = fn()
    end = time.perf_counter()
    return (end - start, result)


class Processor:
    def __init__(self, path: str) -> None:
        self.path = path
        self.read_time
        self.process_time

    def run(self) -> None:
        self.read_time, data = execution_time(self.read)
        process = functools.partial(self.process, data=data)
        self.process_time, result = execution_time(process)
        self.cleanup()
        return result

    @abstractmethod
    def cleanup(self) -> None:
        pass

    @abstractmethod
    def read(self) -> Any:
        pass

    @abstractmethod
    def process(self, data: Any) -> None:
        pass


def main(processor_class: type[Processor], argv: list[str] | None = None) -> None:
    config = get_config(argv)
    processor = processor_class(config.path)
    processor.run()
