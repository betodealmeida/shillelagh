"""
Registry for adapters.

Inspired by SQLAlchemy's ``PluginLoader``.
"""

import logging
from collections import defaultdict
from typing import Dict, List, Optional, Type, cast

from pkg_resources import iter_entry_points

from shillelagh.adapters.base import Adapter
from shillelagh.exceptions import InterfaceError

_logger = logging.getLogger(__name__)


class AdapterLoader:
    """
    Adapter registry, allowing new adapters to be registered.
    """

    def __init__(self):
        self.loaders = defaultdict(list)
        for entry_point in iter_entry_points("shillelagh.adapter"):
            self.loaders[entry_point.name].append(entry_point.load)

    def load(self, name: str, safe: bool = False) -> Type[Adapter]:
        """
        Load a given entry point by its name.
        """
        if safe and len(self.loaders[name]) > 1:
            raise InterfaceError(f"Multiple adapters found with name {name}")

        for load in self.loaders[name]:
            try:
                return cast(Type[Adapter], load())
            except (ImportError, ModuleNotFoundError) as ex:
                _logger.warning("Couldn't load adapter %s", name)
                _logger.debug(ex)
                continue

        raise InterfaceError(f"Unable to load adapter {name}")

    def load_all(
        self,
        adapters: Optional[List[str]] = None,
        safe: bool = False,
    ) -> Dict[str, Type[Adapter]]:
        """
        Load all the adapters given a list of names.

        If ``safe`` is True all adapters must be safe and present in the list of names.
        Otherwise adapters can be unsafe, and if the list is ``None`` everything is
        returned.
        """
        return self._load_all_safe(adapters) if safe else self._load_all(adapters)

    def _load_all_safe(
        self,
        adapters: Optional[List[str]] = None,
    ) -> Dict[str, Type[Adapter]]:
        """
        Load all safe adapters.

        If no adapters are specified, return none.
        """
        if not adapters:
            return {}

        loaded_adapters = {
            name: self.load(name, safe=True)
            for name in self.loaders
            if name in adapters
        }

        return {
            name: adapter for name, adapter in loaded_adapters.items() if adapter.safe
        }

    def _load_all(
        self,
        adapters: Optional[List[str]] = None,
    ) -> Dict[str, Type[Adapter]]:
        """
        Load all adapters, safe and unsafe.

        If no adapters are specified, return all.
        """
        all_adapters = {name: self.load(name, safe=False) for name in self.loaders}

        if adapters is None:
            return all_adapters

        return {
            name: adapter for name, adapter in all_adapters.items() if name in adapters
        }

    def register(self, name: str, modulepath: str, classname: str) -> None:
        """
        Register a new adapter.
        """

        def load() -> Type[Adapter]:
            module = __import__(modulepath)
            try:
                for token in modulepath.split(".")[1:]:
                    module = getattr(module, token)
                return cast(Type[Adapter], getattr(module, classname))
            except AttributeError as ex:
                raise ModuleNotFoundError(
                    f"Unable to load {classname} from {modulepath}",
                ) from ex

        self.loaders[name].append(load)

    def add(self, name: str, adapter: Type[Adapter]) -> None:
        """
        Add an adapter class directly.
        """
        self.loaders[name].append(lambda: adapter)

    def clear(self) -> None:
        """
        Remove all registered adapters.
        """
        self.loaders = defaultdict(list)


registry = AdapterLoader()
