from typing import Protocol


class ServerlessFunctionProvider(Protocol):
    def enable_triggers(self, function_name: str) -> None:
        """enable function event source mappings"""
        ...

    def disable_triggers(self, function_name: str) -> None:
        """disable function event source mappings"""
        ...
