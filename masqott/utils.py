""" Masqott utilities """

from typing import Any


# pylint: disable-next=too-few-public-methods
class LogTextWrapper:
    """ Simple text wrapper """

    def __init__(
            self,
            value: Any,
            max_length: int = 40,
            placeholder: str = "...",
    ) -> None:
        self._value = value
        self._max_length = max_length
        self._placeholder = placeholder

    def __str__(self) -> str:
        value = repr(self._value)
        if len(value) > self._max_length:
            return (
                f'{value[:self._max_length - len(self._placeholder) - 1]}'
                f'{self._placeholder}{value[-1]}')
        return value
