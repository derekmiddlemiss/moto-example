from typing import TypeVar

T = TypeVar('T')


def check_single_or_empty(in_list: list[T]) -> list[T]:
    if len(in_list) > 1:
        raise ValueError("Length of input list must be zero or one.")
    return in_list
