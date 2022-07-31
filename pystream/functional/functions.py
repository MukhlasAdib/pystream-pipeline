from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from typing import Any, Callable, List


def func_parallel_thread(
    funcs: List[Callable[[], Any]], max_workers: int = 5
) -> Callable[[], Any]:
    """Create a function made of functions that are executed in parallel
    using ThreadPoolExecutor from concurrent module.

    Args:
        funcs (List[Callable[[], Any]]): the list of functions
            to be executed. It is assumed that all of the arguments
            and output variables are already contained in each function.
        max_workers (int, optional): Maximum threads to be made. Defaults to 5.

    Returns:
        Callable[[], Any]: The returned function to execute the serial
        functions.
    """

    def wrapper():
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            executor.map(lambda func: func(), funcs)

    return wrapper


def func_parallel_process(
    funcs: List[Callable[[], Any]], max_workers: int = 5
) -> Callable[[], Any]:
    """CURRENTLY UNSTABLE.
    Create a function made of functions that are executed in parallel
    using ProcessPoolExecutor from concurrent module.

    Args:
        funcs (List[Callable[[], Any]]): the list of functions
            to be executed. It is assumed that all of the arguments
            and output variables are already contained in each function.

    Returns:
        Callable[[], Any]: The returned function to execute the serial
        functions.
    """

    def wrapper():
        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            executor.map(lambda func: func(), funcs)

    return wrapper


def func_serial(funcs: List[Callable[[], Any]]) -> Callable[[], Any]:
    """Create a function made of functions that are executed in serial

    Args:
        funcs (List[Callable[[], Any]]): the list of functions
            to be executed. It is assumed that all of the arguments
            and output variables are already contained in each function.

    Returns:
        Callable[[], Any]: The returned function to execute the serial
        functions.
    """

    def wrapper():
        for func in funcs:
            func()

    return wrapper
