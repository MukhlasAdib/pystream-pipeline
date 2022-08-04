from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from typing import Any, Callable, List


_default_executor = ThreadPoolExecutor(max_workers=10)


def func_parallel_thread(
    funcs: List[Callable[[], Any]], executor: ThreadPoolExecutor = _default_executor
) -> Callable[[], Any]:
    """Create a function made of functions that are executed in parallel
    using ThreadPoolExecutor from concurrent module.

    If no executor is provided, a shared default ThreadPoolExecutor is used. This
    executor will not be killed by shutdown method. Therefore, make sure that the
    functions passed here can exit properly. To be safe, please pass your own executor.

    Args:
        funcs (List[Callable[[], Any]]): the list of functions
            to be executed. It is assumed that all of the arguments
            and output variables are already contained in each function.
        executor (ThreadPoolExecutor, optional): ThreadPoolExecutor instance from
            concurrent.futures that handles the threads. By default, executor
            managed by this package will be used.

    Returns:
        Callable[[], Any]: The returned function to execute the serial
        functions.
    """

    def wrapper():
        res = executor.map(lambda func: func(), funcs)
        [r for r in res]

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
