from concurrent.futures import ThreadPoolExecutor
from typing import Any, Callable, List


_default_executor = ThreadPoolExecutor(max_workers=10)


def func_parallel_thread(
    funcs: List[Callable[[Any], Any]], executor: ThreadPoolExecutor = _default_executor
) -> Callable[[Any], Any]:
    """Create a function made of functions that are executed in parallel
    using ThreadPoolExecutor from concurrent module.

    The input functions must only take one mandatory data argument. Note that in the
    wrapper function returned by this functions, this data will be passed and shared
    to all of the input functions. And the same object will be returned by the wrapper.
    Therefore, all input functions must modify the input data inplace to get meaningful
    results.

    If no executor is provided, a shared default ThreadPoolExecutor is used. This
    executor will not be killed by shutdown method. Therefore, make sure that the
    functions passed here can exit properly. To be safe, please pass your own executor.

    Args:
        funcs (List[Callable[[Any], Any]]): the list of functions to be executed. It
            only takes one argument which supposed to be anything. The output of the
            functions will not be used.
        executor (ThreadPoolExecutor, optional): ThreadPoolExecutor instance from
            concurrent.futures that handles the threads. By default, executor
            managed by this package will be used.

    Returns:
        Callable[[Any], Any]: The returned function to execute the parallelized
        input functions. It takes one argument 'x' which will be passed to the input
        functions and returns the same object 'x' as the input after execution.
    """

    def wrapper(x: Any) -> Any:
        """Wrapped parallel threaded pipeline function

        Args:
            x (Any): input data

        Returns:
            Any: output data, the same object as x in input
        """
        res = executor.map(lambda func: func(x), funcs)
        [r for r in res]
        return x

    return wrapper


def func_serial(funcs: List[Callable[[Any], Any]]) -> Callable[[Any], Any]:
    """Create a function made of functions that are executed in serial

    Args:
        funcs (List[Callable[[Any], Any]]): the list of functions to be executed. It
            only takes one argument which supposed to be anything and returns the
            processing results. The output of function i will be used as the input
            argument for function i + 1.

    Returns:
        Callable[[Any], Any]: The returned function to execute the serial functions.
        It takes one argument 'x' which supposed to be the input data. That data will
        be passed to funcs[0]. This function will return the output of the last function
        in funcs.
    """

    def wrapper(x: Any) -> Any:
        """Wrapped serialized pipeline function

        Args:
            x (Any): input data

        Returns:
            Any: output data
        """
        for func in funcs:
            x = func(x)
        return x

    return wrapper
