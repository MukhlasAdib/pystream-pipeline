PyStream API
======================================

Staged Pipeline
--------------------------------------


.. autoclass:: pystream.Pipeline
    :members:


.. autoclass:: pystream.Stage
    :members: __call__, cleanup, name


Functional Pipeline
--------------------------------------


.. autofunction:: pystream.functional.func_serial


.. autofunction:: pystream.functional.func_parallel_thread