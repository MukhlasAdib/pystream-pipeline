Advanced Features
======================================

Here are more advanced features provided in PySytream.
These features are demonstrated in the `demo notebook <https://github.com/MukhlasAdib/pystream-pipeline/blob/main/demo.ipynb>`_.

1. Pipeline Profiling
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

You can use built-in pipeline profiler to get the information about latency and throughput of your pipeline.
To create pipeline with active profiler, you only need to specify ``use_profiler`` as ``True`` when instantiate the ``pystream.Pipeline`` class::

    pipeline = pystream.Pipeline(input_generator=lambda: 0, use_profiler=True)

And then you can get the profiling results by invoke ``get_profiles`` method of the pipeline.
The method will return the recorded throughput and latency of the pipeline as python dictionary::

    latency, throughput = pipeline.get_profiles()

Here is a sample results of latency (dictionary)::

    {
        'MainPipeline': 2.0981452222222226,
        'MainPipeline__ChildPipeline': 0.3867601444444446,
        'MainPipeline__ChildPipeline__StageA': 0.19366966666666485,
        'MainPipeline__ChildPipeline__StageB': 0.1930631444444444,
        'MainPipeline__Stage1': 0.20863497777777618,
        'MainPipeline__Stage2': 0.20957296666666755
    }

Each item's key represents a stage/pipeline whereas the value represents the latency in seconds.
Double underscores ``__`` is used as the saparator between sub-pipeline levels (useful in mixed pipeline).
The overall pipeline data is always named as ``MainPipeline``.
The throughput has the same format as latency, but the values are presented in data/second format. 
