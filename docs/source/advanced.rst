Advanced Features
======================================

Here are more advanced features provided in PySytream.
These features are demonstrated in the `demo notebook <https://github.com/MukhlasAdib/pystream-pipeline/blob/main/demo.ipynb>`_.

1. Pipeline Profiling
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

You can use built-in pipeline profiler to get the information about latency and throughput of your pipeline.
To create pipeline with active profiler, you only need to specify ``use_profiler`` as ``True`` when instantiate the ``pystream.Pipeline`` class::

    pipeline = pystream.Pipeline(input_generator=lambda: 0, use_profiler=True)

And then you can get the profiling results by invoking ``get_profiles`` method of the pipeline.
The method will return the recorded throughput and latency of the pipeline as python dictionary::

    latency, throughput = pipeline.get_profiles()

Here is a sample result of latency::

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
The overall pipeline data are always named as ``MainPipeline``.
The throughput has the same format as latency, but the values are presented in data/second format. 

2. Mixed Pipeline
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

You can also create pipeline inside another pipeline.
This feature is useful  if you want to:

- Groups your stages into several sub-pipelines
- Mix serial and parallel pipelines, e.g., create serial pipeline inside parallel pipeline.

In order to do that, you need to convert the sub-pipeline into a stage, which is easy.
Use ``as_stage`` method of ``pystream.Pipeline`` to get the stage-form of the pipeline.
Here is an example::

    # create serial sub-pipeline
    sub_pipeline = pystream.Pipeline()
    # add normal stages
    sub_pipeline.add(stage31)
    sub_pipeline.add(stage32)
    # serialize
    sub_pipeline.serialize()

    # create parallel main pipeline
    main_pipeline = pystream.Pipeline(use_profiler=True)
    # add normal stages
    main_pipeline.add(stage1)
    main_pipeline.add(stage2)
    # add sub-pipeline as stage
    main_pipeline.add(sub_pipeline.as_stage())
    # parallelize
    main_pipeline.parallelize()


