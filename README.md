# PyStream - Real Time Python Pipeline Manager

This package provides tools to build and boost up a python data pipeline for real time processing. This package is managed using [Poetry](https://python-poetry.org/ ).

For more detailed guidelines, visit this project [documentation](https://pystream-pipeline.readthedocs.io/).

## Concepts

In general, PyStream is a package, fully implemented in python, that helps you manage a data pipeline and optimize its operation performance. The main feature of PyStream is that it can build your data pipeline in asynchronous and independent multi-threaded stages model, and hopefully multi-process model in the future.

A PyStream **pipeline** is constructed by several **stages**, where each stage represents a single set of data processing operations that you define by your own. When the stages have been defined, the pipeline can be operated in two modes:

- **Serial mode:** In this mode, each stage are executed in blocking fashion. The later stages will only be executed when the previous ones have been executed, and the next data can only be processed if the previous data have been processed by the final stage. There is only one data stream that can be processed at any time.
- **Parallel mode:** In this mode, each stage live in a separate parallel thread. If a data has been finished being processed by a stage, the results will be send to the next stage. Since each stage runs in parallel, that stage can immediately take next data input if exist and process it immediately. This way, we can process multiple data at one time, thus increasing the throughput of your pipeline.

Whatever the mode you choose, you only need to focus on implementation of your own data processing codes and pack them into several stages. PyStream will handle the pipeline executions including the threads and the linking of stages for you.

## Installation

You can install this package using `pip`.

```bash
pip install pystream-pipeline
```

If you want to build this package from source or develop it, we recommend you to use Poetry. First install Poetry by following the instructions in its documentation site (you can google it). Then clone this repository and install all the dependencies. Poetry can help you do this and it will also setup a new virtual environment for you.

```bash
poetry install
```

To build the wheel file, you can run

```bash
poetry build
```

You can find the wheel file inside `dist` directory.

## Sample Usage

API of PyStream can be found in this project [documentation](https://pystream-pipeline.readthedocs.io/).

You can also access some examples:

- See [`dummy_pipeline.py`](demo_pipeline.py) to see how PyStream can be used to build a dummy pipeline.
- See how PyStream is used to increase the throughput of a vehicle environment mapping system in [this repository](https://github.com/MukhlasAdib/KITTI_Mapping/tree/main/app).
