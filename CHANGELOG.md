# Development Plan

## Released

### v0.2.0

- [x] Support for native mixed (serial and parallel) operation.
- [x] Specify custom profiler DB path
- [x] Protect against multiple profiling in mixed pipeline
- [x] CI test for notebooks
- [x] CI test using python 3.11

### v0.1.3

- [x] Add built-in pipeline profiling feature.
- [x] `forward` method of `pystream.Pipeline` have default value: the flag to use input generator
- [x] Well-explained sample usage of the demo in `.ipynb` format.
- [x] Unit tests for staged pipeline operations.
- [x] Unit tests for functional pipeline operations.
- [x] Use logger instead of print
- [x] Specify stage name

### v0.1.2

- [x] Option to use blocking pipeline input for parallel pipeline.
- [x] An application example: [KITTI mapping](https://github.com/MukhlasAdib/KITTI_Mapping).

### v0.1.1

- [x] Output reading in parallel thread pipeline.
- [x] Option to use module global ThreadPoolExecutor for functional operation.
- [x] Add data argument to functional pipeline.
- [x] Pipeline is None after `Pipeline`'s `cleanup` method is called.
- [x] Serial pipeline data become None if it has been read once.

### v0.1.0

- [x] Poetry initialization
- [x] Basic staged pipeline operations.
- [x] Basic functional pipeline operations.
- [x] Usage example script -> `demo_pipeline.py`.
- [x] First documentations:
  - [x] Docstrings for classes, methods, and functions.
  - [x] Minimum `README.md`.
  - [x] Documentation in `docs` about how `pystream` works.
  - [x] Documentation in `docs` about basic API.

## On-going

### v0.2.1

- [ ] Code formatting CI.
- [ ] Deny stage name with any non alphanumeric or "_" characters.
- [ ] Measure `time.wait()` error during wait test.

### v0.3.0

- [ ] Support for pipeline branching and merging.

### v0.4.0

- [ ] Add parallelization using multiprocessing.
