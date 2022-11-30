# Development Plan

## Released

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

### v0.1.3

- [x] `forward` method of `pystream.Pipeline` have default value: the flag to use input generator
- [ ] Well-explained sample usage of the demo in `.ipynb` format.
- [x] Unit tests for staged pipeline operations.
- [ ] Unit tests for functional pipeline operations.

### v0.1.4

- [ ] Interface unification of serial and parallel pipeline stages by adding a wrapper for the stages.
- [ ] Unification of serial and parallel stage links in staged mode.
- [ ] Support for native mixed (serial and parallel) operation.

### v0.2.0

- [ ] Add parallelization using multiprocessing.

### v0.2.1

- [ ] Add built-in pipeline profiling feature.

### v0.3.0

- [ ] Support for pipeline branching and merging
