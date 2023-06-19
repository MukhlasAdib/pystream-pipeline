class PipelineUndefined(Exception):
    pass


class PipelineTerminated(Exception):
    pass


class PipelineInitiationError(Exception):
    pass


class InvalidStageName(ValueError):
    pass


class ProfilingError(ValueError):
    pass
