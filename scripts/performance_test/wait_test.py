import argparse
import time

from loguru import logger

from pystream import Pipeline, Stage


class WaitStage(Stage):
    def __init__(self, wait: float):
        self.wait = wait

    def __call__(self, data: list) -> list:
        time.sleep(self.wait)
        return data

    def cleanup(self) -> None:
        pass


def create_pipeline(num_stages: int, wait_time: float) -> Pipeline:
    pipeline = Pipeline(input_generator=list, use_profiler=True)
    for _ in range(num_stages):
        stage = WaitStage(wait_time)
        pipeline.add(stage)
    return pipeline


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--num-stages",
        default=5,
        type=int,
        help="number of stages",
    )
    parser.add_argument(
        "--wait-time",
        default=1,
        type=float,
        help="each stage wait time in seconds",
    )
    parser.add_argument(
        "--mode",
        default="serial",
        type=str,
        help="pipeline mode serial / thread",
    )
    args = parser.parse_args()
    assert args.mode in ["thread", "serial"]
    return args


def main(args):
    logger.info("Creating pipeline ...")
    pipeline = create_pipeline(args.num_stages, args.wait_time)
    if args.mode == "thread":
        logger.info("Running pipeline in thread mode ...")
        pipeline.parallelize()
    elif args.mode == "serial":
        logger.info("Running pipeline in serial mode ...")
        pipeline.serialize()
    else:
        raise ValueError("Invalid pipeline mode")
    pipeline.start_loop(args.wait_time)
    run_time = args.wait_time * 100 * 1.1
    logger.info(f"Waiting for {run_time} s...")
    time.sleep(run_time)
    logger.info(f"Finished")
    pipeline.stop_loop()
    logger.info("")

    logger.info("*REPORT*")
    profile = pipeline.get_profiles()
    logger.info("----- Latency -----")
    for k, v in profile[0].items():
        logger.info(f"{k}: {v} s")
    logger.info("")
    logger.info("---- Throughput----")
    for k, v in profile[1].items():
        logger.info(f"{k}: {v} data/s")


if __name__ == "__main__":
    main(parse_args())
