"""
This is a script to measure the performance of a dummy pipeline.
The pipeline consists of dummy stages that do nothing but wait
for specified seconds. Therefore, any deviation from the wait
[time * number of stages] in the 'Pipeline' profile data can be 
seen as the performance degradation caused by PyStream handling.
"""

import argparse
import time

from loguru import logger
from tabulate import tabulate

from pystream import Pipeline, Stage


_WRITER = None


def write_to_file(msg):
    global _WRITER
    if _WRITER is None:
        _WRITER = open("REPORT.md", "w")
    _WRITER.write(msg + "\n\n")


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
        default=0.5,
        type=float,
        help="each stage wait time in seconds",
    )
    parser.add_argument(
        "--mode",
        default="report",
        type=str,
        help="pipeline mode serial / thread / report",
    )
    args = parser.parse_args()
    assert args.mode in ["thread", "serial", "report"]
    return args


def run(num_stages, wait_time, mode):
    logger.info("Creating pipeline ...")
    pipeline = create_pipeline(num_stages, wait_time)
    if mode == "thread":
        logger.info("Running pipeline in thread mode ...")
        pipeline.parallelize()
    elif mode == "serial":
        logger.info("Running pipeline in serial mode ...")
        pipeline.serialize()
    else:
        raise ValueError("Invalid pipeline mode")
    pipeline.start_loop(wait_time)
    run_time = wait_time * 100 * 1.1
    logger.info(f"Waiting for {run_time} s...")
    time.sleep(run_time)
    logger.info(f"Finished")
    pipeline.stop_loop()
    logger.info("")
    return pipeline.get_profiles()


def write_log(profile):
    logger.info("### Latency")
    for k, v in profile[0].items():
        logger.info(f"**{k}**: {v} s")
    logger.info("")
    logger.info("### Throughput")
    for k, v in profile[1].items():
        logger.info(f"**{k}**: {v} data/s")


def write_report(profile, title=None):
    if title is not None:
        write_to_file(title)

    data = []
    for k in profile[0].keys():
        d = [k, profile[0][k], profile[1][k]]
        data.append(d)
    table = tabulate(data, headers=["Stage", "Latency", "Throughput"], tablefmt="pipe")
    write_to_file(table)


def run_one_mode(args):
    profile = run(args.num_stages, args.wait_time, args.mode)
    write_log(profile)


def run_reporting(args):
    write_to_file("# Profiling Report")
    profile = run(args.num_stages, args.wait_time, "serial")
    write_log(profile)
    write_report(profile, "## Serial Pipeline")
    profile = run(args.num_stages, args.wait_time, "thread")
    write_log(profile)
    write_report(profile, "## Threaded Pipeline")


def main(args):
    if args.mode != "report":
        run_one_mode(args)
    else:
        run_reporting(args)


if __name__ == "__main__":
    main(parse_args())

if _WRITER is not None:
    _WRITER.close()
    _WRITER = None
