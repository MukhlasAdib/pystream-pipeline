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
        start = time.time()
        while (time.time() - start) < self.wait:
            time.sleep(self.wait / 10)
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


def run_one_mode(args):
    profile = run(args.num_stages, args.wait_time, args.mode)
    write_log(profile)


class PipelineTester:
    def __init__(self, wait_time, num_stages):
        self.wait_time = wait_time
        self.num_stages = num_stages

    def run_reporting(self):
        write_to_file("# Profiling Report (Wait Test)")
        write_to_file(f"Wait time: {self.wait_time} s")

        self.report_serial()
        self.report_parallel()

    def report_serial(self):
        profile = run(self.num_stages, self.wait_time, "serial")
        write_log(profile)
        write_to_file("## Serial Pipeline")

        ideal_stage_throughput = 1 / (self.wait_time * self.num_stages)
        ideal_pipeline_throughput = 1 / (self.wait_time * self.num_stages)
        ideal_stage_latency = self.wait_time
        ideal_pipeline_latency = self.wait_time * self.num_stages
        self.write_report(profile)
        self.write_analysis(
            profile,
            ideal_pipeline_throughput,
            ideal_pipeline_latency,
            ideal_stage_throughput,
            ideal_stage_latency,
        )

    def report_parallel(self):
        profile = run(self.num_stages, self.wait_time, "thread")
        write_log(profile)
        write_to_file("## Threaded Pipeline")

        ideal_stage_throughput = 1 / self.wait_time
        ideal_stage_latency = self.wait_time
        ideal_pipeline_throughput = 1 / self.wait_time
        ideal_pipeline_latency = self.wait_time * self.num_stages
        self.write_report(profile)
        self.write_analysis(
            profile,
            ideal_pipeline_throughput,
            ideal_pipeline_latency,
            ideal_stage_throughput,
            ideal_stage_latency,
        )

    def write_report(self, profile):
        data = []
        for k in profile[0].keys():
            d = [k, profile[0][k], profile[1][k]]
            data.append(d)
        table = tabulate(
            data,
            headers=["Stage", "Latency", "Throughput"],
            tablefmt="pipe",
            floatfmt=".3f",
        )
        write_to_file(table)

    def write_analysis(
        self,
        profile,
        ideal_pipeline_throughput,
        ideal_pipeline_latency,
        ideal_stage_throughput,
        ideal_stage_latency,
    ):
        latency = profile[0]
        throughput = profile[1]

        sum_val = 0
        for k in throughput.keys():
            if k == "Pipeline":
                continue
            sum_val += throughput[k]
        actual_stage_throughput = sum_val / (len(throughput) - 1)
        actual_pipeline_throughput = throughput["Pipeline"]

        sum_val = 0
        for k in latency.keys():
            if k == "Pipeline":
                continue
            sum_val += latency[k]
        actual_stage_latency = sum_val / (len(latency) - 1)
        actual_pipeline_latency = latency["Pipeline"]

        table_data = [
            self.create_row(
                "Stage latency", ideal_stage_latency, actual_stage_latency, rev=True
            ),
            self.create_row(
                "Stage throughput",
                ideal_stage_throughput,
                actual_stage_throughput,
                rev=False,
            ),
            self.create_row(
                "Pipeline latency",
                ideal_pipeline_latency,
                actual_pipeline_latency,
                rev=True,
            ),
            self.create_row(
                "Pipeline throughput",
                ideal_pipeline_throughput,
                actual_pipeline_throughput,
                rev=False,
            ),
        ]
        table = tabulate(
            table_data,
            headers=["Metric", "Ideal (s)", "Actual (s)", "Deviation (%)"],
            tablefmt="pipe",
            floatfmt=".3f",
        )
        write_to_file(table)

    def create_row(self, name, ideal_val, actual_val, rev=True):
        if rev:
            err = (actual_val - ideal_val) / actual_val * 100
        else:
            err = (ideal_val - actual_val) / ideal_val * 100
        return [name, ideal_val, actual_val, err]


def main(args):
    if args.mode != "report":
        run_one_mode(args)
    else:
        tester = PipelineTester(args.wait_time, args.num_stages)
        tester.run_reporting()


if __name__ == "__main__":
    main(parse_args())

if _WRITER is not None:
    _WRITER.close()
    _WRITER = None
