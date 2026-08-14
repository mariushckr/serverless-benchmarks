import json
import os
import threading
import time
from typing import Dict, List, TYPE_CHECKING

from sebs.faas.system import System as FaaSSystem
from sebs.faas.function import Trigger
from sebs.experiments.experiment import Experiment
from sebs.experiments.result import Result as ExperimentResult
from sebs.experiments.config import Config as ExperimentConfig
from sebs.utils import serialize

if TYPE_CHECKING:
    from sebs import SeBS


class Batch(Experiment):
    """
    Fires a fixed-size batch of invocations essentially simultaneously (all
    submitted in a tight loop via async_invoke(), no inter-arrival delay),
    then waits for every one of them to complete and measures the total
    wall-clock time to drain the whole batch -- a throughput/"how long to
    handle everything queued at once" measurement, distinct from PerfCost's
    "burst" mode, which measures per-invocation cold/warm stats across
    repeated small forced-cold bursts rather than total drain time for one
    large batch.

    Supports multiple independent concurrent workloads (a "mixed" workload
    is just a workloads list with more than one entry), same pattern as
    Stream -- each workload's batch is submitted in its own thread, all
    starting together, so a mixed-workload batch measures total drain time
    across all workloads' combined queued invocations at once.

    Config (under "batch" in experiments config):
        workloads: list of, each:
            benchmark:   benchmark name, e.g. "110.dynamic-html"
            input-size:  benchmark input size, e.g. "test"/"small"/"large"
            count:       number of invocations to fire simultaneously
    """

    def __init__(self, config: ExperimentConfig):
        super().__init__(config)

    @staticmethod
    def name() -> str:
        return "batch"

    @staticmethod
    def typename() -> str:
        return "Experiment.Batch"

    def prepare(self, sebs_client: "SeBS", deployment_client: FaaSSystem):

        settings = self.config.experiment_settings(self.name())
        self._workload_settings = settings["workloads"]

        self._out_dir = os.path.join(sebs_client.output_dir, "batch")
        if not os.path.exists(self._out_dir):
            os.mkdir(self._out_dir)

        self._deployment_client = deployment_client
        self._sebs_client = sebs_client

        self._workloads = []
        for w in self._workload_settings:
            benchmark = sebs_client.get_benchmark(w["benchmark"], deployment_client, self.config)
            benchmark_input = benchmark.prepare_input(
                deployment_client.system_resources,
                size=w["input-size"],
                replace_existing=self.config.update_storage,
            )
            function = deployment_client.get_function(benchmark)
            triggers = function.triggers(Trigger.TriggerType.HTTP)
            if len(triggers) == 0:
                trigger = deployment_client.create_trigger(function, Trigger.TriggerType.HTTP)
            else:
                trigger = triggers[0]

            self._workloads.append(
                {
                    "name": w["benchmark"],
                    "function": function,
                    "trigger": trigger,
                    "input": benchmark_input,
                    "count": w["count"],
                }
            )

    def _submit_one_workload(self, workload: dict, submitted: dict):
        """Runs in its own thread. Fires workload['count'] invocations back
        to back with no delay, storing the Futures for the main thread to
        wait on. Submission itself is near-instant (async_invoke doesn't
        block), so all workloads' batches start at effectively the same
        moment regardless of thread scheduling order."""

        futures = []
        submit_start = time.time()
        for _ in range(workload["count"]):
            fut = workload["trigger"].async_invoke(workload["input"])
            futures.append(fut)
        submitted[workload["name"]] = {
            "futures": futures,
            "function": workload["function"],
            "submit_start": submit_start,
            "submit_end": time.time(),
        }

    def run(self):

        total_count = sum(w["count"] for w in self._workloads)
        self.logging.info(
            f"Starting batch: {len(self._workloads)} workload(s), "
            f"{total_count} total invocations queued at once"
        )

        submitted: Dict[str, dict] = {}
        threads = [
            threading.Thread(target=self._submit_one_workload, args=(w, submitted))
            for w in self._workloads
        ]

        batch_start = time.time()
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        submit_done = time.time()

        self.logging.info(
            f"All {total_count} invocations submitted in {submit_done - batch_start:.3f}s, "
            "waiting for completion..."
        )

        result = ExperimentResult(self.config, self._deployment_client.config)
        result.begin()

        error_count = 0
        errors: List[str] = []
        completion_times: Dict[str, List[float]] = {name: [] for name in submitted}
        for name, data in submitted.items():
            for fut in data["futures"]:
                try:
                    ret = fut.result()
                    completion_times[name].append(time.time() - batch_start)
                    result.add_invocation(data["function"], ret)
                except Exception as e:
                    error_count += 1
                    errors.append(str(e))

        drain_end = time.time()
        result.end()

        total_drain_time = drain_end - batch_start
        self.logging.info(
            f"Batch drained: {total_count - error_count}/{total_count} succeeded "
            f"in {total_drain_time:.2f}s total"
        )

        out_file = os.path.join(self._out_dir, "batch_results.json")
        with open(out_file, "w") as out_f:
            out_f.write(
                serialize(
                    {
                        **json.loads(serialize(result)),
                        "statistics": {
                            "total_count": total_count,
                            "total_drain_time_seconds": total_drain_time,
                            "submission_time_seconds": submit_done - batch_start,
                            "failures_count": error_count,
                            "failures": errors,
                            "workloads": {
                                name: {
                                    "count": len(data["futures"]),
                                    "completion_times": completion_times[name],
                                    "last_completion_seconds": (
                                        max(completion_times[name])
                                        if completion_times[name]
                                        else None
                                    ),
                                }
                                for name, data in submitted.items()
                            },
                        },
                    }
                )
            )

    def process(
        self,
        sebs_client: "SeBS",
        deployment_client: FaaSSystem,
        directory: str,
        logging_filename: str,
        extend_time_interval: int,
    ):
        import csv

        in_file = os.path.join(directory, "batch", "batch_results.json")
        with open(in_file) as f:
            config = json.load(f)

        statistics = config.get("statistics", {})
        experiments = ExperimentResult.deserialize(
            config,
            sebs_client.cache_client,
            sebs_client.generate_logging_handlers(logging_filename),
        )

        out_file = os.path.join(directory, "batch", "result.csv")
        with open(out_file, "w") as csvfile:
            writer = csv.writer(csvfile, delimiter=",")
            writer.writerow(
                ["is_cold", "exec_time", "connection_time", "client_time", "provider_time"]
            )
            for func in experiments.functions():
                for request_id, invoc in experiments.invocations(func).items():
                    writer.writerow(
                        [
                            invoc.stats.cold_start,
                            invoc.times.benchmark,
                            invoc.times.http_startup,
                            invoc.times.client,
                            invoc.provider_times.execution,
                        ]
                    )

        summary_file = os.path.join(directory, "batch", "summary.json")
        with open(summary_file, "w") as f:
            json.dump(statistics, f, indent=2)