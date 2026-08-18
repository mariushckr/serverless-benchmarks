import json
import os
import random
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


class Stream(Experiment):
    """
    Fires invocations with Poisson-process arrivals: inter-arrival gaps are
    drawn from an exponential distribution with rate lambda (mean gap =
    1/lambda seconds). Each invocation is fired via async_invoke() so the
    scheduling loop never blocks on a previous invocation completing.

    Supports multiple independent, concurrently-running workloads (a "mixed"
    workload is just a workloads list with more than one entry) -- each
    workload gets its own thread and its own seeded RNG, so results are
    fully reproducible regardless of thread-scheduling nondeterminism in
    wall-clock timing.

    Optionally bursty via compound Poisson batch arrivals: each arrival
    fires a batch of Poisson-distributed size (mean = batch_mean).

    Config (under "stream" in experiments config):
        duration: how long to generate arrivals for, in seconds
        seed:     optional int, base seed for reproducibility
        label:    optional string. If set, results are written to
                  <output_dir>/stream/<label>_results.json instead of the
                  fixed stream/stream_results.json -- without this, running
                  multiple stream scenarios back to back silently overwrites
                  each prior run's results, since the output path is
                  otherwise always identical regardless of config content.
        workloads: list of, each:
            benchmark:   benchmark name, e.g. "110.dynamic-html"
            input-size:  benchmark input size, e.g. "test"/"small"/"large"
            rate:        mean arrivals per second (lambda) for this workload
            bursty:      optional bool, default false
            batch_mean:  required if bursty=true -- mean batch size per arrival
            seed:        optional int, overrides base seed + index for this workload
    """

    def __init__(self, config: ExperimentConfig):
        super().__init__(config)

    @staticmethod
    def name() -> str:
        return "stream"

    @staticmethod
    def typename() -> str:
        return "Experiment.Stream"

    def prepare(self, sebs_client: "SeBS", deployment_client: FaaSSystem):

        settings = self.config.experiment_settings(self.name())
        self._duration = settings["duration"]
        self._base_seed = settings.get("seed")
        self._label = settings.get("label")
        self._workload_settings = settings["workloads"]

        self._out_dir = os.path.join(sebs_client.output_dir, "stream")
        if not os.path.exists(self._out_dir):
            os.mkdir(self._out_dir)

        self._deployment_client = deployment_client
        self._sebs_client = sebs_client

        self._workloads = []
        for idx, w in enumerate(self._workload_settings):
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
                    "rate": w["rate"],
                    "bursty": w.get("bursty", False),
                    "batch_mean": w.get("batch_mean"),
                    "seed": w.get("seed", (self._base_seed or 0) + idx),
                }
            )

    def _run_one_workload(self, workload: dict, results: dict):
        rng = random.Random(workload["seed"])
        trigger = workload["trigger"]
        # HTTPTrigger supports the dispatch/wait split -- dispatching
        # inline here (not via the worker pool) guarantees the invocation
        # genuinely reaches MOMOS within `duration`, regardless of how
        # backed up the wait-for-result side gets. Without this, arrival
        # itself could be delayed by hours if the worker pool falls behind
        # -- confirmed directly: a 1800s-duration scenario took ~14000s
        # wall-clock, and the client's own scheduling loop had no way to
        # know or report that most of that gap was arrivals still waiting
        # to be dispatched, not MOMOS being slow to respond.
        supports_split = hasattr(trigger, "dispatch") and hasattr(trigger, "async_invoke_result")

        futures = []
        arrival_times: List[float] = []
        start = time.time()
        next_arrival = start

        while True:
            now = time.time()
            if now - start >= self._duration:
                break

            sleep_for = next_arrival - now
            if sleep_for > 0:
                time.sleep(sleep_for)

            fire_time = time.time()
            batch_size = 1
            if workload["bursty"]:
                batch_size = max(1, rng_poisson(rng, workload["batch_mean"]))

            for _ in range(batch_size):
                if supports_split:
                    # Dispatch happens HERE, synchronously, in the
                    # scheduling loop itself -- this is the real arrival
                    # event at MOMOS. Only the (potentially slow) wait for
                    # the result gets handed to the pool.
                    request_id, begin = trigger.dispatch(workload["input"])
                    fut = trigger.async_invoke_result(request_id, begin)
                else:
                    fut = trigger.async_invoke(workload["input"])
                futures.append(fut)
                arrival_times.append(fire_time - start)

            gap = rng.expovariate(workload["rate"])
            next_arrival = fire_time + gap

        error_count = 0
        errors: List[str] = []
        collected = []
        for fut, arrival_t in zip(futures, arrival_times):
            try:
                ret = fut.result()
            except Exception as e:
                error_count += 1
                errors.append(str(e))
                continue

            # HTTPTrigger.sync_invoke() never raises on an SSE timeout or a
            # failed POST -- it catches the error internally and returns a
            # normal-looking ExecutionResult with .stats.failure=True set
            # as a flag instead. The exception-only check above silently
            # missed every one of these: confirmed directly against real
            # results (215 invocations with times.client ~600s, matching
            # the SSE trigger's own timeout=600, ALL with stats.failure
            # True, yet failures_count reported 0). This check is what
            # was missing.
            #
            # Still added to `collected` regardless of failure -- the full
            # record (timing, request_id) is what let us diagnose this bug
            # in the first place; only the COUNT was ever wrong, not the
            # underlying data.
            if ret.stats.failure:
                error_count += 1
                errors.append(f"Invocation failed (request_id={getattr(ret, 'request_id', '?')})")
            collected.append((workload["function"], ret, arrival_t))

        results[workload["name"]] = {
            "invocations": collected,
            "arrivals_scheduled": len(futures),
            "failures": errors,
            "failures_count": error_count,
        }

    def run(self):

        self.logging.info(
            f"Starting stream with {len(self._workloads)} workload(s) for {self._duration}s"
        )

        results: Dict[str, dict] = {}
        threads = []
        for workload in self._workloads:
            t = threading.Thread(target=self._run_one_workload, args=(workload, results))
            threads.append(t)

        start_wall = time.time()
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        end_wall = time.time()

        self.logging.info(f"Stream complete in {end_wall - start_wall:.1f}s wall-clock")

        result = ExperimentResult(self.config, self._deployment_client.config)
        result.begin()
        for name, data in results.items():
            for func, ret, arrival_t in data["invocations"]:
                ret.stats.arrival_time = arrival_t  # type: ignore[attr-defined]
                ret.stats.workload_name = name  # type: ignore[attr-defined]
                result.add_invocation(func, ret)
        result.end()

        out_filename = f"{self._label}_results.json" if self._label else "stream_results.json"
        out_file = os.path.join(self._out_dir, out_filename)
        with open(out_file, "w") as out_f:
            out_f.write(
                serialize(
                    {
                        **json.loads(serialize(result)),
                        "statistics": {
                            "label": self._label,
                            "duration": self._duration,
                            "wall_clock_seconds": end_wall - start_wall,
                            "workloads": {
                                name: {
                                    "arrivals_scheduled": d["arrivals_scheduled"],
                                    "failures_count": d["failures_count"],
                                    "failures": d["failures"],
                                }
                                for name, d in results.items()
                            },
                        },
                    }
                )
            )
        self.logging.info(f"Stream results written to {out_file}")

    def process(
        self,
        sebs_client: "SeBS",
        deployment_client: FaaSSystem,
        directory: str,
        logging_filename: str,
        extend_time_interval: int,
    ):
        import csv

        settings = self.config.experiment_settings(self.name())
        label = settings.get("label")
        in_filename = f"{label}_results.json" if label else "stream_results.json"
        in_file = os.path.join(directory, "stream", in_filename)
        with open(in_file) as f:
            config = json.load(f)

        experiments = ExperimentResult.deserialize(
            config,
            sebs_client.cache_client,
            sebs_client.generate_logging_handlers(logging_filename),
        )

        out_filename = f"{label}_result.csv" if label else "result.csv"
        out_file = os.path.join(directory, "stream", out_filename)
        with open(out_file, "w") as csvfile:
            writer = csv.writer(csvfile, delimiter=",")
            writer.writerow(
                [
                    "workload",
                    "arrival_time",
                    "is_cold",
                    "exec_time",
                    "connection_time",
                    "client_time",
                    "provider_time",
                ]
            )
            for func in experiments.functions():
                for request_id, invoc in experiments.invocations(func).items():
                    writer.writerow(
                        [
                            getattr(invoc.stats, "workload_name", ""),
                            getattr(invoc.stats, "arrival_time", ""),
                            invoc.stats.cold_start,
                            invoc.times.benchmark,
                            invoc.times.http_startup,
                            invoc.times.client,
                            invoc.provider_times.execution,
                        ]
                    )


def rng_poisson(rng: random.Random, mean: float) -> int:
    """Knuth's algorithm for a Poisson-distributed random integer, using the
    given Random instance so results stay reproducible under a fixed seed."""
    import math

    L = math.exp(-mean)
    k = 0
    p = 1.0
    while True:
        k += 1
        p *= rng.random()
        if p <= L:
            return k - 1