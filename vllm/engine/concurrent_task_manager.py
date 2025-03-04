#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
@File    :   async_process_manager.py
@Time    :   2025/03/04 17:30:49
@Author  :   Zi-Ao Ma 
@Version :   1.0
@Desc    :   A multi-mode asynchronous task manager that:
  • supports task grouping,
  • logs task parameters/results to a file when requested (allowing recovery),
  • supports running each task either in a process or in a thread (with separate concurrency limits),
  • and prints throughput metrics every given interval.
'''

import os
import time
import json
import copy
import queue
import logging
import traceback
import threading
import collections
from enum import Enum
import multiprocessing
from dataclasses import dataclass, field
from typing import Any, Optional, Callable, Tuple, Dict

logger = logging.getLogger(__name__)


class TaskStatus(Enum):
    # new
    ARRIVED = "arrived"
    # in progress
    RUNNING = "running"
    PENDING = "pending"
    # ended
    COMPLETED = "completed"
    ERROR = "error"
    CANCELED = "canceled"
    TIMEOUT = "timeout"
    # incorrect usage / not found
    NOT_FOUND = "not found"
    UNKNOWN = "unknown"


def is_json_serializable(obj):
    try:
        json.dumps(obj)
        return True
    except TypeError:
        return False


@dataclass
class Task:
    task_id: Any
    group_id: Optional[Any]
    task_fn: Callable
    args: Tuple = field(default_factory=tuple)
    kwargs: Dict = field(default_factory=list)
    result: Optional[Any] = None
    log_file: Optional[str] = None
    cached: bool = False

    def load_cached_result_from_log_file(self) -> bool:
        """
        Reads the log file and returns a dictionary with log information.
        The log file must be either a single JSON file ('.json') or a JSON-lines file ('.jsonl').
        If reading fails or the file does not exist, a default error log is returned.
        This function always returns a dict with keys: task_id, status, result, etc.
        """
        if self.log_file is not None and os.path.exists(self.log_file):
            if self.log_file.endswith(".json"):
                try:
                    with open(self.log_file, "r", encoding="utf-8") as f:
                        log_info = json.load(f)

                    # load the logged result only if the logged run is successfully completed
                    if log_info.get("task_id") == self.task_id and log_info.get("status") == TaskStatus.COMPLETED.value:
                        self.result = log_info["result"]
                        self.cached = True
                        return True
                    else:
                        return False
                except (json.decoder.JSONDecodeError, AssertionError, Exception):
                    return False
            elif self.log_file.endswith(".jsonl"):
                try:
                    with open(self.log_file, "r", encoding="utf-8") as f:
                        for line in f:
                            try:
                                log_info = json.loads(line)
                                if log_info.get("task_id") == self.task_id:
                                    if log_info.get("status") == TaskStatus.COMPLETED.value:
                                        self.result = log_info["result"]
                                        self.cached = True
                                        return True
                            except Exception:
                                continue
                    return False
                except Exception:
                    return False
        # If no log or file missing, return an error dict.
        return False

    def load_cached_result_from_database(self):
        raise NotImplementedError

    def to_dict(self):
        args_to_save = ()
        for arg in self.args:
            if is_json_serializable(arg):
                args_to_save += (arg,)
            else:
                args_to_save += ("<NOT_SERIALIZABLE>",)
        kwargs_to_save = {key: value for key, value in self.kwargs.items() if is_json_serializable(key) and is_json_serializable(value)}
        return {
            "task_id": self.task_id,
            "group_id": self.group_id,
            "task_fn": self.task_fn.__name__,
            "args": args_to_save,
            "kwargs": kwargs_to_save,
            "result": self.result,
        }


@dataclass
class TaskRuntime:
    timeout: float
    start_time: float
    end_time: float
    result_queue: Any
    process: Optional[multiprocessing.Process]
    thread: Optional[threading.Thread]
    status: str = TaskStatus.PENDING.value
    exec_mode: str = "process"

    def to_dict(self):
        return {
            "timeout": self.timeout,
            "start_time": self.start_time,
            "end_time": self.end_time,
            "status": self.status,
            "exec_mode": self.exec_mode,
        }


def task_wrapper(task: Task, result_queue):
    """
    Used to launch a task in a child process.
    The wrapper checks if the log file already contains a completed result
    before executing the task_fn.
    """
    if task.load_cached_result_from_log_file():
        result_queue.put((TaskStatus.COMPLETED.value, task))
    else:
        try:
            result = task.task_fn(*task.args, **task.kwargs)
            task.result = result
            result_queue.put((TaskStatus.COMPLETED.value, task))
        except Exception:
            tb = traceback.format_exc()
            task.result = tb
            result_queue.put((TaskStatus.ERROR.value, task))


def task_wrapper_thread(task: Task, result_queue):
    """
    Used to launch a task in a separate thread.
    """
    if task.load_cached_result_from_log_file():
        result_queue.put((TaskStatus.COMPLETED.value, task))
    else:
        try:
            result = task.task_fn(*task.args, **task.kwargs)
            task.result = result
            result_queue.put((TaskStatus.COMPLETED.value, task))
        except Exception:
            tb = traceback.format_exc()
            task.result = tb
            result_queue.put((TaskStatus.ERROR.value, task))


class ConcurrentTaskManager:
    def __init__(
        self,
        max_concurrent_tasks: int = 4,
        max_processes: int = 2,
        default_timeout: int = 10,
        throughput_interval: int = 60,
    ):
        """
        max_concurrent_tasks: upper limit for all running tasks.
        max_processes: how many tasks can run in separate processes.
        (The remaining concurrent slots are for thread–tasks.)
        default_timeout: seconds until a running task is deemed timed out.
        throughput_interval: seconds between printing throughput metrics.
        """
        if max_concurrent_tasks < max_processes:
            raise ValueError("max_concurrent_tasks must be >= max_processes")
        self.total_limit = max_concurrent_tasks
        self.max_processes = max_processes
        self.max_thread_limit = max(0, max_concurrent_tasks - max_processes)
        self.default_timeout = default_timeout

        # Task bookkeeping. tasks: key=task_id, value=dict of task info.
        self.tasks = {}
        self.pending = collections.deque()  # FIFO order of pending task_ids.
        self.task_groups = {}  # group_id -> list of task_ids.
        # Global throughput metrics by task_fn name.
        self.throughput_metrics = {}
        self.throughput_metrics_last = {}

        # Lock for thread–safe access.
        self.lock = threading.Lock()

        # Event signals to stop background threads.
        self.stop_event = threading.Event()
        self.throughput_stop_event = threading.Event()

        # Logging queue and stop event for asynchronous log writing.
        self.log_queue = queue.Queue()
        self.log_stop_event = threading.Event()

        assert throughput_interval >= 2.0, (
            f"Throughput interval too short may cause log errors, got {throughput_interval}s."
        )
        self.throughput_interval = throughput_interval

        # Start monitor thread (daemon so it won’t block shutdown).
        self.monitor_thread = threading.Thread(target=self._monitor_tasks, daemon=True)
        self.monitor_thread.start()

        # Start throughput logger thread.
        self.throughput_thread = threading.Thread(target=self._log_throughput, daemon=True)
        self.throughput_thread.start()

        # Start log writer thread.
        self.log_thread = threading.Thread(target=self._log_writer, daemon=True)
        self.log_thread.start()

    def add_task(
        self,
        task_id,
        task_fn,
        *args,
        timeout=None,
        exec_mode="process",
        log_file=None,
        group_id=None,
        **kwargs,
    ):
        """
        Schedules a new task for execution.
        If a log_file is provided and it already contains a completed record for this task_id,
        the cached result is immediately returned without scheduling.
        """
        if timeout is None:
            timeout = self.default_timeout

        with self.lock:
            # Avoid duplicate task ids.
            if task_id in self.tasks:
                raise ValueError(f"Task with id '{task_id}' already exists.")

            # Initialize throughput metrics for this task_fn name.
            fn_name = task_fn.__name__
            if fn_name not in self.throughput_metrics:
                metrics = {
                    TaskStatus.ARRIVED.value: 0,
                    TaskStatus.RUNNING.value: 0,
                    TaskStatus.PENDING.value: 0,
                    TaskStatus.COMPLETED.value: 0,
                    TaskStatus.ERROR.value: 0,
                    TaskStatus.CANCELED.value: 0,
                    TaskStatus.TIMEOUT.value: 0,
                }
                self.throughput_metrics[fn_name] = metrics
                self.throughput_metrics_last[fn_name] = copy.deepcopy(metrics)
            self.throughput_metrics[fn_name][TaskStatus.ARRIVED.value] += 1
            self.throughput_metrics[fn_name][TaskStatus.PENDING.value] += 1

            # Create a task
            task = Task(
                task_id=task_id,
                group_id=group_id,
                task_fn=task_fn,
                args=args,
                kwargs=kwargs,
                result=None,
                log_file=log_file
            )

            # Create a result queue. (Use a multiprocessing.Queue for process tasks; else a regular queue.)
            if exec_mode == "process":
                result_queue = multiprocessing.Queue()
                process_obj = multiprocessing.Process(
                    target=task_wrapper,
                    args=(),
                    kwargs={"task": task, "result_queue": result_queue},
                )
                thread_obj = None
            elif exec_mode == "thread":
                result_queue = queue.Queue()
                process_obj = None
                thread_obj = threading.Thread(
                    target=task_wrapper_thread,
                    args=(),
                    kwargs={"task": task, "result_queue": result_queue},
                )
            else:
                raise ValueError("exec_mode must be 'process' or 'thread'")

            runtime = TaskRuntime(
                timeout=timeout,
                start_time=None,
                end_time=None,
                status=TaskStatus.PENDING.value,
                exec_mode=exec_mode,
                result_queue=result_queue,
                process=process_obj,
                thread=thread_obj
            )

            self.tasks[task_id] = {
                "task": task,
                "runtime": runtime
            }

            if group_id:
                self.task_groups.setdefault(group_id, []).append(task_id)

            # If allowed by current resource limits, start the task immediately.
            if self._can_start_task(exec_mode):
                self._start_task(task, runtime)
            else:
                self.pending.append(task_id)
        return {"status": "added"}

    def _can_start_task(self, exec_mode):
        process_running = sum(
            1
            for t in self.tasks.values()
            if t["runtime"].status == TaskStatus.RUNNING.value and t["runtime"].exec_mode == "process"
        )
        thread_running = sum(
            1
            for t in self.tasks.values()
            if t["runtime"].status == TaskStatus.RUNNING.value and t["runtime"].exec_mode == "thread"
        )
        total_running = process_running + thread_running

        if exec_mode == "process":
            return (process_running < self.max_processes) and (total_running < self.total_limit)
        elif exec_mode == "thread":
            return (thread_running < self.max_thread_limit) and (total_running < self.total_limit)
        else:
            return False

    def _start_task(self, task: Task, runtime: TaskRuntime):
        runtime.start_time = time.time()
        runtime.status = TaskStatus.RUNNING.value
        fn_name = task.task_fn.__name__
        self.throughput_metrics[fn_name][TaskStatus.PENDING.value] -= 1
        self.throughput_metrics[fn_name][TaskStatus.RUNNING.value] += 1
        if runtime.exec_mode == "process":
            runtime.process.start()
        elif runtime.exec_mode == "thread":
            runtime.thread.start()

    def _monitor_tasks(self):
        """
        Periodically checks all running tasks.
        When a task produces a result, the manager updates its status, throughput counters,
        and enqueues its log information for asynchronous writing.
        """
        while not self.stop_event.is_set():
            with self.lock:
                now = time.time()
                for _, task_data in list(self.tasks.items()):
                    task: Task = task_data["task"]
                    runtime: TaskRuntime = task_data["runtime"]

                    if runtime.status == TaskStatus.RUNNING.value:
                        try:
                            res = runtime.result_queue.get_nowait()
                            status_from_fn, _task = res

                            # Update result to task dict
                            task.result = _task.result
                            task.cached = _task.cached
                            runtime.end_time = time.time()
                            fn_name = task.task_fn.__name__
                            self.throughput_metrics[fn_name][TaskStatus.RUNNING.value] -= 1
                            if status_from_fn == TaskStatus.COMPLETED.value:
                                runtime.status = TaskStatus.COMPLETED.value
                                self.throughput_metrics[fn_name][TaskStatus.COMPLETED.value] += 1
                            elif status_from_fn == TaskStatus.ERROR.value:
                                runtime.status = TaskStatus.ERROR.value
                                self.throughput_metrics[fn_name][TaskStatus.ERROR.value] += 1

                            # Ensure the process/thread has finished.
                            if runtime.exec_mode == "process":
                                if runtime.process.is_alive():
                                    runtime.process.join()
                            elif runtime.exec_mode == "thread":
                                if runtime.thread.is_alive():
                                    runtime.thread.join()
                            # Enqueue log info asynchronously.
                            if task.log_file and not task.cached:
                                self._enqueue_log(task, runtime)
                        except queue.Empty:
                            # If still waiting, check for timeout.
                            if runtime.start_time and (now - runtime.start_time > runtime.timeout):
                                fn_name = task.task_fn.__name__
                                if runtime.exec_mode == "process":
                                    if runtime.process.is_alive():
                                        runtime.process.terminate()
                                        runtime.process.join()
                                # For thread tasks we cannot forcibly kill the thread.
                                runtime.status = TaskStatus.TIMEOUT.value
                                runtime.end_time = time.time()
                                self.throughput_metrics[fn_name][TaskStatus.RUNNING.value] -= 1
                                self.throughput_metrics[fn_name][TaskStatus.TIMEOUT.value] += 1
                                if task.log_file:
                                    self._enqueue_log(task, runtime)
                # Try to start pending tasks.
                while self.pending:
                    next_task_id = self.pending[0]
                    if next_task_id not in self.tasks:
                        self.pending.popleft()
                        continue
                    next_task_data = self.tasks[next_task_id]

                    next_task: Task = next_task_data["task"]
                    next_runtime: TaskRuntime = next_task_data["runtime"]
                    if next_runtime.status != TaskStatus.PENDING.value:
                        self.pending.popleft()
                        continue
                    if self._can_start_task(next_runtime.exec_mode):
                        self.pending.popleft()
                        self._start_task(next_task, next_runtime)
                    else:
                        break
            time.sleep(0.1)

    def _enqueue_log(self, task: Task, runtime: TaskRuntime):
        """
        Instead of writing the log directly under lock, we build a log_info dict
        and enqueue it. The separate _log_writer thread will do the file I/O.
        """
        log_info = {
            **task.to_dict(),
            **runtime.to_dict(),
        }
        self.log_queue.put((task.log_file, log_info))

    def _log_writer(self):
        """
        A dedicated thread that monitors self.log_queue and writes the logs to disk.
        No global lock is needed during actual file I/O.
        """
        while not self.log_stop_event.is_set() or not self.log_queue.empty():
            try:
                log_file, log_info = self.log_queue.get(timeout=0.05)
                try:
                    if log_file.endswith(".json"):
                        with open(log_file, "w+", encoding="utf-8") as f:
                            json.dump(log_info, f, indent=4, ensure_ascii=False)
                    elif log_file.endswith(".jsonl"):
                        with open(log_file, "a+", encoding="utf-8") as f:
                            f.write(json.dumps(log_info, ensure_ascii=False) + "\n")
                except Exception as e:
                    logger.exception(f"Error writing log for task {log_info.get('task_id')}: {e}")
                self.log_queue.task_done()
            except queue.Empty:
                continue

    def get_task_result(self, task_id):
        """
        Returns the result for a given task and drops it from internal storage.
        """
        with self.lock:
            if task_id not in self.tasks:
                return {"status": TaskStatus.NOT_FOUND.value}
            task_data = self.tasks[task_id]

            task: Task = task_data["task"]
            runtime: TaskRuntime = task_data["runtime"]

            if runtime.status in (TaskStatus.PENDING.value, TaskStatus.RUNNING.value):
                return {"status": TaskStatus.RUNNING.value}
            if runtime.status == TaskStatus.COMPLETED.value:
                result = {"status": TaskStatus.COMPLETED.value, "result": task.result}
            elif runtime.status == TaskStatus.ERROR.value:
                result = {"status": TaskStatus.ERROR.value, "error": task.result}
            elif runtime.status in (TaskStatus.CANCELED.value, TaskStatus.TIMEOUT.value):
                result = {"status": runtime.status}
            else:
                result = {"status": TaskStatus.UNKNOWN.value}
            self.tasks.pop(task_id)
            return result

    def cancel_task(self, task_id):
        """
        Cancels a task if it has not already been completed.
        """
        with self.lock:
            if task_id not in self.tasks:
                return {"status": TaskStatus.NOT_FOUND.value}
            task_data = self.tasks.pop(task_id)

            task: Task = task_data["task"]
            runtime: TaskRuntime = task_data["runtime"]

            fn_name = task.task_fn.__name__
            if runtime.status in (
                TaskStatus.COMPLETED.value,
                TaskStatus.ERROR.value,
                TaskStatus.CANCELED.value,
                TaskStatus.TIMEOUT.value,
            ):
                return {"status": runtime.status}
            if runtime.status == TaskStatus.PENDING.value:
                try:
                    self.pending.remove(task_id)
                except ValueError:
                    pass
                runtime.status = TaskStatus.CANCELED.value
                self.throughput_metrics[fn_name][TaskStatus.PENDING.value] -= 1
                self.throughput_metrics[fn_name][TaskStatus.CANCELED.value] += 1
                return {"status": TaskStatus.CANCELED.value}
            if runtime.status == TaskStatus.RUNNING.value:
                if runtime.exec_mode == "process":
                    if runtime.process.is_alive():
                        runtime.process.terminate()
                        runtime.process.join()
                # Note: For thread tasks we cannot safely terminate.
                runtime.status = TaskStatus.CANCELED.value
                self.throughput_metrics[fn_name][TaskStatus.RUNNING.value] -= 1
                self.throughput_metrics[fn_name][TaskStatus.CANCELED.value] += 1
                return {"status": TaskStatus.CANCELED.value}
        return {"status": TaskStatus.CANCELED.value}

    def add_task_group(self, group_id, task_list):
        """
        Adds a group of tasks. Each task in task_list is a dictionary.
        """
        group_task_ids = []
        with self.lock:
            if group_id in self.task_groups:
                raise ValueError(f"Task group '{group_id}' already exists.")
        for i, task in enumerate(task_list):
            task_id = task.get("task_id", f"{group_id}_{i}")
            group_task_ids.append(task_id)
            self.add_task(
                task_id,
                task["task_fn"],
                *(task.get("args", ())),
                timeout=task.get("timeout", None),
                exec_mode=task.get("exec_mode", "process"),
                log_file=task.get("log_file", None),
                group_id=group_id,
                **task.get("kwargs", {}),
            )
        with self.lock:
            self.task_groups[group_id] = group_task_ids
        return {"status": "group added", "group_id": group_id, "tasks": group_task_ids}

    def get_task_group_result(self, group_id):
        """
        Returns the combined results for a group of tasks.
        If any task is still running, returns a running status.
        """
        with self.lock:
            if group_id not in self.task_groups:
                return {"status": TaskStatus.NOT_FOUND.value}
            task_ids = self.task_groups[group_id]
            for task_id in task_ids:
                if task_id in self.tasks and self.tasks[task_id]["runtime"].status in (
                    TaskStatus.PENDING.value,
                    TaskStatus.RUNNING.value,
                ):
                    return {"status": TaskStatus.RUNNING.value}
            results = {}
            for task_id in task_ids:
                if task_id in self.tasks:
                    task_data = self.tasks.pop(task_id)

                    task: Task = task_data["task"]
                    runtime: TaskRuntime = task_data["runtime"]

                    if runtime.status == TaskStatus.COMPLETED.value:
                        results[task_id] = {"status": TaskStatus.COMPLETED.value, "result": task.result}
                    elif runtime.status == TaskStatus.ERROR.value:
                        results[task_id] = {"status": TaskStatus.ERROR.value, "error": task.result}
                    elif runtime.status in (TaskStatus.CANCELED.value, TaskStatus.TIMEOUT.value):
                        results[task_id] = {"status": runtime.status}
                    else:
                        results[task_id] = {"status": TaskStatus.UNKNOWN.value}
                else:
                    results[task_id] = {"status": TaskStatus.NOT_FOUND.value}
            self.task_groups.pop(group_id)
            return {"status": TaskStatus.COMPLETED.value, "results": results}

    def _log_throughput(self):
        """
        Once every throughput_interval seconds we copy throughput metrics, compute the changes
        since last interval, and print them.
        """
        while not self.throughput_stop_event.is_set():
            time.sleep(self.throughput_interval)
            with self.lock:
                metrics_copy = copy.deepcopy(self.throughput_metrics)
                metrics_last_copy = copy.deepcopy(self.throughput_metrics_last)
                self.throughput_metrics_last = metrics_copy
            interval_metrics = copy.deepcopy(metrics_copy)
            for task_fn_name, metrics in interval_metrics.items():
                if task_fn_name in metrics_last_copy:
                    for key in (
                        TaskStatus.ARRIVED.value,
                        TaskStatus.COMPLETED.value,
                        TaskStatus.ERROR.value,
                        TaskStatus.CANCELED.value,
                        TaskStatus.TIMEOUT.value,
                    ):
                        metrics[key] = metrics[key] - metrics_last_copy[task_fn_name][key]
            metrics_copy["interval"] = self.throughput_interval
            logger.info(f"Throughput metrics: {json.dumps(interval_metrics)}")

    def shutdown(self):
        """
        Shuts down all background threads and terminates any running process tasks.
        """
        self.stop_event.set()
        self.throughput_stop_event.set()
        self.log_stop_event.set()
        self.monitor_thread.join()
        self.throughput_thread.join()
        self.log_thread.join()
        with self.lock:
            for _, task_data in self.tasks.items():
                runtime: TaskRuntime = task_data["runtime"]

                if runtime.status == TaskStatus.RUNNING.value:
                    if runtime.exec_mode == "process" and runtime.process.is_alive():
                        runtime.process.terminate()
                        runtime.process.join()
            self.tasks.clear()
