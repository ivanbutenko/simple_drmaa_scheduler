from collections import deque
from threading import Thread, Lock
from typing import Dict

import time

from scheduler.executor.base import Executor
from scheduler.job import Job, JobSpec
import logging
import subprocess


logger = logging.getLogger(__name__)


class LocalExecutor(Executor):
    def __init__(self, stop_on_first_error: bool=False, max_jobs: int=None, skip_already_done=False):
        super().__init__(stop_on_first_error, max_jobs, skip_already_done)
        self._executor_thread = ExecutorThread()
        self._executor_thread.start()

    def shutdown(self):
        logger.warning('showtdown() is not implemented')
        pass

    def _cancel_job(self, job: Job):
        logger.warning('_cancel_job(job) is not implemented')
        pass

    def _job_status(self, job: Job) -> Executor.JobStatus:
        return self._executor_thread.job_status(job)

    def _submit(self, job_spec: JobSpec)->Job:
        return self._executor_thread.queue_job(job_spec)


class ExecutorThread(Thread):
    def __init__(self):
        super().__init__()
        self.setDaemon(True)
        self._current_jobs = deque()
        self._executor_lock = Lock()
        self._job_statuses = dict()  # type: Dict[int, Executor.JobStatus]

    def job_status(self, job: Job) -> Executor.JobStatus:
        with self._executor_lock:
            return next((
                status
                for _, status in self._job_statuses.items()
                if status.job.job_id == job.job_id
            ), None)

    def _next_job_id(self)->int:
        return max(self._job_statuses.keys() or [0]) + 1

    def queue_job(self, job_spec: JobSpec)->Job:
        with self._executor_lock:
            job_id = self._next_job_id()
            job = Job(spec=job_spec, job_id=job_id)
            self._current_jobs.append(job)
            self._job_statuses[job_id] = Executor.JobStatus(
                has_exited=False,
                exit_status=None,
                job=job
            )
            return job

    def run(self):
        while True:
            if len(self._current_jobs):
                job = self._current_jobs.popleft()
                self._execute_job(job)
                self._job_statuses[job.job_id].has_exited = True
                self._job_statuses[job.job_id].exit_status = 0
            else:
                time.sleep(0.3)

    @staticmethod
    def _execute_job(job: Job)->int:
        stdout_f = None
        if job.spec.log_path:
            stdout_f = open(job.spec.log_path, 'w')
        try:
            process = subprocess.Popen(
                args=[job.spec.command] + job.spec.args,
                cwd=job.spec.work_dir,
                stdout=stdout_f,
                close_fds=True
            )
            return process.wait()
        except FileNotFoundError as e:
            logger.error(e)
            return 1
