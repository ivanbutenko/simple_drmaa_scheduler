import logging
import time
from abc import ABCMeta, abstractmethod
from collections import deque
from math import inf
from typing import Dict

from scheduler.executor.util import print_job_error, write_time, read_status, write_status, print_job_ok
from scheduler.job import Job, JobSpec

logger = logging.getLogger(__name__)


class Executor(metaclass=ABCMeta):
    JOB_STATUS_OK = 'ok'
    JOB_STATUS_ERROR = 'error'

    class JobStatus:
        def __init__(self, has_exited: bool, exit_status: int, job: Job):
            self.job = job
            self.has_exited = has_exited
            self.exit_status = exit_status

    def __init__(self, stop_on_first_error: bool=False, max_jobs: int=None, skip_already_done=False):
        self._active_jobs = dict()  # type: Dict[int, Job]
        self._stop_on_first_error = stop_on_first_error
        self._queued_jobs = deque()
        self._max_jobs = max_jobs or inf
        self._skip_alreagy_done = skip_already_done

    @abstractmethod
    def _job_status(self, job: Job)->JobStatus:
        pass

    @abstractmethod
    def _cancel_job(self, job: Job):
        pass

    @abstractmethod
    def shutdown(self):
        pass

    @abstractmethod
    def _submit(self, job_spec: JobSpec)->Job:
        pass

    def cancel(self):
        logger.warning("Cancelling {} jobs".format(len(self._active_jobs)))
        for job in self._active_jobs.values():
            self._cancel_job(job)

    def queue(self, job_spec: JobSpec):
        if self._skip_alreagy_done and read_status(job_spec) == self.JOB_STATUS_OK:
            logger.info("Job {name} is already done".format(name=job_spec.name))
            return
        self._queued_jobs.append(job_spec)

    def _submit_new_jobs(self):
        can_take = self._max_jobs - len(self._active_jobs)
        to_submit_count = min(can_take, len(self._queued_jobs))
        to_submit = (
            self._queued_jobs.popleft()
            for _ in range(to_submit_count)
        )
        for job_spec in to_submit:
            job = self._submit(job_spec)
            self._active_jobs[job.job_id] = job
            job.start_time = time.time()
            logger.info("Submitted job {name} (id: {id})".format(
                id=job.job_id,
                name=job_spec.name,
            ))

    # TODO: move drmaa not specific code to base
    def wait_for_jobs(self):
        status_ok = True
        while True:
            self._submit_new_jobs()

            for job_id, job in list(self._active_jobs.items()):
                status = self._job_status(job)
                if not status.has_exited:
                    continue
                del self._active_jobs[job_id]

                job.end_time = time.time()
                if status.exit_status == 0:
                    print_job_ok(job)
                    write_status(job, self.JOB_STATUS_OK)
                    write_time(job)
                else:
                    print_job_error(job)
                    write_status(job, self.JOB_STATUS_ERROR)
                    if self._stop_on_first_error:
                        return False
                    else:
                        status_ok = False
                logger.info('{} jobs left'.format(
                    len(self._active_jobs) + len(self._queued_jobs)
                ))
            if not self._active_jobs:
                break
            time.sleep(1)
        return status_ok
