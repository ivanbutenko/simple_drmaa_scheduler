import time
from abc import ABCMeta, abstractmethod
from collections import deque
from typing import List

from scheduler.executor.util import print_job_error, write_time, read_status, write_status
from scheduler.job import Job, JobSpec
import logging
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
        self._active_jobs = list()  # type: List[Job]
        self._stop_on_first_error = stop_on_first_error
        self._job_queue = deque()
        self._max_jobs = max_jobs
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
        for job in self._active_jobs:
            self._cancel_job(job)

    def queue(self, job_spec: JobSpec):
        if self._skip_alreagy_done and read_status(job_spec) == self.JOB_STATUS_OK:
            logger.info("Job {name} is already done".format(name=job_spec.name))
            return
        self._job_queue.append(job_spec)

    # TODO: move drmaa not specific code to base
    def wait_for_jobs(self):
        status_ok = True
        while True:
            while len(self._job_queue) != 0:
                if self._max_jobs is not None and len(self._active_jobs) >= self._max_jobs:
                    break
                job_spec = self._job_queue.popleft()  # type: JobSpec
                job = self._submit(job_spec)
                self._active_jobs.append(job)
                job.start_time = time.time()
                logger.info("Submitted job {name} (id: {id})".format(
                    id=job.job_id,
                    name=job_spec.name,
                ))

            active_jobs = list(self._active_jobs)
            self._active_jobs.clear()
            for job in active_jobs:
                status = self._job_status(job)
                if not status.has_exited:
                    self._active_jobs.append(job)
                    continue

                job.end_time = time.time()
                if status.exit_status == 0:
                    logger.info("Job {name} (id: {id}) successfully finished, time: {time} s.".format(
                        name=job.spec.name,
                        id=job.job_id,
                        time=(job.end_time - job.start_time)
                    ))
                    write_status(job, self.JOB_STATUS_OK)
                    write_time(job)
                else:
                    write_status(job, self.JOB_STATUS_ERROR)
                    print_job_error(job)
                    if self._stop_on_first_error:
                        return False
                    else:
                        status_ok = False
                logger.info('{} jobs left'.format(
                    len(active_jobs) + len(self._job_queue)
                ))
            if not self._active_jobs:
                break
            time.sleep(1)
        return status_ok


