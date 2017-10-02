import logging
import re
import shutil
import time
from collections import deque
from os import makedirs
from typing import List

import drmaa
from drmaa.const import JobControlAction
from drmaa.errors import InvalidJobException, InternalException

from scheduler.executor.base import Executor, _write_status, _print_job_error, _write_time, _read_status
from scheduler.job import Job, JobSpec

logger = logging.getLogger(__name__)


class DRMAAExecutor(Executor):
    def __init__(self, stop_on_first_error: bool=False, max_jobs: int=None, skip_already_done=False):
        self._session = drmaa.Session()
        self._drmaa_log_dir = ''
        self._session.initialize()
        self._active_jobs = list()  # type: List[Job]
        self._stop_on_first_error = stop_on_first_error
        self._job_queue = deque()
        self._max_jobs = max_jobs
        self._skip_alreagy_done = skip_already_done

    def cancel(self):
        logger.warning("Cancelling {} jobs".format(len(self._active_jobs)))
        for job in self._active_jobs:
            try:
                self._session.control(job.job_id, JobControlAction.TERMINATE)
            except (InvalidJobException, InternalException):
                # FIXME: This is common - logging a warning would probably confuse the user.
                pass

    def _create_template(self, spec: JobSpec)->drmaa.JobTemplate:
        jt = self._session.createJobTemplate()
        jt.remoteCommand = shutil.which(spec.command)
        jt.args = spec.args

        if spec.num_slots > 1:
            # TODO: remove logger.info below
            logger.info('Job {name} requested {n} slots'.format(
                name=spec.name,
                n=spec.num_slots,
            ))
            jt.nativeSpecification = "-pe make {}".format(spec.num_slots)

        if spec.log_path:
            jt.outputPath = ':'+spec.log_path
            jt.joinFiles = True
        if spec.name:
            if re.match(r'^\d+', spec.name):
                jt.jobName = 'j'+spec.name
            else:
                jt.jobName = spec.name
        if spec.work_dir:
            jt.workingDirectory = spec.work_dir

        return jt

    def queue(self, job_spec: JobSpec):
        self._job_queue.append(Job(spec=job_spec))

    def _run(self, job: Job):
        if self._drmaa_log_dir:
            makedirs(self._drmaa_log_dir)

        try:
            jt = self._create_template(job.spec)
            job.job_id = self._session.runJob(jt)
            job.start_time = time.time()
        except (drmaa.InternalException,
                drmaa.InvalidAttributeValueException) as e:
            logger.warning('drmaa exception in _run: {}'.format(e))
            return

        logger.info("Submitted job {name} (id: {id})".format(
            id=job.job_id,
            name=job.spec.name,
        ))
        self._session.deleteJobTemplate(jt)
        self._active_jobs.append(job)

    def shutdown(self):
        self._session.exit()

    # TODO: move drmaa not specific code to base
    def wait_for_jobs(self):
        status_ok = True
        while True:
            while len(self._job_queue) != 0:
                if self._max_jobs is not None and len(self._active_jobs) >= self._max_jobs:
                    break
                job = self._job_queue.popleft()  # type: Job
                if self._skip_alreagy_done and _read_status(job) == self.JOB_STATUS_OK:
                    logger.info("Job {name} is already done".format(name=job.spec.name))
                    continue
                self._run(job)

            active_jobs = list(self._active_jobs)
            self._active_jobs.clear()
            for job in active_jobs:
                try:
                    res = self._session.wait(job.job_id,
                                             drmaa.Session.TIMEOUT_NO_WAIT)
                except drmaa.ExitTimeoutException as e:
                    # job still active
                    self._active_jobs.append(job)
                    continue
                except (drmaa.InternalException, Exception) as e:
                    # Dirty hack allowing to catch cancelled job in "queued" status
                    if 'code 24' in str(e):
                        logger.warning("Cancelled job in 'queued' status: {}".format(e))
                        class FakeRes:
                            hasExited = True
                            exitStatus = 42
                        res = FakeRes()
                    else:
                        logger.warning('Unknown exception: {}: {}'.format(type(e), e))
                        continue

                job.end_time = time.time()
                if res.hasExited and res.exitStatus == 0:
                    logger.info("Job {name} (id: {id}) successfully finished, time: {time} s.".format(
                        name=job.spec.name,
                        id=job.job_id,
                        time=(job.end_time - job.start_time)
                    ))
                    _write_status(job, self.JOB_STATUS_OK)
                    _write_time(job)
                else:
                    _write_status(job, self.JOB_STATUS_ERROR)
                    _print_job_error(job)
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
