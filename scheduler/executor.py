import logging
from collections import deque
import shlex
import shutil
import time
from os import makedirs
from typing import List

import drmaa
from drmaa.const import JobControlAction
from drmaa.errors import InvalidJobException, InternalException
from os.path import exists

from scheduler.job import Job

logger = logging.getLogger(__name__)


class DRMAAExecutor:
    JOB_STATUS_OK = 'ok'
    JOB_STATUS_ERROR = 'error'

    def __init__(self, stop_on_first_error: bool=False, max_jobs: int=None, skip_already_done=False, dry_run: bool=False):
        self._session = drmaa.Session()
        self._drmaa_log_dir = ''
        self._session.initialize()
        self._active_jobs = list()  # type: List[Job]
        self._stop_on_first_error = stop_on_first_error
        self._job_queue = deque()
        self._max_jobs = max_jobs
        self._skip_alreagy_done = skip_already_done
        self._dryrun = dry_run

    def cancel(self):
        logger.warning("Cancelling {} jobs".format(len(self._active_jobs)))
        for job in self._active_jobs:
            try:
                self._session.control(job.job_id, JobControlAction.TERMINATE)
            except (InvalidJobException, InternalException):
                # FIXME: This is common - logging a warning would probably confuse the user.
                pass

    def _write_status(self, job: Job, status: str):
        with open(job.status_path, 'w') as f:
            f.write(status)

    def _read_status(self, job: Job)->str:
        if not exists(job.status_path):
            return ''
        with open(job.status_path) as f:
            return f.read()

    def _create_template(self, job: Job)->drmaa.JobTemplate:
        jt = self._session.createJobTemplate()
        jt.remoteCommand = shutil.which(job.command)
        jt.args = job.args

        params = job.params
        if params.get('num_slots', 1) > 1:
            # TODO: remove logger.info below
            logger.info('Job {name} requested {n} slots'.format(
                name=job.name,
                n=params['num_slots'],
            ))
            jt.nativeSpecification = "-pe make {}".format(params['num_slots'])

        if params.get('stdout'):
            jt.outputPath = params['stdout']
        if params.get('stderr'):
            jt.errorPath = params['stderr']
        if params.get('join_streams'):
            jt.joinFiles = True
        if params.get('job_name'):
            jt.jobName = params['job_name']
        if params.get('work_dir'):
            jt.workingDirectory = params['work_dir']

        jt.params = params
        return jt

    def queue(self, job: Job):
        self._job_queue.append(job)

    def _run(self, job: Job):
        if self._drmaa_log_dir:
            makedirs(self._drmaa_log_dir)

        try:
            jt = self._create_template(job)
            job.job_id = self._session.runJob(jt)
            job.start_time = time.time()
        except (drmaa.InternalException,
                drmaa.InvalidAttributeValueException) as e:
            return

        logger.info("Submitted job {name} (id: {id})".format(
            id=job.job_id,
            name=job.name,
        ))
        self._session.deleteJobTemplate(jt)
        self._active_jobs.append(job)

    def shutdown(self):
        self._session.exit()

    @staticmethod
    def _print_job_error(job: Job):
        logger.error("Job {name} (drmaa id: {d}) finished with error. Log file: {log}".format(
            name=job.name,
            d=job.job_id,
            log=job.params.get('stdout', '')
        ))
        args_str = " ".join([shlex.quote(arg) for arg in job.args])
        full_command = "{command} {args}".format(
            command=job.command,
            args=args_str,
        )

        logger.error("\tCommand was: {command}".format(
            command=full_command,
        ))

        logger.error("\tStart time: {start_time}, end time: {end_time}, total seconds: {time} s.".format(
            start_time=job.start_time,
            end_time=job.end_time,
            time=(job.end_time - job.start_time)
        ))

    def wait_for_jobs(self):
        status_ok = True
        while True:
            while len(self._job_queue) != 0:
                if self._max_jobs is not None and len(self._active_jobs) >= self._max_jobs:
                    break
                job = self._job_queue.popleft()
                if self._skip_alreagy_done and self._read_status(job) == self.JOB_STATUS_OK:
                    logger.info("Job {name} is already done".format(name=job.name))
                    continue
                if self._dryrun:
                    logger.info('Job: {name}'.format(name=job.name))
                    continue
                self._run(job)

            active_jobs = self._active_jobs
            self._active_jobs = list()  # type: List[Job]
            for job in active_jobs:
                try:
                    res = self._session.wait(job.job_id,
                                             drmaa.Session.TIMEOUT_NO_WAIT)
                except drmaa.ExitTimeoutException as e:
                    # job still active
                    self._active_jobs.append(job)
                    continue
                except (drmaa.InternalException, Exception) as e:
                    continue

                job.end_time = time.time()
                if res.hasExited and res.exitStatus == 0:
                    logger.info("Job {name} (id: {id}) successfully finished, time: {time} s.".format(
                        name=job.name,
                        id=job.job_id,
                        time=(job.end_time - job.start_time)
                    ))
                    self._write_status(job, self.JOB_STATUS_OK)
                else:
                    self._write_status(job, self.JOB_STATUS_ERROR)
                    self._print_job_error(job)
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
