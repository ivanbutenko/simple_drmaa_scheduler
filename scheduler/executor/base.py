import shlex
from abc import ABCMeta, abstractmethod
import logging

from os.path import exists

from scheduler.job import Job, JobSpec

logger = logging.getLogger(__name__)


class Executor(metaclass=ABCMeta):
    JOB_STATUS_OK = 'ok'
    JOB_STATUS_ERROR = 'error'

    @abstractmethod
    def cancel(self):
        pass

    @abstractmethod
    def queue(self, job_spec: JobSpec):
        pass

    @abstractmethod
    def shutdown(self):
        pass

    @abstractmethod
    def wait_for_jobs(self)->bool:
        pass


def _print_job_error(job: Job):
    logger.error("Job {name} (drmaa id: {d}) finished with error. Log file: {log}".format(
        name=job.spec.name,
        d=job.job_id,
        log=(job.spec.log_path or '').rstrip(':')
    ))
    args_str = " ".join([shlex.quote(arg) for arg in job.spec.args])
    full_command = "{command} {args}".format(
        command=job.spec.command,
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


def _write_time(job: Job):
    with open(job.spec.time_path, 'w') as f:
        f.write('{}\n'.format(job.end_time - job.start_time))


def _read_status(job: Job)->str:
    if not exists(job.spec.status_path):
        return ''
    with open(job.spec.status_path) as f:
        return f.read()


def _write_status(job: Job, status: str):
    with open(job.spec.status_path, 'w') as f:
        f.write(status)
