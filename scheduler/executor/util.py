import shlex
from genericpath import exists

import datetime

import math

from scheduler.job import Job, JobSpec
import logging
logger = logging.getLogger(__name__)


def print_job_error(job: Job):
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

    logger.error("\tStart time: {start_time}, end time: {end_time}, total seconds: {time}".format(
        start_time=job.start_time,
        end_time=job.end_time,
        time=datetime.timedelta(seconds=math.trunc(job.end_time - job.start_time))
    ))


def print_job_ok(job: Job):
    logger.info("Job {name} (id: {id}) successfully finished, time: {time}".format(
        name=job.spec.name,
        id=job.job_id,
        time=datetime.timedelta(seconds=math.trunc(job.end_time - job.start_time))
    ))


def write_time(job: Job):
    with open(job.spec.time_path, 'w') as f:
        f.write('{}\n'.format(job.end_time - job.start_time))


def read_status(job_spec: JobSpec)->str:
    if not exists(job_spec.status_path):
        return ''
    with open(job_spec.status_path) as f:
        return f.read()


def write_status(job: Job, status: str):
    with open(job.spec.status_path, 'w') as f:
        f.write(status)