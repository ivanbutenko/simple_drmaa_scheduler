import argparse
from itertools import groupby
from typing import List, TextIO
from shlex import split

from scheduler.job import Batch, Job, JobSpec


def _init_parser()->argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument('--batch', '-b', default='default')
    parser.add_argument('--name', '-j')
    parser.add_argument('--work-dir')
    parser.add_argument('--time-path')
    parser.add_argument('--status-path')
    parser.add_argument('--log-path')
    parser.add_argument('--threads', '-t', default=1, type=int)
    parser.add_argument('command')
    parser.add_argument('arguments', nargs='*')

    return parser


def _parse_batch(batch_name: str, job_args_list: List[argparse.Namespace])->Batch:
    jobs = [
        _parse_job(
            job_args=job_args,
            default_name='{}-{}'.format(batch_name, i+1)
        )
        for i, job_args in enumerate(job_args_list)
    ]
    return Batch(
        name=batch_name,
        jobs=jobs
    )


def _parse_job(job_args, default_name: str)->Job:
    return Job(
        spec=JobSpec(
            command=job_args.command,
            args=job_args.arguments,
            name=job_args.name or default_name,
            num_slots=job_args.threads,
            work_dir=job_args.work_dir,
            time_path=job_args.time_path,
            status_path=job_args.status_path,
            log_path=job_args.log_path,
        )
    )


def parse_config(file: TextIO)->List[Batch]:
    parser = _init_parser()

    job_args_list = [
        parser.parse_args(split(l.strip())) for l in file
    ]

    batches = [
        _parse_batch(batch, group)
        for batch, group in groupby(job_args_list, lambda ja: ja.batch)
    ]
    return batches
