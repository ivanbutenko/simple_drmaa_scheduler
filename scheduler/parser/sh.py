import argparse
from itertools import groupby
from shlex import split, quote
from typing import List, TextIO

from scheduler.job import Batch, JobSpec


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
    parser.add_argument('arguments', nargs=argparse.REMAINDER)

    return parser


def _job_to_args(batch: Batch, job_spec: JobSpec)->List[str]:
    job_args = []
    job_args.extend(('--name', job_spec.name))
    job_args.extend(('--batch', batch.name))

    if job_spec.num_slots:
        job_args.extend(('--threads', job_spec.num_slots))

    if job_spec.work_dir:
        job_args.extend(('--work-dir', job_spec.work_dir))

    if job_spec.status_path:
        job_args.extend(('--status-path', job_spec.status_path))

    if job_spec.time_path:
        job_args.extend(('--time-path', job_spec.time_path))

    job_args.append(job_spec.command)
    job_args.extend(job_spec.args)

    job_args = [
        quote(str(arg))
        for arg in job_args
    ]
    return job_args


def _batches_to_args(batches: List[Batch])->List[List[str]]:
    return [
        _job_to_args(batch, job)
        for batch in batches
        for job in batch.jobs
    ]


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


def _parse_job(job_args, default_name: str)->JobSpec:
    return JobSpec(
            command=job_args.command,
            args=job_args.arguments,
            name=job_args.name or default_name,
            num_slots=job_args.threads,
            work_dir=job_args.work_dir,
            time_path=job_args.time_path,
            status_path=job_args.status_path,
            log_path=job_args.log_path,
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


def write_config(f: TextIO, batches: List[Batch]):
    for line in _batches_to_args(batches):
        f.write(" ".join(line)+'\n')