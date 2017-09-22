import ujson as json
from typing import List, Any, Dict, io, TextIO

import sys
from scheduler.job import Batch, JobSpec


def _parse_job(job_e: Dict[str, Any])->JobSpec:
    return JobSpec(
            command=job_e['command'],
            args=job_e.get('args', []),
            num_slots=job_e.get('params', {}).get('num_slots', 1),
            log_path=job_e.get('log_path'),
            time_path=job_e.get('time_path'),
            status_path=job_e.get('status_path'),
            work_dir=job_e.get('work_dir'),
            name=job_e.get('name'),
        )


def _job_to_dict(job_spec: JobSpec)->Dict[str, Any]:
    res = {
        'name': job_spec.name,
        'command': job_spec.command,
        'args': job_spec.args,
    }

    if job_spec.num_slots:
        res['params'] = {'num_slots': job_spec.num_slots}

    if job_spec.work_dir:
        res['work_dir'] = job_spec.work_dir

    if job_spec.status_path:
        res['status_path'] = job_spec.status_path

    if job_spec.log_path:
        res['log_path'] = job_spec.log_path

    if job_spec.time_path:
        res['time_path'] = job_spec.time_path

    return res

def _batch_to_dict(batch: Batch)->Dict[str, Any]:
    return dict(
        name=batch.name,
        jobs=[
            _job_to_dict(j)
            for j in batch.jobs
        ]
    )

def _parse_batch(batch_e: Dict[str, Any])->Batch:
    jobs = [
        _parse_job(job_e)
        for job_e in batch_e['jobs']
    ]
    return Batch(name=batch_e['name'], jobs=jobs)


def parse_config(file)->List[Batch]:
    data = json.load(file)

    return [_parse_batch(batch_e)
            for batch_e in data]


def write_config(f: TextIO, batches: List[Batch]):
    batches_dicts = [
        _batch_to_dict(b)
        for b in batches
    ]

    json.dump(batches_dicts, f)