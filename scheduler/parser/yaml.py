from typing import List, Any, Dict
import yaml
from scheduler.job import Batch, Job


def _parse_job(job_e: Dict[str, Any])->Job:
    return Job(
        command=job_e['command'],
        params=job_e.get('params', {}),
        args=job_e.get('args', []),
        native_specification=job_e.get('native_specification'),
        name=job_e.get('name'),
    )


def _parse_batch(batch_e: Dict[str, Any])->Batch:
    jobs = [
        _parse_job(job_e)
        for job_e in batch_e['jobs']
    ]
    return Batch(name=batch_e['name'], jobs=jobs)


def parse_config(file)->List[Batch]:
    data = yaml.load(file)

    return [_parse_batch(batch_e)
            for batch_e in data]
