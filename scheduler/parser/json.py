from typing import List, Any, Dict
import ujson as json

from scheduler.job import Batch, Job, JobSpec


def _parse_job(job_e: Dict[str, Any])->Job:
    return Job(
        spec=JobSpec(
            command=job_e['command'],
            args=job_e.get('args', []),
            num_slots=job_e.get('params', {}).get('num_slots', 1),
            log_path=job_e.get('log_path'),
            time_path=job_e.get('time_path'),
            status_path=job_e.get('status_path'),
            work_dir=job_e.get('work_dir'),
            name=job_e.get('name'),
        ),
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
