from typing import List
import csv
from scheduler.job import Batch, Job
from itertools import groupby


def _parse_job(row: List[str])->Job:
    num, batch, command, arguments = row
    arguments = [a.strip('"') for a in arguments.split(" ")]

    return Job(
        args=arguments,
        command=command,
        params={},
    )


def _parse_batch(name, rows: List[List[str]])->Batch:
    jobs = [
        _parse_job(row)
        for row in rows
    ]
    return Batch(name=name, jobs=jobs)


def parse_config(file_path: str)->List[Batch]:
    with open(file_path) as f:
        reader = csv.reader(f, delimiter='\t')
        rows = list(reader)
        batches = [
            _parse_batch(batch, rows)
            for batch, group in groupby(rows, lambda r: r[1])
        ]
        return batches
