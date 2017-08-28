from typing import List, Dict, Any


class Job:
    def __init__(self, command: str,
                 args: List[str],
                 params: Dict[str, Any],
                 native_specification: str=None,
                 name: str=None,
                 job_id: int=None,
                 status_path: str=None,
                 ):
        self.command = command
        self.args = args
        self.params = params
        # self.nativeSpecification = native_specification
        self.job_id = job_id
        self.start_time = None
        self.end_time = None
        self.batch = None  # type: Batch
        self.name = name
        self.status_path = status_path


class Batch:
    def __init__(self, name: str, jobs: List[Job]):
        self.name = name
        self.jobs = jobs
        for j in self.jobs:
            j.batch = self

